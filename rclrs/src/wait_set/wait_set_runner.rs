use futures::{
    channel::{
        mpsc::{unbounded, UnboundedReceiver, UnboundedSender},
        oneshot,
    },
    future::BoxFuture,
};

use std::{
    any::Any,
    sync::{
        atomic::{AtomicBool, Ordering},
        mpsc::{channel as request_channel, Receiver as RequestReceiver, Sender as RequestSender},
        Arc, Mutex,
    },
    thread::JoinHandle,
    time::{Duration, Instant},
};

use crate::{
    log_debug, log_fatal, ActivityListenerCallback, Context, ExecutorWorkerOptions, GuardCondition,
    PayloadTask, RclReturnCode, RclrsError, WaitSet, Waitable, WeakActivityListener,
};

/// This is a utility class that executors can use to easily run and manage
/// their wait set.
///
/// Each `WaitSetRunner` owns a dedicated worker thread that is spawned once when
/// the runner is created and **reused for every spin**. The thread parks on a
/// channel between spins, so a [`spin`][crate::Executor::spin] only sends a
/// request to the existing thread instead of spawning (and later joining) a new
/// OS thread each time. This matters for the common pattern of calling `spin`
/// repeatedly with a short timeout, where per-spin thread creation otherwise
/// dominates the cost (see ros2-rust#627).
pub struct WaitSetRunner {
    waitable_sender: UnboundedSender<Waitable>,
    task_sender: UnboundedSender<PayloadTask>,
    activity_listeners: Arc<Mutex<Vec<WeakActivityListener>>>,
    guard_condition: Arc<GuardCondition>,
    /// Sends a spin request to the worker thread. Wrapped in `Option` so [`Drop`]
    /// can close the channel — waking the parked thread so it can exit — before
    /// joining it.
    spin_request: Option<RequestSender<SpinRequest>>,
    join_handle: Option<JoinHandle<()>>,
    /// Set by [`Drop`] to tell an in-flight `run_blocking` to stop. Closing the
    /// request channel only wakes a *parked* worker; a worker blocked inside
    /// `rcl_wait` also needs this flag (checked each loop) plus a guard-condition
    /// trigger to wake the wait, so that `Drop` can never block on `join`.
    shutdown: Arc<AtomicBool>,
}

/// A request for the worker thread to run its wait set once with the given
/// conditions. The worker resolves `result` when it finishes that run.
struct SpinRequest {
    conditions: WaitSetRunConditions,
    result: oneshot::Sender<Result<(), RclrsError>>,
}

/// The mutable wait-set state owned exclusively by the worker thread.
struct WaitSetWorker {
    wait_set: WaitSet,
    waitable_receiver: UnboundedReceiver<Waitable>,
    task_receiver: UnboundedReceiver<PayloadTask>,
    activity_listeners: Arc<Mutex<Vec<WeakActivityListener>>>,
    payload: Box<dyn Any + Send>,
    /// Shared with the owning [`WaitSetRunner`]; set on drop to break out of an
    /// in-flight spin. See [`WaitSetRunner::shutdown`].
    shutdown: Arc<AtomicBool>,
}

/// These are the conditions used by the [`WaitSetRunner`] to determine when it
/// needs to halt.
#[derive(Clone, Debug)]
pub struct WaitSetRunConditions {
    /// Only perform the next available work. This is similar to spin_once in
    /// rclcpp and rclpy.
    ///
    /// To only process work that is immediately available without waiting at all,
    /// set a timeout of zero.
    pub only_next_available_work: bool,
    /// Stop spinning once this instant in time is reached.
    pub stop_time: Option<Instant>,
    /// Use this to check [`Context::ok`] to make sure that the context is still
    /// valid. When the context is invalid, the executor runtime should stop
    /// spinning.
    pub context: Context,
    /// Halt trigger that gets set by [`ExecutorCommands`][1].
    ///
    /// [1]: crate::ExecutorCommands
    pub halt_spinning: Arc<AtomicBool>,
}

impl WaitSetRunner {
    /// Create a new WaitSetRunner. This spawns the worker thread that will be
    /// reused for every spin of this runner.
    pub fn new(worker_options: ExecutorWorkerOptions) -> Self {
        let (waitable_sender, waitable_receiver) = unbounded();
        let (task_sender, task_receiver) = unbounded();
        let activity_listeners: Arc<Mutex<Vec<WeakActivityListener>>> = Arc::default();
        let shutdown = Arc::new(AtomicBool::new(false));

        let worker = WaitSetWorker {
            wait_set: WaitSet::new(&worker_options.context)
                // SAFETY: This only gets called from Context which ensures that
                // everything is valid when creating a wait set.
                .expect("Unable to create wait set for basic executor"),
            waitable_receiver,
            task_receiver,
            activity_listeners: Arc::clone(&activity_listeners),
            payload: worker_options.payload,
            shutdown: Arc::clone(&shutdown),
        };

        let (spin_request, spin_requests) = request_channel::<SpinRequest>();
        let join_handle = std::thread::Builder::new()
            .name("rclrs-worker".to_owned())
            .spawn(move || worker.work(spin_requests))
            .expect("Failed to spawn the wait set worker thread");

        Self {
            waitable_sender,
            task_sender,
            activity_listeners,
            guard_condition: worker_options.guard_condition,
            spin_request: Some(spin_request),
            join_handle: Some(join_handle),
            shutdown,
        }
    }

    /// Get the sender that allows users to send new [`Waitable`]s to this
    /// `WaitSetRunner`.
    pub fn waitable_sender(&self) -> UnboundedSender<Waitable> {
        self.waitable_sender.clone()
    }

    /// Get the sender that allows users to send new [`PayloadTask`]s to this
    /// `WaitSetRunner`.
    pub fn payload_task_sender(&self) -> UnboundedSender<PayloadTask> {
        self.task_sender.clone()
    }

    /// Get the group of senders that will be triggered each time the wait set
    /// is woken up. This is used
    pub fn activity_listeners(&self) -> Arc<Mutex<Vec<WeakActivityListener>>> {
        Arc::clone(&self.activity_listeners)
    }

    /// Get the guard condition associated with the wait set of this runner.
    pub fn guard_condition(&self) -> &Arc<GuardCondition> {
        &self.guard_condition
    }

    /// Ask the worker thread to run the wait set once with the given conditions.
    /// You receive a future that resolves to `(self, result)` once the wait set
    /// stops spinning; `self` is handed back so the runner can be reused for the
    /// next spin. This does **not** spawn a thread — it sends a request to the
    /// runner's existing worker thread.
    ///
    /// Note that if the user gives a [`SpinOptions::until_promise_resolved`][1],
    /// the best practice is for your executor runtime to swap that out with a
    /// new promise which ensures that the [`ExecutorWorkerOptions::guard_condition`]
    /// will be triggered after the user-provided promise is resolved.
    ///
    /// [1]: crate::SpinOptions::until_promise_resolved
    pub fn run(
        self,
        conditions: WaitSetRunConditions,
    ) -> BoxFuture<'static, (Self, Result<(), RclrsError>)> {
        // Hand the spin request to the worker thread and keep the receiver it
        // will resolve when the spin finishes.
        let result_receiver = self.spin_request.as_ref().and_then(|spin_request| {
            let (result_sender, result_receiver) = oneshot::channel();
            spin_request
                .send(SpinRequest {
                    conditions,
                    result: result_sender,
                })
                .ok()
                .map(|_| result_receiver)
        });

        Box::pin(async move {
            // The worker thread should always be alive while we hold the runner.
            // If the request could not be sent, or the worker dropped the result
            // sender without responding (e.g. its thread unwound because a
            // callback panicked), the worker is gone. Surface that as an error
            // rather than reporting a successful spin that did no work — otherwise
            // the executor would silently appear healthy while running nothing.
            let result = match result_receiver {
                Some(result_receiver) => result_receiver.await.unwrap_or_else(|_| Err(worker_lost())),
                None => Err(worker_lost()),
            };
            (self, result)
        })
    }
}

/// The error reported when a [`WaitSetRunner`]'s worker thread has died (it
/// should never happen while the runner is alive). Logged as fatal and returned
/// from the spin so the failure is not silently swallowed.
fn worker_lost() -> RclrsError {
    log_fatal!(
        "rclrs.wait_set_runner",
        "The wait set worker thread is gone. This should never happen while the \
        runner is alive; it usually means a callback panicked and unwound the \
        worker thread. Spinning can no longer run callbacks on this worker. \
        Please report this to the rclrs maintainers with a minimal reproduction.",
    );
    RclrsError::RclError {
        code: RclReturnCode::Error,
        msg: None,
    }
}

impl Drop for WaitSetRunner {
    fn drop(&mut self) {
        // Tell an in-flight spin to stop: set the shutdown flag (checked each
        // loop of `run_blocking`) and trigger the guard condition to wake the
        // worker out of `rcl_wait`. Then close the request channel (waking the
        // worker if it is instead parked waiting for a request) and join. This
        // ordering guarantees `join` cannot block on an active wait.
        self.shutdown.store(true, Ordering::Release);
        let _ = self.guard_condition.trigger();
        self.spin_request = None;
        if let Some(join_handle) = self.join_handle.take() {
            let _ = join_handle.join();
        }
    }
}

impl WaitSetWorker {
    /// The body of the worker thread: park until a spin is requested, run the
    /// wait set for that spin, report the result, and repeat. The loop ends — and
    /// the thread exits — once the [`WaitSetRunner`] (and thus the request sender)
    /// is dropped.
    fn work(mut self, spin_requests: RequestReceiver<SpinRequest>) {
        while let Ok(request) = spin_requests.recv() {
            let result = self.run_blocking(request.conditions);
            // The receiver may be gone if the executor is winding down; then no
            // one is waiting for the result, which is a normal occurrence.
            if request.result.send(result).is_err() {
                log_debug!(
                    "rclrs.wait_set_runner.work",
                    "Unable to return the result of a wait set spin"
                );
            }
        }
    }

    /// Run the wait set on the worker thread until the spin conditions say to
    /// stop. This blocks the worker thread for the duration of the spin.
    ///
    /// Note that if the user gives a [`SpinOptions::until_promise_resolved`][1],
    /// the best practice is for your executor runtime to swap that out with a
    /// new promise which ensures that the [`ExecutorWorkerOptions::guard_condition`]
    /// will be triggered after the user-provided promise is resolved.
    ///
    /// [1]: crate::SpinOptions::until_promise_resolved
    fn run_blocking(&mut self, conditions: WaitSetRunConditions) -> Result<(), RclrsError> {
        let mut first_spin = true;
        let mut listeners = Vec::new();
        loop {
            // TODO(@mxgrey): SmallVec would be better suited here if we are
            // okay with adding that as a dependency.
            let mut new_waitables = Vec::new();
            while let Ok(new_waitable) = self.waitable_receiver.try_recv() {
                new_waitables.push(new_waitable);
            }
            if !new_waitables.is_empty() {
                if let Err(err) = self.wait_set.add(new_waitables) {
                    log_fatal!(
                        "rclrs.wait_set_runner.run_blocking",
                        "Failed to add an item to the wait set: {err}",
                    );
                }
            }

            while let Ok(task) = self.task_receiver.try_recv() {
                task(&mut *self.payload);
            }

            if conditions.only_next_available_work && !first_spin {
                // We've already completed a spin and were asked to only do one,
                // so break here
                return Ok(());
            }
            first_spin = false;

            if self.shutdown.load(Ordering::Acquire) {
                // The runner is being dropped; stop spinning so it can be joined.
                return Ok(());
            }

            if conditions.halt_spinning.load(Ordering::Acquire) {
                // The user has manually asked for the spinning to stop
                return Ok(());
            }

            if !conditions.context.ok() {
                // The ROS context has switched to being invalid, so we should
                // stop spinning.
                return Ok(());
            }

            let timeout = conditions.stop_time.map(|t| {
                let timeout = t - Instant::now();
                if timeout < Duration::ZERO {
                    Duration::ZERO
                } else {
                    timeout
                }
            });

            let mut at_least_one = false;
            self.wait_set.wait(timeout, |ready, executable| {
                at_least_one = true;
                // SAFETY: The user of WaitSetRunner is responsible for ensuring
                // the runner has the same payload type as the executables that
                // are given to it.
                unsafe { executable.execute(ready, &mut *self.payload) }
            })?;

            if at_least_one {
                // We drain all listeners from activity_listeners to ensure that we
                // don't get a deadlock from double-locking the activity_listeners
                // mutex while executing one of the listeners. If the listener has
                // access to the Worker<T> then it could attempt to add another
                // listener while we have the vector locked, which would cause a
                // deadlock.
                listeners.extend(
                    self.activity_listeners
                        .lock()
                        .unwrap()
                        .drain(..)
                        .filter_map(|x| x.upgrade()),
                );

                for arc_listener in &listeners {
                    // We pull the callback out of its mutex entirely and release
                    // the lock on the mutex before executing the callback. Otherwise
                    // if the callback triggers its own WorkerActivity to change the
                    // callback then we would get a deadlock from double-locking the
                    // mutex.
                    let listener = { arc_listener.lock().unwrap().take() };
                    if let Some(mut listener) = listener {
                        match &mut listener {
                            ActivityListenerCallback::Listen(listen) => {
                                listen(&mut *self.payload);
                            }
                            ActivityListenerCallback::Inert => {
                                // Do nothing
                            }
                        }

                        // We replace instead of assigning in case the callback
                        // inserted its own
                        arc_listener.lock().unwrap().replace(listener);
                    }
                }

                self.activity_listeners
                    .lock()
                    .unwrap()
                    .extend(listeners.drain(..).map(|x| Arc::downgrade(&x)));
            }

            if let Some(stop_time) = conditions.stop_time {
                if stop_time <= Instant::now() {
                    // If we have exceeded the stop time, then quit spinning.
                    // self.wait_set.wait will not always return Err after a
                    // timeout because it's possible for a primitive to produce
                    // new worker faster than this loop spins.
                    return Err(RclrsError::RclError {
                        code: RclReturnCode::Timeout,
                        msg: None,
                    });
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{Context, ExecutorWorkerOptions, GuardCondition};
    use std::sync::mpsc::channel as std_channel;

    /// Dropping a runner while its worker is blocked in an active spin must not
    /// hang: `Drop` has to interrupt the in-flight `rcl_wait` (via the guard
    /// condition + shutdown flag), not just wake a parked worker, before it joins
    /// the thread. Regression for the teardown path. Without the fix the worker
    /// stays blocked in `rcl_wait` forever and `Drop`'s `join` deadlocks.
    #[test]
    fn dropping_runner_during_active_spin_does_not_hang() {
        let context = Context::default();
        // No callback; this guard condition exists only to populate the wait set
        // so that `rcl_wait` genuinely blocks (rather than returning immediately
        // on an empty wait set).
        let (guard_condition, guard_waitable) = GuardCondition::new(&context.handle, None);

        let runner = WaitSetRunner::new(ExecutorWorkerOptions {
            context: context.clone(),
            payload: Box::new(()),
            guard_condition,
        });
        runner.waitable_sender().unbounded_send(guard_waitable).unwrap();

        // Start a blocking spin (no timeout, not "next available only"): the
        // worker thread enters rcl_wait and blocks.
        let spin_future = runner.run(WaitSetRunConditions {
            only_next_available_work: false,
            stop_time: None,
            context: context.clone(),
            halt_spinning: Arc::new(AtomicBool::new(false)),
        });
        // Give the worker a moment to actually enter the wait.
        std::thread::sleep(Duration::from_millis(100));

        // Dropping the (never-polled) future drops the runner it holds, which
        // runs WaitSetRunner::drop. Do it on a side thread and require it to
        // finish promptly; if Drop hangs, recv_timeout reports the failure
        // instead of hanging the whole test binary.
        let (done_tx, done_rx) = std_channel();
        std::thread::spawn(move || {
            drop(spin_future);
            let _ = done_tx.send(());
        });

        assert!(
            done_rx
                .recv_timeout(Duration::from_secs(5))
                .is_ok(),
            "dropping a runner during an active spin hung: Drop did not interrupt \
             the in-flight rcl_wait before joining the worker thread",
        );
    }
}
