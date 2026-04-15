use futures::{
    channel::{mpsc::UnboundedSender, oneshot},
    future::BoxFuture,
};
use std::sync::{
    atomic::Ordering,
    Arc, Mutex,
};
use tokio::task::JoinSet;

use crate::{
    log_debug, log_fatal, log_warn, Context, ExecutorChannel, ExecutorRuntime,
    ExecutorWorkerOptions, PayloadTask, RclrsError, SpinConditions, WaitSetRunConditions,
    WaitSetRunner, Waitable, WeakActivityListener, WorkerChannel,
};

use super::guard_conditions::AllGuardConditions;
use super::Executor;

static FAILED_TO_SEND_WORKER: &str =
    "Failed to send the new runner. This should never happen. \
    Please report this to the rclrs maintainers with a minimal reproducible example.";

/// A multi-threaded async executor backed by a Tokio runtime.
///
/// Unlike [`BasicExecutorRuntime`][crate::BasicExecutorRuntime] which polls all
/// async tasks on a single thread, this executor spawns async tasks on Tokio's
/// multi-threaded work-stealing pool via [`tokio::task::spawn`]. This means:
///
/// - Async callbacks run concurrently across multiple threads.
/// - The full Tokio ecosystem is available inside callbacks (`tokio::time`,
///   `tokio::net`, `reqwest`, `sqlx`, `tonic`, etc.).
/// - [`WaitSetRunner`]s are dispatched to Tokio's blocking thread pool via
///   [`tokio::task::spawn_blocking`], rather than raw `std::thread::spawn`.
///
/// # Example
///
/// ```ignore
/// # use rclrs::*;
/// let context = Context::default();
/// let mut executor = context.create_tokio_executor();
/// let node = executor.create_node("my_node").unwrap();
///
/// // Async callbacks can now use the Tokio ecosystem
/// let _sub = node.create_async_subscription::<rclrs::vendor::example_interfaces::msg::String, _>(
///     "topic",
///     |msg: rclrs::vendor::example_interfaces::msg::String| async move {
///         tokio::time::sleep(std::time::Duration::from_millis(100)).await;
///         println!("Got: {}", msg.data);
///     },
/// ).unwrap();
///
/// executor.spin(SpinOptions::default().timeout(std::time::Duration::from_millis(100)));
/// ```
pub struct TokioExecutorRuntime {
    runtime: tokio::runtime::Runtime,
    wait_set_runners: Vec<WaitSetRunner>,
    all_guard_conditions: AllGuardConditions,
    new_worker_sender: tokio::sync::mpsc::UnboundedSender<WaitSetRunner>,
    new_worker_receiver: Option<tokio::sync::mpsc::UnboundedReceiver<WaitSetRunner>>,
    /// Handle to the spawned promise-watcher task from `process_spin_conditions`.
    /// Aborted at the start of each spin to prevent stale watchers from
    /// incorrectly halting a subsequent spin cycle.
    promise_watcher: Option<tokio::task::JoinHandle<()>>,
}

impl ExecutorRuntime for TokioExecutorRuntime {
    fn spin(&mut self, conditions: SpinConditions) -> Vec<RclrsError> {
        let conditions = self.process_spin_conditions(conditions);

        for runner in self.wait_set_runners.drain(..) {
            if let Err(err) = self.new_worker_sender.send(runner) {
                log_fatal!(
                    "rclrs.executor.tokio_executor",
                    "{FAILED_TO_SEND_WORKER} Error: {err}",
                );
            }
        }

        let new_worker_receiver = self.new_worker_receiver.take().expect(
            "Tokio executor was missing its new_worker_receiver at the start of its spinning. \
            This is a critical bug in rclrs. \
            Please report this bug to the maintainers of rclrs by providing \
            a minimum reproduction of the problem.",
        );
        let all_guard_conditions = self.all_guard_conditions.clone();

        let (runners, receiver, errors) = self.runtime.block_on(manage_workers(
            new_worker_receiver,
            all_guard_conditions,
            conditions,
        ));

        self.wait_set_runners = runners;
        self.new_worker_receiver = Some(receiver);
        errors
    }

    fn spin_async(
        mut self: Box<Self>,
        conditions: SpinConditions,
    ) -> BoxFuture<'static, (Box<dyn ExecutorRuntime>, Vec<RclrsError>)> {
        let (sender, receiver) = oneshot::channel();
        // Spawn a thread that calls spin(). We cannot just return an async block
        // that calls runtime.block_on() because the caller may already be inside
        // a Tokio runtime (e.g. #[tokio::main]), and nested block_on panics.
        std::thread::spawn(move || {
            let result = self.spin(conditions);
            sender.send((self as Box<dyn ExecutorRuntime>, result)).ok();
        });

        Box::pin(async move {
            receiver.await.expect(
                "The tokio executor async spin thread was dropped without finishing. \
                This is a critical bug in rclrs. \
                Please report this bug to the maintainers of rclrs by providing \
                a minimum reproduction of the problem.",
            )
        })
    }

    fn channel(&self) -> Arc<dyn ExecutorChannel> {
        Arc::new(TokioExecutorChannel {
            handle: self.runtime.handle().clone(),
            new_worker_sender: self.new_worker_sender.clone(),
            all_guard_conditions: self.all_guard_conditions.clone(),
        })
    }
}

impl TokioExecutorRuntime {
    /// Create a new `TokioExecutorRuntime` with a default multi-threaded Tokio
    /// runtime.
    ///
    /// Users should call [`CreateTokioExecutor::create_tokio_executor`] instead.
    pub(crate) fn new() -> Self {
        let runtime = tokio::runtime::Builder::new_multi_thread()
            .enable_all()
            .build()
            .expect("Failed to create Tokio runtime for rclrs executor");
        Self::with_runtime(runtime)
    }

    /// Create a new `TokioExecutorRuntime` with a caller-provided Tokio runtime.
    ///
    /// Users should call [`CreateTokioExecutor::create_tokio_executor_with_runtime`]
    /// instead.
    pub(crate) fn with_runtime(runtime: tokio::runtime::Runtime) -> Self {
        let (new_worker_sender, new_worker_receiver) = tokio::sync::mpsc::unbounded_channel();

        Self {
            runtime,
            wait_set_runners: Vec::new(),
            all_guard_conditions: AllGuardConditions::default(),
            new_worker_sender,
            new_worker_receiver: Some(new_worker_receiver),
            promise_watcher: None,
        }
    }

    fn process_spin_conditions(&mut self, mut conditions: SpinConditions) -> WaitSetRunConditions {
        // Abort any stale promise-watcher from a previous spin. Without this,
        // a lingering watcher could set halt_spinning after it has been reset,
        // incorrectly halting the new spin cycle.
        if let Some(handle) = self.promise_watcher.take() {
            handle.abort();
        }

        if let Some(promise) = conditions.options.until_promise_resolved.take() {
            let halt_spinning = Arc::clone(&conditions.halt_spinning);
            let all_guard_conditions = self.all_guard_conditions.clone();
            self.promise_watcher = Some(self.runtime.handle().spawn(async move {
                if let Err(err) = promise.await {
                    log_warn!(
                        "rclrs.executor.tokio_executor",
                        "Sender for SpinOptions::until_promise_resolved was \
                        dropped, so the Promise will never be fulfilled. \
                        Spinning will stop now. Error message: {err}"
                    );
                }

                // Ordering is very important here. halt_spinning must be set
                // before we lock and trigger the guard conditions. This ensures
                // that when the wait sets wake up, the halt_spinning value is
                // already set to true. Ordering::Release is also important for
                // that purpose.
                halt_spinning.store(true, Ordering::Release);
                all_guard_conditions.trigger();
            }));
        }

        WaitSetRunConditions {
            only_next_available_work: conditions.options.only_next_available_work,
            stop_time: conditions
                .options
                .timeout
                .map(|t| std::time::Instant::now() + t),
            context: conditions.context,
            halt_spinning: conditions.halt_spinning,
        }
    }
}

struct TokioExecutorChannel {
    handle: tokio::runtime::Handle,
    new_worker_sender: tokio::sync::mpsc::UnboundedSender<WaitSetRunner>,
    all_guard_conditions: AllGuardConditions,
}

impl ExecutorChannel for TokioExecutorChannel {
    fn create_worker(&self, options: ExecutorWorkerOptions) -> Arc<dyn WorkerChannel> {
        let runner = WaitSetRunner::new(options);
        let waitable_sender = runner.waitable_sender();
        let payload_task_sender = runner.payload_task_sender();
        let activity_listeners = runner.activity_listeners();

        if let Err(err) = self.new_worker_sender.send(runner) {
            log_fatal!(
                "rclrs.executor.tokio_executor",
                "{FAILED_TO_SEND_WORKER} Error: {err}",
            );
        }

        Arc::new(TokioWorkerChannel {
            handle: self.handle.clone(),
            waitable_sender,
            payload_task_sender,
            activity_listeners,
        })
    }

    fn wake_all_wait_sets(&self) {
        self.all_guard_conditions.trigger();
    }
}

struct TokioWorkerChannel {
    handle: tokio::runtime::Handle,
    waitable_sender: UnboundedSender<Waitable>,
    payload_task_sender: UnboundedSender<PayloadTask>,
    activity_listeners: Arc<Mutex<Vec<WeakActivityListener>>>,
}

impl WorkerChannel for TokioWorkerChannel {
    fn add_to_wait_set(&self, new_entity: Waitable) {
        if let Err(err) = self.waitable_sender.unbounded_send(new_entity) {
            log_debug!(
                "rclrs.tokio_executor.add_to_waitset",
                "Failed to add an item to the waitset: {err}",
            );
        }
    }

    fn add_async_task(&self, f: BoxFuture<'static, ()>) {
        // This is THE key difference from BasicExecutor: async tasks are
        // spawned on Tokio's multi-threaded work-stealing pool instead of
        // being polled on a single thread via ArcWake.
        self.handle.spawn(f);
    }

    fn send_payload_task(&self, f: PayloadTask) {
        if let Err(err) = self.payload_task_sender.unbounded_send(f) {
            log_debug!(
                "rclrs.TokioWorkerChannel",
                "Failed to send a payload task: {err}",
            );
        }
    }

    fn add_activity_listener(&self, listener: WeakActivityListener) {
        self.activity_listeners.lock().unwrap().push(listener);
    }
}

/// Manage worker lifecycles using Tokio's JoinSet and spawn_blocking.
///
/// Each [`WaitSetRunner`] is dispatched to Tokio's blocking thread pool.
/// New workers that arrive while spinning are also dispatched.
/// The function returns once all workers have finished.
async fn manage_workers(
    mut new_worker_rx: tokio::sync::mpsc::UnboundedReceiver<WaitSetRunner>,
    all_guard_conditions: AllGuardConditions,
    conditions: WaitSetRunConditions,
) -> (
    Vec<WaitSetRunner>,
    tokio::sync::mpsc::UnboundedReceiver<WaitSetRunner>,
    Vec<RclrsError>,
) {
    let mut active_workers: JoinSet<(WaitSetRunner, Result<(), RclrsError>)> = JoinSet::new();
    let mut finished_runners: Vec<WaitSetRunner> = Vec::new();
    let mut errors: Vec<RclrsError> = Vec::new();

    let spawn_worker = |runner: WaitSetRunner,
                        workers: &mut JoinSet<(WaitSetRunner, Result<(), RclrsError>)>,
                        conditions: &WaitSetRunConditions| {
        all_guard_conditions.push(Arc::downgrade(runner.guard_condition()));
        let conds = conditions.clone();
        workers.spawn_blocking(move || {
            let mut runner = runner;
            let result = runner.run_blocking(conds);
            (runner, result)
        });
    };

    // Wait for the first worker before entering the main loop.
    if let Some(initial_worker) = new_worker_rx.recv().await {
        if conditions.halt_spinning.load(Ordering::Acquire) {
            finished_runners.push(initial_worker);
        } else {
            spawn_worker(initial_worker, &mut active_workers, &conditions);
        }
    }

    while !active_workers.is_empty() {
        tokio::select! {
            Some(result) = active_workers.join_next() => {
                match result {
                    Ok((runner, run_result)) => {
                        finished_runners.push(runner);
                        if let Err(err) = run_result {
                            errors.push(err);
                        }
                    }
                    Err(join_err) => {
                        // The runner is lost (consumed by the panicked task).
                        // This mirrors BasicExecutor which also loses the runner
                        // when the oneshot channel is unexpectedly dropped.
                        log_fatal!(
                            "rclrs.tokio_executor",
                            "WaitSetRunner task panicked: {join_err}",
                        );
                    }
                }
            }
            Some(new_runner) = new_worker_rx.recv() => {
                if conditions.halt_spinning.load(Ordering::Acquire) {
                    finished_runners.push(new_runner);
                } else {
                    spawn_worker(new_runner, &mut active_workers, &conditions);
                }
            }
        }
    }

    (finished_runners, new_worker_rx, errors)
}

/// This trait allows [`Context`] to create a Tokio-based executor.
pub trait CreateTokioExecutor {
    /// Create a Tokio-based executor associated with this [`Context`].
    ///
    /// The executor will create its own multi-threaded Tokio runtime with
    /// default settings.
    fn create_tokio_executor(&self) -> Executor;

    /// Create a Tokio-based executor with a caller-provided Tokio runtime.
    ///
    /// This is useful when you need control over the number of worker threads,
    /// thread names, or other runtime configuration.
    fn create_tokio_executor_with_runtime(&self, runtime: tokio::runtime::Runtime) -> Executor;
}

impl CreateTokioExecutor for Context {
    fn create_tokio_executor(&self) -> Executor {
        let runtime = TokioExecutorRuntime::new();
        self.create_executor(runtime)
    }

    fn create_tokio_executor_with_runtime(&self, runtime: tokio::runtime::Runtime) -> Executor {
        let runtime = TokioExecutorRuntime::with_runtime(runtime);
        self.create_executor(runtime)
    }
}

#[cfg(test)]
mod tests {
    use crate::*;
    use std::time::Duration;

    #[test]
    fn test_tokio_timeout() {
        let context = Context::default();
        let mut executor = context.create_tokio_executor();
        let _node = executor
            .create_node(&format!("test_tokio_timeout_{}", line!()))
            .unwrap();

        for _ in 0..10 {
            let r = executor.spin(SpinOptions::default().timeout(Duration::from_millis(1)));
            assert_eq!(r.len(), 1);
            assert!(matches!(
                r[0],
                RclrsError::RclError {
                    code: RclReturnCode::Timeout,
                    ..
                }
            ));
        }
    }

    #[test]
    fn test_tokio_spin_async() {
        let context = Context::default();
        let executor = context.create_tokio_executor();
        let _node = executor
            .create_node(&format!("test_tokio_spin_async_{}", line!()))
            .unwrap();

        // Use a separate Tokio runtime to drive the spin_async future.
        let rt = tokio::runtime::Runtime::new().unwrap();
        rt.block_on(async {
            let (executor, errors) = executor
                .spin_async(SpinOptions::default().timeout(Duration::from_millis(10)))
                .await;
            assert_eq!(errors.len(), 1);
            assert!(matches!(
                errors[0],
                RclrsError::RclError {
                    code: RclReturnCode::Timeout,
                    ..
                }
            ));
            // Verify we get the executor back
            drop(executor);
        });
    }

    #[test]
    fn test_tokio_async_task_runs() {
        let context = Context::default();
        let mut executor = context.create_tokio_executor();
        let _node = executor
            .create_node(&format!("test_tokio_async_task_{}", line!()))
            .unwrap();

        let done = Arc::new(std::sync::atomic::AtomicBool::new(false));
        let done_clone = Arc::clone(&done);

        // Submit an async task that uses tokio::time::sleep.
        // This would panic on BasicExecutor because there's no Tokio context.
        let promise = executor.commands().run(async move {
            tokio::time::sleep(Duration::from_millis(1)).await;
            done_clone.store(true, std::sync::atomic::Ordering::Release);
        });

        let (_, notice) = executor.commands().create_notice(promise);
        executor
            .spin(
                SpinOptions::new()
                    .until_promise_resolved(notice)
                    .timeout(Duration::from_secs(5)),
            )
            .first_error()
            .unwrap();

        assert!(done.load(std::sync::atomic::Ordering::Acquire));
    }
}
