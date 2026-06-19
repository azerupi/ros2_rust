//! Event-driven, Tokio-backed executor for rclrs.
//!
//! Readiness is push-based (rcl `set_on_new_*_callback`), not polled. Each
//! **Worker** (the node's default group is its main worker) gets its own
//! `tokio::mpsc` mailbox and one spawned task that drains it. Because a Tokio
//! task is never polled by two threads at once, that single task gives
//! per-worker mutual exclusion *and* FIFO ordering for free; Tokio's scheduler
//! provides the thread pool, work-stealing, and M:N multiplexing — so different
//! workers run concurrently automatically, with no per-event spawn and nothing
//! for the user to configure.
//!
//! Worker tasks are **gated by spinning**: they only execute ROS entity
//! callbacks (subscriptions, services, clients, timers, actions) while `spin()`
//! is active, preserving rclrs's contract that those callbacks do not run until
//! you spin and that none are still running once `spin()` returns (quiescence is
//! enforced by waiting for in-flight callbacks before returning).
//!
//! This gating applies to entity callbacks, not to free-standing async tasks
//! spawned through the executor commands (e.g. `commands().run(..)`): those are
//! ordinary Tokio tasks and run on the runtime independently of `spin()`, which
//! is the point of an async executor. Code that needs work confined to spinning
//! should put it in an entity callback rather than a spawned task.

use std::any::Any;
use std::collections::HashMap;
use std::panic::AssertUnwindSafe;
use std::sync::{
    atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering},
    Arc, Mutex,
};
use std::time::{Duration, Instant};

use futures::{channel::oneshot, future::BoxFuture};
use tokio::sync::{
    mpsc::{UnboundedReceiver, UnboundedSender},
    watch, Notify,
};

use crate::rcl_bindings::{rcl_timer_get_time_until_next_call, rcl_timer_is_ready, rcl_timer_t};
use crate::{
    log_error, ActionClientReady, ActionServerReady, Context, ExecutorChannel, ExecutorRuntime,
    ExecutorWorkerOptions, OnReadyHandle, PayloadTask, RclPrimitiveKind, RclReturnCode, RclrsError,
    ReadyKind, SpinConditions, ToResult, Waitable, WeakActivityListener, WorkerChannel,
};

use super::Executor;

/// Identifies an entity within a worker.
type EntityId = u64;

/// A message delivered to a worker's task.
enum WorkerMsg {
    /// The entity became ready; take and run its callback(s). At most one such
    /// message is outstanding per entity at a time (see [`WorkerEntity::scheduled`]);
    /// the worker takes [`WorkerEntity::pending`] items when it handles it.
    Ready { entity: EntityId },
    /// Run a one-shot task against the worker's payload.
    Payload(PayloadTask),
}

/// An entity owned by a worker: registration inserts it, the worker task runs it.
struct WorkerEntity {
    waitable: Mutex<Waitable>,
    /// Set when a `Ready` for this entity is already queued and not yet handled,
    /// so concurrent middleware notifications coalesce into a single mailbox
    /// message. This bounds the mailbox to at most one pending `Ready` per
    /// entity (rather than one per message) even while spinning is paused. The
    /// worker clears it *before* draining, so notifications that race the drain
    /// re-arm the entity and are never lost.
    scheduled: Arc<AtomicBool>,
    /// Accumulated count of ready events reported since the last time the worker
    /// handled this entity. Notifications add their `number_of_events` here; the
    /// worker swaps it to zero and takes that many items. Using the reported
    /// count (rather than looping until a take fails) keeps the take bounded for
    /// every primitive kind, including those that report "empty" as success.
    pending: Arc<AtomicUsize>,
    /// Merged readiness for composite primitives (action servers/clients) whose
    /// sub-entities report *different* [`ReadyKind`]s through the same entity.
    /// Notifications OR their flags in; the worker swaps it out (resetting to the
    /// kind's neutral value) and runs the primitive with it. `None` for primitives
    /// with a single readiness path (subscriptions/services/clients/timers), which
    /// are always [`ReadyKind::Basic`] — keeping their hot path lock-free.
    ready: Option<Arc<Mutex<ReadyKind>>>,
    /// Keeps the push callback registered; dropping it deregisters. `None` for
    /// passive entities (e.g. guard conditions) or timers (driven separately).
    /// Behind a `Mutex` so it can be filled in *after* the entity is inserted
    /// into the registry: registering before insertion would let an early
    /// middleware callback enqueue a `Ready` the worker can't resolve, which it
    /// would drop — wedging the entity with its `scheduled` flag stuck set.
    _on_ready: Mutex<Option<Box<dyn OnReadyHandle>>>,
}

/// State shared between the runtime and all workers.
struct ExecutorShared {
    /// Gate the worker tasks observe: they execute only while this is `true`.
    spin: watch::Sender<bool>,
    /// Promptly wakes `spin()` when a halt is requested.
    halt: Arc<Notify>,
    /// Number of callbacks currently executing across all workers. `spin()`
    /// waits for this to reach zero before returning, so no ROS callback is
    /// running once `spin()` has returned (quiescence).
    active: Arc<AtomicUsize>,
    /// Number of mailbox messages enqueued across all workers but not yet
    /// handled (queued *or* in flight). `spin()` with `only_next_available_work`
    /// uses this to detect when the currently-available work has drained.
    outstanding: Arc<AtomicUsize>,
    /// Errors produced by callbacks; drained and returned by `spin()`.
    errors: Arc<Mutex<Vec<RclrsError>>>,
    /// Allocates entity ids across all workers.
    next_entity_id: Arc<AtomicU64>,
}

/// OR the readiness flags of `new` into `acc`. Used to merge the
/// per-sub-entity readiness of an action server/client into one value before the
/// worker runs the primitive. Basic and mismatched variants leave `acc` as-is.
fn merge_ready(acc: &mut ReadyKind, new: ReadyKind) {
    match (acc, new) {
        (ReadyKind::ActionServer(a), ReadyKind::ActionServer(b)) => {
            a.goal_request |= b.goal_request;
            a.cancel_request |= b.cancel_request;
            a.result_request |= b.result_request;
            a.goal_expired |= b.goal_expired;
        }
        (ReadyKind::ActionClient(a), ReadyKind::ActionClient(b)) => {
            a.feedback |= b.feedback;
            a.status |= b.status;
            a.goal_response |= b.goal_response;
            a.cancel_response |= b.cancel_response;
            a.result_response |= b.result_response;
        }
        _ => {}
    }
}

/// The "no readiness" value for `kind`'s variant, used to reset an accumulator
/// after the worker has taken its merged readiness.
fn neutral_ready(kind: &ReadyKind) -> ReadyKind {
    match kind {
        ReadyKind::Basic => ReadyKind::Basic,
        ReadyKind::ActionServer(_) => ReadyKind::ActionServer(ActionServerReady::default()),
        ReadyKind::ActionClient(_) => ReadyKind::ActionClient(ActionClientReady::default()),
    }
}

/// The per-worker task: drain the mailbox, gated by `spinning`, running each
/// message against this worker's payload (and its activity listeners).
async fn worker_task(
    mut mailbox: UnboundedReceiver<WorkerMsg>,
    entities: Arc<Mutex<HashMap<EntityId, Arc<WorkerEntity>>>>,
    mut payload: Box<dyn Any + Send>,
    listeners: Arc<Mutex<Vec<WeakActivityListener>>>,
    mut spinning: watch::Receiver<bool>,
    error_sink: Arc<Mutex<Vec<RclrsError>>>,
    active: Arc<AtomicUsize>,
    outstanding: Arc<AtomicUsize>,
) {
    // Periodically drop entities whose owning handle has been released, so we
    // stop holding their rcl handle and push-callback registration. Entities on
    // active topics are also reaped on-event below; this catches idle ones.
    let mut reap = tokio::time::interval(Duration::from_secs(1));

    loop {
        let msg = tokio::select! {
            _ = reap.tick() => {
                entities
                    .lock()
                    .unwrap()
                    .retain(|_, e| e.waitable.lock().unwrap().in_use());
                continue;
            }
            msg = mailbox.recv() => match msg {
                Some(msg) => msg,
                None => return, // worker dropped
            },
        };

        // Count this unit as in-flight *before* checking the gate, so a
        // concurrent `spin()` close either observes it (and waits for it) or sees
        // the gate already closed below (and we don't run the callback). Either
        // way no callback runs after `spin()` returns.
        active.fetch_add(1, Ordering::AcqRel);

        // Gate: hold the message until the executor is spinning.
        loop {
            let is_spinning = *spinning.borrow_and_update();
            if is_spinning {
                break;
            }
            // Not spinning: release the in-flight count while parked.
            active.fetch_sub(1, Ordering::AcqRel);
            if spinning.changed().await.is_err() {
                return; // executor dropped
            }
            active.fetch_add(1, Ordering::AcqRel);
        }

        let mut errors = Vec::new();
        match msg {
            WorkerMsg::Ready { entity } => {
                // Clone the entry out under a brief lock so a callback may create
                // new entities on this worker without deadlocking.
                let entry = entities.lock().unwrap().get(&entity).cloned();
                if let Some(entry) = entry {
                    // Re-arm coalescing *before* taking: a notification that
                    // arrives while we run re-sets `scheduled` and re-accumulates
                    // `pending` (and `ready`), enqueueing a fresh `Ready`, so no
                    // wakeup is lost.
                    entry.scheduled.store(false, Ordering::Release);
                    let count = entry.pending.swap(0, Ordering::AcqRel);
                    // Determine which readiness to run with. Simple primitives are
                    // always `Basic`; composite ones report their merged flags.
                    let ready = match &entry.ready {
                        None => ReadyKind::Basic,
                        Some(acc) => {
                            let mut acc = acc.lock().unwrap();
                            let taken = *acc;
                            *acc = neutral_ready(&taken);
                            taken
                        }
                    };
                    let mut waitable = entry.waitable.lock().unwrap();
                    if !waitable.in_use() {
                        // The owning handle was dropped: deregister and never run
                        // a callback for a dropped entity.
                        drop(waitable);
                        entities.lock().unwrap().remove(&entity);
                    } else {
                        // Take exactly the number of events reported (at least
                        // one), stopping early if a take turns up empty. The work
                        // runs inside `catch_unwind` so a panicking callback cannot
                        // leak the `active`/`outstanding` counters (which would
                        // wedge spin() quiescence forever) or kill the worker task.
                        // The mutex guard is held *outside* the closure, so it
                        // drops normally (unpoisoned) if the callback unwinds.
                        let exec = std::panic::catch_unwind(AssertUnwindSafe(|| {
                            let mut ran = false;
                            let mut errs = Vec::new();
                            for _ in 0..count.max(1) {
                                match waitable.execute_with(ready, &mut *payload) {
                                    Ok(()) => ran = true,
                                    Err(err) if err.is_take_failed() => break,
                                    Err(err) => {
                                        errs.push(err);
                                        break;
                                    }
                                }
                            }
                            (ran, errs)
                        }));
                        drop(waitable);

                        let ran = match exec {
                            Ok((ran, errs)) => {
                                errors.extend(errs);
                                ran
                            }
                            Err(_) => {
                                log_error!(
                                    "rclrs.executor.tokio_executor",
                                    "A callback panicked while spinning; the executor \
                                     contained the panic and continues. The worker's \
                                     payload may now be in an inconsistent state.",
                                );
                                false
                            }
                        };
                        if ran
                            && std::panic::catch_unwind(AssertUnwindSafe(|| {
                                crate::worker::run_activity_listeners(&listeners, &mut *payload);
                            }))
                            .is_err()
                        {
                            log_error!(
                                "rclrs.executor.tokio_executor",
                                "A worker activity listener panicked; the executor \
                                 contained the panic and continues.",
                            );
                        }
                    }
                }
            }
            WorkerMsg::Payload(task) => {
                if std::panic::catch_unwind(AssertUnwindSafe(|| task(&mut *payload))).is_err() {
                    log_error!(
                        "rclrs.executor.tokio_executor",
                        "A payload task panicked; the executor contained the panic \
                         and continues.",
                    );
                }
            }
        }

        if !errors.is_empty() {
            error_sink.lock().unwrap().extend(errors);
        }
        active.fetch_sub(1, Ordering::AcqRel);
        // This mailbox message is fully handled; it no longer counts as work.
        outstanding.fetch_sub(1, Ordering::AcqRel);
    }
}

/// Drive a timer from the Tokio clock (timers have no rcl push callback): sleep
/// until the next deadline, enqueue a ready message into the worker's mailbox,
/// and wait until the worker task has called it before computing the next
/// deadline. Stops when the timer's owning entity is dropped.
async fn timer_driver(
    rcl_timer: Arc<Mutex<rcl_timer_t>>,
    in_use: Arc<AtomicBool>,
    id: EntityId,
    mailbox: UnboundedSender<WorkerMsg>,
    scheduled: Arc<AtomicBool>,
    pending: Arc<AtomicUsize>,
    outstanding: Arc<AtomicUsize>,
) {
    loop {
        if !in_use.load(Ordering::Acquire) {
            return;
        }
        let ns = {
            let timer = rcl_timer.lock().unwrap();
            let mut value: i64 = 0;
            // SAFETY: handle valid and locked; out-pointer valid.
            let ret = unsafe { rcl_timer_get_time_until_next_call(&*timer, &mut value) };
            ret.ok().map(|()| value)
        };
        match ns {
            Ok(value) if value > 0 => {
                tokio::time::sleep(Duration::from_nanos(value as u64)).await;
            }
            Ok(_) => tokio::task::yield_now().await, // due now
            Err(_) => {
                // The timer is canceled (or errored): don't busy-loop. Back off
                // and re-check; the driver exits when the timer is dropped.
                tokio::time::sleep(Duration::from_millis(50)).await;
                continue;
            }
        }

        if !in_use.load(Ordering::Acquire) {
            return;
        }
        pending.fetch_add(1, Ordering::AcqRel);
        if !scheduled.swap(true, Ordering::AcqRel) {
            outstanding.fetch_add(1, Ordering::AcqRel);
            if mailbox.send(WorkerMsg::Ready { entity: id }).is_err() {
                return; // worker gone
            }
        }

        // Wait until the worker task has actually called the timer (no longer
        // ready) before computing the next deadline, to avoid re-firing.
        loop {
            if !in_use.load(Ordering::Acquire) {
                return;
            }
            let ready = {
                let timer = rcl_timer.lock().unwrap();
                let mut ready = false;
                // SAFETY: handle valid and locked; out-pointer valid.
                let ret = unsafe { rcl_timer_is_ready(&*timer, &mut ready) };
                // On error (e.g. canceled), treat as not ready so we break out
                // and let the outer loop's time query handle the canceled state.
                ret.ok().map(|()| ready).unwrap_or(false)
            };
            if !ready {
                break;
            }
            tokio::time::sleep(Duration::from_millis(1)).await;
        }
    }
}

/// How often to poll an action server for expired goals. Goal expiration is
/// driven by an rcl-internal timer with no push callback, so we poll instead.
/// The interval only bounds how promptly a *completed* goal is cleaned up
/// (typically well after its multi-second result timeout), so it can be coarse.
const ACTION_EXPIRE_POLL: Duration = Duration::from_millis(100);

/// Periodically nudge an action server to expire completed goals (there is no
/// rcl push callback for expiration). Enqueues a `goal_expired` readiness through
/// the same coalescing path as other events; the worker runs
/// `rcl_action_expire_goals`, which is a cheap no-op when nothing has expired.
/// Stops once the action server's owning entity is dropped.
async fn action_expire_driver(
    in_use: Arc<AtomicBool>,
    id: EntityId,
    mailbox: UnboundedSender<WorkerMsg>,
    scheduled: Arc<AtomicBool>,
    pending: Arc<AtomicUsize>,
    ready: Arc<Mutex<ReadyKind>>,
    outstanding: Arc<AtomicUsize>,
) {
    loop {
        tokio::time::sleep(ACTION_EXPIRE_POLL).await;
        if !in_use.load(Ordering::Acquire) {
            return;
        }
        // Always set the flag so expiration is checked on the next wakeup, but
        // only enqueue/count a fresh take when one isn't already pending. Expiry
        // is idempotent (one run clears all expired goals), so coalescing this
        // way keeps `pending` bounded even while spinning is paused — otherwise
        // the 100ms poll would accumulate redundant work for the whole pause.
        merge_ready(
            &mut ready.lock().unwrap(),
            ReadyKind::ActionServer(ActionServerReady {
                goal_expired: true,
                ..Default::default()
            }),
        );
        if !scheduled.swap(true, Ordering::AcqRel) {
            pending.fetch_add(1, Ordering::AcqRel);
            outstanding.fetch_add(1, Ordering::AcqRel);
            if mailbox.send(WorkerMsg::Ready { entity: id }).is_err() {
                return; // worker gone
            }
        }
    }
}

/// A multi-threaded async executor backed by a Tokio runtime, driven by rcl push
/// callbacks, with one task per worker (see the module docs).
pub struct TokioExecutorRuntime {
    runtime: tokio::runtime::Runtime,
    shared: Arc<ExecutorShared>,
}

impl TokioExecutorRuntime {
    /// Create a runtime with a default multi-threaded Tokio runtime.
    ///
    /// Users should call [`CreateTokioExecutor::create_tokio_executor`] instead.
    pub(crate) fn new() -> Self {
        let runtime = tokio::runtime::Builder::new_multi_thread()
            .enable_all()
            .build()
            .expect("Failed to create Tokio runtime for rclrs executor");
        Self::with_runtime(runtime)
    }

    /// Create a runtime with a caller-provided Tokio runtime.
    ///
    /// Users should call
    /// [`CreateTokioExecutor::create_tokio_executor_with_runtime`] instead.
    pub(crate) fn with_runtime(runtime: tokio::runtime::Runtime) -> Self {
        let (spin, _) = watch::channel(false);
        Self {
            runtime,
            shared: Arc::new(ExecutorShared {
                spin,
                halt: Arc::new(Notify::new()),
                active: Arc::new(AtomicUsize::new(0)),
                outstanding: Arc::new(AtomicUsize::new(0)),
                errors: Arc::new(Mutex::new(Vec::new())),
                next_entity_id: Arc::new(AtomicU64::new(0)),
            }),
        }
    }

    fn take_errors(&self) -> Vec<RclrsError> {
        std::mem::take(&mut *self.shared.errors.lock().unwrap())
    }
}

impl ExecutorRuntime for TokioExecutorRuntime {
    fn channel(&self) -> Arc<dyn ExecutorChannel> {
        Arc::new(TokioExecutorChannel {
            handle: self.runtime.handle().clone(),
            shared: Arc::clone(&self.shared),
        })
    }

    fn spin(&mut self, mut conditions: SpinConditions) -> Vec<RclrsError> {
        // When the until-promise resolves, request a halt and wake the waiter.
        // We keep the JoinHandle and abort it when this spin returns: otherwise a
        // spin that ends for another reason (timeout, context shutdown) would
        // leave the task parked on `promise.await` forever, accumulating one
        // detached task per such spin.
        let promise_task = conditions.options.until_promise_resolved.take().map(|promise| {
            let halt_flag = Arc::clone(&conditions.halt_spinning);
            let halt_notify = Arc::clone(&self.shared.halt);
            self.runtime.spawn(async move {
                let _ = promise.await;
                halt_flag.store(true, Ordering::Release);
                halt_notify.notify_waiters();
            })
        });

        let stop_time = conditions.options.timeout.map(|t| Instant::now() + t);
        let only_once = conditions.options.only_next_available_work;
        let halt_flag = Arc::clone(&conditions.halt_spinning);
        let halt_notify = Arc::clone(&self.shared.halt);
        let context = conditions.context.clone();
        let active = Arc::clone(&self.shared.active);
        let outstanding = Arc::clone(&self.shared.outstanding);

        // Open the gate so the worker tasks process while we block here.
        let _ = self.shared.spin.send(true);
        let timed_out = self.runtime.block_on(async move {
            // For `only_next_available_work`, poll tightly so we detect that the
            // available work has drained without adding latency; otherwise a
            // coarse poll is enough (we only re-check halt/timeout/context).
            let poll = if only_once {
                Duration::from_micros(200)
            } else {
                Duration::from_millis(100)
            };
            // Tracks whether any work has been seen this spin, so `only_once`
            // waits for work to arrive (up to the timeout) before declaring the
            // batch drained.
            let mut saw_work = false;
            loop {
                if halt_flag.load(Ordering::Acquire) {
                    return false;
                }
                // Stop spinning once the ROS context is no longer valid (shutdown).
                if !context.ok() {
                    return false;
                }

                let busy = outstanding.load(Ordering::Acquire) > 0
                    || active.load(Ordering::Acquire) > 0;
                if busy {
                    saw_work = true;
                }

                if only_once {
                    // Process the currently-available work, then stop. While work
                    // is in flight we keep draining (never time out mid-batch);
                    // once it has drained we're done. If no work is in flight we
                    // wait for some to arrive, up to the timeout.
                    if saw_work && !busy {
                        return false;
                    }
                    if !busy && stop_time.is_some_and(|st| Instant::now() >= st) {
                        return true;
                    }
                } else if stop_time.is_some_and(|st| Instant::now() >= st) {
                    // Ran for the requested duration.
                    return true;
                }

                let wait = stop_time
                    .map(|st| st.saturating_duration_since(Instant::now()))
                    .unwrap_or(poll)
                    .min(poll);
                tokio::select! {
                    _ = halt_notify.notified() => {}
                    _ = tokio::time::sleep(wait) => {}
                }
            }
        });
        // Close the gate: worker tasks park after finishing any in-flight message.
        let _ = self.shared.spin.send(false);
        // Cancel the until-promise watcher so it doesn't outlive this spin.
        if let Some(task) = promise_task {
            task.abort();
        }

        // Quiescence: wait for any in-flight callbacks to finish before returning,
        // so the contract "no ROS callbacks are running once spin() returns" holds.
        let active = Arc::clone(&self.shared.active);
        self.runtime.block_on(async move {
            while active.load(Ordering::Acquire) > 0 {
                tokio::time::sleep(Duration::from_micros(100)).await;
            }
        });

        // Match the basic executor's contract: a timeout is reported as a
        // `Timeout` error rather than a silent return.
        let mut errors = self.take_errors();
        if timed_out {
            errors.push(RclrsError::RclError {
                code: RclReturnCode::Timeout,
                msg: None,
            });
        }
        errors
    }

    fn spin_async(
        mut self: Box<Self>,
        conditions: SpinConditions,
    ) -> BoxFuture<'static, (Box<dyn ExecutorRuntime>, Vec<RclrsError>)> {
        let (sender, receiver) = oneshot::channel();
        std::thread::spawn(move || {
            let result = self.spin(conditions);
            sender.send((self as Box<dyn ExecutorRuntime>, result)).ok();
        });

        Box::pin(async move {
            receiver.await.expect(
                "The Tokio executor's async spin thread was dropped without finishing. \
                This is a critical bug in rclrs; please report it with a reproduction.",
            )
        })
    }
}

struct TokioExecutorChannel {
    handle: tokio::runtime::Handle,
    shared: Arc<ExecutorShared>,
}

impl ExecutorChannel for TokioExecutorChannel {
    fn create_worker(&self, options: ExecutorWorkerOptions) -> Arc<dyn WorkerChannel> {
        let (mailbox_tx, mailbox_rx) = tokio::sync::mpsc::unbounded_channel();
        let entities = Arc::new(Mutex::new(HashMap::new()));
        let listeners = Arc::new(Mutex::new(Vec::new()));

        // One task per worker. Tokio schedules it; different workers therefore run
        // concurrently, while this worker's callbacks stay serialized and ordered.
        self.handle.spawn(worker_task(
            mailbox_rx,
            Arc::clone(&entities),
            options.payload,
            Arc::clone(&listeners),
            self.shared.spin.subscribe(),
            Arc::clone(&self.shared.errors),
            Arc::clone(&self.shared.active),
            Arc::clone(&self.shared.outstanding),
        ));

        Arc::new(TokioWorkerChannel {
            handle: self.handle.clone(),
            mailbox: mailbox_tx,
            entities,
            listeners,
            errors: Arc::clone(&self.shared.errors),
            next_entity_id: Arc::clone(&self.shared.next_entity_id),
            outstanding: Arc::clone(&self.shared.outstanding),
        })
    }

    fn wake_all_wait_sets(&self) {
        // Wake any in-progress spin so it re-checks halt_spinning promptly.
        self.shared.halt.notify_waiters();
    }
}

struct TokioWorkerChannel {
    handle: tokio::runtime::Handle,
    mailbox: UnboundedSender<WorkerMsg>,
    entities: Arc<Mutex<HashMap<EntityId, Arc<WorkerEntity>>>>,
    listeners: Arc<Mutex<Vec<WeakActivityListener>>>,
    errors: Arc<Mutex<Vec<RclrsError>>>,
    next_entity_id: Arc<AtomicU64>,
    outstanding: Arc<AtomicUsize>,
}

impl WorkerChannel for TokioWorkerChannel {
    fn add_async_task(&self, f: BoxFuture<'static, ()>) {
        self.handle.spawn(f);
    }

    fn add_to_wait_set(&self, new_entity: Waitable) {
        let id = self.next_entity_id.fetch_add(1, Ordering::Relaxed);
        let kind = new_entity.kind();

        // Guard conditions have no rcl push-callback API, so `register_on_ready`
        // returns `None` for them and they sit inert in the registry — there is
        // no wait set here to interrupt, so the per-worker wakeup guard condition
        // (callback-less) is simply unnecessary: new entities register their push
        // callback immediately, payload tasks go straight to the mailbox, and
        // removals are reaped.
        //
        // The one guard condition that *does* carry a callback is the node graph
        // guard condition (see `node_options.rs`), whose callback forwards graph
        // changes to the node's graph task. We cannot push-drive it (rmw exposes
        // no "on trigger" callback for guard conditions), so on this executor it
        // is not event-driven. This is not a correctness regression: graph-change
        // listeners (`Node::notify_on_graph_change`) re-check their condition on a
        // period regardless of notifications, so they still resolve — within that
        // period rather than immediately. Driving graph changes with lower latency
        // would require polling the guard condition and is left as future work.

        // Composite primitives (action servers/clients) report different readiness
        // per sub-entity through the same entity; accumulate the merged readiness
        // here. Simple primitives are always `Basic` and skip this (lock-free).
        let ready: Option<Arc<Mutex<ReadyKind>>> = match kind {
            RclPrimitiveKind::ActionServer => Some(Arc::new(Mutex::new(ReadyKind::ActionServer(
                ActionServerReady::default(),
            )))),
            RclPrimitiveKind::ActionClient => Some(Arc::new(Mutex::new(ReadyKind::ActionClient(
                ActionClientReady::default(),
            )))),
            _ => None,
        };

        // Coalesce middleware notifications: accumulate their event counts (and,
        // for composite primitives, merge their readiness flags) and enqueue a
        // single `Ready` per entity (one is enough — the worker takes `pending`
        // items when it handles it). This bounds the mailbox to at most one
        // pending `Ready` per entity even while spinning is paused.
        let scheduled = Arc::new(AtomicBool::new(false));
        let pending = Arc::new(AtomicUsize::new(0));
        let mailbox = self.mailbox.clone();
        let sched_cb = Arc::clone(&scheduled);
        let pending_cb = Arc::clone(&pending);
        let ready_cb = ready.clone();
        let outstanding_cb = Arc::clone(&self.outstanding);
        let on_ready: Box<dyn Fn(ReadyKind, usize) + Send + Sync> = Box::new(move |kind, count| {
            if let Some(acc) = &ready_cb {
                merge_ready(&mut acc.lock().unwrap(), kind);
            }
            pending_cb.fetch_add(count.max(1), Ordering::AcqRel);
            if !sched_cb.swap(true, Ordering::AcqRel) {
                outstanding_cb.fetch_add(1, Ordering::AcqRel);
                let _ = mailbox.send(WorkerMsg::Ready { entity: id });
            }
        });

        // Grab the timer-driver inputs before `new_entity` is moved into the
        // registry below.
        let timer = new_entity.timer_handle();
        let in_use = new_entity.in_use_handle();

        // Insert into the registry BEFORE registering the push callback (or
        // spawning the timer/expiration drivers), so the entity is always
        // resolvable by the time any readiness can enqueue a `Ready` for it.
        // Registering first would race: an early middleware callback could fire,
        // enqueue a `Ready`, and have the worker drop it (entity not found yet),
        // leaving `scheduled` stuck set so no further `Ready` is ever sent. The
        // `_on_ready` handle is filled in just below, once the callback is live.
        let entry = Arc::new(WorkerEntity {
            waitable: Mutex::new(new_entity),
            scheduled: Arc::clone(&scheduled),
            pending: Arc::clone(&pending),
            ready: ready.clone(),
            _on_ready: Mutex::new(None),
        });
        self.entities.lock().unwrap().insert(id, Arc::clone(&entry));

        // Now register the push callback against the (already-inserted) entity.
        // Holding the waitable lock here is safe: the callback only touches the
        // coalescing atomics and the mailbox, never the waitable.
        let registration = match entry.waitable.lock().unwrap().register_on_ready(on_ready) {
            Ok(registration) => registration,
            Err(err) => {
                // Surface the failure both in the log and via spin()'s error
                // return, rather than silently leaving an inert entity.
                log_error!(
                    "rclrs.executor.tokio_executor",
                    "Failed to register an on-ready callback: {err}",
                );
                self.errors.lock().unwrap().push(err);
                None
            }
        };
        *entry._on_ready.lock().unwrap() = registration;

        // Timers have no rcl push callback; drive them from the Tokio clock. The
        // driver coalesces through the same scheduled/pending pair as push
        // callbacks, so a timer fire is one bounded take like any other event.
        if let Some(rcl_timer) = timer {
            self.handle.spawn(timer_driver(
                rcl_timer,
                Arc::clone(&in_use),
                id,
                self.mailbox.clone(),
                Arc::clone(&scheduled),
                Arc::clone(&pending),
                Arc::clone(&self.outstanding),
            ));
        }

        // Action-server goal expiration has no rcl push callback (rcl uses an
        // internal timer); poll it periodically so completed goals are cleaned up.
        if kind == RclPrimitiveKind::ActionServer {
            if let Some(acc) = ready {
                self.handle.spawn(action_expire_driver(
                    in_use,
                    id,
                    self.mailbox.clone(),
                    scheduled,
                    pending,
                    acc,
                    Arc::clone(&self.outstanding),
                ));
            }
        }
    }

    fn send_payload_task(&self, f: PayloadTask) {
        // Counts as outstanding work until a worker handles it (so
        // `only_next_available_work` waits for payload tasks too).
        self.outstanding.fetch_add(1, Ordering::AcqRel);
        let _ = self.mailbox.send(WorkerMsg::Payload(f));
    }

    fn add_activity_listener(&self, listener: WeakActivityListener) {
        self.listeners.lock().unwrap().push(listener);
    }
}

/// This trait allows [`Context`] to create a Tokio-based executor.
pub trait CreateTokioExecutor {
    /// Create an event-driven Tokio-based executor associated with this
    /// [`Context`], with its own default multi-threaded Tokio runtime.
    fn create_tokio_executor(&self) -> Executor;

    /// Create an event-driven Tokio-based executor with a caller-provided Tokio
    /// runtime (e.g. to control worker-thread count or names).
    fn create_tokio_executor_with_runtime(&self, runtime: tokio::runtime::Runtime) -> Executor;
}

impl CreateTokioExecutor for Context {
    fn create_tokio_executor(&self) -> Executor {
        self.create_executor(TokioExecutorRuntime::new())
    }

    fn create_tokio_executor_with_runtime(&self, runtime: tokio::runtime::Runtime) -> Executor {
        self.create_executor(TokioExecutorRuntime::with_runtime(runtime))
    }
}

#[cfg(test)]
mod tests {
    use crate::*;
    use ros_env::test_msgs;
    use ros_env::test_msgs::msg;
    use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
    use std::sync::Arc;
    use std::time::{Duration, Instant};

    /// A spin with a timeout and no work reports a `Timeout` error, matching the
    /// basic executor's contract (rather than returning silently).
    #[test]
    fn tokio_spin_timeout_reports_error() -> Result<(), RclrsError> {
        let mut executor = Context::default().create_tokio_executor();
        let _node = executor.create_node(
            format!("test_tokio_timeout_{}", line!()).start_parameter_services(false),
        )?;

        let errors = executor.spin(SpinOptions::default().timeout(Duration::from_millis(20)));
        assert!(
            errors.iter().any(|e| matches!(
                e,
                RclrsError::RclError {
                    code: RclReturnCode::Timeout,
                    ..
                }
            )),
            "expected a Timeout error from a timed-out spin, got {errors:?}",
        );
        Ok(())
    }

    /// `only_next_available_work` (spin_once) drains the currently-available work
    /// and returns promptly — it must not be ignored (loop forever) on the Tokio
    /// path. We publish then spin_once until the message is delivered.
    #[test]
    fn tokio_spin_once_processes_available_work() -> Result<(), RclrsError> {
        let mut executor = Context::default().create_tokio_executor();
        let node = executor.create_node(
            format!("test_tokio_spin_once_{}", line!()).start_parameter_services(false),
        )?;
        let qos = QoSProfile::default().reliable().keep_last(10);

        let received = Arc::new(AtomicUsize::new(0));
        let received_cb = Arc::clone(&received);
        let _sub = node.create_subscription::<msg::Empty, _>(
            "tokio_spin_once_topic".qos(qos),
            move |_m: msg::Empty| {
                received_cb.fetch_add(1, Ordering::Relaxed);
            },
        )?;
        let publisher = node.create_publisher::<msg::Empty>("tokio_spin_once_topic".qos(qos))?;

        // Each spin_once waits up to its timeout for work, drains it, and returns;
        // republish to ride out discovery. A wedged/ignored spin_once would never
        // deliver the message and this would time out at the outer deadline.
        let deadline = Instant::now() + Duration::from_secs(10);
        while received.load(Ordering::Relaxed) == 0 && Instant::now() < deadline {
            publisher.publish(msg::Empty::default())?;
            let _ = executor.spin(SpinOptions::spin_once().timeout(Duration::from_millis(200)));
        }

        assert!(
            received.load(Ordering::Relaxed) > 0,
            "spin_once never delivered the message (only_next_available_work ignored?)",
        );
        Ok(())
    }

    /// Regression for strict quiescence: once `spin()` returns, no callback may
    /// still be running. The callback signals the moment it starts (resolving the
    /// until-promise, so spinning is asked to stop *while it runs*) and then
    /// blocks for 400ms. `spin()` must not return until it has finished — proven
    /// by `completed` being set and by the elapsed time exceeding the callback's
    /// duration. Using the start-signal (rather than a fixed sleep) makes the
    /// test robust to discovery/delivery latency.
    #[test]
    fn tokio_spin_waits_for_in_flight_callback() -> Result<(), RclrsError> {
        use futures::channel::oneshot;
        use std::sync::Mutex;

        let mut executor = Context::default().create_tokio_executor();
        let node = executor.create_node(
            format!("test_tokio_quiescence_{}", line!()).start_parameter_services(false),
        )?;
        let qos = QoSProfile::default().reliable().keep_last(10);

        let count = Arc::new(AtomicUsize::new(0));
        let completed = Arc::new(AtomicBool::new(false));
        // The long blocking body runs only once "armed", so discovery stays fast.
        let armed = Arc::new(AtomicBool::new(false));
        // Sender the callback uses to announce that it has started running.
        let start_tx = Arc::new(Mutex::new(None::<oneshot::Sender<()>>));

        let (count_cb, completed_cb, armed_cb, tx_cb) = (
            Arc::clone(&count),
            Arc::clone(&completed),
            Arc::clone(&armed),
            Arc::clone(&start_tx),
        );
        let _sub = node.create_subscription::<msg::Empty, _>(
            "tokio_quiescence_topic".qos(qos),
            move |_m: msg::Empty| {
                count_cb.fetch_add(1, Ordering::Relaxed);
                if armed_cb.swap(false, Ordering::AcqRel) {
                    if let Some(tx) = tx_cb.lock().unwrap().take() {
                        let _ = tx.send(());
                    }
                    std::thread::sleep(Duration::from_millis(400));
                    completed_cb.store(true, Ordering::Release);
                }
            },
        )?;
        let publisher = node.create_publisher::<msg::Empty>("tokio_quiescence_topic".qos(qos))?;

        // Discovery: spin_once (fast callback) until a message lands.
        let deadline = Instant::now() + Duration::from_secs(10);
        while count.load(Ordering::Relaxed) == 0 && Instant::now() < deadline {
            publisher.publish(msg::Empty::default())?;
            let _ = executor.spin(SpinOptions::spin_once().timeout(Duration::from_millis(200)));
        }
        assert!(count.load(Ordering::Relaxed) > 0, "discovery never delivered");

        // Arm, then spin until the callback *starts* (the promise resolves from
        // inside it). spin() waits for the message and the callback to begin, so
        // there is no fixed-window race; the 10s timeout is just a safety net.
        let (tx, rx) = oneshot::channel();
        *start_tx.lock().unwrap() = Some(tx);
        armed.store(true, Ordering::Release);
        let halt_on_start = executor.commands().run(async move {
            let _ = rx.await;
        });

        publisher.publish(msg::Empty::default())?;
        let start = Instant::now();
        executor.spin(
            SpinOptions::default()
                .until_promise_resolved(halt_on_start)
                .timeout(Duration::from_secs(10)),
        );
        let elapsed = start.elapsed();

        assert!(
            completed.load(Ordering::Acquire),
            "spin() returned while a callback was still running (quiescence violated)",
        );
        assert!(
            elapsed >= Duration::from_millis(350),
            "spin() returned after {elapsed:?}, before the in-flight callback finished",
        );
        Ok(())
    }

    /// Regression for mailbox coalescing / no message loss: a burst of messages
    /// published while the executor is NOT spinning must all be delivered once it
    /// resumes (the per-entity `pending` accumulator preserves the count), and the
    /// entity must not wedge.
    #[test]
    fn tokio_burst_while_paused_is_delivered() -> Result<(), RclrsError> {
        const BURST: usize = 20;

        let mut executor = Context::default().create_tokio_executor();
        let node = executor.create_node(
            format!("test_tokio_burst_{}", line!()).start_parameter_services(false),
        )?;
        let qos = QoSProfile::default().reliable().keep_last(100);

        let received = Arc::new(AtomicUsize::new(0));
        let received_cb = Arc::clone(&received);
        let _sub = node.create_subscription::<msg::Empty, _>(
            "tokio_burst_topic".qos(qos),
            move |_m: msg::Empty| {
                received_cb.fetch_add(1, Ordering::Relaxed);
            },
        )?;
        let publisher = node.create_publisher::<msg::Empty>("tokio_burst_topic".qos(qos))?;

        // Discovery: get one message through so pub/sub are matched.
        let deadline = Instant::now() + Duration::from_secs(10);
        while received.load(Ordering::Relaxed) == 0 && Instant::now() < deadline {
            publisher.publish(msg::Empty::default())?;
            let _ = executor.spin(SpinOptions::spin_once().timeout(Duration::from_millis(200)));
        }
        let baseline = received.load(Ordering::Relaxed);

        // Burst while NOT spinning: these fire push callbacks that coalesce into a
        // single queued `Ready` whose accumulated count is BURST.
        for _ in 0..BURST {
            publisher.publish(msg::Empty::default())?;
        }
        std::thread::sleep(Duration::from_millis(300));

        // Resume: a wedged entity or lost count would leave us short.
        let target = baseline + BURST;
        let deadline = Instant::now() + Duration::from_secs(10);
        while received.load(Ordering::Relaxed) < target && Instant::now() < deadline {
            let _ = executor.spin(SpinOptions::spin_once().timeout(Duration::from_millis(200)));
        }

        assert!(
            received.load(Ordering::Relaxed) >= target,
            "only {} of {} messages delivered after a paused burst (coalescing lost work or wedged)",
            received.load(Ordering::Relaxed),
            target,
        );
        Ok(())
    }

    /// A panicking callback must not wedge the executor: spin() must still return
    /// (quiescence counters not leaked) and the worker must survive to run other
    /// callbacks. Without panic containment the first spin would hang forever on
    /// quiescence and this test would time out.
    #[test]
    fn tokio_panicking_callback_does_not_wedge() -> Result<(), RclrsError> {
        let mut executor = Context::default().create_tokio_executor();
        let node = executor.create_node(
            format!("test_tokio_panic_{}", line!()).start_parameter_services(false),
        )?;
        let qos = QoSProfile::default().reliable().keep_last(10);

        // A subscription whose callback always panics.
        let _panic_sub = node.create_subscription::<msg::Empty, _>(
            "tokio_panic_topic".qos(qos),
            |_m: msg::Empty| panic!("intentional test panic in a callback"),
        )?;
        let panic_pub = node.create_publisher::<msg::Empty>("tokio_panic_topic".qos(qos))?;

        // A healthy subscription on the same worker — it must still run.
        let healthy = Arc::new(AtomicUsize::new(0));
        let healthy_cb = Arc::clone(&healthy);
        let _healthy_sub = node.create_subscription::<msg::Empty, _>(
            "tokio_healthy_topic".qos(qos),
            move |_m: msg::Empty| {
                healthy_cb.fetch_add(1, Ordering::Relaxed);
            },
        )?;
        let healthy_pub = node.create_publisher::<msg::Empty>("tokio_healthy_topic".qos(qos))?;

        let deadline = Instant::now() + Duration::from_secs(10);
        while healthy.load(Ordering::Relaxed) == 0 && Instant::now() < deadline {
            // Each spin processes the panicking callback (contained) and the
            // healthy one. If quiescence leaked, spin() here would never return.
            let _ = panic_pub.publish(msg::Empty::default());
            let _ = healthy_pub.publish(msg::Empty::default());
            let _ = executor.spin(SpinOptions::spin_once().timeout(Duration::from_millis(200)));
        }

        assert!(
            healthy.load(Ordering::Relaxed) > 0,
            "a panicking callback wedged the worker or spin() quiescence",
        );
        Ok(())
    }

    /// End-to-end: a node-scoped subscription receives messages via the
    /// event-driven path (push callback -> mailbox -> worker task -> callback).
    #[test]
    fn tokio_events_pubsub() -> Result<(), RclrsError> {
        let mut executor = Context::default().create_tokio_executor();
        let node = executor.create_node(
            format!("test_tokio_events_pubsub_{}", line!()).start_parameter_services(false),
        )?;
        let qos = QoSProfile::default().reliable().keep_last(10);

        let publisher = node.create_publisher::<msg::Empty>("tokio_events_topic".qos(qos))?;
        let received = Arc::new(AtomicUsize::new(0));
        let received_cb = Arc::clone(&received);
        let _sub = node.create_subscription::<msg::Empty, _>(
            "tokio_events_topic".qos(qos),
            move |_: msg::Empty| {
                received_cb.fetch_add(1, Ordering::Relaxed);
            },
        )?;

        let deadline = Instant::now() + Duration::from_secs(10);
        while received.load(Ordering::Relaxed) == 0 && Instant::now() < deadline {
            publisher.publish(msg::Empty::default())?;
            executor.spin(SpinOptions::new().timeout(Duration::from_millis(50)));
            std::thread::sleep(Duration::from_millis(20));
        }

        assert!(
            received.load(Ordering::Relaxed) > 0,
            "subscription callback never ran via the event-driven path"
        );
        Ok(())
    }

    /// Callbacks must NOT run before spinning (deferred-execution guarantee that
    /// the spin gate provides).
    #[test]
    fn tokio_events_no_callbacks_before_spin() -> Result<(), RclrsError> {
        let mut executor = Context::default().create_tokio_executor();
        let node = executor.create_node(
            format!("test_tokio_no_early_{}", line!()).start_parameter_services(false),
        )?;
        let qos = QoSProfile::default().reliable().keep_last(10);

        let publisher = node.create_publisher::<msg::Empty>("tokio_no_early_topic".qos(qos))?;
        let received = Arc::new(AtomicUsize::new(0));
        let received_cb = Arc::clone(&received);
        let _sub = node.create_subscription::<msg::Empty, _>(
            "tokio_no_early_topic".qos(qos),
            move |_: msg::Empty| {
                received_cb.fetch_add(1, Ordering::Relaxed);
            },
        )?;

        // Publish and wait WITHOUT spinning; the callback must not run.
        for _ in 0..5 {
            publisher.publish(msg::Empty::default())?;
        }
        std::thread::sleep(Duration::from_millis(300));
        assert_eq!(
            received.load(Ordering::Relaxed),
            0,
            "callback ran before the executor was spun"
        );

        // Now spin and confirm the buffered messages are delivered.
        let deadline = Instant::now() + Duration::from_secs(10);
        while received.load(Ordering::Relaxed) == 0 && Instant::now() < deadline {
            publisher.publish(msg::Empty::default())?;
            executor.spin(SpinOptions::new().timeout(Duration::from_millis(50)));
            std::thread::sleep(Duration::from_millis(20));
        }
        assert!(received.load(Ordering::Relaxed) > 0, "callback never ran while spinning");
        Ok(())
    }

    /// A dropped subscription must stop firing callbacks (its entity is pruned).
    #[test]
    fn tokio_events_dropped_subscription_stops() -> Result<(), RclrsError> {
        let mut executor = Context::default().create_tokio_executor();
        let node = executor.create_node(
            format!("test_tokio_drop_{}", line!()).start_parameter_services(false),
        )?;
        let qos = QoSProfile::default().reliable().keep_last(10);

        let publisher = node.create_publisher::<msg::Empty>("tokio_drop_topic".qos(qos))?;
        let count = Arc::new(AtomicUsize::new(0));
        let count_cb = Arc::clone(&count);
        let sub = node.create_subscription::<msg::Empty, _>(
            "tokio_drop_topic".qos(qos),
            move |_: msg::Empty| {
                count_cb.fetch_add(1, Ordering::Relaxed);
            },
        )?;

        // Confirm the subscription is delivering.
        let deadline = Instant::now() + Duration::from_secs(10);
        while count.load(Ordering::Relaxed) == 0 && Instant::now() < deadline {
            publisher.publish(msg::Empty::default())?;
            executor.spin(SpinOptions::new().timeout(Duration::from_millis(50)));
            std::thread::sleep(Duration::from_millis(20));
        }
        assert!(count.load(Ordering::Relaxed) > 0, "subscription never delivered");

        // Drop it, then keep publishing + spinning: the callback must not fire again.
        drop(sub);
        let after_drop = count.load(Ordering::Relaxed);
        for _ in 0..10 {
            publisher.publish(msg::Empty::default())?;
            executor.spin(SpinOptions::new().timeout(Duration::from_millis(50)));
        }
        assert_eq!(
            count.load(Ordering::Relaxed),
            after_drop,
            "callback fired after the subscription was dropped"
        );
        Ok(())
    }

    /// End-to-end service round-trip driven entirely by the event-driven executor.
    #[test]
    fn tokio_events_service_roundtrip() -> Result<(), RclrsError> {
        let mut executor = Context::default().create_tokio_executor();
        let node = executor.create_node(
            format!("test_tokio_events_service_{}", line!()).start_parameter_services(false),
        )?;

        let _service = node.create_service::<test_msgs::srv::Empty, _>(
            "tokio_events_service",
            |_request: test_msgs::srv::Empty_Request| test_msgs::srv::Empty_Response::default(),
        )?;
        let client = node.create_client::<test_msgs::srv::Empty>("tokio_events_service")?;

        let deadline = Instant::now() + Duration::from_secs(10);
        while !client.service_is_ready()? {
            assert!(Instant::now() < deadline, "service never became ready");
            std::thread::sleep(Duration::from_millis(20));
        }

        let response: Promise<test_msgs::srv::Empty_Response> =
            client.call(test_msgs::srv::Empty_Request::default())?;
        let (mut response, notice) = executor.commands().create_notice(response);
        executor.spin(
            SpinOptions::new()
                .until_promise_resolved(notice)
                .timeout(Duration::from_secs(5)),
        );

        assert!(
            response.try_recv().ok().flatten().is_some(),
            "client never received the service response via the event-driven path"
        );
        Ok(())
    }

    /// Timers fire on the event-driven executor.
    #[test]
    fn tokio_events_timer_fires() -> Result<(), RclrsError> {
        let mut executor = Context::default().create_tokio_executor();
        let node = executor.create_node(
            format!("test_tokio_events_timer_{}", line!()).start_parameter_services(false),
        )?;

        let count = Arc::new(AtomicUsize::new(0));
        let count_cb = Arc::clone(&count);
        let _timer = node.create_timer_repeating(Duration::from_millis(10), move || {
            count_cb.fetch_add(1, Ordering::Relaxed);
        })?;

        executor.spin(SpinOptions::new().timeout(Duration::from_millis(300)));

        let fired = count.load(Ordering::Relaxed);
        assert!(
            fired >= 3,
            "timer fired only {fired} times in ~300ms (expected several)"
        );
        Ok(())
    }

    /// Worker-scoped subscription + `listen_until` activity listener on the
    /// event-driven executor.
    #[test]
    fn tokio_events_worker() -> Result<(), RclrsError> {
        let mut executor = Context::default().create_tokio_executor();
        let node = executor.create_node(
            format!("test_tokio_worker_{}", line!()).start_parameter_services(false),
        )?;

        let worker = node.create_worker::<usize>(0);
        let _sub = worker.create_subscription(
            "tokio_worker_topic",
            |payload: &mut usize, _msg: msg::Empty| {
                *payload += 1;
            },
        )?;
        let promise = worker.listen_until(|payload: &mut usize| (*payload > 0).then_some(*payload));

        let publisher = node.create_publisher::<msg::Empty>("tokio_worker_topic")?;
        let stop = Arc::new(AtomicBool::new(false));
        let stop_pub = Arc::clone(&stop);
        let pub_thread = std::thread::spawn(move || {
            while !stop_pub.load(Ordering::Acquire) {
                let _ = publisher.publish(msg::Empty::default());
                std::thread::sleep(Duration::from_millis(10));
            }
        });

        let (mut promise, notice) = executor.commands().create_notice(promise);
        executor.spin(
            SpinOptions::new()
                .until_promise_resolved(notice)
                .timeout(Duration::from_secs(5)),
        );
        stop.store(true, Ordering::Release);
        pub_thread.join().unwrap();

        assert!(
            promise.try_recv().ok().flatten().is_some(),
            "worker subscription / activity listener never fired on the event-driven executor"
        );
        Ok(())
    }

    /// A node with parameter services enabled drives cleanly on the executor.
    #[test]
    fn tokio_events_node_with_parameter_services() -> Result<(), RclrsError> {
        let mut executor = Context::default().create_tokio_executor();
        let _node = executor.create_node(&format!("test_tokio_paramsvc_{}", line!()))?;
        let errors = executor.spin(SpinOptions::new().timeout(Duration::from_millis(200)));
        // A bare-timeout spin reports a `Timeout` error (matching the basic
        // executor); assert nothing *other* than that was produced.
        assert!(
            errors.iter().all(|e| matches!(
                e,
                RclrsError::RclError {
                    code: RclReturnCode::Timeout,
                    ..
                }
            )),
            "spinning a node with parameter services produced unexpected errors: {errors:?}"
        );
        Ok(())
    }

    /// Async tasks run on the Tokio runtime (would panic on the basic executor).
    #[test]
    fn tokio_async_task_runs() {
        let mut executor = Context::default().create_tokio_executor();
        let _node = executor
            .create_node(&format!("test_tokio_async_task_{}", line!()))
            .unwrap();

        let done = Arc::new(AtomicBool::new(false));
        let done_clone = Arc::clone(&done);

        let promise = executor.commands().run(async move {
            tokio::time::sleep(Duration::from_millis(1)).await;
            done_clone.store(true, Ordering::Release);
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

        assert!(done.load(Ordering::Acquire));
    }
}
