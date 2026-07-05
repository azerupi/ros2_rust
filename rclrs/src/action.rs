use std::{collections::HashMap, ops::Deref};

pub(crate) mod action_client;
pub use action_client::*;

pub(crate) mod action_goal_receiver;
pub use action_goal_receiver::*;

pub(crate) mod action_server;
pub use action_server::*;

use crate::{log_error, rcl_bindings::*, DropGuard};
use ros_env::builtin_interfaces::msg::Time;
use std::fmt;

#[cfg(feature = "serde")]
use serde::{Deserialize, Serialize};

/// A unique identifier for a goal request.
#[cfg_attr(feature = "serde", derive(Deserialize, Serialize))]
#[cfg_attr(feature = "serde", serde(transparent))]
#[derive(Copy, Clone, Debug, Default, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct GoalUuid(pub [u8; RCL_ACTION_UUID_SIZE]);

impl GoalUuid {
    /// A zeroed-out goal ID has a special meaning for cancellation requests
    /// which indicates that no specific goal is being requested.
    fn zero() -> Self {
        Self([0; RCL_ACTION_UUID_SIZE])
    }
}

impl fmt::Display for GoalUuid {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> Result<(), fmt::Error> {
        write!(f, "{:02x}{:02x}{:02x}{:02x}-{:02x}{:02x}-{:02x}{:02x}-{:02x}{:02x}-{:02x}{:02x}{:02x}{:02x}{:02x}{:02x}",
               self.0[0],
               self.0[1],
               self.0[2],
               self.0[3],
               self.0[4],
               self.0[5],
               self.0[6],
               self.0[7],
               self.0[8],
               self.0[9],
               self.0[10],
               self.0[11],
               self.0[12],
               self.0[13],
               self.0[14],
               self.0[15],
               )
    }
}

impl Deref for GoalUuid {
    type Target = [u8; RCL_ACTION_UUID_SIZE];

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl From<[u8; RCL_ACTION_UUID_SIZE]> for GoalUuid {
    fn from(value: [u8; RCL_ACTION_UUID_SIZE]) -> Self {
        Self(value)
    }
}

impl From<&[u8; RCL_ACTION_UUID_SIZE]> for GoalUuid {
    fn from(value: &[u8; RCL_ACTION_UUID_SIZE]) -> Self {
        Self(*value)
    }
}

/// The response returned by an [`ActionServer`]'s cancel callback when a goal is requested to be cancelled.
#[cfg_attr(feature = "serde", derive(Deserialize, Serialize))]
#[cfg_attr(feature = "serde", serde(rename = "snake_case"))]
#[repr(i8)]
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub enum CancelResponseCode {
    /// The server will try to cancel the goal.
    Accept = 0,
    /// The server will not try to cancel the goal.
    Reject = 1,
    /// The requested goal is unknown.
    UnknownGoal = 2,
    /// The goal already reached a terminal state.
    GoalTerminated = 3,
}

impl CancelResponseCode {
    /// Check if the cancellation was accepted.
    pub fn is_accepted(&self) -> bool {
        matches!(self, Self::Accept)
    }

    /// Check if the cancellation was rejected.
    pub fn is_rejected(&self) -> bool {
        matches!(self, Self::Reject)
    }
}

impl From<i8> for CancelResponseCode {
    fn from(value: i8) -> Self {
        if 0 <= value && value <= 3 {
            unsafe {
                // SAFETY: We have already ensured that the integer value is
                // within the acceptable range for the enum, so transmuting is
                // safe.
                return std::mem::transmute(value);
            }
        }

        log_error!(
            "cancel_response.from",
            "Invalid integer value being cast to a cancel response: {value}. \
            Values should be in the range [0, 3]. We will set this as 1 (Reject).",
        );
        CancelResponseCode::Reject
    }
}

/// This is returned by [`CancellationClient`] to inform whether a cancellation
/// of a single goal was successful.
///
/// When a cancellation request might cancel multiple goals, [`MultiCancelResponse`]
/// will be used.
#[cfg_attr(feature = "serde", derive(Deserialize, Serialize))]
#[derive(Debug, Clone, PartialEq, PartialOrd)]
pub struct CancelResponse {
    /// What kind of response was given.
    pub code: CancelResponseCode,
    /// What time the response took effect according to the action server.
    /// This will be default-initialized if no goal was cancelled.
    pub stamp: Option<Time>,
}

impl CancelResponse {
    /// Check whether the request was accepted.
    pub fn is_accepted(&self) -> bool {
        self.code.is_accepted()
    }

    /// Check whether the request was rejected.
    pub fn is_rejected(&self) -> bool {
        self.code.is_rejected()
    }
}

/// This is returned by [`ActionClientState::cancel_all_goals`] and
/// [`ActionClientState::cancel_goals_prior_to`].
#[derive(Debug, Clone, PartialEq)]
pub struct MultiCancelResponse {
    /// What kind of response was given.
    pub code: CancelResponseCode,
    /// The time stamp that the response took effect for each goal that is being
    /// cancelled. If the request was not accepted then this may be empty.
    pub stamps: HashMap<GoalUuid, Time>,
}

impl MultiCancelResponse {
    /// Check whether the request was accepted.
    pub fn is_accepted(&self) -> bool {
        self.code.is_accepted()
    }

    /// Check whether the request was rejected.
    pub fn is_rejected(&self) -> bool {
        self.code.is_rejected()
    }
}

/// Values defined by `action_msgs/msg/GoalStatus`
#[cfg_attr(feature = "serde", derive(Deserialize, Serialize))]
#[cfg_attr(feature = "serde", serde(rename = "snake_case"))]
#[repr(i8)]
#[derive(Debug, Clone, Copy, Hash, PartialEq, Eq, PartialOrd, Ord)]
pub enum GoalStatusCode {
    /// The goal status has never been initialized. This likely means it has not
    /// yet been accepted.
    Unknown = 0,
    /// The goal was accepted by the action server.
    Accepted = 1,
    /// The goal is being executed by the action server.
    Executing = 2,
    /// The action server has accepting cancelling the goal and is in the process
    /// of cancelling it.
    Cancelling = 3,
    /// The action server has successfully reached the goal.
    Succeeded = 4,
    /// The action server has finished cancelling the goal.
    Cancelled = 5,
    /// The action server has aborted the goal. This suggests an error happened
    /// during execution or cancelling.
    Aborted = 6,
}

impl GoalStatusCode {
    /// Check if the status belongs to one of the terminated modes
    pub fn is_terminated(&self) -> bool {
        matches!(self, Self::Succeeded | Self::Cancelled | Self::Aborted)
    }
}

impl From<i8> for GoalStatusCode {
    fn from(value: i8) -> Self {
        if 0 <= value && value <= 6 {
            unsafe {
                // SAFETY: We have already ensured that the integer value is
                // within the acceptable range for the enum, so transmuting is
                // safe.
                return std::mem::transmute(value);
            }
        }

        log_error!(
            "goal_status_code.from",
            "Invalid integer value being cast to a goal status code: {value}. \
            Values should be in the range [0, 6]. We will set this as 0 (Unknown).",
        );
        GoalStatusCode::Unknown
    }
}

/// A status update for a goal. Includes the status code, the goal uuid, and the
/// timestamp of when the status was set by the action server.
#[cfg_attr(feature = "serde", derive(Deserialize, Serialize))]
#[derive(Debug, Clone, PartialEq, PartialOrd)]
pub struct GoalStatus {
    /// The status code describing what status was set by the action server.
    pub code: GoalStatusCode,
    /// The uuid of the goal whose status was updated.
    pub goal_id: GoalUuid,
    /// Time that the status was set by the action server. The time measured by
    /// the action server might not align with the time measured by the action
    /// client, so care should be taken when using this time value.
    pub stamp: Time,
}

fn empty_goal_status_array() -> DropGuard<rcl_action_goal_status_array_t> {
    DropGuard::new(
        unsafe {
            // SAFETY: No preconditions
            let mut array = rcl_action_get_zero_initialized_goal_status_array();
            array.allocator = rcutils_get_default_allocator();
            array
        },
        |mut goal_statuses| unsafe {
            // SAFETY: The goal_status array is either zero-initialized and empty or populated by
            // `rcl_action_get_goal_status_array`. In either case, it can be safely finalized.
            rcl_action_goal_status_array_fini(&mut goal_statuses);
        },
    )
}

#[cfg(test)]
mod tests {
    use crate::{test_helpers::test_with_executors, *};
    use futures::StreamExt;
    use ros_env::example_interfaces::action::{
        Fibonacci, Fibonacci_Feedback, Fibonacci_Goal, Fibonacci_Result,
    };
    use std::{
        sync::{
            atomic::{AtomicBool, Ordering},
            Arc,
        },
        time::Duration,
    };
    use tokio::sync::mpsc::unbounded_channel;

    test_with_executors! {
        fn test_action_server_availability(executor, node_name) {
            let node = executor
                .create_node(node_name)
                .unwrap();
            let action_name = format!("{node_name}_action");

            let client = node
                .create_action_client::<Fibonacci>(&action_name)
                .unwrap();

            assert!(!client.server_is_available().unwrap());

            let _action_server = node
                .create_action_server(&action_name, |handle| {
                    fibonacci_action(handle, TestActionSettings::default())
                })
                .unwrap();

            let done = Arc::new(AtomicBool::new(false));
            let done_cb = Arc::clone(&done);
            let promise = executor.commands().run(async move {
                let timeout = Duration::from_secs(1);
                let start = std::time::Instant::now();
                let mut is_available = false;

                while start.elapsed() < timeout {
                    if client.server_is_available().unwrap() {
                        is_available = true;
                        break;
                    }
                    tokio::time::sleep(Duration::from_millis(50)).await;
                }

                assert!(
                    is_available,
                    "Server is not available after {} seconds",
                    timeout.as_secs()
                );
                done_cb.store(true, Ordering::Relaxed);
            });

            // A timeout keeps the multi-threaded Tokio executor from blocking forever
            // if the promise never resolves; the `done` flag turns a timeout into a
            // failure rather than a silent pass (an assertion that panics inside the
            // async task on Tokio aborts the task instead of failing the test).
            executor.spin(
                SpinOptions::default()
                    .until_promise_resolved(promise)
                    .timeout(Duration::from_secs(10)),
            );
            assert!(
                done.load(Ordering::Relaxed),
                "server availability check did not complete",
            );
        }
    }

    test_with_executors! {
        fn test_action_success_streaming(executor, node_name) {
            let node = executor
                .create_node(node_name)
                .unwrap();
            let action_name = format!("{node_name}_action");
            let _action_server = node
                .create_action_server(&action_name, |handle| {
                    fibonacci_action(handle, TestActionSettings::default())
                })
                .unwrap();

            let client = node
                .create_action_client::<Fibonacci>(&action_name)
                .unwrap();

            let order_10_sequence = [1, 1, 2, 3, 5, 8, 13, 21, 34, 55];

            let request = client.request_goal(Fibonacci_Goal { order: 10 });

            let streamed = Arc::new(AtomicBool::new(false));
            let streamed_cb = Arc::clone(&streamed);
            let promise = executor.commands().run(async move {
                let mut goal_client_stream = request.await.unwrap().stream();
                let mut expected_feedback_len = 0;
                while let Some(event) = goal_client_stream.next().await {
                    match event {
                        GoalEvent::Feedback(feedback) => {
                            expected_feedback_len += 1;
                            assert_eq!(feedback.sequence.len(), expected_feedback_len);
                        }
                        GoalEvent::Status(s) => {
                            assert!(
                                matches!(
                                    s.code,
                                    GoalStatusCode::Unknown
                                        | GoalStatusCode::Executing
                                        | GoalStatusCode::Succeeded
                                ),
                                "Actual code: {:?}",
                                s.code,
                            );
                        }
                        GoalEvent::Result((status, result)) => {
                            assert_eq!(status, GoalStatusCode::Succeeded);
                            assert_eq!(result.sequence, order_10_sequence);
                            streamed_cb.store(true, Ordering::Relaxed);
                            return;
                        }
                    }
                }
            });

            // Timeout + completion flag so the multi-threaded Tokio executor cannot
            // hang, and a timeout fails the test instead of passing silently.
            executor.spin(
                SpinOptions::default()
                    .until_promise_resolved(promise)
                    .timeout(Duration::from_secs(10)),
            );
            assert!(
                streamed.load(Ordering::Relaxed),
                "action goal streaming round-trip did not complete",
            );

            let request = client.request_goal(Fibonacci_Goal { order: 10 });

            let got_result = Arc::new(AtomicBool::new(false));
            let got_result_cb = Arc::clone(&got_result);
            let promise = executor.commands().run(async move {
                let (status, result) = request.await.unwrap().result.await;
                assert_eq!(status, GoalStatusCode::Succeeded);
                assert_eq!(result.sequence, order_10_sequence);
                got_result_cb.store(true, Ordering::Relaxed);
            });

            executor.spin(
                SpinOptions::default()
                    .until_promise_resolved(promise)
                    .timeout(Duration::from_secs(10)),
            );
            assert!(
                got_result.load(Ordering::Relaxed),
                "action goal result round-trip did not complete",
            );
        }
    }

    test_with_executors! {
        fn test_action_cancel(executor, node_name) {
            let node = executor
                .create_node(node_name)
                .unwrap();
            let action_name = format!("{node_name}_action");
            let _action_server = node
                .create_action_server(&action_name, |handle| {
                    fibonacci_action(handle, TestActionSettings::slow())
                })
                .unwrap();

            let client = node
                .create_action_client::<Fibonacci>(&action_name)
                .unwrap();

            let request = client.request_goal(Fibonacci_Goal { order: 10 });

            let done = Arc::new(AtomicBool::new(false));
            let done_cb = Arc::clone(&done);
            let promise = executor.commands().run(async move {
                let goal_client = request.await.unwrap();
                let cancellation = goal_client.cancellation.cancel().await;
                assert!(cancellation.is_accepted());
                let (status, _) = goal_client.result.await;
                assert_eq!(status, GoalStatusCode::Cancelled);
                done_cb.store(true, Ordering::Relaxed);
            });

            executor.spin(
                SpinOptions::default()
                    .until_promise_resolved(promise)
                    .timeout(Duration::from_secs(10)),
            );
            assert!(
                done.load(Ordering::Relaxed),
                "action cancellation did not complete",
            );
        }
    }

    test_with_executors! {
        fn test_action_cancel_rejection(executor, node_name) {
            let node = executor
                .create_node(node_name)
                .unwrap();
            let action_name = format!("{node_name}_action");
            let _action_server = node
                .create_action_server(&action_name, |handle| {
                    // This action server will intentionally reject 3 cancellation requests
                    fibonacci_action(handle, TestActionSettings::slow().cancel_refusal(3))
                })
                .unwrap();

            let client = node
                .create_action_client::<Fibonacci>(&action_name)
                .unwrap();

            let request = client.request_goal(Fibonacci_Goal { order: 10 });

            let done = Arc::new(AtomicBool::new(false));
            let done_cb = Arc::clone(&done);
            let promise = executor.commands().run(async move {
                let goal_client = request.await.unwrap();

                // The first three cancellation requests should be rejected
                for _ in 0..3 {
                    let cancellation = goal_client.cancellation.cancel().await;
                    assert!(cancellation.is_rejected());
                }

                // The next cancellation request should be accepted
                let cancellation = goal_client.cancellation.cancel().await;
                assert!(cancellation.is_accepted());

                // The next one should also be accepted or we get notified that the
                // goal no longer exists.
                let late_cancellation = goal_client.cancellation.cancel().await;
                assert!(matches!(
                    late_cancellation.code,
                    CancelResponseCode::Accept | CancelResponseCode::GoalTerminated
                ));

                let (status, _) = goal_client.result.await;
                assert_eq!(status, GoalStatusCode::Cancelled);

                // After we have received the response, we can be confident that the
                // action server will report back that the goal was terminated.
                let very_late_cancellation = goal_client.cancellation.cancel().await;
                assert!(matches!(
                    very_late_cancellation.code,
                    CancelResponseCode::GoalTerminated
                ));
                done_cb.store(true, Ordering::Relaxed);
            });

            executor.spin(
                SpinOptions::default()
                    .until_promise_resolved(promise)
                    .timeout(Duration::from_secs(10)),
            );
            assert!(
                done.load(Ordering::Relaxed),
                "action cancel-rejection sequence did not complete",
            );
        }
    }

    test_with_executors! {
        fn test_action_slow_cancel(executor, node_name) {
            let node = executor
                .create_node(node_name)
                .unwrap();
            let action_name = format!("{node_name}_action");
            let _action_server = node
                .create_action_server(&action_name, |handle| {
                    // This action server will intentionally reject 3 cancellation requests
                    fibonacci_action(
                        handle,
                        TestActionSettings::slow()
                            .cancel_refusal(3)
                            .continue_after_cancelling(),
                    )
                })
                .unwrap();

            let client = node
                .create_action_client::<Fibonacci>(&action_name)
                .unwrap();

            let request = client.request_goal(Fibonacci_Goal { order: 10 });

            let done = Arc::new(AtomicBool::new(false));
            let done_cb = Arc::clone(&done);
            let promise = executor.commands().run(async move {
                let goal_client = request.await.unwrap();

                // The first three cancellation requests should be rejected
                for _ in 0..3 {
                    let cancellation = goal_client.cancellation.cancel().await;
                    assert!(cancellation.is_rejected());
                }

                // The next cancellation request should be accepted
                let cancellation = goal_client.cancellation.cancel().await;
                assert!(cancellation.is_accepted());

                // The next one should also be accepted or we get notified that the
                // goal no longer exists.
                let late_cancellation = goal_client.cancellation.cancel().await;
                assert!(late_cancellation.is_accepted());

                let very_late_cancellation = goal_client.cancellation.cancel().await;
                assert!(very_late_cancellation.is_accepted());
                done_cb.store(true, Ordering::Relaxed);
            });

            executor.spin(
                SpinOptions::default()
                    .until_promise_resolved(promise)
                    .timeout(Duration::from_secs(10)),
            );
            assert!(
                done.load(Ordering::Relaxed),
                "action slow-cancel sequence did not complete",
            );
        }
    }

    async fn fibonacci_action(
        handle: RequestedGoal<Fibonacci>,
        TestActionSettings {
            period,
            cancel_refusal_limit,
            continue_after_cancelling,
        }: TestActionSettings,
    ) -> TerminatedGoal {
        let goal_order = handle.goal().order;
        if goal_order < 0 {
            return handle.reject();
        }

        let mut result = Fibonacci_Result::default();

        let executing = match handle.accept().begin() {
            BeginAcceptedGoal::Execute(executing) => executing,
            BeginAcceptedGoal::Cancel(cancelling) => {
                return cancelling.cancelled_with(result);
            }
        };

        let (sender, mut receiver) = unbounded_channel();
        std::thread::spawn(move || {
            let mut previous = 0;
            let mut current = 1;

            for _ in 0..goal_order {
                if let Err(_) = sender.send(current) {
                    // The action has been cancelled early, so just drop this thread.
                    return;
                }

                let next = previous + current;
                previous = current;
                current = next;
                std::thread::sleep(period);
            }
        });

        let mut sequence = Vec::new();
        let mut cancel_requests = 0;
        let cancelling = loop {
            match executing.unless_cancel_requested(receiver.recv()).await {
                Ok(Some(next)) => {
                    // We have a new item in the sequence
                    sequence.push(next);
                    executing.publish_feedback(Fibonacci_Feedback {
                        sequence: sequence.clone(),
                    });
                }
                Ok(None) => {
                    // The sequence has finished
                    result.sequence = sequence;
                    return executing.succeeded_with(result);
                }
                Err(_) => {
                    // The user has asked for the action to be cancelled
                    cancel_requests += 1;
                    if cancel_requests > cancel_refusal_limit {
                        let cancelling = executing.begin_cancelling();
                        if !continue_after_cancelling {
                            result.sequence = sequence;
                            return cancelling.cancelled_with(result);
                        }

                        break cancelling;
                    }

                    // We have not yet reached the number of cancel requests that
                    // we intend to reject. Reject this cancellation and wait for
                    // the next one.
                    executing.reject_cancellation();
                }
            }
        };

        // We will continue to iterate to the finish even though we are in the
        // cancelling mode. We only do this as a way of running tests on a
        // prolonged action cancelling state.
        loop {
            match receiver.recv().await {
                Some(next) => {
                    sequence.push(next);
                    cancelling.publish_feedback(Fibonacci_Feedback {
                        sequence: sequence.clone(),
                    });
                }
                None => {
                    // The sequence has finished
                    result.sequence = sequence;
                    return cancelling.succeeded_with(result);
                }
            }
        }
    }

    struct TestActionSettings {
        period: Duration,
        cancel_refusal_limit: usize,
        continue_after_cancelling: bool,
    }

    impl Default for TestActionSettings {
        fn default() -> Self {
            TestActionSettings {
                period: Duration::from_micros(10),
                cancel_refusal_limit: 0,
                continue_after_cancelling: false,
            }
        }
    }

    impl TestActionSettings {
        fn slow() -> Self {
            TestActionSettings {
                period: Duration::from_secs(1),
                ..Default::default()
            }
        }

        fn cancel_refusal(mut self, limit: usize) -> Self {
            self.cancel_refusal_limit = limit;
            self
        }

        fn continue_after_cancelling(mut self) -> Self {
            self.continue_after_cancelling = true;
            self
        }
    }
}
