use std::sync::{Arc, Mutex, Weak};

use crate::{log_fatal, GuardCondition};

/// Tracks all guard conditions across workers in an executor so they can all be
/// triggered at once (e.g. to wake wait sets when halting).
///
/// This is shared between executor implementations.
#[derive(Clone, Default)]
pub(crate) struct AllGuardConditions {
    inner: Arc<Mutex<Vec<Weak<GuardCondition>>>>,
}

impl AllGuardConditions {
    pub(crate) fn trigger(&self) {
        self.inner.lock().unwrap().retain(|guard_condition| {
            if let Some(guard_condition) = guard_condition.upgrade() {
                if let Err(err) = guard_condition.trigger() {
                    log_fatal!(
                        "rclrs.executor",
                        "Failed to trigger a guard condition. This should never happen. \
                        Please report this to the rclrs maintainers with a minimal reproducible example. \
                        Error: {err}",
                    );
                }
                true
            } else {
                false
            }
        });
    }

    pub(crate) fn push(&self, guard_condition: Weak<GuardCondition>) {
        let mut inner = self.inner.lock().unwrap();
        if inner
            .iter()
            .find(|other| guard_condition.ptr_eq(other))
            .is_some()
        {
            // This guard condition is already known
            return;
        }

        inner.push(guard_condition);
    }
}
