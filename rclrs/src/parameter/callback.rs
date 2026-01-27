use std::fmt::Debug;
use std::sync::{Arc, Mutex, Weak};

use tokio::sync::watch;

use crate::ParameterValue;

/// A subscription to parameter changes.
///
/// This allows async code to be notified when a parameter value changes.
/// Obtain this by calling [`MandatoryParameter::subscribe()`][1] or
/// [`OptionalParameter::subscribe()`][2].
///
/// [1]: crate::MandatoryParameter::subscribe
/// [2]: crate::OptionalParameter::subscribe
pub struct ParameterSubscription<T> {
    rx: watch::Receiver<()>,
    get_value: Arc<dyn Fn() -> T + Send + Sync>,
}

impl<T: Clone> ParameterSubscription<T> {
    /// Create a new parameter subscription.
    pub(crate) fn new(
        rx: watch::Receiver<()>,
        get_value: Arc<dyn Fn() -> T + Send + Sync>,
    ) -> Self {
        Self { rx, get_value }
    }

    /// Wait for the next parameter change.
    ///
    /// Returns `Some(new_value)` when the parameter changes, or `None` if
    /// the parameter has been dropped.
    ///
    /// # Example
    ///
    /// ```ignore
    /// let mut subscription = param.subscribe();
    /// while let Some(new_value) = subscription.changed().await {
    ///     println!("Parameter changed to: {:?}", new_value);
    /// }
    /// ```
    pub async fn changed(&mut self) -> Option<T> {
        self.rx.changed().await.ok()?;
        Some((self.get_value)())
    }

    /// Get the current value without waiting for a change.
    pub fn get(&self) -> T {
        (self.get_value)()
    }
}

/// Handle for a node-level parameter callback.
///
/// The callback is automatically unregistered when this handle is dropped.
/// Keep this handle alive for as long as you want the callback to remain active.
///
/// # Example
///
/// ```ignore
/// let handle = node.add_on_set_parameters_callback(|name, old, new| {
///     println!("Parameter {} changed", name);
///     Ok(())
/// });
/// // Callback is active while `handle` is in scope
/// drop(handle); // Callback is now unregistered
/// ```
pub struct ParameterCallbackHandle {
    id: usize,
    registry: Weak<Mutex<CallbackRegistry>>,
}

impl Drop for ParameterCallbackHandle {
    fn drop(&mut self) {
        if let Some(registry) = self.registry.upgrade() {
            if let Ok(mut registry) = registry.lock() {
                registry.remove(self.id);
            }
        }
    }
}

/// Callback type for validating parameter changes at the node level.
///
/// The callback receives:
/// - `name`: The parameter name
/// - `old_value`: The current value before the change
/// - `new_value`: The proposed new value
///
/// Returns `Ok(())` to accept the change, or `Err(reason)` to reject it.
pub type NodeParameterValidator =
    Box<dyn Fn(&str, &ParameterValue, &ParameterValue) -> Result<(), String> + Send + Sync>;

/// Callback type for reacting to parameter changes at the node level.
///
/// This callback is invoked after a parameter change has been successfully applied.
///
/// The callback receives:
/// - `name`: The parameter name
/// - `old_value`: The previous value
/// - `new_value`: The new value that was just set
pub type NodeParameterChangeCallback =
    Box<dyn Fn(&str, &ParameterValue, &ParameterValue) + Send + Sync>;

/// Registry for node-level parameter callbacks.
///
/// This is used internally to manage validator and post-change callbacks
/// registered at the node level.
pub(crate) struct CallbackRegistry {
    next_id: usize,
    validators: Vec<(usize, NodeParameterValidator)>,
    post_change: Vec<(usize, NodeParameterChangeCallback)>,
}

impl Debug for CallbackRegistry {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("CallbackRegistry")
            .field("next_id", &self.next_id)
            .field("validators_count", &self.validators.len())
            .field("post_change_count", &self.post_change.len())
            .finish()
    }
}

impl Default for CallbackRegistry {
    fn default() -> Self {
        Self::new()
    }
}

impl CallbackRegistry {
    /// Create a new empty callback registry.
    pub(crate) fn new() -> Self {
        Self {
            next_id: 0,
            validators: Vec::new(),
            post_change: Vec::new(),
        }
    }

    /// Add a validator callback. Returns the callback ID.
    pub(crate) fn add_validator(&mut self, callback: NodeParameterValidator) -> usize {
        let id = self.next_id;
        self.next_id += 1;
        self.validators.push((id, callback));
        id
    }

    /// Add a post-change callback. Returns the callback ID.
    pub(crate) fn add_post_change(&mut self, callback: NodeParameterChangeCallback) -> usize {
        let id = self.next_id;
        self.next_id += 1;
        self.post_change.push((id, callback));
        id
    }

    /// Remove a callback by ID.
    pub(crate) fn remove(&mut self, id: usize) {
        self.validators.retain(|(i, _)| *i != id);
        self.post_change.retain(|(i, _)| *i != id);
    }

    /// Run all validator callbacks. Returns an error if any validator rejects.
    pub(crate) fn run_validators(
        &self,
        name: &str,
        old_value: &ParameterValue,
        new_value: &ParameterValue,
    ) -> Result<(), String> {
        for (_, validator) in &self.validators {
            validator(name, old_value, new_value)?;
        }
        Ok(())
    }

    /// Notify all post-change callbacks.
    pub(crate) fn notify_change(
        &self,
        name: &str,
        old_value: &ParameterValue,
        new_value: &ParameterValue,
    ) {
        for (_, callback) in &self.post_change {
            callback(name, old_value, new_value);
        }
    }

    /// Create a handle for a callback with the given ID.
    pub(crate) fn create_handle(
        registry: &Arc<Mutex<CallbackRegistry>>,
        id: usize,
    ) -> ParameterCallbackHandle {
        ParameterCallbackHandle {
            id,
            registry: Arc::downgrade(registry),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicBool, Ordering};

    #[test]
    fn test_callback_registry_validators() {
        let mut registry = CallbackRegistry::new();

        let called = Arc::new(AtomicBool::new(false));
        let called_clone = called.clone();

        let id = registry.add_validator(Box::new(move |name, _old, _new| {
            called_clone.store(true, Ordering::SeqCst);
            if name == "reject_me" {
                Err("Rejected!".into())
            } else {
                Ok(())
            }
        }));

        // Test successful validation
        let old = ParameterValue::Bool(false);
        let new = ParameterValue::Bool(true);
        assert!(registry.run_validators("allowed", &old, &new).is_ok());
        assert!(called.load(Ordering::SeqCst));

        // Test rejected validation
        called.store(false, Ordering::SeqCst);
        let result = registry.run_validators("reject_me", &old, &new);
        assert!(result.is_err());
        assert_eq!(result.unwrap_err(), "Rejected!");

        // Test removal
        registry.remove(id);
        called.store(false, Ordering::SeqCst);
        assert!(registry.run_validators("reject_me", &old, &new).is_ok());
        assert!(!called.load(Ordering::SeqCst)); // Callback was removed
    }

    #[test]
    fn test_callback_registry_post_change() {
        let mut registry = CallbackRegistry::new();

        let called = Arc::new(AtomicBool::new(false));
        let called_clone = called.clone();

        registry.add_post_change(Box::new(move |_name, _old, _new| {
            called_clone.store(true, Ordering::SeqCst);
        }));

        let old = ParameterValue::Integer(1);
        let new = ParameterValue::Integer(2);
        registry.notify_change("test", &old, &new);
        assert!(called.load(Ordering::SeqCst));
    }

    #[test]
    fn test_callback_handle_drop() {
        let registry = Arc::new(Mutex::new(CallbackRegistry::new()));
        let called = Arc::new(AtomicBool::new(false));
        let called_clone = called.clone();

        let id = registry.lock().unwrap().add_validator(Box::new(move |_, _, _| {
            called_clone.store(true, Ordering::SeqCst);
            Ok(())
        }));

        let handle = CallbackRegistry::create_handle(&registry, id);

        // Callback should work
        let old = ParameterValue::Bool(false);
        let new = ParameterValue::Bool(true);
        assert!(registry.lock().unwrap().run_validators("test", &old, &new).is_ok());
        assert!(called.load(Ordering::SeqCst));

        // Drop handle - callback should be removed
        drop(handle);
        called.store(false, Ordering::SeqCst);
        assert!(registry.lock().unwrap().run_validators("test", &old, &new).is_ok());
        assert!(!called.load(Ordering::SeqCst)); // Not called because removed
    }
}
