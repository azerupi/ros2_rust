// #[cfg(test)]

pub(crate) mod graph_helpers;
pub(crate) use self::graph_helpers::*;

use crate::{Executor, SpinOptions};
use std::{
    sync::atomic::{AtomicUsize, Ordering},
    time::{Duration, Instant},
};

pub(crate) fn assert_send<T: Send>() {}
pub(crate) fn assert_sync<T: Sync>() {}

/// Spins the executor until a condition returns true or timeout is reached.
/// Returns true if condition was met, false if timed out.
pub(crate) fn spin_until_condition<F>(
    executor: &mut Executor,
    mut condition: F,
    timeout: Duration,
) -> bool
where
    F: FnMut() -> bool,
{
    let start = Instant::now();
    while !condition() {
        if start.elapsed() >= timeout {
            return false;
        }
        executor.spin(SpinOptions::spin_once().timeout(Duration::from_millis(10)));
    }
    true
}

/// Hands out a distinct ROS domain id to each executor created by
/// [`test_with_executors!`].
///
/// Cargo runs tests in parallel within a single process on the default DDS
/// domain, so two variants generated from one body (`_basic` and `_tokio`) would
/// otherwise share topics/services/`/rosout` and cross-talk. This is harmless for
/// tests that check for presence but it is a problem for tests that assert on topic counts
/// Giving every macro-created executor its own domain isolates the variants from each
/// other (and from unrelated tests) without touching any test body.
pub(crate) fn next_test_domain_id() -> usize {
    // Stay clear of domain 0 (the ambient default used by doctests / external
    // processes) and within the conservative RTPS domain range.
    const BASE: usize = 1;
    const SPAN: usize = 100;
    static NEXT: AtomicUsize = AtomicUsize::new(0);
    BASE + NEXT.fetch_add(1, Ordering::Relaxed) % SPAN
}

/// Generate a `_basic` and (feature-gated) `_tokio` `#[test]` from one body, so
/// the same test exercises every executor runtime: a `_basic` variant on the
/// [`BasicExecutorRuntime`] and, when the `tokio-executor` feature is enabled, a
/// `_tokio` variant on the [`TokioExecutorRuntime`]. The body is handed a freshly
/// created `mut <name>: Executor` (on its own ROS domain) for the runtime under
/// test, so any behavioral divergence shows up as a failing variant.
///
/// An optional second parameter binds a unique `&str` node name for the variant,
/// formed from the test's own name plus the runtime (`<test>_basic` /
/// `<test>_tokio`).
///
/// Both a unit-returning and a `Result`-returning form are supported:
///
/// ```ignore
/// test_with_executors! {
///     fn my_test(executor, node_name) {
///         let node = executor.create_node(node_name).unwrap();
///         // ... publish, spin, assert ...
///     }
/// }
///
/// test_with_executors! {
///     fn my_fallible_test(executor) -> Result<(), RclrsError> {
///         let node = executor.create_node("my_node")?;
///         Ok(())
///     }
/// }
/// ```
///
/// [`BasicExecutorRuntime`]: crate::BasicExecutorRuntime
/// [`TokioExecutorRuntime`]: crate::TokioExecutorRuntime
macro_rules! test_with_executors {
    (
        $(#[$meta:meta])*
        fn $name:ident($executor:ident $(, $node_name:ident)?) $(-> $ret:ty)? $body:block
    ) => {
        // `paste!` concatenates identifiers, which plain `macro_rules!` cannot do:
        // `[<$name _basic>]` turns a caller's `foo` into the real fn name `foo_basic`
        // (and `foo_tokio` below), giving each executor variant a unique test name.
        ::paste::paste! {
            // Forward the caller's attributes (e.g. `#[ignore]`, `#[should_panic]`)
            // onto every generated variant so they behave like a hand-written test.
            $(#[$meta])*
            #[test]
            fn [<$name _basic>]() $(-> $ret)? {
                // Brings the `create_basic_executor()` extension method into scope;
                // `allow(unused_imports)` since some test modules already import it.
                #[allow(unused_imports)]
                use $crate::CreateBasicExecutor;
                let mut $executor = $crate::Context::new(
                    [],
                    $crate::InitOptions::default()
                        .with_domain_id(Some($crate::test_helpers::next_test_domain_id())),
                )
                .expect("test context creation should succeed")
                .create_basic_executor();
                $( #[allow(unused_variables)] let $node_name: &str = concat!(stringify!($name), "_basic"); )?
                $body
            }

            // The tokio variant is compiled only when the feature is enabled; the
            // `_basic` variant above always builds, so turning the feature off never
            // makes a test silently disappear from the suite.
            $(#[$meta])*
            #[cfg(feature = "tokio-executor")]
            #[test]
            fn [<$name _tokio>]() $(-> $ret)? {
                #[allow(unused_imports)]
                use $crate::CreateTokioExecutor;
                let mut $executor = $crate::Context::new(
                    [],
                    $crate::InitOptions::default()
                        .with_domain_id(Some($crate::test_helpers::next_test_domain_id())),
                )
                .expect("test context creation should succeed")
                .create_tokio_executor();
                $( #[allow(unused_variables)] let $node_name: &str = concat!(stringify!($name), "_tokio"); )?
                $body
            }
        }
    };
}

pub(crate) use test_with_executors;
