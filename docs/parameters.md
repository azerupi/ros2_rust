# Parameters

ROS 2 parameters let a node be configured from the outside: from a YAML file, from the command
line, or at runtime through the parameter services. rclrs offers two ways to work with them.

Declare them one at a time with `declare_parameter` when a node has a handful of parameters, or
describe them all at once as a **parameter set**, a plain Rust struct, when there are more than a
few or when they are grouped. This chapter covers parameter sets. The single-parameter builder is
documented on `NodeState::declare_parameter`.

## A parameter set

```rust
use rclrs::*;
use std::path::PathBuf;

/// Configuration for a differential drive controller.
#[derive(ParameterSet, Debug)]
struct DriveConfig {
    /// Maximum forward speed in m/s.
    #[param(default = 1.5, range = 0.0..=10.0)]
    max_speed: f64,

    /// Names of the wheel joints, in left-to-right order.
    #[param(default = ["left_wheel", "right_wheel"])]
    wheels: Vec<String>,

    /// Watchdog timeout. Unset disables the watchdog.
    watchdog: Option<DurationSecs>,

    /// Serial device the motor controller is attached to.
    #[param(read_only)]
    device: PathBuf,

    /// Safety limits.
    limits: Limits,
}

/// Safety limits.
#[derive(ParameterSet, Debug)]
struct Limits {
    /// Maximum motor force in N.
    #[param(default = 100.0, range = 0.0..=250.0)]
    max_force: f64,
}
```

Every field is a parameter, and the struct is the shape of the parameter file that configures it:

```yaml
/drive_controller:
  ros__parameters:
    max_speed: 2.0
    wheels: ["fl", "fr"]
    watchdog: 0.5
    device: /dev/ttyACM0
    limits:
      max_force: 180.0
```

A file only has to say what it wants to change. Anything it leaves out keeps the default from the
struct, and a field with no default and no value in the file fails the declaration, naming itself.

Three things follow from the field types alone:

| Field type | Parameter |
|---|---|
| `T` | mandatory: always has a value |
| `Option<T>` | optional: may be unset |
| `T` with `#[param(read_only)]` | read-only: cannot be changed after declaration |

A field whose type is itself a `ParameterSet` is a nested group of parameters, declared under the
field's name, which is `limits.max_force` above. This needs no annotation: the derive macro emits
the same code for every field and lets the type system work out what each one is.

## Declaring a set on a node

```rust
let executor = Context::default_from_env()?.create_basic_executor();
let node = executor.create_node("drive_controller")?;
```

**Read a static configuration once**, and let the node keep the parameters declared for as long
as it lives:

```rust
let config: DriveConfig = node.load_parameters()?;
println!("max speed is {} m/s", config.max_speed);
```

`config` is a plain struct. It has no connection to the node, which makes it convenient to pass
into the code that uses it, and to construct directly in a unit test with no node, no executor and
no ROS 2 at all:

```rust
#[test]
fn stops_in_time() {
    let config = DriveConfig { max_speed: 2.0, /* ... */ };
    assert!(stopping_distance(&config) < 0.5);
}
```

**Watch and change parameters at runtime** by keeping the handles instead:

```rust
let params = node.declare_parameters::<DriveConfig>()?;

params.max_speed.get();              // 1.5
params.max_speed.set(2.0)?;          // rejected if outside the range
params.limits.max_force.get();       // nested handles, same field names

// React to changes made through the parameter services.
params.max_speed.on_change(|speed| println!("speed is now {speed}"));

// Or read the whole set at once.
let config: DriveConfig = params.snapshot();
```

The handles own the declarations: when `params` is dropped the parameters are undeclared. To have
the node keep them alive *and* still be able to read or write them, use `retain_parameters`,
which hands back an `Arc` that can be cloned into callbacks and timers:

```rust
let params = node.retain_parameters::<DriveConfig>()?;
let params_for_timer = Arc::clone(&params);
node.create_timer_repeating(Duration::from_secs(1), move || {
    println!("{:?}", params_for_timer.snapshot());
})?;
```

`load_parameters` is exactly `retain_parameters(..)?.snapshot()`.

## Namespaces

By default a set declares its parameters at the node's root, so the struct and the YAML have the
same shape. Two ways to change that:

```rust
// Everything under `drive.`
#[derive(ParameterSet)]
#[parameters(namespace = "drive")]
struct DriveConfig { /* ... */ }

// Or at the point of declaration: parameters under `front.drive.`
let params = node.declare_parameters_with_prefix::<DriveConfig>("front")?;
```

A nested set uses its field name as its namespace. `#[param(flatten)]` declares its parameters
directly under the parent's namespace instead, which is useful for grouping fields in Rust
without that grouping appearing in the parameter names:

```rust
#[derive(ParameterSet)]
struct DriveConfig {
    #[param(default = 1.5)]
    max_speed: f64,
    // Declares `max_force`, not `limits.max_force`.
    #[param(flatten)]
    limits: Limits,
}
```

## Value types

Beyond the nine types ROS 2 has parameters for, a field may be any of:

`bool`, `i64`, `f64`, `f32`, `i8`, `i16`, `i32`, `u8`, `u16`, `u32`, `String`, `PathBuf`,
`DurationSecs`, `DurationMillis`, `ParameterValue` for a dynamically typed parameter, a `Vec` of
any of those, the `Arc<[..]>` forms of the ROS 2 array types, and `Option<T>` of any of them.

An array uses whichever ROS 2 array type holds its element's representation, so a `Vec<u16>` is an
integer array whose elements are each checked against the range of a `u16`, and a rejection says
which element was at fault. `Vec<u8>` is the one exception: a sequence of bytes is a ROS 2 byte
array rather than an integer array.

Some of these are narrower than the ROS 2 type they are stored as: not every `i64` is a `u16`. A
value that does not fit is rejected when it arrives, including when it arrives over the
`SetParameters` service, so a parameter declared as `u16` can never hold anything else.

`u64`, `usize`, `i128` and `u128` are deliberately not parameter types: a parameter value is
stored as an `i64`, and every way of handling a value above `i64::MAX` would silently change it.
Use `i64`, or `u32` when the value must be unsigned.

`Duration` is not a parameter type either, because the unit it was stored in would be left
implicit. `DurationSecs` and `DurationMillis` say which, and both deref to `Duration`.

To use a type of your own, implement `ParameterVariant` for it and then call
`declare_parameter_field!`:

```rust
declare_parameter_field!(Hostname);
```

## Field options

All of these are `#[param(...)]` on a field:

| Option | Meaning |
|---|---|
| `default = expr` | Value to use when nothing else supplies one. String and array literals are converted for you, so `default = ["a", "b"]` works for a `Vec<String>`. |
| `description = "…"` | Descriptor description. Defaults to the field's doc comment. |
| `constraints = "…"` | Descriptor constraints. Defaults to whatever the field's type says about itself. |
| `range = 0.0..=10.0`, `step = 0.5` | Valid range, in the field's own units. `a..=b`, `a..` and `..=b` are all accepted. ROS 2 ranges are inclusive, so `a..b` is not. |
| `read_only` | Declare as read-only. |
| `validate = expr` | `fn(&T) -> Result<(), String>`, run before a value is applied, including values from the parameter services, which are rejected with the reason. |
| `on_change = expr` | `fn(&T)`, or `fn(Option<&T>)` for an `Option` field, run after a value has been applied. For side effects. |
| `discriminate = expr` | Choose the initial value from the default, the override and any prior value. |
| `ignore_override` | Ignore any override supplied for this parameter. |
| `discard_mismatching_prior_value` | Discard, rather than reject, a prior value of the wrong type. |
| `rename = "name"` | Use a different ROS 2 name from the field name. |
| `flatten` | For a nested set: declare its parameters under this set's namespace. |
| `skip` | Not a parameter. Filled in with `Default::default()` when the set is read back. |

`validate` and `on_change` are two halves of reacting to a change, and the distinction matters:
`validate` decides whether a change is allowed and must have no side effects, because it also runs
on values that end up rejected. `on_change` runs after the change has been applied and is where
side effects belong.

Defaults can also come from a `Default` implementation, which is worth it for a large set:

```rust
#[derive(ParameterSet, Default)]
#[parameters(default = Self::default())]
struct DriveConfig {
    max_speed: f64,      // default comes from `Default`
    limits: Limits,      // and so do the nested set's
}
```

Where a value is supplied for a set as a whole, by `#[parameters(default = ...)]` or by a parent
field's `default`, that value wins. A field's own `#[param(default = ...)]` is then the fallback for
whatever it does not cover. The supplied value is specific to that one instance of the set, whereas
a field attribute says what the type defaults to in general, so a parent can configure a set it did
not write:

```rust
#[derive(ParameterSet)]
struct DriveConfig {
    // Overrides whatever defaults `Limits` declares for itself.
    #[param(default = Limits { max_force: 250.0 })]
    limits: Limits,
}
```

## Errors

Declaring a set returns `ParameterSetError`, which names the parameter that failed:

```text
failed to declare parameter 'limits.max_force': parameter was declared as non-optional but no
value was available, either through a user specified default, a command-line override, or a
previously set value
```

It converts into `RclrsError`, so `?` works in a function that returns one.
