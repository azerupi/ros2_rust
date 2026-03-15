# ros-env
Top-level Rust crate to include generated Rust code found in a sourced ROS 2 workspace.

## Usage
The [shape/msg/Plane](https://github.com/ros2/common_interfaces/blob/rolling/shape_msgs/msg/Plane.msg) message can be included with:
```rust
// Assuming the rust crate for `shape_msgs` is in the `AMENT_PREFIX_PATH`
use ros_env::shape_msgs::msg::Plane;
```

## Details
Any Rust crate found in the `AMENT_PREFIX_PATH` environment variable, that has opted in, will be `include!()`d.

To opt in, the crate must have the following metadata present in the Cargo.toml
```toml
[package.metadata.ros-env]
include = true
```

By default, crates generated from `rosidl_generator_rs` opt in.

## Limitations
- The [include!()](https://doc.rust-lang.org/std/macro.include.html) macro is literal text inclusion. As such, depending 
  on the number of generated crates found in `AMENT_PREFIX_PATH`, the build times for this crate can be long.
- The dependencies of the included crates are not included. You cannot dynamically alter cargo dependencies through 
  anything other than features, and features need to be explicitly declared and enabled. As such, this crate must have 
  all expected dependencies itself (hence why this crate has a `serde` dependency for example).
