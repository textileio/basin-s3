#![forbid(unsafe_code)]
#![deny(
clippy::all, //
clippy::cargo, //
clippy::pedantic, //
)]
#![allow(
clippy::wildcard_imports,
clippy::missing_errors_doc, // TODO: docs
clippy::let_underscore_untyped,
clippy::module_name_repetitions,
clippy::multiple_crate_versions, // TODO: check later
)]

pub use self::error::*;
pub use self::ipc::Ipc;

#[macro_use]
mod error;

mod checksum;
pub mod fendermint;
mod ipc;
mod s3;
mod utils;
