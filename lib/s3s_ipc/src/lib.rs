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

pub use self::basin::Basin;
pub use self::basin::BasinWallet;
pub use self::error::*;

#[macro_use]
mod error;

mod basin;
mod s3;
mod utils;
