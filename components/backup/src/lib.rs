// Copyright 2019 TiKV Project Authors. Licensed under Apache-2.0.

#![recursion_limit = "200"]
#![allow(unused_imports)]

#[macro_use(
    kv,
    slog_kv,
    slog_trace,
    slog_debug,
    slog_info,
    slog_warn,
    slog_error,
    slog_record,
    slog_b,
    slog_log,
    slog_record_static
)]
extern crate slog;
#[macro_use]
extern crate slog_global;
#[macro_use]
extern crate quick_error;
#[macro_use]
extern crate serde_derive;

mod config;
mod endpoint;
mod errors;
mod service;
mod writer;

pub use config::Config;
pub use endpoint::{Endpoint, Task};
pub use errors::{Error, Result};
pub use service::Service;
pub use writer::{name_to_cf, BackupWriter};
