// Copyright 2019 TiKV Project Authors. Licensed under Apache-2.0.

use std::error::Error;
use tikv_util::config::ReadableDuration;

/// The configuration for backup components.
#[derive(Clone, Serialize, Deserialize, PartialEq, Debug)]
#[serde(default)]
#[serde(deny_unknown_fields)]
#[serde(rename_all = "kebab-case")]
pub struct Config {
    /// The ID of the store.
    #[serde(skip)]
    pub store_id: u64,

    /// The number of backup threads.
    pub concurrency: u64,
}

impl Default for Config {
    fn default() -> Self {
        Config {
            store_id: 0,
            concurrency: 4,
        }
    }
}
