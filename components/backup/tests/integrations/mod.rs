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

use std::path::*;
use std::sync::*;
use std::{thread, time};

use engine::{Engines, CF_DEFAULT};
use kvproto::metapb::{Peer, Region};
use kvproto::raft_serverpb::*;
use tikv::raftstore::store::fsm::*;
use tikv::raftstore::store::msg::*;
use tikv::raftstore::store::SnapManager;
use tikv::raftstore::store::{util, Config};

pub fn fixture_region() -> (Region, u64) {
    let mut region = Region::new();
    region.mut_region_epoch().set_version(1);
    region.mut_region_epoch().set_conf_ver(1);
    region.mut_peers().push(util::new_peer(1, 2));
    region.mut_peers().push(util::new_peer(2, 3));
    region.mut_peers().push(util::new_learner_peer(3, 4));
    (region, 3)
}

pub fn init() {
    static INIT: Once = ONCE_INIT;
    INIT.call_once(|| {
        test_util::init_log_for_test();
    })
}
