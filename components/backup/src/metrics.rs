// Copyright 2019 TiKV Project Authors. Licensed under Apache-2.0.

use lazy_static::*;
use prometheus::*;

lazy_static! {
    pub static ref BACKUP_REQUEST_HISTOGRAM: Histogram = register_histogram!(
        "tikv_backup_request_duration_seconds",
        "Bucketed histogram of backup requests duration"
    )
    .unwrap();
    pub static ref BACKUP_RANGE_HISTOGRAM_VEC: HistogramVec = register_histogram_vec!(
        "tikv_backup_range_duration_seconds",
        "Bucketed histogram of backup range duration",
        &["type"]
    )
    .unwrap();
    pub static ref BACKUP_RANGE_SIZE_HISTOGRAM_VEC: HistogramVec = register_histogram_vec!(
        "tikv_backup_range_size_bytes",
        "Bucketed histogram of backup range size",
        &["cf"],
        exponential_buckets(32.0, 2.0, 20).unwrap()
    )
    .unwrap();
    pub static ref BACKUP_RANGE_ERROR_VEC: IntCounterVec = register_int_counter_vec!(
        "tikv_backup_range_error_counter",
        "Total number of backup range errors",
        &["error"]
    )
    .unwrap();
    // pub static ref BACKUP_HEARTBEAT_COUNTER_VEC: IntCounterVec = register_int_counter_vec!(
    //     "tikv_backup_heartbeat_message_total",
    //     "Total number of PD heartbeat messages.",
    //     &["type"]
    // )
    // .unwrap();
    // pub static ref REGION_READ_KEYS_HISTOGRAM: Histogram = register_histogram!(
    //     "tikv_region_read_keys",
    //     "Histogram of keys written for regions",
    //     exponential_buckets(1.0, 2.0, 20).unwrap()
    // )
    // .unwrap();
    // pub static ref REGION_READ_BYTES_HISTOGRAM: Histogram = register_histogram!(
    //     "tikv_region_read_bytes",
    //     "Histogram of bytes written for regions",
    //     exponential_buckets(256.0, 2.0, 20).unwrap()
    // )
    // .unwrap();
    // pub static ref REGION_WRITTEN_BYTES_HISTOGRAM: Histogram = register_histogram!(
    //     "tikv_region_written_bytes",
    //     "Histogram of bytes written for regions",
    //     exponential_buckets(256.0, 2.0, 20).unwrap()
    // )
    // .unwrap();
    // pub static ref REGION_WRITTEN_KEYS_HISTOGRAM: Histogram = register_histogram!(
    //     "tikv_region_written_keys",
    //     "Histogram of keys written for regions",
    //     exponential_buckets(1.0, 2.0, 20).unwrap()
    // )
    // .unwrap();
}
