// Copyright 2024 TiKV Project Authors. Licensed under Apache-2.0.

use std::time::Duration;

use concurrency_manager::ConcurrencyManager;
use criterion::{black_box, criterion_group, criterion_main, Criterion};
use txn_types::TimeStamp;

fn benchmark_update_max_ts(c: &mut Criterion) {
    let latest_ts = TimeStamp::new(1000);
    let limit_valid_time = Duration::from_secs(20);
    let cm = ConcurrencyManager::new_with_config(latest_ts, limit_valid_time, false);

    let valid_new_ts = TimeStamp::new(1000);
    let invalid_new_ts = TimeStamp::new(3000);
    cm.set_max_ts_limit(TimeStamp::new(2000));

    c.bench_function("update_max_ts_valid", |b| {
        b.iter(|| {
            // let _ = cm.update_max_ts(black_box(new_ts), "benchmark");
            let _ = cm.update_max_ts(black_box(valid_new_ts), format!("benchmark-{}", valid_new_ts));
        })
    });

    c.bench_function("update_max_ts_valid_args", |b| {
        b.iter(|| {
            let _ = cm.update_max_ts(black_box(valid_new_ts), format_args!("benchmark-{}", invalid_new_ts));
        })
    });

    c.bench_function("update_max_ts_invalid", |b| {
        b.iter(|| {
            let _ = cm.update_max_ts(black_box(invalid_new_ts), format!("benchmark-{}", invalid_new_ts));
        })
    });

    c.bench_function("update_max_ts_invalid_args", |b| {
        b.iter(|| {
            let _ = cm.update_max_ts(black_box(invalid_new_ts), format_args!("benchmark-{}", invalid_new_ts));
        })
    });

    // lazy
    c.bench_function("update_max_ts_lazy_valid", |b| {
        b.iter(|| {
            let _ = cm.update_max_ts_lazy(black_box(valid_new_ts), || format!("benchmark-{}", valid_new_ts));
        })
    });

    // c.bench_function("update_max_ts_lazy_valid_args", |b| {
    //     b.iter(|| {
    //         let _ = cm.update_max_ts_lazy(black_box(valid_new_ts), || format_args!("benchmark-{}", valid_new_ts));
    //     })
    // });

    c.bench_function("update_max_ts_lazy_invalid", |b| {
        b.iter(|| {
            let _ = cm.update_max_ts_lazy(black_box(invalid_new_ts), || format!("benchmark-{}", invalid_new_ts));
        })
    });

    // c.bench_function("update_max_ts_lazy_invalid", |b| {
    //     b.iter(|| {
    //         let _ = cm.update_max_ts_lazy(black_box(invalid_new_ts), || format_args!("benchmark-{}", invalid_new_ts));
    //     })
    // });
}

criterion_group!(benches, benchmark_update_max_ts);
criterion_main!(benches);
