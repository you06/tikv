// Copyright 2017 TiKV Project Authors. Licensed under Apache-2.0.

// #[PerformanceCriticalPath]
use api_version::KvFormat;
use kvproto::kvrpcpb::*;
use protobuf::Message;
use tikv_util::{
    future::poll_future_notify,
    mpsc::future::{Sender, WakePolicy},
    time::Instant,
};
use tracker::{GLOBAL_TRACKERS, RequestInfo, RequestType, Tracker, TrackerToken, with_tls_tracker};
use txn_types::ValueEntry;

use crate::{
    server::{
        metrics::{GrpcTypeKind, REQUEST_BATCH_SIZE_HISTOGRAM_VEC, ResourcePriority},
        service::kv::{GrpcRequestDuration, MeasuredSingleResponse, batch_commands_response},
    },
    storage::{
        ResponseBatchConsumer, Result, Storage,
        errors::{extract_key_error, extract_region_error},
        kv::{Engine, Statistics},
        lock_manager::LockManager,
    },
};

pub const MAX_BATCH_GET_REQUEST_COUNT: usize = 10;
pub const MIN_BATCH_GET_REQUEST_COUNT: usize = 4;
pub const MAX_QUEUE_SIZE_PER_WORKER: usize = 16;

// Constants for BatchGet batching
// Maximum number of keys in a single BatchGetRequest to be eligible for
// batching
pub const BATCH_GET_MAX_BATCHED_REQ_SIZE: usize = 16;
// Maximum total number of keys across all batched requests before flushing
pub const BATCH_GET_MAX_BATCH_SIZE: usize = 1024;

pub struct ReqBatcher {
    gets: Vec<GetRequest>,
    raw_gets: Vec<RawGetRequest>,
    get_ids: Vec<u64>,
    get_trackers: Vec<TrackerToken>,
    raw_get_ids: Vec<u64>,
    // Fields for BatchGet batching
    batch_gets: Vec<BatchGetRequest>,
    batch_get_ids: Vec<u64>,
    batch_get_trackers: Vec<TrackerToken>,
    batch_get_total_keys: usize,
    begin_instant: Instant,
    batch_size: usize,
}

impl ReqBatcher {
    pub fn new(batch_size: usize) -> ReqBatcher {
        let begin_instant = Instant::now();
        ReqBatcher {
            gets: vec![],
            raw_gets: vec![],
            get_ids: vec![],
            get_trackers: vec![],
            raw_get_ids: vec![],
            batch_gets: vec![],
            batch_get_ids: vec![],
            batch_get_trackers: vec![],
            batch_get_total_keys: 0,
            begin_instant,
            batch_size: std::cmp::min(batch_size, MAX_BATCH_GET_REQUEST_COUNT),
        }
    }

    pub fn can_batch_get(&self, req: &GetRequest) -> bool {
        req.get_context().get_priority() == CommandPri::Normal
    }

    pub fn can_batch_raw_get(&self, req: &RawGetRequest) -> bool {
        req.get_context().get_priority() == CommandPri::Normal
    }

    /// Check if a BatchGetRequest can be batched.
    /// Returns true if priority is Normal and the number of keys is small
    /// enough.
    pub fn can_batch_batch_get(&self, req: &BatchGetRequest) -> bool {
        req.get_context().get_priority() == CommandPri::Normal
            && req.get_keys().len() <= BATCH_GET_MAX_BATCHED_REQ_SIZE
    }

    pub fn add_get_request(&mut self, req: GetRequest, id: u64) {
        let tracker = GLOBAL_TRACKERS.insert(Tracker::new(RequestInfo::new(
            req.get_context(),
            RequestType::KvBatchGetCommand,
            req.get_version(),
        )));
        GLOBAL_TRACKERS.with_tracker(tracker, |the_tracker| {
            the_tracker.metrics.grpc_req_size = req.compute_size() as u64;
        });
        self.gets.push(req);
        self.get_ids.push(id);
        self.get_trackers.push(tracker);
    }

    pub fn add_raw_get_request(&mut self, req: RawGetRequest, id: u64) {
        self.raw_gets.push(req);
        self.raw_get_ids.push(id);
    }

    pub fn add_batch_get_request(&mut self, req: BatchGetRequest, id: u64) {
        let tracker = GLOBAL_TRACKERS.insert(Tracker::new(RequestInfo::new(
            req.get_context(),
            RequestType::KvBatchBatchGetCommand,
            req.get_version(),
        )));
        GLOBAL_TRACKERS.with_tracker(tracker, |the_tracker| {
            the_tracker.metrics.grpc_req_size = req.compute_size() as u64;
        });
        self.batch_get_total_keys += req.get_keys().len();
        self.batch_gets.push(req);
        self.batch_get_ids.push(id);
        self.batch_get_trackers.push(tracker);
    }

    pub fn maybe_commit<E: Engine, L: LockManager, F: KvFormat>(
        &mut self,
        storage: &Storage<E, L, F>,
        tx: &Sender<MeasuredSingleResponse>,
    ) {
        if self.gets.len() >= self.batch_size {
            let gets = std::mem::take(&mut self.gets);
            let ids = std::mem::take(&mut self.get_ids);
            let trackers = std::mem::take(&mut self.get_trackers);
            future_batch_get_command(storage, ids, gets, trackers, tx.clone(), self.begin_instant);
        }

        if self.raw_gets.len() >= self.batch_size {
            let gets = std::mem::take(&mut self.raw_gets);
            let ids = std::mem::take(&mut self.raw_get_ids);
            future_batch_raw_get_command(storage, ids, gets, tx.clone(), self.begin_instant);
        }

        if self.batch_get_total_keys >= BATCH_GET_MAX_BATCH_SIZE {
            let batch_gets = std::mem::take(&mut self.batch_gets);
            let ids = std::mem::take(&mut self.batch_get_ids);
            let trackers = std::mem::take(&mut self.batch_get_trackers);
            self.batch_get_total_keys = 0;
            future_batch_batch_get_command(
                storage,
                ids,
                batch_gets,
                trackers,
                tx.clone(),
                self.begin_instant,
            );
        }
    }

    pub fn commit<E: Engine, L: LockManager, F: KvFormat>(
        self,
        storage: &Storage<E, L, F>,
        tx: &Sender<MeasuredSingleResponse>,
    ) {
        if !self.gets.is_empty() {
            future_batch_get_command(
                storage,
                self.get_ids,
                self.gets,
                self.get_trackers,
                tx.clone(),
                self.begin_instant,
            );
        }
        if !self.raw_gets.is_empty() {
            future_batch_raw_get_command(
                storage,
                self.raw_get_ids,
                self.raw_gets,
                tx.clone(),
                self.begin_instant,
            );
        }
        if !self.batch_gets.is_empty() {
            future_batch_batch_get_command(
                storage,
                self.batch_get_ids,
                self.batch_gets,
                self.batch_get_trackers,
                tx.clone(),
                self.begin_instant,
            );
        }
    }
}

pub struct BatcherBuilder {
    pool_size: usize,
    enable_batch: bool,
}

impl BatcherBuilder {
    pub fn new(enable_batch: bool, pool_size: usize) -> Self {
        BatcherBuilder {
            enable_batch,
            pool_size,
        }
    }
    pub fn build(&self, queue_per_worker: usize, req_batch_size: usize) -> Option<ReqBatcher> {
        if !self.enable_batch {
            return None;
        }
        if req_batch_size > self.pool_size * MIN_BATCH_GET_REQUEST_COUNT
            && queue_per_worker >= MIN_BATCH_GET_REQUEST_COUNT
        {
            return Some(ReqBatcher::new(req_batch_size / self.pool_size));
        }
        if req_batch_size >= MIN_BATCH_GET_REQUEST_COUNT
            && queue_per_worker >= MAX_QUEUE_SIZE_PER_WORKER
        {
            return Some(ReqBatcher::new(req_batch_size));
        }
        None
    }
}

pub struct GetCommandResponseConsumer {
    tx: Sender<MeasuredSingleResponse>,
}

impl ResponseBatchConsumer<(Option<ValueEntry>, Statistics)> for GetCommandResponseConsumer {
    fn consume(
        &self,
        id: u64,
        res: Result<(Option<ValueEntry>, Statistics)>,
        begin: Instant,
        request_source: String,
        resource_priority: ResourcePriority,
    ) {
        let mut resp = GetResponse::default();
        if let Some(err) = extract_region_error(&res) {
            resp.set_region_error(err);
        } else {
            match res {
                Ok((val, statistics)) => {
                    let scan_detail_v2 = resp.mut_exec_details_v2().mut_scan_detail_v2();
                    statistics.write_scan_detail(scan_detail_v2);
                    with_tls_tracker(|tracker| tracker.write_scan_detail(scan_detail_v2));
                    match val {
                        Some(val) => {
                            resp.set_value(val.value);
                            if let Some(commit_ts) = val.commit_ts {
                                resp.set_commit_ts(commit_ts.into_inner());
                            }
                        }
                        None => resp.set_not_found(true),
                    }
                }
                Err(e) => resp.set_error(extract_key_error(&e)),
            }
        }

        let res = batch_commands_response::Response {
            cmd: Some(batch_commands_response::response::Cmd::Get(resp)),
            ..Default::default()
        };
        let measure = GrpcRequestDuration::new(
            begin,
            GrpcTypeKind::kv_batch_get_command,
            request_source,
            resource_priority,
        );
        let task = MeasuredSingleResponse::new(id, res, measure, None);
        if self.tx.send_with(task, WakePolicy::Immediately).is_err() {
            warn!("KvService response batch commands fail");
        }
    }
}

impl ResponseBatchConsumer<Option<ValueEntry>> for GetCommandResponseConsumer {
    fn consume(
        &self,
        id: u64,
        res: Result<Option<ValueEntry>>,
        begin: Instant,
        request_source: String,
        resource_priority: ResourcePriority,
    ) {
        let mut resp = RawGetResponse::default();
        if let Some(err) = extract_region_error(&res) {
            resp.set_region_error(err);
        } else {
            match res {
                Ok(Some(val)) => resp.set_value(val.value),
                Ok(None) => resp.set_not_found(true),
                Err(e) => resp.set_error(format!("{}", e)),
            }
        }
        let res = batch_commands_response::Response {
            cmd: Some(batch_commands_response::response::Cmd::RawGet(resp)),
            ..Default::default()
        };
        let measure = GrpcRequestDuration::new(
            begin,
            GrpcTypeKind::raw_batch_get_command,
            request_source,
            resource_priority,
        );
        let task = MeasuredSingleResponse::new(id, res, measure, None);
        if self.tx.send_with(task, WakePolicy::Immediately).is_err() {
            warn!("KvService response batch commands fail");
        }
    }
}

fn future_batch_get_command<E: Engine, L: LockManager, F: KvFormat>(
    storage: &Storage<E, L, F>,
    requests: Vec<u64>,
    gets: Vec<GetRequest>,
    trackers: Vec<TrackerToken>,
    tx: Sender<MeasuredSingleResponse>,
    begin_instant: tikv_util::time::Instant,
) {
    REQUEST_BATCH_SIZE_HISTOGRAM_VEC
        .kv_get
        .observe(gets.len() as f64);
    let id_sources: Vec<_> = requests
        .iter()
        .zip(gets.iter())
        .map(|(id, req)| (*id, req.get_context().get_request_source().to_string()))
        .collect();

    let group_priority = gets
        .first()
        .unwrap()
        .get_context()
        .get_resource_control_context()
        .get_override_priority();
    let resource_priority = ResourcePriority::from(group_priority);

    let res = storage.batch_get_command(
        gets,
        requests,
        trackers.clone(),
        GetCommandResponseConsumer { tx: tx.clone() },
        begin_instant,
    );
    let f = async move {
        // This error can only cause by readpool busy.
        let res = res.await;
        for tracker in trackers {
            GLOBAL_TRACKERS.remove(tracker);
        }
        if let Some(e) = extract_region_error(&res) {
            let mut resp = GetResponse::default();
            resp.set_region_error(e);
            for (id, source) in id_sources {
                let res = batch_commands_response::Response {
                    cmd: Some(batch_commands_response::response::Cmd::Get(resp.clone())),
                    ..Default::default()
                };
                let measure = GrpcRequestDuration::new(
                    begin_instant,
                    GrpcTypeKind::kv_batch_get_command,
                    source,
                    resource_priority,
                );
                let task = MeasuredSingleResponse::new(id, res, measure, None);
                if tx.send_with(task, WakePolicy::Immediately).is_err() {
                    warn!("KvService response batch commands fail");
                }
            }
        }
    };
    poll_future_notify(f);
}

fn future_batch_raw_get_command<E: Engine, L: LockManager, F: KvFormat>(
    storage: &Storage<E, L, F>,
    requests: Vec<u64>,
    gets: Vec<RawGetRequest>,
    tx: Sender<MeasuredSingleResponse>,
    begin_instant: tikv_util::time::Instant,
) {
    REQUEST_BATCH_SIZE_HISTOGRAM_VEC
        .raw_get
        .observe(gets.len() as f64);
    let id_sources: Vec<_> = requests
        .iter()
        .zip(gets.iter())
        .map(|(id, req)| (*id, req.get_context().get_request_source().to_string()))
        .collect();

    let group_priority = gets
        .first()
        .unwrap()
        .get_context()
        .get_resource_control_context()
        .get_override_priority();
    let resource_priority = ResourcePriority::from(group_priority);

    let res = storage.raw_batch_get_command(
        gets,
        requests,
        GetCommandResponseConsumer { tx: tx.clone() },
    );
    let f = async move {
        // This error can only cause by readpool busy.
        let res = res.await;
        if let Some(e) = extract_region_error(&res) {
            let mut resp = RawGetResponse::default();
            resp.set_region_error(e);
            for (id, source) in id_sources {
                let res = batch_commands_response::Response {
                    cmd: Some(batch_commands_response::response::Cmd::RawGet(resp.clone())),
                    ..Default::default()
                };
                let measure = GrpcRequestDuration::new(
                    begin_instant,
                    GrpcTypeKind::raw_batch_get_command,
                    source,
                    resource_priority,
                );
                let task = MeasuredSingleResponse::new(id, res, measure, None);
                if tx.send_with(task, WakePolicy::Immediately).is_err() {
                    warn!("KvService response batch commands fail");
                }
            }
        }
    };
    poll_future_notify(f);
}

/// Response consumer for batched BatchGetRequest commands.
/// Converts the storage result into a BatchGetResponse and sends it.
pub struct BatchGetCommandResponseConsumer {
    tx: Sender<MeasuredSingleResponse>,
}

impl ResponseBatchConsumer<(Vec<Option<ValueEntry>>, Statistics)>
    for BatchGetCommandResponseConsumer
{
    fn consume(
        &self,
        id: u64,
        res: Result<(Vec<Option<ValueEntry>>, Statistics)>,
        begin: Instant,
        request_source: String,
        resource_priority: ResourcePriority,
    ) {
        let mut resp = BatchGetResponse::default();
        if let Some(err) = extract_region_error(&res) {
            resp.set_region_error(err);
        } else {
            match res {
                Ok((values, statistics)) => {
                    let scan_detail_v2 = resp.mut_exec_details_v2().mut_scan_detail_v2();
                    statistics.write_scan_detail(scan_detail_v2);
                    with_tls_tracker(|tracker| tracker.write_scan_detail(scan_detail_v2));
                    let pairs: Vec<KvPair> = values
                        .into_iter()
                        .filter_map(|v| {
                            v.map(|entry| {
                                let mut pair = KvPair::default();
                                pair.set_value(entry.value);
                                pair
                            })
                        })
                        .collect();
                    resp.set_pairs(pairs.into());
                }
                Err(e) => {
                    let key_error = extract_key_error(&e);
                    resp.set_error(key_error.clone());
                    // Set key_error in the first kv_pair for backward compatibility.
                    let mut pair = KvPair::default();
                    pair.set_error(key_error);
                    resp.mut_pairs().push(pair);
                }
            }
        }

        let res = batch_commands_response::Response {
            cmd: Some(batch_commands_response::response::Cmd::BatchGet(resp)),
            ..Default::default()
        };
        let measure = GrpcRequestDuration::new(
            begin,
            GrpcTypeKind::kv_batch_batch_get_command,
            request_source,
            resource_priority,
        );
        let task = MeasuredSingleResponse::new(id, res, measure, None);
        if self.tx.send_with(task, WakePolicy::Immediately).is_err() {
            warn!("KvService response batch commands fail");
        }
    }
}

fn future_batch_batch_get_command<E: Engine, L: LockManager, F: KvFormat>(
    storage: &Storage<E, L, F>,
    requests: Vec<u64>,
    batch_gets: Vec<BatchGetRequest>,
    trackers: Vec<TrackerToken>,
    tx: Sender<MeasuredSingleResponse>,
    begin_instant: tikv_util::time::Instant,
) {
    let total_keys: usize = batch_gets.iter().map(|r| r.get_keys().len()).sum();
    REQUEST_BATCH_SIZE_HISTOGRAM_VEC
        .kv_get
        .observe(total_keys as f64);
    let id_sources: Vec<_> = requests
        .iter()
        .zip(batch_gets.iter())
        .map(|(id, req)| (*id, req.get_context().get_request_source().to_string()))
        .collect();

    let group_priority = batch_gets
        .first()
        .unwrap()
        .get_context()
        .get_resource_control_context()
        .get_override_priority();
    let resource_priority = ResourcePriority::from(group_priority);

    let res = storage.batch_batch_get_command(
        batch_gets,
        requests,
        trackers.clone(),
        BatchGetCommandResponseConsumer { tx: tx.clone() },
        begin_instant,
    );
    let f = async move {
        // This error can only cause by readpool busy.
        let res = res.await;
        for tracker in trackers {
            GLOBAL_TRACKERS.remove(tracker);
        }
        if let Some(e) = extract_region_error(&res) {
            let mut resp = BatchGetResponse::default();
            resp.set_region_error(e);
            for (id, source) in id_sources {
                let res = batch_commands_response::Response {
                    cmd: Some(batch_commands_response::response::Cmd::BatchGet(
                        resp.clone(),
                    )),
                    ..Default::default()
                };
                let measure = GrpcRequestDuration::new(
                    begin_instant,
                    GrpcTypeKind::kv_batch_batch_get_command,
                    source,
                    resource_priority,
                );
                let task = MeasuredSingleResponse::new(id, res, measure, None);
                if tx.send_with(task, WakePolicy::Immediately).is_err() {
                    warn!("KvService response batch commands fail");
                }
            }
        }
    };
    poll_future_notify(f);
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use tikv_util::mpsc::future::{WakePolicy, unbounded};
    use txn_types::{TimeStamp, ValueEntry};

    use super::*;
    use crate::storage::kv::Statistics;

    #[test]
    fn test_get_command_response_consumer_sets_commit_ts() {
        let (tx, mut rx) = unbounded(WakePolicy::Immediately);
        let consumer = GetCommandResponseConsumer { tx };

        consumer.consume(
            7,
            Ok((
                Some(ValueEntry::new(b"v".to_vec(), Some(TimeStamp::new(42)))),
                Statistics::default(),
            )),
            Instant::now(),
            "".to_string(),
            ResourcePriority::unknown,
        );

        let mut task = rx.recv_timeout(Duration::from_secs(1)).unwrap();
        assert_eq!(task.id, 7);

        let resp = task.resp.consume();
        let get = resp.get_get();
        assert_eq!(get.get_value(), b"v");
        assert_eq!(get.get_commit_ts(), 42);
    }

    #[test]
    fn test_get_command_response_consumer_commit_ts_default_zero() {
        let (tx, mut rx) = unbounded(WakePolicy::Immediately);
        let consumer = GetCommandResponseConsumer { tx };

        consumer.consume(
            8,
            Ok((
                Some(ValueEntry::from_value(b"v".to_vec())),
                Statistics::default(),
            )),
            Instant::now(),
            "".to_string(),
            ResourcePriority::unknown,
        );

        let mut task = rx.recv_timeout(Duration::from_secs(1)).unwrap();
        let resp = task.resp.consume();
        let get = resp.get_get();
        assert_eq!(get.get_value(), b"v");
        assert_eq!(get.get_commit_ts(), 0);
    }

    #[test]
    fn test_batch_get_command_response_consumer_values() {
        let (tx, mut rx) = unbounded(WakePolicy::Immediately);
        let consumer = BatchGetCommandResponseConsumer { tx };

        // Test with multiple values, some found and some not
        let values = vec![
            Some(ValueEntry::from_value(b"v1".to_vec())),
            None, // Not found - will be filtered out
            Some(ValueEntry::from_value(b"v2".to_vec())),
        ];

        consumer.consume(
            10,
            Ok((values, Statistics::default())),
            Instant::now(),
            "test".to_string(),
            ResourcePriority::unknown,
        );

        let mut task = rx.recv_timeout(Duration::from_secs(1)).unwrap();
        assert_eq!(task.id, 10);

        let resp = task.resp.consume();
        let batch_get = resp.get_batch_get();
        // Only non-None values should be in pairs
        assert_eq!(batch_get.get_pairs().len(), 2);
        assert_eq!(batch_get.get_pairs()[0].get_value(), b"v1");
        assert_eq!(batch_get.get_pairs()[1].get_value(), b"v2");
    }

    #[test]
    fn test_batch_get_command_response_consumer_error() {
        use crate::storage::{Error, ErrorInner};

        let (tx, mut rx) = unbounded(WakePolicy::Immediately);
        let consumer = BatchGetCommandResponseConsumer { tx };

        // Use KeyTooLarge error which is not a region error
        consumer.consume(
            11,
            Err(Error::from(ErrorInner::KeyTooLarge {
                size: 100,
                limit: 50,
            })),
            Instant::now(),
            "test".to_string(),
            ResourcePriority::unknown,
        );

        let mut task = rx.recv_timeout(Duration::from_secs(1)).unwrap();
        assert_eq!(task.id, 11);

        let resp = task.resp.consume();
        let batch_get = resp.get_batch_get();
        // Error should be set
        assert!(batch_get.has_error());
        // First pair should also have the error for backward compatibility
        assert_eq!(batch_get.get_pairs().len(), 1);
        assert!(batch_get.get_pairs()[0].has_error());
    }

    #[test]
    fn test_batch_get_command_response_consumer_empty() {
        let (tx, mut rx) = unbounded(WakePolicy::Immediately);
        let consumer = BatchGetCommandResponseConsumer { tx };

        consumer.consume(
            12,
            Ok((vec![], Statistics::default())),
            Instant::now(),
            "test".to_string(),
            ResourcePriority::unknown,
        );

        let mut task = rx.recv_timeout(Duration::from_secs(1)).unwrap();
        assert_eq!(task.id, 12);

        let resp = task.resp.consume();
        let batch_get = resp.get_batch_get();
        assert!(batch_get.get_pairs().is_empty());
        assert!(!batch_get.has_error());
    }
}
