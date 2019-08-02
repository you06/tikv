use std::cmp;
use std::fmt;
use std::sync::*;
use std::time::*;

use futures::sync::mpsc::UnboundedSender;
use futures::{lazy, Future};
use kvproto::backup::*;
use kvproto::kvrpcpb::{Context, IsolationLevel};
use kvproto::metapb::*;
use raft::StateRole;
use tikv::raftstore::coprocessor::RegionInfoAccessor;
use tikv::raftstore::store::util::find_peer;
use tikv::server::transport::ServerRaftStoreRouter;
use tikv::storage::kv::{
    Engine, Error as EngineError, RegionInfoProvider, ScanMode, StatisticsSummary,
};
use tikv::storage::txn::{EntryBatch, Error as TxnError, Msg, Scanner, SnapshotStore, Store};
use tikv::storage::{Key, Statistics};
use tikv_util::worker::{Runnable, RunnableWithTimer};
use tokio_threadpool::ThreadPool;

pub enum Task {
    Latest {
        start_key: Vec<u8>,
        end_key: Vec<u8>,
        end_ts: u64,

        // TODO: support different kinds of storages.
        storage: (),
        sink: (),
        resp: UnboundedSender<Option<BackupResponse>>,
    },
    Incremental {
        start_key: Vec<u8>,
        end_key: Vec<u8>,
        start_ts: u64,
        end_ts: u64,

        // TODO: support different kinds of storages.
        storage: (),
        sink: (),
        resp: UnboundedSender<Option<BackupResponse>>,
    },
}

impl fmt::Display for Task {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Task::Latest {
                start_key,
                end_key,
                end_ts,
                ..
            } => f
                .debug_struct("Latest")
                .field("end_ts", &end_ts)
                .field("start_key", &start_key)
                .field("end_key", &end_key)
                .finish(),
            Task::Incremental {
                start_key,
                end_key,
                end_ts,
                start_ts,
                ..
            } => f
                .debug_struct("Incremental")
                .field("start_ts", &start_ts)
                .field("end_ts", &end_ts)
                .field("start_key", &start_key)
                .field("end_key", &end_key)
                .finish(),
        }
    }
}

impl Task {
    pub fn new(req: BackupRequest, resp: UnboundedSender<Option<BackupResponse>>) -> Task {
        let start_key = req.get_start_key().to_owned();
        let end_key = req.get_end_key().to_owned();
        let start_ts = req.get_start_version();
        let end_ts = req.get_end_version();
        if start_ts == end_ts {
            Task::Latest {
                start_key,
                end_key,
                end_ts,
                resp,
                storage: (),
                sink: (),
            }
        } else {
            Task::Incremental {
                start_key,
                end_key,
                start_ts,
                end_ts,
                resp,
                storage: (),
                sink: (),
            }
        }
    }
}

#[derive(Debug)]
pub struct BackupRange {
    start_key: Option<Key>,
    end_key: Option<Key>,
    region: Region,
    leader: Peer,
}

pub struct Endpoint<E: Engine, R: RegionInfoProvider> {
    store_id: u64,
    engine: E,
    region_info: R,
    workers: ThreadPool,
}

impl<E: Engine, R: RegionInfoProvider> Endpoint<E, R> {
    pub fn new(store_id: u64, engine: E, region_info: R) -> Endpoint<E, R> {
        Endpoint {
            store_id,
            engine,
            region_info,
            // TODO: support more config.
            workers: ThreadPool::new(),
        }
    }

    fn seek_backup_range(
        &self,
        start_key: Option<Key>,
        end_key: Option<Key>,
    ) -> mpsc::Receiver<BackupRange> {
        let store_id = self.store_id;
        let (tx, rx) = mpsc::channel();
        let start_key_ = start_key
            .clone()
            .map_or_else(Vec::new, |k| k.into_encoded());
        let res = self.region_info.seek_region(
            &start_key_,
            Box::new(move |iter| {
                for info in iter {
                    let region = &info.region;
                    if !end_key.is_none() {
                        let end_slice = end_key.as_ref().unwrap().as_encoded().as_slice();
                        if end_slice < region.get_start_key() {
                            // println!("break {:?}, {:?}", end_slice, region.get_start_key());
                            // We have reached the end.
                            break;
                        }
                    }
                    if info.role == StateRole::Leader {
                        let (region_start, region_end) = key_from_region(region);
                        let ekey = if region.get_end_key().is_empty() {
                            end_key.clone()
                        } else if end_key.is_none() {
                            region_end
                        } else {
                            let end_slice = end_key.as_ref().unwrap().as_encoded().as_slice();
                            if end_slice < region.get_end_key() {
                                end_key.clone()
                            } else {
                                region_end
                            }
                        };
                        let skey = if start_key.is_none() {
                            region_start
                        } else {
                            let start_slice = start_key.as_ref().unwrap().as_encoded().as_slice();
                            if start_slice < region.get_start_key() {
                                region_start
                            } else {
                                start_key.clone()
                            }
                        };
                        let leader = find_peer(region, store_id).unwrap().to_owned();
                        let backup_range = BackupRange {
                            start_key: skey,
                            end_key: ekey,
                            region: region.clone(),
                            leader,
                        };
                        tx.send(backup_range).unwrap();
                    }
                }
            }),
        );
        if let Err(e) = res {
            // TODO: handle error.
            error!("backup seek region failed"; "error" => ?e);
        }
        rx
    }

    fn dispatch_backup_range(
        &self,
        brange: BackupRange,
        start_ts: u64,
        end_ts: u64,
        tx: mpsc::Sender<Result<(BackupRange, Statistics), TxnError>>,
    ) {
        // TODO: support incremental backup
        let _ = start_ts;

        let backup_ts = end_ts;
        let mut ctx = Context::new();
        ctx.set_region_id(brange.region.get_id());
        ctx.set_region_epoch(brange.region.get_region_epoch().to_owned());
        ctx.set_peer(brange.leader.clone());
        // TODO: make it async and handle error.
        let snapshot = self.engine.snapshot(&ctx).unwrap();
        self.workers.spawn(lazy(move || {
            let snap_store = SnapshotStore::new(
                snapshot,
                backup_ts,
                IsolationLevel::SI,
                false, /* fill_cache */
            );
            let start_key = brange.start_key.clone();
            let end_key = brange.end_key.clone();
            let mut scanner = snap_store.entry_scanner(start_key, end_key).unwrap();
            let mut batch = EntryBatch::with_capacity(1024);
            loop {
                if let Err(e) = scanner.scan_entries(&mut batch) {
                    // TODO: handle error
                    error!("backup scan error"; "error" => ?e);
                    return tx.send(Err(e)).map_err(|_| ());
                };
                if batch.len() == 0 {
                    break;
                }
                debug!("backup scan entries"; "len" => batch.len());
                // TODO: write to sst before clearing it.
                batch.clear();
            }
            let stat = scanner.take_statistics();
            tx.send(Ok((brange, stat))).map_err(|_| ())
        }));
    }

    pub fn handle_latest_backup(
        &self,
        start_key: Vec<u8>,
        end_key: Vec<u8>,
        end_ts: u64,
        resp: UnboundedSender<Option<BackupResponse>>,
    ) {
        let start = Instant::now();
        let start_key = if start_key.is_empty() {
            None
        } else {
            Some(Key::from_raw(&start_key))
        };
        let end_key = if end_key.is_empty() {
            None
        } else {
            Some(Key::from_raw(&end_key))
        };
        let rx = self.seek_backup_range(start_key, end_key);

        // TODO: should we combine seek_backup_range and dispatch_backup_range?
        let (res_tx, res_rx) = mpsc::channel();
        for brange in rx {
            let tx = res_tx.clone();
            self.dispatch_backup_range(brange, 0, end_ts, tx);
        }

        // Drop the extra sender so that for loop does not hang up.
        drop(res_tx);
        let mut total_stat = Statistics::default();
        for res in res_rx {
            match res {
                Ok((brange, stat)) => {
                    info!("backup region finish";
                        "region" => ?brange.region,
                        "start_key" => ?brange.start_key,
                        "end_key" => ?brange.end_key,
                        "details" => ?stat);
                    total_stat.add(&stat);
                    let start_key = brange
                        .start_key
                        .map_or_else(|| vec![], |k| k.into_raw().unwrap());
                    let end_key = brange
                        .end_key
                        .map_or_else(|| vec![], |k| k.into_raw().unwrap());
                    let mut response = BackupResponse::new();
                    response.set_start_key(start_key);
                    response.set_end_key(end_key);
                    resp.unbounded_send(Some(response)).unwrap();
                }
                Err(e) => error!("backup region failed"; "error" => ?e),
            }
        }
        info!("backup finished";
            "take" => ?start.elapsed(),
            "detail" => ?total_stat);
        resp.unbounded_send(None).unwrap();
    }
}

impl<E: Engine, R: RegionInfoProvider> Runnable<Task> for Endpoint<E, R> {
    fn run(&mut self, task: Task) {
        info!("run backup task"; "task" => %task);
        match task {
            Task::Latest {
                start_key,
                end_key,
                end_ts,
                resp,
                ..
            } => {
                self.handle_latest_backup(start_key, end_key, end_ts, resp);
            }
            Task::Incremental { .. } => unimplemented!(),
        }
    }
}

fn key_from_region(region: &Region) -> (Option<Key>, Option<Key>) {
    let start = if region.get_start_key().is_empty() {
        None
    } else {
        Some(Key::from_encoded_slice(region.get_start_key()))
    };
    let end = if region.get_end_key().is_empty() {
        None
    } else {
        Some(Key::from_encoded_slice(region.get_end_key()))
    };
    (start, end)
}

#[cfg(test)]
mod tests {
    use super::*;
    use futures::sync::mpsc::unbounded;
    use futures::{Future, Stream};
    use kvproto::metapb;
    use std::collections::BTreeMap;
    use std::sync::mpsc::{channel, Receiver, Sender};
    use tempfile::TempDir;
    use tikv::raftstore::coprocessor::{RegionInfo, SeekRegionCallback};
    use tikv::raftstore::store::util::new_peer;
    use tikv::storage::kv::Result as EngineResult;
    use tikv::storage::{
        Mutation, Options, RocksEngine, Storage, TestEngineBuilder, TestStorageBuilder,
    };

    #[derive(Clone)]
    struct MockRegionInfoProvider {
        // start_key -> (region_id, end_key)
        regions: Arc<Mutex<BTreeMap<Vec<u8>, RegionInfo>>>,
    }
    impl MockRegionInfoProvider {
        fn new() -> Self {
            MockRegionInfoProvider {
                regions: Arc::default(),
            }
        }
        fn set_regions(&self, regions: Vec<(Vec<u8>, Vec<u8>, u64)>) {
            let mut map = self.regions.lock().unwrap();
            let regions: BTreeMap<_, _> = regions
                .into_iter()
                .map(|(mut start_key, mut end_key, id)| {
                    if !start_key.is_empty() {
                        start_key = Key::from_raw(&start_key).into_encoded();
                    }
                    if !end_key.is_empty() {
                        end_key = Key::from_raw(&end_key).into_encoded();
                    }
                    let mut r = metapb::Region::default();
                    r.set_id(id);
                    r.set_start_key(start_key.clone());
                    r.set_end_key(end_key);
                    r.mut_peers().push(new_peer(1, 1));
                    let info = RegionInfo::new(r, StateRole::Leader);
                    (start_key, info)
                })
                .collect();
            *map = regions;
        }
    }
    impl RegionInfoProvider for MockRegionInfoProvider {
        fn seek_region(&self, from: &[u8], callback: SeekRegionCallback) -> EngineResult<()> {
            let from = from.to_vec();
            let regions = self.regions.lock().unwrap();
            callback(&mut regions.range(from..).map(|(_, v)| v));
            Ok(())
        }
    }

    fn new_endpoint() -> (TempDir, Endpoint<RocksEngine, MockRegionInfoProvider>) {
        let temp = TempDir::new().unwrap();
        let rocks = TestEngineBuilder::new()
            .path(temp.path())
            .cfs(&[engine::CF_DEFAULT, engine::CF_LOCK, engine::CF_WRITE])
            .build()
            .unwrap();
        (temp, Endpoint::new(1, rocks, MockRegionInfoProvider::new()))
    }

    #[test]
    fn test_seek_range() {
        let (_tmp, endpoint) = new_endpoint();

        endpoint.region_info.set_regions(vec![
            (b"".to_vec(), b"1".to_vec(), 1),
            (b"1".to_vec(), b"2".to_vec(), 2),
            (b"3".to_vec(), b"4".to_vec(), 3),
            (b"7".to_vec(), b"".to_vec(), 4),
        ]);
        let t = |start_key: &[u8], end_key: &[u8], expect: Vec<(&[u8], &[u8])>| {
            // println!("{:?}", (start_key, end_key, expect.clone()));
            let start_key = if start_key.is_empty() {
                None
            } else {
                Some(Key::from_raw(start_key))
            };
            let end_key = if end_key.is_empty() {
                None
            } else {
                Some(Key::from_raw(end_key))
            };
            let rx = endpoint.seek_backup_range(start_key, end_key);
            let ranges: Vec<BackupRange> = rx.into_iter().collect();
            // println!("got {:?}, expect {:?}", ranges, expect);
            assert_eq!(
                ranges.len(),
                expect.len(),
                "got {:?}, expect {:?}",
                ranges,
                expect
            );
            for (a, b) in ranges.into_iter().zip(expect) {
                assert_eq!(
                    a.start_key.map_or_else(Vec::new, |k| k.into_raw().unwrap()),
                    b.0
                );
                assert_eq!(
                    a.end_key.map_or_else(Vec::new, |k| k.into_raw().unwrap()),
                    b.1
                );
            }
        };

        // Test whether responses contain correct range.
        let tt = |start_key: &[u8], end_key: &[u8], expect: Vec<(&[u8], &[u8])>| {
            // println!("{:?}", (start_key, end_key, expect.clone()));
            let (tx, rx) = unbounded();
            endpoint.handle_latest_backup(
                start_key.to_vec(),
                end_key.to_vec(),
                1, /* ts */
                tx,
            );
            let resps: Vec<_> = rx.collect().wait().unwrap();
            let mut counter = 0;
            for a in resps.iter().filter_map(Option::as_ref) {
                counter += 1;
                assert!(
                    expect
                        .iter()
                        .any(|b| { a.get_start_key() == b.0 && a.get_end_key() == b.1 }),
                    "{:?} {:?}",
                    resps,
                    expect
                );
            }
            assert_eq!(counter, expect.len());
        };

        let case: Vec<(&[u8], &[u8], Vec<(&[u8], &[u8])>)> = vec![
            (b"", b"1", vec![(b"", b"1"), (b"1", b"1")]),
            (b"", b"2", vec![(b"", b"1"), (b"1", b"2")]),
            (b"1", b"2", vec![(b"1", b"2")]),
            (b"1", b"3", vec![(b"1", b"2"), (b"3", b"3")]),
            (b"1", b"4", vec![(b"1", b"2"), (b"3", b"4")]),
            (b"4", b"6", vec![]),
            (b"4", b"5", vec![]),
            (b"2", b"7", vec![(b"3", b"4"), (b"7", b"7")]),
            (b"3", b"", vec![(b"3", b"4"), (b"7", b"")]),
            (b"5", b"", vec![(b"7", b"")]),
            (b"7", b"", vec![(b"7", b"")]),
            (
                b"",
                b"",
                vec![(b"", b"1"), (b"1", b"2"), (b"3", b"4"), (b"7", b"")],
            ),
        ];
        for (start_key, end_key, ranges) in case {
            t(start_key, end_key, ranges.clone());
            tt(start_key, end_key, ranges);
        }
    }
}
