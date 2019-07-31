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
use tikv::storage::txn::{EntryBatch, Msg, Scanner, SnapshotStore, Store};
use tikv::storage::Key;
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
        resp: UnboundedSender<BackupResponse>,
    },
    Incremental {
        start_key: Vec<u8>,
        end_key: Vec<u8>,
        start_ts: u64,
        end_ts: u64,

        // TODO: support different kinds of storages.
        storage: (),
        sink: (),
        resp: UnboundedSender<BackupResponse>,
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
    pub fn new(req: BackupRequest, resp: UnboundedSender<BackupResponse>) -> Task {
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

pub struct BackupRange {
    start_key: Key,
    end_key: Key,
    region: Region,
    leader: Peer,
    backup_ts: u64,
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

    pub fn handle_latest_backup(
        &self,
        start_key: Vec<u8>,
        end_key: Vec<u8>,
        end_ts: u64,
        resp: UnboundedSender<BackupResponse>,
    ) {
        let start = Instant::now();
        let store_id = self.store_id;
        let (tx, rx) = mpsc::channel();
        let start_key = Key::from_raw(&start_key).into_encoded();
        let start_key_ = start_key.clone();
        let end_key = Key::from_raw(&end_key).into_encoded();
        let res = self.region_info.seek_region(
            &start_key_,
            Box::new(move |iter| {
                for info in iter {
                    let region = &info.region;
                    let ekey = if region.get_end_key().is_empty() {
                        &end_key
                    } else if end_key.as_slice() < region.get_start_key() {
                        // We have reached the end.
                        break;
                    } else {
                        cmp::min(end_key.as_slice(), region.get_end_key())
                    };
                    let skey = cmp::max(start_key.as_slice(), region.get_start_key());
                    if info.role == StateRole::Leader {
                        let leader = find_peer(region, store_id).unwrap().to_owned();
                        let backup_range = BackupRange {
                            start_key: Key::from_encoded(skey.to_owned()),
                            end_key: Key::from_encoded(ekey.to_owned()),
                            region: region.clone(),
                            leader,
                            backup_ts: end_ts,
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

        let (res_tx, res_rx) = mpsc::channel();
        for brange in rx {
            let backup_ts = brange.backup_ts;
            let mut ctx = Context::new();
            ctx.set_region_id(brange.region.get_id());
            ctx.set_region_epoch(brange.region.get_region_epoch().to_owned());
            ctx.set_peer(brange.leader.clone());
            let tx = res_tx.clone();
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
                let mut scanner = snap_store
                    .entry_scanner(Some(start_key), Some(end_key))
                    .unwrap();
                let mut batch = EntryBatch::with_capacity(1024);
                let mut res = Ok(brange);
                loop {
                    if let Err(e) = scanner.scan_entries(&mut batch) {
                        // TODO: handle error
                        error!("backup scan error"; "error" => ?e);
                        res = Err(e);
                        break;
                    };
                    if batch.len() == 0 {
                        break;
                    }
                    info!("backup scan entries"; "len" => batch.len());
                    // TODO: write to sst before clearing it.
                    batch.clear();
                }
                tx.send(res).map_err(|_| ())
            }))
        }

        for res in res_rx {
            match res {
                Ok(brange) => {
                    info!("backup region finish";
                        "region" => ?brange.region,
                        "start_key" => ?brange.start_key,
                        "end_key" => ?brange.end_key);
                    let mut response = BackupResponse::new();
                    response.set_start_key(brange.start_key.into_encoded());
                    response.set_end_key(brange.end_key.into_encoded());
                    resp.unbounded_send(response);
                }
                Err(e) => error!("backup region failed"; "error" => ?e),
            }
        }
        info!("backup finished"; "take" => ?start.elapsed());
    }
}

impl<E: Engine, R: RegionInfoProvider> Runnable<Task> for Endpoint<E, R> {
    fn run(&mut self, task: Task) {
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
