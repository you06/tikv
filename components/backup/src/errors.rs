// Copyright 2019 TiKV Project Authors. Licensed under Apache-2.0.

use std::io::Error as IoError;
use std::{error, result};

use kvproto::backup::Error as ErrorPb;
use kvproto::errorpb::Error as RegionError;
use kvproto::kvrpcpb::KeyError;
use tikv::storage::kv::Error as EngineError;
use tikv::storage::mvcc::Error as MvccError;
use tikv::storage::txn::Error as TxnError;

impl Into<ErrorPb> for Error {
    fn into(self) -> ErrorPb {
        let mut err = ErrorPb::new();
        match self {
            Error::ClusterID(current, request) => {
                err.mut_cluster_id_error().set_current(current);
                err.mut_cluster_id_error().set_request(request);
            }
            Error::Txn(TxnError::Engine(EngineError::Request(e)))
            | Error::Txn(TxnError::Mvcc(MvccError::Engine(EngineError::Request(e)))) => {
                err.set_region_error(e);
            }
            Error::Txn(TxnError::Mvcc(MvccError::KeyIsLocked {
                primary,
                ts,
                key,
                ttl,
                txn_size,
            })) => {
                let mut e = KeyError::new();
                let info = e.mut_locked();
                info.set_primary_lock(primary);
                info.set_lock_version(ts);
                info.set_key(key);
                info.set_lock_ttl(ttl);
                info.set_txn_size(txn_size);
                err.set_kv_error(e);
            }
            other => {
                err.set_msg(format!("{:?}", other));
            }
        }
        err
    }
}

quick_error! {
    #[derive(Debug)]
    pub enum Error {
        Other(err: Box<dyn error::Error + Sync + Send>) {
            from()
            cause(err.as_ref())
            description(err.description())
            display("{}", err)
        }
        Rocks(err: String) {
            from()
            description("Rocksdb error")
            display("{}", err)
        }
        Io(err: IoError) {
            from()
            cause(err)
            display("{}", err)
            description(err.description())
        }
        Txn(err: TxnError) {
            from()
            display("transaction error {:?}", err)
            description("transaction error")
        }
        ClusterID(current: u64, request: u64) {
            display("current {:?}, request {:?}", current, request)
            description("cluster ID mismatch")
        }
    }
}

pub type Result<T> = result::Result<T, Error>;
