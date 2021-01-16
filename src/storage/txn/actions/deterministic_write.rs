// Copyright 2021 TiKV Project Authors. Licensed under Apache-2.0.

use crate::storage::mvcc::{
    metrics::{MVCC_CONFLICT_COUNTER, MVCC_DUPLICATE_CMD_COUNTER_VEC},
    ErrorInner, LockType, MvccTxn, ReleasedLock, Result as MvccResult,
};
use crate::storage::Snapshot;
use txn_types::{is_short_value, Mutation, MutationType, TimeStamp, Write, WriteType};

pub fn deterministic_write<S: Snapshot>(
    txn: &mut MvccTxn<S>,
    mutation: Mutation,
    commit_ts: TimeStamp,
    skip_lock: bool,
) -> MvccResult<Option<ReleasedLock>> {
    // Currently no normal 2PC transaction is allowed to run with deterministic transaction.

    let has_lock = match txn.reader.load_lock(mutation.key())? {
        Some(lock) if lock.ts == txn.start_ts => {
            if lock.lock_type != LockType::Deterministic {
                return Err(ErrorInner::LockTypeNotMatch {
                    start_ts: txn.start_ts,
                    key: mutation.key().to_raw()?,
                    pessimistic: false,
                }
                .into());
            }
            true
        }
        l => {
            // Copied from action/commit.rs
            match txn
                .reader
                .get_txn_commit_record(mutation.key(), txn.start_ts)?
                .info()
            {
                Some((_, WriteType::Rollback)) | None => {
                    if skip_lock {
                        if let Some(l) = l {
                            return Err(ErrorInner::KeyIsLocked(
                                l.into_lock_info(mutation.key().to_raw()?),
                            )
                            .into());
                        }
                    } else {
                        MVCC_CONFLICT_COUNTER.commit_lock_not_found.inc();
                        // None: related Rollback has been collapsed.
                        // Rollback: rollback by concurrent transaction.
                        info!(
                            "deterministic_write: lock not found";
                            "key" => %mutation.key(),
                            "start_ts" => txn.start_ts,
                            "commit_ts" => commit_ts,
                        );
                        return Err(ErrorInner::TxnLockNotFound {
                            start_ts: txn.start_ts,
                            commit_ts,
                            key: mutation.key().to_raw()?,
                        }
                        .into());
                    }
                }
                // Committed by concurrent transaction.
                Some((_, WriteType::Put))
                | Some((_, WriteType::Delete))
                | Some((_, WriteType::Lock)) => {
                    MVCC_DUPLICATE_CMD_COUNTER_VEC.deterministic_write.inc();
                    return Ok(None);
                }
            };
            false
        }
    };

    // TODO: This is unnecessary after completely implementing the lock mechanism (with
    // coinflict check).
    // if let Some((written_commit_ts, write)) =
    //     txn.reader.seek_write(mutation.key(), TimeStamp::max())?
    // {
    //     if written_commit_ts > txn.start_ts {
    //         MVCC_CONFLICT_COUNTER.stale_deterministic_write.inc();
    //         return Err(ErrorInner::WriteConflict {
    //             start_ts: txn.start_ts,
    //             conflict_start_ts: write.start_ts,
    //             conflict_commit_ts: written_commit_ts,
    //             key: mutation.key().to_raw()?,
    //             primary: vec![],
    //         }
    //         .into());
    //     }
    //     if written_commit_ts == commit_ts {
    //         // Is here other cases?
    //         assert_eq!(write.start_ts, txn.start_ts);
    //         MVCC_DUPLICATE_CMD_COUNTER_VEC.deterministic_write.inc();
    //         return Ok(None);
    //     }
    // }

    match mutation.mutation_type() {
        MutationType::Put | MutationType::Lock | MutationType::Delete => {}
        MutationType::Insert => {
            // Insert should not exist in this phase of deterministic transaction. But ignore it for
            // the time being to quickly produce a runnable demo.
            // panic!("insert happens in deterministic write. mutation: {:?}", mutation)
        }
        MutationType::Other => {
            panic!("unknown mutation type. mutation: {:?}", mutation);
        }
    }

    let write_type =
        WriteType::from_lock_type(LockType::from_mutation(&mutation).unwrap()).unwrap();
    let (key, mut value) = mutation.into_key_value();

    let short_value = if value.as_ref().map_or(false, |v| is_short_value(v)) {
        value.take()
    } else {
        None
    };

    let write = Write::new(write_type, txn.start_ts, short_value);

    if let Some(value) = value {
        txn.put_value(key.clone(), txn.start_ts, value);
    }
    txn.put_write(key.clone(), commit_ts, write.as_ref().to_bytes());

    Ok(if has_lock {
        txn.unlock_key(key, true)
    } else {
        None
    })
}
