// Copyright 2020 TiKV Project Authors. Licensed under Apache-2.0.

use crate::storage::mvcc::{
    metrics::{MVCC_CONFLICT_COUNTER, MVCC_DUPLICATE_CMD_COUNTER_VEC},
    ErrorInner, LockType, MvccTxn, Result as MvccResult,
};
use crate::storage::Snapshot;
use txn_types::{is_short_value, Mutation, MutationType, TimeStamp, Write, WriteType};

pub fn deterministic_write<S: Snapshot>(
    txn: &mut MvccTxn<S>,
    mutation: Mutation,
    commit_ts: TimeStamp,
) -> MvccResult<()> {
    // Currently no normal 2PC transaction is allowed to run with deterministic transaction.
    // Skip lock checking.

    if let Some((written_commit_ts, write)) =
        txn.reader.seek_write(mutation.key(), TimeStamp::max())?
    {
        if written_commit_ts > txn.start_ts {
            MVCC_CONFLICT_COUNTER.stale_deterministic_write.inc();
            return Err(ErrorInner::WriteConflict {
                start_ts: txn.start_ts,
                conflict_start_ts: write.start_ts,
                conflict_commit_ts: written_commit_ts,
                key: mutation.key().to_raw()?,
                primary: vec![],
            }
            .into());
        }
        if written_commit_ts == commit_ts {
            // Is here other cases?
            assert_eq!(write.start_ts, txn.start_ts);
            MVCC_DUPLICATE_CMD_COUNTER_VEC.deterministic_write.inc();
            return Ok(());
        }
    }

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
    txn.put_write(key, commit_ts, write.as_ref().to_bytes());

    Ok(())
}
