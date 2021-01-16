// Copyright 2021 TiKV Project Authors. Licensed under Apache-2.0.

use collections::HashMap;
use txn_types::{Mutation, TimeStamp};

use crate::storage::kv::WriteData;
use crate::storage::lock_manager::LockManager;
use crate::storage::mvcc::MvccTxn;
use crate::storage::txn::commands::{
    Command, CommandExt, ReleasedLocks, ResponsePolicy, TypedCommand, WriteCommand, WriteContext,
    WriteResult,
};
use crate::storage::txn::{deterministic_write, Error, ErrorInner, Result};
use crate::storage::{ProcessResult, Snapshot, TxnStatus};

// To implement Display
#[derive(Debug, Clone)]
pub struct VecU64Wrapper(pub Vec<u64>);

impl std::fmt::Display for VecU64Wrapper {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        std::fmt::Debug::fmt(self, f)
    }
}

command! {
    /// Commit the transaction that started at `lock_ts`.
    ///
    /// This should be following a [`Prewrite`](Command::Prewrite).
    DeterministicWrite:
        cmd_ty => TxnStatus,
        display => "kv::command::deterministic_write({}) {} -> {} | {:?}", (mutations.len, start_ts, commit_ts, ctx),
        content => {
            mutations: Vec<Mutation>,
            start_ts: VecU64Wrapper,
            commit_ts: TimeStamp,
            skip_lock: bool,
        }
}

impl CommandExt for DeterministicWrite {
    ctx!();
    tag!(deterministic_write);
    // ts!(start_ts);

    fn write_bytes(&self) -> usize {
        let mut bytes = 0;
        for m in &self.mutations {
            match *m {
                Mutation::Put((ref key, ref value)) | Mutation::Insert((ref key, ref value)) => {
                    bytes += key.as_encoded().len();
                    bytes += value.len();
                }
                Mutation::Delete(ref key) | Mutation::Lock(ref key) => {
                    bytes += key.as_encoded().len();
                }
                Mutation::CheckNotExists(_) => (),
            }
        }
        bytes
    }

    gen_lock!(mutations: multiple(|x| x.key()));
}

impl<S: Snapshot, L: LockManager> WriteCommand<S, L> for DeterministicWrite {
    fn process_write(self, snapshot: S, context: WriteContext<'_, L>) -> Result<WriteResult> {
        assert_eq!(self.start_ts.0.len(), self.mutations.len());

        let mut txns = HashMap::default();

        let rows = self.mutations.len();

        // To pass into the closure
        let not_fill_cache = self.ctx.get_not_fill_cache();
        let commit_ts = self.commit_ts;

        for (start_ts, m) in self.start_ts.0.into_iter().zip(self.mutations) {
            let start_ts = start_ts.into();

            if self.commit_ts <= start_ts {
                return Err(Error::from(ErrorInner::InvalidTxnTso {
                    start_ts,
                    commit_ts: self.commit_ts,
                }));
            }

            let (txn, released_locks) = txns.entry(start_ts).or_insert_with(|| {
                let t = MvccTxn::new(
                    snapshot.clone(),
                    start_ts,
                    !not_fill_cache,
                    context.concurrency_manager.clone(),
                );
                let l = ReleasedLocks::new(start_ts, commit_ts);
                (t, l)
            });

            released_locks.push(deterministic_write(txn, m, self.commit_ts, self.skip_lock)?);
        }
        // released_locks.wake_up(context.lock_mgr);
        let modifies = txns
            .into_iter()
            .map(|(_, (mut t, l))| {
                l.wake_up(context.lock_mgr);
                context.statistics.add(&t.take_statistics());
                t.into_modifies()
            })
            .flatten()
            .collect();

        let pr = ProcessResult::TxnStatus {
            txn_status: TxnStatus::committed(self.commit_ts),
        };
        let write_data = WriteData::from_modifies(modifies);
        Ok(WriteResult {
            ctx: self.ctx,
            to_be_write: write_data,
            rows,
            pr,
            lock_info: None,
            lock_guards: vec![],
            response_policy: ResponsePolicy::OnApplied,
        })
    }
}
