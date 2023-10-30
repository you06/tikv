// Copyright 2020 TiKV Project Authors. Licensed under Apache-2.0.

// #[PerformanceCriticalPath]
use txn_types::Key;

use crate::storage::{
    kv::WriteData,
    lock_manager::LockManager,
    mvcc::{MvccTxn, SnapshotReader},
    txn::{
        commands::{
            Command, CommandExt, ReadCommand, ReaderWithStats, ReleasedLocks, ResponsePolicy,
            TypedCommand, WriteCommand, WriteContext, WriteResult,
        },
        commit, Error, ErrorInner, ProcessResult, Result,
    },
    Snapshot, Statistics, TxnStatus,
};

command! {
    /// Commit the transaction that started at `lock_ts`.
    ///
    /// This should be following a [`Prewrite`](Command::Prewrite).
    Commit:
        cmd_ty => TxnStatus,
        display => "kv::command::commit {:?} {} -> {} | {:?}", (keys, lock_ts, commit_ts, ctx),
        content => {
            /// The keys affected.
            keys: Vec<Key>,
            /// The lock timestamp.
            lock_ts: txn_types::TimeStamp,
            /// The commit timestamp.
            commit_ts: txn_types::TimeStamp,
            /// The bound hint, hack.
            bound: Vec<Key>,
        }
}

impl CommandExt for Commit {
    ctx!();
    tag!(commit);
    request_type!(KvCommit);
    ts!(commit_ts);
    write_bytes!(keys: multiple);
    gen_lock!(keys: multiple);

    fn readonly(&self) -> bool {
        self.keys.len() == 0 && self.bound.len() == 2
    }
}

impl<S: Snapshot> ReadCommand<S> for Commit {
    fn process_read(self, snapshot: S, statistics: &mut Statistics) -> Result<ProcessResult> {
        if self.keys.len() != 0 || self.bound.len() != 2 {
            unreachable!();
        }

        let mut reader = SnapshotReader::new_with_ctx(self.lock_ts, snapshot, &self.ctx);
        let keys = reader.load_lock_keys(&self.bound[0], &self.bound[1], self.lock_ts)?;
        statistics.add(&reader.take_statistics());
        let execution_duration_limit = if self.ctx.max_execution_duration_ms == 0 {
            crate::storage::txn::scheduler::DEFAULT_EXECUTION_DURATION_LIMIT
        } else {
            ::std::time::Duration::from_millis(self.ctx.max_execution_duration_ms)
        };
        let deadline = ::tikv_util::deadline::Deadline::from_now(execution_duration_limit);
        println!("generate self.keys: {}", keys.len());
        return Ok(ProcessResult::NextCommand {
            cmd: Command::Commit(Commit {
                ctx: self.ctx,
                deadline,
                keys,
                lock_ts: self.lock_ts,
                commit_ts: self.commit_ts,
                bound: vec![],
            }),
        });
    }
}

impl<S: Snapshot, L: LockManager> WriteCommand<S, L> for Commit {
    fn process_write(self, snapshot: S, context: WriteContext<'_, L>) -> Result<WriteResult> {
        println!("process commit with keys: {}", self.keys.len());
        if self.commit_ts <= self.lock_ts {
            return Err(Error::from(ErrorInner::InvalidTxnTso {
                start_ts: self.lock_ts,
                commit_ts: self.commit_ts,
            }));
        }
        let mut txn = MvccTxn::new(self.lock_ts, context.concurrency_manager);
        let mut reader = ReaderWithStats::new(
            SnapshotReader::new_with_ctx(self.lock_ts, snapshot, &self.ctx),
            context.statistics,
        );

        let rows = self.keys.len();

        // Pessimistic txn needs key_hashes to wake up waiters
        let mut released_locks = ReleasedLocks::new();
        for k in self.keys {
            released_locks.push(commit(&mut txn, &mut reader, k, self.commit_ts)?);
        }

        let pr = ProcessResult::TxnStatus {
            txn_status: TxnStatus::committed(self.commit_ts),
        };
        let new_acquired_locks = txn.take_new_locks();
        let mut write_data = WriteData::from_modifies(txn.into_modifies());
        write_data.set_allowed_on_disk_almost_full();
        Ok(WriteResult {
            ctx: self.ctx,
            to_be_write: write_data,
            rows,
            pr,
            lock_info: vec![],
            released_locks,
            new_acquired_locks,
            lock_guards: vec![],
            response_policy: ResponsePolicy::OnApplied,
        })
    }
}
