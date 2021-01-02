// Copyright 2021 TiKV Project Authors. Licensed under Apache-2.0.

use kvproto::kvrpcpb::{CheckpointEntry, CheckpointRequestType, Context};
use std::sync::{Arc, Mutex};
use txn_types::{Key, TimeStamp};

use crate::storage::{Engine, Result};

#[derive(Clone, Copy, Debug)]
enum CheckpointType {
    Start,
    Commit,
    Rollback,
}

impl From<CheckpointRequestType> for CheckpointType {
    fn from(t: CheckpointRequestType) -> CheckpointType {
        match t {
            CheckpointRequestType::CheckpointStart => CheckpointType::Start,
            CheckpointRequestType::CheckpointCommit => CheckpointType::Commit,
            CheckpointRequestType::CheckpointRollback => CheckpointType::Rollback,
        }
    }
}

impl Into<CheckpointRequestType> for CheckpointType {
    fn into(self) -> CheckpointRequestType {
        match self {
            CheckpointType::Start => CheckpointRequestType::CheckpointStart,
            CheckpointType::Commit => CheckpointRequestType::CheckpointCommit,
            CheckpointType::Rollback => CheckpointRequestType::CheckpointRollback,
        }
    }
}

#[derive(Clone, Debug)]
struct Checkpoint {
    checkpoint_type: CheckpointType,
    ts: TimeStamp,
}

#[derive(Clone)]
pub struct CheckpointStore<E: Engine> {
    engine: E,
    mutex: Arc<Mutex<()>>,
}

impl<E: Engine> CheckpointStore<E> {
    pub fn from_engine(engine: E) -> Self {
        Self {
            engine,
            mutex: Arc::new(Mutex::new(())),
        }
    }

    pub fn write_checkpoint(
        &self,
        ctx: &Context,
        key: Key,
        start_ts: TimeStamp,
        entry: CheckpointEntry,
    ) -> Result<()> {
        unimplemented!()
    }

    pub fn get_checkpoint(&self, ctx: &Context, key: Key) -> Result<CheckpointEntry> {
        unimplemented!()
    }
}
