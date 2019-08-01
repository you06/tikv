use futures::future::*;
use futures::prelude::*;
use futures::sync::mpsc;
use grpcio::*;
use kvproto::backup::{BackupRequest, BackupResponse};
use kvproto::backup_grpc::*;
use tikv_util::worker::Scheduler;

use super::Task;

#[derive(Clone)]
pub struct Service {
    scheduler: Scheduler<Task>,
}

impl Service {
    pub fn new(scheduler: Scheduler<Task>) -> Service {
        Service { scheduler }
    }
}

impl Backup for Service {
    // TODO: should we make it a server streaming?
    fn backup(
        &mut self,
        ctx: RpcContext,
        stream: RequestStream<BackupRequest>,
        sink: DuplexSink<BackupResponse>,
    ) {
        let scheduler = self.scheduler.clone();
        // TODO: make it a bounded channel.
        let (tx, rx) = mpsc::unbounded();
        let send_resp = sink.send_all(
            rx.take_while(|resp: &Option<_>| {
                // Close the stream once resp is None.
                // TODO: test whether it can close a stream by None.
                Ok(resp.is_some())
            })
            .filter_map(|resp| resp)
            .then(|resp| match resp {
                Ok(resp) => Ok((resp, WriteFlags::default())),
                Err(e) => {
                    error!("backup send failed"; "error" => ?e);
                    Err(Error::RpcFailure(RpcStatus::new(
                        RpcStatusCode::GRPC_STATUS_UNKNOWN,
                        Some(format!("{:?}", e)),
                    )))
                }
            }),
        );
        ctx.spawn(
            send_resp
                .map(|_s /* the sink */| {
                    info!("backup send half closed");
                })
                .map_err(|e| {
                    error!("backup send failed"; "error" => ?e);
                }),
        );
        ctx.spawn(
            stream
                .for_each(move |req| {
                    let task = Task::new(req, tx.clone());
                    scheduler.schedule(task).map_err(|e| {
                        error!("backup schedule failed"; "error" => ?e);
                        Error::RpcFailure(RpcStatus::new(
                            RpcStatusCode::GRPC_STATUS_UNKNOWN,
                            Some(format!("{:?}", e)),
                        ))
                    })
                })
                .map(|_s| {
                    info!("backup recv half closed");
                })
                .map_err(|e| {
                    error!("backup schedule failed"; "error" => ?e);
                }),
        );
    }
}
