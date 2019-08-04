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
        req: BackupRequest,
        sink: ServerStreamingSink<BackupResponse>,
    ) {
        // TODO: make it a bounded channel.
        let (tx, rx) = mpsc::unbounded();
        if let Err(status) = match Task::new(req, tx) {
            Ok(task) => self.scheduler.schedule(task).map_err(|e| {
                RpcStatus::new(
                    RpcStatusCode::GRPC_STATUS_INVALID_ARGUMENT,
                    Some(format!("{:?}", e)),
                )
            }),
            Err(e) => Err(RpcStatus::new(
                RpcStatusCode::GRPC_STATUS_UNKNOWN,
                Some(format!("{:?}", e)),
            )),
        } {
            error!("backup task initiate failed"; "error" => ?status);
            ctx.spawn(sink.fail(status).map_err(|e| {
                error!("backup failed to send error"; "error" => ?e);
            }));
            return;
        };

        let send_resp = sink.send_all(rx.then(|resp| match resp {
            Ok(resp) => Ok((resp, WriteFlags::default())),
            Err(e) => {
                error!("backup send failed"; "error" => ?e);
                Err(Error::RpcFailure(RpcStatus::new(
                    RpcStatusCode::GRPC_STATUS_UNKNOWN,
                    Some(format!("{:?}", e)),
                )))
            }
        }));
        ctx.spawn(
            send_resp
                .map(|_s /* the sink */| {
                    info!("backup send half closed");
                })
                .map_err(|e| {
                    error!("backup send failed"; "error" => ?e);
                }),
        );
    }
}
