use anyhow::Result;
use std::str::FromStr;
use tonic::{
    transport::{Certificate, Identity, Server, ServerTlsConfig},
    Request, Response, Status,
};

use stat_common::server_status;
use stat_common::server_status::server_status_server::{ServerStatus, ServerStatusServer};
use stat_common::server_status::StatRequest;

use crate::config::Config;
use crate::G_CONFIG;
use crate::G_STATS_MGR;

#[derive(Default)]
pub struct ServerStatusSrv {}

#[tonic::async_trait]
impl ServerStatus for ServerStatusSrv {
    async fn report(&self, request: Request<StatRequest>) -> Result<Response<server_status::Response>, Status> {
        // 优化 1: 将可能阻塞的操作放到 blocking 线程池
        // 如果直接在 async fn 中调用 mgr.report (如果是同步send)，当队列满时会卡死 gRPC 线程
        let req_data = request.into_inner();
        
        tokio::task::spawn_blocking(move || {
            if let Some(mgr) = G_STATS_MGR.get() {
                // 这里的序列化和 channel 发送都是同步/CPU密集型操作，
                // 放到 spawn_blocking 里最安全
                match serde_json::to_value(req_data) {
                    Ok(v) => {
                        // 即使队列满了阻塞，也只会阻塞这个 blocking 线程，不会卡死 gRPC 服务
                        if let Err(e) = mgr.report(v) {
                             error!("mgr report failed: {:?}", e);
                        }
                    }
                    Err(err) => {
                        error!("serde_json::to_value err => {:?}", err);
                    }
                }
            }
        }); 
        // 注意：spawn_blocking 是异步的，这里不等待结果直接返回 OK，提高吞吐量
        // 探针上报通常允许偶尔的丢失，不需要严格等待落库结果

        Ok(Response::new(server_status::Response {
            code: 0,
            message: "ok".to_string(),
        }))
    }
}

fn check_auth(req: Request<()>) -> Result<Request<()>, Status> {
    // 优化 2: 零分配鉴权，避免 collect::<Vec>
    let mut group_auth = false;
    if let Some(v) = req.metadata().get("ssr-auth") {
        if let Ok(s) = v.to_str() {
            group_auth = s == "group";
        }
    }

    match req.metadata().get("authorization") {
        Some(token) => {
            let token_str = token.to_str().map_err(|_| Status::unauthenticated("invalid token format"))?;
            
            // 使用迭代器而不是 collect Vec，减少内存分配
            let mut parts = token_str.splitn(2, "@_@");
            let user = parts.next();
            let pass = parts.next();

            match (user, pass) {
                (Some(u), Some(p)) => {
                     if let Some(cfg) = G_CONFIG.get() {
                        let verified = if group_auth {
                            cfg.group_auth(u, p)
                        } else {
                            cfg.auth(u, p)
                        };

                        if verified {
                            return Ok(req);
                        }
                    }
                }
                _ => {} // 格式不对，进入下方错误处理
            }

            Err(Status::unauthenticated("invalid user/group && pass"))
        }
        _ => Err(Status::unauthenticated("missing authorization header")),
    }
}

// 定义一个内部的 shutdown 信号监听，与 main.rs 类似但独立
async fn grpc_shutdown_signal() {
    let ctrl_c = async {
        let _ = tokio::signal::ctrl_c().await;
    };

    #[cfg(unix)]
    let terminate = async {
        let _ = tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())
            .expect("failed to install signal handler")
            .recv()
            .await;
    };

    #[cfg(not(unix))]
    let terminate = std::future::pending::<()>();

    tokio::select! {
        _ = ctrl_c => {},
        _ = terminate => {},
    }
    eprintln!("🛑 gRPC server received shutdown signal");
}

pub async fn serv_grpc(cfg: &Config) -> anyhow::Result<()> {
    let sock_addr = cfg.grpc_addr.parse().unwrap();
    let sss = ServerStatusSrv::default();
    let svc = ServerStatusServer::with_interceptor(sss, check_auth);

    // 构建 Server Builder
    let mut builder = Server::builder();
    
    // TLS 配置
    if cfg.grpc_tls > 0 {
        let tls_dir = std::path::PathBuf::from_str(&cfg.tls_dir)?;
        
        // 这里的 fs::read 是阻塞的，但只在启动时执行一次，可以接受
        // 如果追求极致，可用 tokio::fs
        let cert = std::fs::read_to_string(tls_dir.join("server.pem"))?;
        let key = std::fs::read_to_string(tls_dir.join("server.key"))?;
        let identity = Identity::from_pem(cert, key);

        let mut tls_config = ServerTlsConfig::new().identity(identity);
        
        let mut proto = " + TLS";
        if cfg.grpc_tls > 1 {
            let ca = Certificate::from_pem(std::fs::read_to_string(tls_dir.join("ca.pem"))?);
            tls_config = tls_config.client_ca_root(ca);
            proto = " + mTLS";
        }
        builder = builder.tls_config(tls_config)?;
        eprintln!("🚀 listening on grpc://{sock_addr}{proto}");
    } else {
        eprintln!("🚀 listening on grpc://{sock_addr}");
        // 如果是 HTTP1 (非 TLS 且需要兼容性)
        builder = builder.accept_http1(true);
    }

    // 优化 3: 使用 serve_with_shutdown
    // 这样当 Ctrl+C 发生时，gRPC 服务会停止接收新请求，并等待当前请求处理完毕后返回
    builder
        .add_service(svc)
        .serve_with_shutdown(sock_addr, grpc_shutdown_signal())
        .await
        .map_err(anyhow::Error::new)
}
