use anyhow::Result;
use std::{str::FromStr};
use tonic::{
    transport::{Certificate, Identity, Server, ServerTlsConfig},
    Request, Response, Status,Streaming,
};

use stat_common::server_status;
use stat_common::server_status::server_status_server::{ServerStatus, ServerStatusServer};
use stat_common::server_status::StatRequest;
// use futures::Stream; // 引入 Stream trait
use tokio_stream::wrappers::ReceiverStream;
use tokio::sync::mpsc; // 引入异步通道
use crate::config::Config;
use crate::G_CONFIG;
use crate::G_STATS_MGR;

#[derive(Default)]
pub struct ServerStatusSrv {}

// type ResponseStream = Pin<Box<dyn Stream<Item = Result<server_status::Response, Status>> + Send>>;
type ResponseStream = ReceiverStream<Result<server_status::Response, Status>>;

#[tonic::async_trait]
impl ServerStatus for ServerStatusSrv {
    type ReportStream = ResponseStream;
    async fn report(&self, request: Request<Streaming<StatRequest>>) -> Result<Response<ResponseStream>, Status> {
        // 1. 获取输入流
        let mut in_stream = request.into_inner();

        // 2. 创建输出流的通道 (用于给客户端回传 ACK)
        // 缓冲区设为 16 即可，通常只需要偶尔回复，或者是每收到一条回复一条
        let (tx, rx) = mpsc::channel(16);

        // 3. 启动一个异步任务来处理这个连接的流数据
        tokio::spawn(async move {
            // 循环读取流中的每一条消息
            while let Ok(Some(stat)) = in_stream.message().await {
                
                // 将数据处理逻辑放入 blocking 线程，防止卡死 gRPC 所在的 Async Runtime
                // 因为 StatsMgr 内部用了同步锁和同步 channel
                let process_result = tokio::task::spawn_blocking(move || {
                    if let Some(mgr) = G_STATS_MGR.get() {
                        // 序列化为 Value
                        match serde_json::to_value(stat) {
                            Ok(v) => {
                                // 提交给 StatsMgr
                                if let Err(e) = mgr.report(v) {
                                    error!("mgr report failed: {:?}", e);
                                    return false; // 失败
                                }
                                return true; // 成功
                            }
                            Err(err) => {
                                error!("serde_json error: {:?}", err);
                            }
                        }
                    }
                    false
                }).await;

                // 根据处理结果决定是否回复 (可选)
                // 在流式传输中，通常不需要每条都回复，可以累积回复，或者只回复错误
                // 这里为了演示，假设我们每收到一条都回复一个简单的 OK
                match process_result {
                    Ok(_) => {
                        // 构建回复消息
                        let resp = server_status::Response {
                            code: 0,
                            message: "ok".to_string(),
                        };
                        
                        // 发送回复给客户端
                        // 如果发送失败(客户端断开)，退出循环
                        if tx.send(Ok(resp)).await.is_err() {
                            break;
                        }
                    }
                    Err(e) => {
                        error!("Spawn blocking join error: {}", e);
                        break;
                    }
                }
            }
            
            // 流结束或出错，连接断开
            debug!("Client disconnected");
        });

        // 4. 立即返回响应流的 Receiver，握手完成
        Ok(Response::new(ReceiverStream::new(rx)))
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
