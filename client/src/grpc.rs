// #![allow(unused)]
use std::str::FromStr;
// use std::thread;
use std::time::Duration;
use tokio::net::lookup_host;
use tokio::sync::mpsc;
use tokio_stream::wrappers::ReceiverStream;
use tonic::transport::{Certificate, Channel, ClientTlsConfig, Identity};
use tonic::{metadata::MetadataValue, Request};
// use tower::timeout::Timeout;
use url::Url;
// use prost::Message;

use stat_common::server_status::server_status_client::ServerStatusClient;
use stat_common::server_status::StatRequest;

// 确保引入 sys_info 模块
use crate::sys_info;
use crate::sample_all;
use crate::Args;

pub async fn report(args: &Args, stat_base: &mut StatRequest) -> anyhow::Result<()> {
    let auth_user: String;
    let ssr_auth: &[u8];
    if args.gid.is_empty() {
        auth_user = args.user.to_string();
        ssr_auth = b"single";
    } else {
        auth_user = args.gid.to_string();
        ssr_auth = b"group";
    }
    let token = MetadataValue::try_from(format!("{}@_@{}", auth_user, args.pass))?;

    let addr = args.addr.replace("grpcs://", "https://");

    if let Ok(u) = Url::parse(&addr) {
        if let Some(host) = u.host_str() {
            let port = u.port().unwrap_or(443);
            
            // lookup_host 是异步非阻塞的，且利用 OS 缓存，开销极低
            // 我们只需要取第一个解析结果即可
            if let Ok(mut addrs) = lookup_host((host, port)).await {
                if let Some(socket_addr) = addrs.next() {
                    // 根据解析结果如实更新状态
                    if socket_addr.is_ipv4() {
                        stat_base.online4 = true;
                    } else if socket_addr.is_ipv6() {
                        stat_base.online6 = true;
                    }
                }
            }
        }
    }
    
    let channel: Channel;
    // mTLS
    if args.mtls {
        let u = Url::parse(addr.as_str())?;

        let tls_dir = std::path::PathBuf::from_str(&args.tls_dir)?;
        let ca = std::fs::read_to_string(tls_dir.join("ca.pem"))?;
        let client_cert = std::fs::read_to_string(tls_dir.join("client.pem"))?;
        let client_key = std::fs::read_to_string(tls_dir.join("client.key"))?;
        let client_identity = Identity::from_pem(client_cert, client_key);
        let ca = Certificate::from_pem(ca);

        let tls = ClientTlsConfig::new()
            .domain_name(u.host_str().expect("invalid domain"))
            .ca_certificate(ca)
            .identity(client_identity);
        channel = Channel::from_shared(addr)?.tls_config(tls)?.connect().await?;
    } else {
        // TLS
        if addr.starts_with("https://") {
            let tls = ClientTlsConfig::new();
            channel = Channel::from_shared(addr)?.tls_config(tls)?.connect().await?;
        } else {
            channel = Channel::from_shared(addr)?.connect().await?;
        }
    }

    let grpc_client = ServerStatusClient::with_interceptor(channel, move |mut req: Request<()>| {
        req.metadata_mut().insert("authorization", token.clone());
        req.metadata_mut()
            .insert("ssr-auth", MetadataValue::try_from(ssr_auth).unwrap());
        Ok(req)
    });

    // === 核心优化: 初始化 Monitor 上下文 ===
    // 只创建一次 System/Disks/Networks 对象
    let mut monitor = sys_info::Monitor::new(); 

    let mut report_count: u64 = 0;


    loop {
        info!("Establishing gRPC stream connection...");

        // 1. 创建一个 MPSC 通道 (缓冲区大小 10 即可)
        let (tx, rx) = mpsc::channel(10);
        
        // 2. 将 Receiver 包装成 gRPC 认可的 Stream
        let request_stream = ReceiverStream::new(rx);

        // 3. 克隆 client 并发起请求
        let mut client = grpc_client.clone();
        
        // 异步发起流式请求
        // 注意：这里只是建立了连接，只要我们持有 tx，就可以一直发
        // let response_future = client.report(request_stream);

        // 4. 启动一个后台任务来等待服务端的最终响应 (或错误)
        // 当流断开时，这个任务会结束
        tokio::spawn(async move {
            // 注意：这里可能需要包一层 Request::new，取决于 tonic 版本和生成的代码
            // 如果报错类型不匹配，请尝试: client.report(Request::new(request_stream)).await
            match client.report(request_stream).await {
                Ok(resp) => info!("Stream closed by server: {:?}", resp),
                Err(e) => error!("Stream error: {:?}", e),
            }
        });

        // === 内层循环：负责每秒采集并发送数据 ===
        loop {
            // 逻辑与之前相同：判断是否全量包
            let is_full_report = !args.lite || (report_count % 60 == 0);
            report_count = report_count.wrapping_add(1);

            // 采集数据
            let stat_rt = sample_all(args, stat_base, &mut monitor, is_full_report);

            // 发送数据到流中
            // 如果 send 返回 Err，说明接收端关闭了（连接断了），我们需要跳出内层循环进行重连
            if tx.send(stat_rt).await.is_err() {
                error!("gRPC stream disconnected, reconnecting...");
                break; // 跳出内层循环 -> 触发外层循环 -> 重建流
            }

            // 成功发送
            if args.debug {
                debug!("Report sent (stream), full: {}", is_full_report);
            }

            // 等待间隔
            // 建议：如果使用流式，这里可以用 tokio::time::sleep 替代 thread::sleep 以便更好地让出 CPU
            tokio::time::sleep(Duration::from_secs(args.report_interval)).await;
        }

        // 避免疯狂重试，断线后稍作等待
        tokio::time::sleep(Duration::from_secs(3)).await;
    }
}
