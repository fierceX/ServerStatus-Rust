// #![allow(unused)]
use std::str::FromStr;
use std::thread;
use std::time::Duration;
use tokio::net::lookup_host;
use tonic::transport::{Certificate, Channel, ClientTlsConfig, Identity};
use tonic::{metadata::MetadataValue, Request};
use tower::timeout::Timeout;
use url::Url;

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

    let timeout_channel = Timeout::new(channel, Duration::from_millis(3000));
    let grpc_client = ServerStatusClient::with_interceptor(timeout_channel, move |mut req: Request<()>| {
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
        // === 核心优化: 传入 monitor 引用进行复用 ===
        // 假设 sample_all 已经改为: fn sample_all(..., monitor: &mut sys_info::Monitor)
        let is_full_report = !args.lite || (report_count % 60 == 0);
        report_count = report_count.wrapping_add(1);
        let stat_rt = sample_all(args, stat_base, &mut monitor,is_full_report);
        
        let mut client = grpc_client.clone();

        tokio::spawn(async move {
            let request = tonic::Request::new(stat_rt);

            match client.report(request).await {
                Ok(resp) => {
                    info!("grpc report resp => {:?}", resp);
                }
                Err(status) => {
                    error!("grpc report status => {:?}", status);
                }
            }
        });

        thread::sleep(Duration::from_secs(args.report_interval));
    }
}
