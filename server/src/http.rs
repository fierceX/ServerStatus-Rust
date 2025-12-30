use crate::assets::Asset;
use tokio::task::JoinHandle;
use once_cell::sync::OnceCell;
use tokio::runtime::Runtime;
use axum::extract::{Path, Query};
use axum::{
    body::Bytes,
    http::{header, header::HeaderMap, StatusCode, Uri},
    response::{IntoResponse, Response},
    Json,
};
use minijinja::context;
use prettytable::Table;
use prost::Message;
use serde_json::{json, Value};
use std::collections::HashMap;
use std::fmt::Write as _;

use stat_common::{server_status::StatRequest, utils::bytes2human};

use crate::auth;
use crate::jinja;
use crate::jwt;
use crate::G_CONFIG;
use crate::G_STATS_MGR;

const KIND: &str = "http";

// --- 辅助函数 ---

// 统一的时间范围解析逻辑
fn parse_timerange(params: &HashMap<String, String>) -> (i64, i64) {
    let now = chrono::Utc::now().timestamp();
    let start_time = params
        .get("start_time")
        .and_then(|s| s.parse::<i64>().ok())
        .unwrap_or(now - 600); // 默认10分钟前
    
    let end_time = params
        .get("end_time")
        .and_then(|s| s.parse::<i64>().ok())
        .unwrap_or(now);
    (start_time, end_time)
}

// 获取 StatsMgr 的帮助函数，避免到处写 unwrap
fn get_stats_mgr() -> Option<&'static crate::stats::StatsMgr> {
    G_STATS_MGR.get()
}

// --- Handlers ---

// 优化：使用 Axum 的 Json 包装器，自动处理 Header
pub async fn get_stats_json() -> impl IntoResponse {
    match get_stats_mgr() {
        Some(mgr) => {
            // 注意：这里假设 get_stats_json 返回的是 String。
            // 如果能返回 serde_json::Value 或 struct，直接用 Json() 更好。
            // 维持原逻辑，手动设置 Content-Type 因为返回的是预序列化的 String
            ([(header::CONTENT_TYPE, "application/json")], mgr.get_stats_json())
        },
        None => ([(header::CONTENT_TYPE, "application/json")], "{}".to_string()),
    }
}

static HISTORY_RUNTIME: OnceCell<Runtime> = OnceCell::new();

pub fn init_history_runtime(runtime: Runtime) -> Result<(), Runtime> {
    HISTORY_RUNTIME.set(runtime)
}

pub async fn get_history_stats(Query(params): Query<HashMap<String, String>>) -> impl IntoResponse {
    let (start_time, end_time) = parse_timerange(&params);
    
    // 使用专用线程池
    let runtime = match HISTORY_RUNTIME.get() {
        Some(rt) => rt,
        None => return (
            StatusCode::INTERNAL_SERVER_ERROR, 
            Json(json!({"error": "History runtime not initialized", "code": 500}))
        ).into_response()
    };

    let handle: JoinHandle<Result<String, String>> = runtime.spawn(async move {
        match get_stats_mgr() {
            Some(mgr) => mgr.get_stats_by_timerange(start_time, end_time)
                .map(|stats| serde_json::to_string(&stats).unwrap_or_else(|_| "{}".to_string()))
                .map_err(|e| e.to_string()),
            None => Err("Stats manager not initialized".to_string()),
        }
    });
    
    match handle.await {
        Ok(Ok(json_str)) => (
            [(header::CONTENT_TYPE, "application/json")],
            json_str
        ).into_response(),
        Ok(Err(e)) => (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(json!({ "error": format!("Failed to get stats: {}", e), "code": 500 }))
        ).into_response(),
        Err(e) => (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(json!({ "error": format!("Thread error: {}", e), "code": 500 }))
        ).into_response(),
    }
}

#[allow(unused)]
pub async fn get_site_config_json() -> impl IntoResponse {
    Json(json!({}))
}

// 优化：统一返回 Json<Value>
pub async fn admin_api(_claims: jwt::Claims, Path(path): Path<String>, Query(params): Query<HashMap<String, String>>) -> Json<Value> {
    let mgr = match get_stats_mgr() {
        Some(m) => m,
        None => return Json(json!({ "code": 500, "message": "StatsMgr not ready" })),
    };

    match path.as_str() {
        "stats.json" => {
            if params.contains_key("start_time") || params.contains_key("end_time") {
                let (start_time, end_time) = parse_timerange(&params);
                match mgr.get_stats_by_timerange(start_time, end_time) {
                    Ok(stats) => Json(stats),
                    Err(e) => {
                        error!("Failed to get stats by timerange: {}", e);
                        Json(json!({ "error": format!("Failed to get stats: {}", e), "code": 500 }))
                    }
                }
            } else {
                match mgr.get_all_info() {
                    Ok(resp) => Json(resp),
                    Err(e) => Json(json!({ "error": e.to_string(), "code": 500 }))
                }
            }
        }
        "config.json" => {
            match G_CONFIG.get() {
                Some(cfg) => Json(cfg.to_json_value().unwrap_or(json!({}))),
                None => Json(json!({ "code": 500, "message": "Config not ready" }))
            }
        }
        _ => Json(json!({ "code": 404, "message": "Not found" })),
    }
}

pub fn init_jinja_tpl() -> Result<(), anyhow::Error> {
    // 保持原样，逻辑没问题
    let detail_data = Asset::get("/jinja/detail.jinja.html").expect("detail.jinja.html not found");
    jinja::add_template(KIND, "detail", String::from_utf8(detail_data.data.into())?);

    let map_data = Asset::get("/jinja/map.jinja.html").expect("map.jinja.html not found");
    jinja::add_template(KIND, "map", String::from_utf8(map_data.data.into())?);

    let client_init_sh = Asset::get("/jinja/client-init.jinja.sh").expect("client-init.jinja.sh not found");
    jinja::add_template(KIND, "client-init", String::from_utf8(client_init_sh.data.into())?);
    Ok(())
}

pub async fn init_client(uri: Uri, req_header: HeaderMap, Query(params): Query<HashMap<String, String>>) -> Response {
    let invalid = "".to_string();
    // 使用闭包简化参数获取
    let get_param = |k: &str| params.get(k).unwrap_or(&invalid);

    let pass = get_param("pass");
    let uid = get_param("uid");
    let gid = get_param("gid");
    let alias = get_param("alias");

    if pass.is_empty() || (uid.is_empty() && gid.is_empty()) || (uid.is_empty() && alias.is_empty()) {
        return (StatusCode::UNAUTHORIZED, "Unauthorized").into_response();
    }

    let mut auth_ok = false;
    if let Some(cfg) = G_CONFIG.get() {
        auth_ok = if gid.is_empty() {
            cfg.auth(uid, pass)
        } else {
            cfg.group_auth(gid, pass)
        };
    }
    if !auth_ok {
        return (StatusCode::UNAUTHORIZED, "Unauthorized").into_response();
    }

    // URL 构建逻辑保持不变...
    let mut domain = "localhost".to_string();
    let mut scheme = "http".to_string();
    let mut server_url = String::new();
    let mut workspace = String::new();

    if let Some(cfg) = G_CONFIG.get() {
        server_url = cfg.server_url.to_string();
        workspace = cfg.workspace.to_string();
    }

    if server_url.is_empty() {
        if let Some(v) = uri.scheme() { scheme = v.to_string(); }
        if let Some(v) = req_header.get("x-forwarded-proto").and_then(|v| v.to_str().ok()) { scheme = v.to_string(); }
        if let Some(v) = req_header.get("Host").and_then(|v| v.to_str().ok()) { domain = v.to_string(); }
        if let Some(v) = req_header.get("x-forwarded-host").and_then(|v| v.to_str().ok()) { domain = v.to_string(); }
        server_url = format!("{scheme}://{domain}/report");
    }

    // 参数处理逻辑优化：减少重复代码
    let debug = params.get("debug").map(|p| p == "1").unwrap_or(false);
    let vnstat = params.get("vnstat").map(|p| p == "1").unwrap_or(false);
    let notify = params.get("notify").map(|p| p != "0").unwrap_or(true);
    
    let mut client_opts = format!(r#"-a "{server_url}" -p "{pass}""#);
    
    // 简单的 bool 开关
    if debug { client_opts.push_str(" -d"); }
    if vnstat { client_opts.push_str(" -n"); }
    if params.get("ping").map(|p| p == "0").unwrap_or(false) { client_opts.push_str(" --disable-ping"); }
    if params.get("tupd").map(|p| p == "0").unwrap_or(false) { client_opts.push_str(" --disable-tupd"); }
    if params.get("extra").map(|p| p == "0").unwrap_or(false) { client_opts.push_str(" --disable-extra"); }
    if !notify { client_opts.push_str(" --disable-notify"); }

    // 带值的参数
    if let Ok(w) = get_param("weight").parse::<u64>() { if w > 0 { let _ = write!(client_opts, " -w {w}"); } }
    if let Ok(mr) = get_param("vnstat-mr").parse::<u32>() { if mr > 1 && mr <= 28 { let _ = write!(client_opts, " --vnstat-mr {mr}"); } }
    if let Ok(inv) = get_param("interval").parse::<u32>() { if inv > 0 { let _ = write!(client_opts, " --interval {inv}"); } }
    
    if !gid.is_empty() { let _ = write!(client_opts, r#" -g "{gid}" --alias "{alias}""#); }
    if !uid.is_empty() { let _ = write!(client_opts, r#" -u "{uid}""#); }
    
    // 字符串参数
    for (key, opt) in [
        ("type", "-t"), ("loc", "--location"), ("iface", "--iface"), 
        ("exclude-iface", "--exclude-iface"), ("ip-source", "--ip-source")
    ] {
        let val = get_param(key);
        if !val.is_empty() { let _ = write!(client_opts, r#" {opt} "{val}""#); }
    }

    // 特殊参数
    for key in ["cm", "ct", "cu"] {
        let val = get_param(key);
        if !val.is_empty() && val.contains(':') { let _ = write!(client_opts, r#" --{key} "{val}""#); }
    }

    let cn = params.get("cn").map(|p| p == "1").unwrap_or(false);

    // 渲染，虽然有一定CPU消耗，但字符串拼接通常较快，暂不放入 spawn_blocking
    jinja::render_template(
        KIND,
        "client-init",
        context!(
            pass => pass, uid => uid, gid => gid, alias => alias,
            vnstat => vnstat, weight => get_param("weight"), cn => cn,
            domain => domain, scheme => scheme,
            server_url => server_url, workspace => workspace,
            client_opts => client_opts,
            pkg_version => env!("CARGO_PKG_VERSION"),
        ),
        false,
    )
    .map(|contents| {
        (
            [
                (header::CONTENT_TYPE, "text/x-sh"),
                (header::CONTENT_DISPOSITION, r#"attachment; filename="ssr-client-init.sh""#),
            ],
            contents,
        ).into_response()
    })
    .unwrap_or_else(|_| (StatusCode::INTERNAL_SERVER_ERROR, "Internal Error").into_response())
}

// 优化：将 heavy 的模板渲染放入 blocking thread
async fn render_jinja_ht_tpl(tag: &'static str) -> Response {
    let mgr = match get_stats_mgr() {
        Some(m) => m,
        None => return (StatusCode::INTERNAL_SERVER_ERROR, "StatsMgr not ready").into_response(),
    };

    // 1. 先快速获取数据快照，尽量减少在锁内的时间
    // 注意：get_all_info 可能会对数据进行序列化，这个开销在主线程是可以接受的，
    // 但如果数据量极大，也应放入 spawn_blocking。这里假设 get_all_info 较快。
    let info = match mgr.get_all_info() {
        Ok(i) => i,
        Err(_) => return (StatusCode::INTERNAL_SERVER_ERROR, "Failed to get info").into_response(),
    };

    // 2. 将渲染逻辑放入 blocking 线程
    tokio::task::spawn_blocking(move || {
        jinja::render_template(KIND, tag, context!(resp => &info), false)
            .map(|contents| {
                ([(header::CONTENT_TYPE, "text/html; charset=utf-8")], contents).into_response()
            })
            .unwrap_or_else(|_| {
                (StatusCode::INTERNAL_SERVER_ERROR, "Template render error").into_response()
            })
    }).await.unwrap_or_else(|e| {
        error!("Join error: {}", e);
        (StatusCode::INTERNAL_SERVER_ERROR, "Internal server error").into_response()
    })
}

pub async fn get_map(_auth: auth::AdminAuth) -> Response {
    render_jinja_ht_tpl("map").await
}

// 重点优化：get_detail 是 CPU 密集型操作，必须移出 Async 线程
pub async fn get_detail(_auth: auth::AdminAuth) -> Response {
    let mgr = match get_stats_mgr() {
        Some(m) => m,
        None => return (StatusCode::INTERNAL_SERVER_ERROR, "StatsMgr not ready").into_response(),
    };

    // 1. 获取数据的快照 (Clone)
    // 我们只需要 servers 数据，快速获取并克隆出来，尽快释放锁
    let servers = {
        let resp = mgr.get_stats(); // 获取 Arc<Mutex<StatsResp>>
        let guard = resp.lock().unwrap();
        guard.servers.clone() // Clone data so we can release lock and move data to thread
    };

    // 2. 在 blocking 线程中进行大量的字符串格式化和表格生成
    tokio::task::spawn_blocking(move || {
        let mut table = Table::new();
        table.set_titles(row![
            "#", "Id", "节点名", "位置", "在线时间", "IP", "系统信息", "IP信息", "存储信息"
        ]);

        for (idx, host) in servers.iter().enumerate() {
            // 系统信息格式化 (大量字符串拼接)
            let sys_info = host.sys_info.as_ref().map(|o| {
                format!(
                    "version:        {}\nhost_name:      {}\nos_name:        {}\nos_arch:        {}\nos_family:      {}\nos_release:     {}\nkernel_version: {}\ncpu_num:        {}\ncpu_brand:      {}\ncpu_vender_id:  {}",
                    o.version, o.host_name, o.os_name, o.os_arch, o.os_family, o.os_release, o.kernel_version, o.cpu_num, o.cpu_brand, o.cpu_vender_id
                )
            }).unwrap_or_default();

            // 磁盘信息表格生成 (PrettyTable 操作是 CPU 密集的)
            let mut di = String::new();
            if !host.disks.is_empty() {
                let mut t = Table::new();
                t.set_titles(row!["名称", "挂载点", "类型", "总容量", "已用", "可用"]);
                
                // Normal Disks
                for disk in host.disks.iter().filter(|d| d.file_system.to_lowercase() != "zfs" && !d.name.starts_with("zpool-")) {
                    t.add_row(row![
                        disk.name, disk.mount_point, disk.file_system,
                        bytes2human(disk.total, 2, host.si),
                        bytes2human(disk.used, 2, host.si),
                        bytes2human(disk.free, 2, host.si),
                    ]);
                }
                
                // ZFS Pools
                let zfs_pools: Vec<_> = host.disks.iter().filter(|d| d.name.starts_with("zpool-")).collect();
                if !zfs_pools.is_empty() {
                    t.add_row(row!["--- ZFS ---", "---", "---", "---", "---", "---"]);
                    for pool in zfs_pools {
                        let usage = if pool.total > 0 { (pool.used as f64 * 100.0 / pool.total as f64).round() } else { 0.0 };
                        t.add_row(row![
                            pool.name.strip_prefix("zpool-").unwrap_or(&pool.name),
                            pool.mount_point, "ZFS",
                            bytes2human(pool.total, 2, host.si),
                            format!("{} ({}%)", bytes2human(pool.used, 2, host.si), usage),
                            bytes2human(pool.free, 2, host.si),
                        ]);
                    }
                }
                di = t.to_string();
            }

            // IP Info 格式化
            let (query_ip, addrs, isp) = if let Some(ip_info) = &host.ip_info {
                let addrs = [ip_info.continent.as_str(), ip_info.country.as_str(), ip_info.region_name.as_str(), ip_info.city.as_str()]
                    .iter().map(|s| s.trim()).filter(|s| !s.is_empty()).collect::<Vec<_>>().join("/");
                
                let isp_str = [ip_info.isp.as_str(), ip_info.org.as_str(), ip_info.r#as.as_str(), ip_info.asname.as_str()]
                    .iter().map(|s| s.trim()).filter(|s| !s.is_empty()).collect::<Vec<_>>().join("\n");
                
                (ip_info.query.clone(), addrs, isp_str)
            } else {
                ("xx.xx.xx.xx".to_string(), String::new(), String::new())
            };

            table.add_row(row![
                idx.to_string(), host.name, host.alias, host.location, host.uptime_str,
                query_ip, sys_info, format!("{addrs}\n{isp}"), di
            ]);
        }

        // 3. 渲染模板
        jinja::render_template(KIND, "detail", context!(pretty_content => table.to_string()), true)
            .map(|contents| {
                ([(header::CONTENT_TYPE, "text/html; charset=utf-8")], contents).into_response()
            })
            .unwrap_or_else(|_| {
                (StatusCode::INTERNAL_SERVER_ERROR, "Render error").into_response()
            })
    }).await.unwrap_or_else(|e| {
        error!("Blocking task join error: {}", e);
        (StatusCode::INTERNAL_SERVER_ERROR, "Internal Server Error").into_response()
    })
}

// report
pub async fn report(_auth: auth::HostAuth, req_header: HeaderMap, body: Bytes) -> impl IntoResponse {
    let content_type = req_header.get(header::CONTENT_TYPE)
        .and_then(|v| v.to_str().ok())
        .unwrap_or("");

    let json_data = if content_type.starts_with("application/octet-stream") {
        match StatRequest::decode(body) {
            Ok(stat) => match serde_json::to_value(stat) {
                Ok(v) => Some(v),
                Err(e) => { error!("Protobuf to Json failed: {:?}", e); None }
            },
            Err(e) => { error!("Invalid pb data: {:?}", e); None }
        }
    } else if content_type.starts_with("application/json") {
        match serde_json::from_slice(&body) {
            Ok(v) => Some(v),
            Err(e) => { error!("Invalid json data: {:?}", e); None }
        }
    } else {
        return StatusCode::UNSUPPORTED_MEDIA_TYPE;
    };

    match json_data {
        Some(data) => {
            if let Some(mgr) = get_stats_mgr() {
                if mgr.report(data).is_err() {
                    return StatusCode::BAD_REQUEST;
                }
                StatusCode::OK
            } else {
                StatusCode::INTERNAL_SERVER_ERROR
            }
        },
        None => StatusCode::BAD_REQUEST
    }
}
