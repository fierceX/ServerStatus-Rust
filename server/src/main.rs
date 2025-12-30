#![deny(warnings)]
// #![allow(unused)]
#[macro_use]
extern crate log;
extern crate pretty_env_logger;
#[macro_use]
extern crate prettytable;

use clap::Parser;
use once_cell::sync::OnceCell;
use std::process;
use std::sync::Arc;
use std::sync::Mutex;
use std::time::Duration;
use tokio::net::TcpListener;
use tokio::runtime::Handle;
use tokio::signal;
use tokio::runtime::Builder;
use tokio::time;

use axum::{
    http::{Method, Uri},
    response::IntoResponse,
    routing::{get, post},
    Router,
};
use tower_http::cors::{Any, CorsLayer};

mod assets;
mod auth;
mod config;
mod grpc;
mod http;
mod jinja;
mod jwt;
mod notifier;
mod payload;
mod stats;
mod db;

static G_CONFIG: OnceCell<crate::config::Config> = OnceCell::new();
static G_STATS_MGR: OnceCell<crate::stats::StatsMgr> = OnceCell::new();

#[derive(Parser, Debug)]
#[command(author, version = env!("APP_VERSION"), about, long_about = None)]
struct Args {
    #[arg(short, long, default_value = "config.toml")]
    config: String,
    #[arg(short = 't', long, help = "config test, default:false")]
    config_test: bool,
    #[arg(long = "notify-test", help = "notify test, default:false")]
    notify_test: bool,
    #[arg(long = "cloud", help = "cloud mode, load cfg from env var: SRV_CONF")]
    cloud: bool,
}

fn create_app_router() -> Router {
    let cors_layer = CorsLayer::new()
        .allow_methods([Method::GET, Method::POST])
        .allow_origin(Any);
    
    Router::new()
        .route("/report", post(http::report))
        .route("/json/stats.json", get(http::get_stats_json))
        .route("/json/history.json", get(http::get_history_stats))
        .route("/api/admin/authorize", post(jwt::authorize))
        .route("/api/admin/:path", get(http::admin_api))
        .route("/detail", get(http::get_detail))
        .route("/map", get(http::get_map))
        .route("/i", get(http::init_client))
        .route("/", get(assets::index_handler))
        .fallback(fallback)
        .layer(cors_layer)
}

async fn fallback(uri: Uri) -> impl IntoResponse {
    assets::static_handler(uri).await
}
pub async fn shutdown_signal() {
    let ctrl_c = async {
        signal::ctrl_c().await.expect("failed to install Ctrl+C handler");
    };

    #[cfg(unix)]
    let terminate = async {
        signal::unix::signal(signal::unix::SignalKind::terminate())
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

    eprintln!("🛑 Signal received, starting graceful shutdown...");
    eprintln!("⏳ Waiting for existing connections to close... (Press Ctrl+C again to force exit)");

    // 1. 启动看门狗线程（3秒后强杀）
    std::thread::spawn(move || {
        std::thread::sleep(std::time::Duration::from_secs(3));
        eprintln!("💀 Shutdown timed out (3s), forcing exit!");
        std::process::exit(1);
    });

    // 2. 监听第二次 Ctrl+C，实现立即强杀
    tokio::spawn(async {
        signal::ctrl_c().await.ok();
        eprintln!("💀 Forced exit by user!");
        std::process::exit(1);
    });
}


#[tokio::main]
async fn main() -> Result<(), anyhow::Error> {
    pretty_env_logger::init();
    let args = Args::parse();

    eprintln!("✨ {} {}", env!("CARGO_BIN_NAME"), env!("APP_VERSION"));

    // config test
    if args.config_test {
        config::test_from_file(&args.config).unwrap();
        eprintln!("✨ the conf file {} syntax is ok", &args.config);
        eprintln!("✨ the conf file {} test is successful", &args.config);
        process::exit(0);
    }

    // config load
    if let Some(cfg) = if args.cloud {
        eprintln!("✨ run in cloud mode, load config from env");
        config::from_env()
    } else {
        eprintln!("✨ run in normal mode, load conf from local file `{}", &args.config);
        config::from_file(&args.config)
    } {
        debug!("{}", serde_json::to_string_pretty(&cfg).unwrap());
        G_CONFIG.set(cfg).unwrap();
    } else {
        error!("can't parse config");
        process::exit(1);
    }

    // init tpl
    http::init_jinja_tpl().unwrap();

    // init notifier
    *notifier::NOTIFIER_HANDLE.lock().unwrap() = Some(Handle::current());
    let cfg = G_CONFIG.get().unwrap();
    let notifies: Arc<Mutex<Vec<Box<dyn notifier::Notifier + Send>>>> = Arc::new(Mutex::new(Vec::new()));
    
    if cfg.tgbot.enabled {
        let o = Box::new(notifier::tgbot::TGBot::new(&cfg.tgbot));
        notifies.lock().unwrap().push(o);
    }
    if cfg.wechat.enabled {
        let o = Box::new(notifier::wechat::WeChat::new(&cfg.wechat));
        notifies.lock().unwrap().push(o);
    }
    if cfg.email.enabled {
        let o = Box::new(notifier::email::Email::new(&cfg.email));
        notifies.lock().unwrap().push(o);
    }
    if cfg.log.enabled {
        let o = Box::new(notifier::log::Log::new(&cfg.log));
        notifies.lock().unwrap().push(o);
    }
    if cfg.webhook.enabled {
        let o = Box::new(notifier::webhook::Webhook::new(&cfg.webhook));
        notifies.lock().unwrap().push(o);
    }

    // notify test
    if args.notify_test {
        for notifier in &*notifies.lock().unwrap() {
            eprintln!("send test message to {}", notifier.kind());
            notifier.notify_test().unwrap();
        }
        time::sleep(Duration::from_millis(7000)).await;
        eprintln!("Please check for notifications");
        process::exit(0);
    }

    // init mgr
    let mut mgr = crate::stats::StatsMgr::new();
    mgr.init(G_CONFIG.get().unwrap(), notifies)?;
    if G_STATS_MGR.set(mgr).is_err() {
        error!("can't set G_STATS_MGR");
        process::exit(1);
    }
    let db = Arc::new(db::Database::new("stats.db")?);

    // // 修复：使用 spawn_blocking 包裹阻塞的数据库任务，避免卡死 Async Runtime
    // let db_clone = db.clone();
    // tokio::spawn(async move {
    //     let mut interval = time::interval(Duration::from_secs(300));
    //     loop {
    //         interval.tick().await;
    //         let db = db_clone.clone();
    //         // 放到 blocking 线程池运行
    //         let _ = tokio::task::spawn_blocking(move || {
    //             if let Err(e) = db.run_scheduled_aggregation() {
    //                 eprintln!("Error running data aggregation: {}", e);
    //             }
    //         }).await;
    //     }
    // });

    // let db_clone2 = db.clone();
    // tokio::spawn(async move {
    //     let mut interval = time::interval(Duration::from_secs(24 * 60 * 60));
    //     loop {
    //         interval.tick().await;
    //         let db = db_clone2.clone();
    //         // 放到 blocking 线程池运行
    //         let _ = tokio::task::spawn_blocking(move || {
    //             if let Err(e) = db.optimize() {
    //                 eprintln!("Error running data optimize: {}", e);
    //             }
    //         }).await;
    //     }
    // });
    let db_clone = db.clone();
    std::thread::spawn(move || {
        // 手动实现简单的 loop + sleep
        loop {
            // 每5分钟 (300秒)
            std::thread::sleep(Duration::from_secs(300));
            if let Err(e) = db_clone.run_scheduled_aggregation() {
                eprintln!("Error running data aggregation: {}", e);
            }
        }
    });

    let db_clone2 = db.clone();
    std::thread::spawn(move || {
        loop {
            // 每天 (86400秒)
            std::thread::sleep(Duration::from_secs(24 * 60 * 60));
            if let Err(e) = db_clone2.optimize() {
                eprintln!("Error running data optimize: {}", e);
            }
        }
    });

    // serv grpc
    tokio::spawn(async move { grpc::serv_grpc(cfg).await });

    // 创建专用于处理历史数据的线程池
    let history_runtime = Builder::new_multi_thread()
        .worker_threads(4)
        .thread_name("history-worker")
        .enable_all()
        .build()
        .unwrap();
    
    if crate::http::init_history_runtime(history_runtime).is_err() {
        error!("can't set history runtime");
        process::exit(1);
    }

    let http_addr = cfg.http_addr.to_string();
    eprintln!("🚀 listening on http://{http_addr}");

    let listener = TcpListener::bind(&http_addr).await.unwrap();
    axum::serve(listener, create_app_router())
        .with_graceful_shutdown(shutdown_signal())
        .await
        .unwrap();

    Ok(())
}
