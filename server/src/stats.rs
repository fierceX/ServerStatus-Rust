#![allow(unused)]
use anyhow::Result;
use chrono::{Datelike, Local, Timelike};
use lazy_static::lazy_static;
use once_cell::sync::OnceCell;
use serde::Serialize;
use std::borrow::Borrow;
use std::borrow::BorrowMut;
use std::borrow::Cow;
use std::collections::{HashMap, HashSet};
use std::fmt::Write as _;
use std::sync::mpsc::sync_channel;
use std::sync::mpsc::SyncSender;
use std::sync::Arc;
use std::sync::Mutex;
use std::thread;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

// 引入 Tokio 的通道 (用于通知器)
use tokio::sync::mpsc::{channel as tokio_channel, Sender as TokioSender};

use crate::config::Host;
use crate::db::Database;
use crate::notifier::{Event, Notifier};
use crate::payload::{HostStat, StatsResp};

const SAVE_INTERVAL: u64 = 60;

static STAT_SENDER: OnceCell<SyncSender<Cow<HostStat>>> = OnceCell::new();

pub struct StatsMgr {
    resp_json: Arc<Mutex<String>>,
    stats_data: Arc<Mutex<StatsResp>>,
    db: Arc<Database>,
}

#[derive(Serialize)]
struct TimeValue<T> {
    timestamp: i64,
    value: T,
}

#[derive(Serialize)]
struct MemValue {
    timestamp: i64,
    value: f64,
    total: i64,
    used: i64,
}

#[derive(Serialize)]
struct NetValue {
    timestamp: i64,
    value: i64,
    total: i64,
}

#[derive(Serialize)]
struct DiskValue {
    timestamp: i64,
    value: f64,
    total: i64,
    used: i64,
}

impl StatsMgr {
    pub fn new() -> Self {
        let db = Database::new("stats.db").expect("Failed to initialize database");
        Self {
            resp_json: Arc::new(Mutex::new("{}".to_string())),
            stats_data: Arc::new(Mutex::new(StatsResp::new())),
            db: Arc::new(db),
        }
    }

    fn load_last_network(&mut self, hosts_map: &mut HashMap<String, Host>) {
        if let Ok(last_network_data) = self.db.get_last_network_data() {
            for (name, last_in, last_out) in last_network_data {
                if let Some(srv) = hosts_map.get_mut(&name) {
                    srv.last_network_in = last_in;
                    srv.last_network_out = last_out;
                    trace!("{} => last in/out ({}/{}))", &name, last_in, last_out);
                }
            }
            trace!("load network data from database succ!");
        }
    }

    pub fn init(
        &mut self,
        cfg: &'static crate::config::Config,
        notifies: Arc<Mutex<Vec<Box<dyn Notifier + Send>>>>,
    ) -> Result<()> {
        let hosts_map_base = Arc::new(Mutex::new(cfg.hosts_map.clone()));

        if let Ok(mut hosts_map) = hosts_map_base.lock() {
            self.load_last_network(&mut hosts_map);
        }

        let (stat_tx, stat_rx) = sync_channel(512);
        STAT_SENDER.set(stat_tx).unwrap();
        
        let (notifier_tx, notifier_rx) = sync_channel(512);

        // 修复：使用 std::sync::mpsc 代替 Tokio channel，确保 DB 写入线程完全独立于 Async Runtime
        let (persist_tx, persist_rx) = sync_channel::<HostStat>(1024);

        let stat_map: Arc<Mutex<HashMap<String, Cow<HostStat>>>> = Arc::new(Mutex::new(HashMap::new()));
        let db = self.db.clone();

        // 修复：使用 std::thread 运行阻塞的数据库写入，绝不使用 tokio::spawn 跑阻塞代码
        let db_writer = db.clone();
        thread::spawn(move || {
            while let Ok(stat) = persist_rx.recv() {
                if let Err(e) = db_writer.save_stat(&stat) {
                    error!("DB write failed: {}", e);
                }
            }
        });

        // Main Stat Processing Thread
        thread::spawn({
            let hosts_group_map = cfg.hosts_group_map.clone();
            let hosts_map = hosts_map_base.clone();
            let stat_map = stat_map.clone();
            let notifier_tx = notifier_tx.clone();
            let db = db.clone(); 

            move || loop {
                while let Ok(mut stat) = stat_rx.recv() {
                    trace!("recv stat `{:?}", stat);
                    let mut stat_t = stat.to_mut();

                    // 1. Group Logic
                    if !stat_t.gid.is_empty() {
                        if stat_t.alias.is_empty() {
                            stat_t.alias = stat_t.name.to_string();
                        }
                        if let Ok(mut hosts_map) = hosts_map.lock() {
                            let host = hosts_map.get(&stat_t.name);
                            if host.is_none() || !host.unwrap().gid.eq(&stat_t.gid) {
                                if let Some(group) = hosts_group_map.get(&stat_t.gid) {
                                    let mut inst = group.inst_host(&stat_t.name);
                                    if let Some(o) = host {
                                        inst.last_network_in = o.last_network_in;
                                        inst.last_network_out = o.last_network_out;
                                    };
                                    hosts_map.insert(stat_t.name.to_string(), inst);
                                } else {
                                    continue;
                                }
                            }
                        }
                    }

                    // 2. Main Logic
                    if let Ok(mut hosts_map) = hosts_map.lock() {
                        let host_info = hosts_map.get_mut(&stat_t.name);
                        if host_info.is_none() {
                            error!("invalid stat `{:?}", stat_t);
                            continue;
                        }
                        let info = host_info.unwrap();

                        if info.disabled {
                            continue;
                        }

                        if stat_t.location.is_empty() { stat_t.location = info.location.to_string(); }
                        if stat_t.host_type.is_empty() { stat_t.host_type = info.r#type.to_owned(); }
                        stat_t.notify = info.notify && stat_t.notify;
                        stat_t.pos = info.pos;
                        stat_t.disabled = info.disabled;
                        stat_t.weight += info.weight;
                        stat_t.labels = info.labels.to_owned();
                        if !info.alias.is_empty() { stat_t.alias = info.alias.to_owned(); }

                        if !stat_t.vnstat {
                            let local_now = Local::now();
                            if info.last_network_in == 0
                                || (stat_t.network_in != 0 && info.last_network_in > stat_t.network_in)
                                || (local_now.day() == info.monthstart && local_now.hour() == 0 && local_now.minute() < 5)
                            {
                                info.last_network_in = stat_t.network_in;
                                info.last_network_out = stat_t.network_out;
                                if let Err(e) = db.update_last_network(&stat_t.name, stat_t.network_in, stat_t.network_out) {
                                    error!("Failed to update last network data: {}", e);
                                }
                            } else {
                                stat_t.last_network_in = info.last_network_in;
                                stat_t.last_network_out = info.last_network_out;
                            }
                        }

                        let day = (stat_t.uptime as f64 / 3600.0 / 24.0) as i64;
                        if day > 0 {
                            stat_t.uptime_str = format!("{day} 天");
                        } else {
                            stat_t.uptime_str = format!("{:02}:{:02}:{:02}", (stat_t.uptime as f64 / 3600.0) as i64, (stat_t.uptime as f64 / 60.0) as i64 % 60, stat_t.uptime % 60);
                        }

                        info!("update stat `{:?}", stat_t);

                        // 3. Update State & Notify
                        if let Ok(mut host_stat_map) = stat_map.lock() {
                            let mut need_notify = false;
                            // 定义缓存变量
                            let mut cached_ip_info = None;
                            let mut cached_sys_info = None;

                            // 检查是否存在上一条记录
                            if let Some(pre_stat) = host_stat_map.get(&stat_t.name) {
                                // 【新增】如果当前包没有 ip_info，使用上一条的缓存
                                if stat_t.ip_info.is_none() {
                                    cached_ip_info = pre_stat.ip_info.clone();
                                }
                                
                                // 【新增】如果当前包没有 sys_info (包含 version)，使用上一条的缓存
                                if stat_t.sys_info.is_none() {
                                    cached_sys_info = pre_stat.sys_info.clone();
                                }

                                // 掉线重连通知检查
                                if stat_t.notify && (pre_stat.latest_ts + cfg.offline_threshold < stat_t.latest_ts) {
                                    need_notify = true;
                                }
                            }

                            // 【新增】回填缓存数据到当前状态
                            if let Some(ip_info) = cached_ip_info {
                                stat_t.ip_info = Some(ip_info);
                            }
                            if let Some(sys_info) = cached_sys_info {
                                stat_t.sys_info = Some(sys_info);
                            }

                            let mut ip_info_to_copy = None;
                            
                            if let Some(pre_stat) = host_stat_map.get(&stat_t.name) {
                                if stat_t.ip_info.is_none() { ip_info_to_copy = pre_stat.ip_info.clone(); }
                                if stat_t.notify && (pre_stat.latest_ts + cfg.offline_threshold < stat_t.latest_ts) {
                                    need_notify = true;
                                }
                            }
                            
                            if let Some(ip_info) = ip_info_to_copy { stat_t.ip_info = Some(ip_info); }
                            
                            let stat_clone: Cow<'static, HostStat> = Cow::Owned(stat_t.clone());
                            if need_notify {
                                notifier_tx.send((Event::NodeUp, stat_clone.clone()));
                            }
                            host_stat_map.insert(stat_t.name.to_string(), stat_clone);
                        }
                        
                        // 修复：将 persist_tx.send 移出 Mutex 锁范围
                        // 即使数据库写入变慢导致 channel 满，也不会阻塞持有 Mutex 的线程
                        // 从而避免阻塞 Timer 线程（该线程需要获取同一个 Mutex）
                        if let Err(_) = persist_tx.try_send(stat_t.clone()) {
                            // 如果队列满，丢弃该条数据或记录错误，绝不阻塞主循环
                            error!("DB persist queue full, dropping stat for {}", stat_t.name);
                        }
                    }
                }
            }
        });

        // Timer Task
        tokio::spawn({
            let resp_json = self.resp_json.clone();
            let stats_data = self.stats_data.clone();
            let hosts_map = hosts_map_base.clone();
            let stat_map = stat_map.clone();
            let notifier_tx = notifier_tx.clone();
            
            async move {
                let mut interval = tokio::time::interval(Duration::from_millis(500));
                let mut latest_notify_ts = 0_u64;
                let mut latest_group_gc = 0_u64;
                let mut last_serialized_ts = 0_u64;

                loop {
                    interval.tick().await;

                    let mut resp = StatsResp::new();
                    let now = resp.updated;
                    let mut notified = false;
                    let mut data_changed = false;

                    if latest_group_gc + cfg.group_gc < now {
                        latest_group_gc = now;
                        let mut map_changed = false;
                        if let Ok(mut hosts_map) = hosts_map.lock() {
                            let before = hosts_map.len();
                            hosts_map.retain(|_, o| o.gid.is_empty() || o.latest_ts + cfg.group_gc >= now);
                            if hosts_map.len() != before { map_changed = true; }
                        }
                        if let Ok(mut stat_map) = stat_map.lock() {
                            let before = stat_map.len();
                            stat_map.retain(|_, o| o.gid.is_empty() || o.latest_ts + cfg.group_gc >= now);
                            if stat_map.len() != before { map_changed = true; }
                        }
                        if map_changed { data_changed = true; }
                    }

                    if let Ok(mut host_stat_map) = stat_map.lock() {
                        for (_, stat) in host_stat_map.iter_mut() {
                            if stat.disabled {
                                resp.servers.push(stat.as_ref().clone());
                                continue;
                            }
                            let stat = stat.borrow_mut();
                            let o = stat.to_mut();
                            
                            let was_online = o.online4 || o.online6;
                            if o.latest_ts + cfg.offline_threshold < now {
                                o.online4 = false;
                                o.online6 = false;
                            }
                            if was_online != (o.online4 || o.online6) { data_changed = true; }

                            if !o.labels.contains("os=") {
                                const OS_LIST: [&str; 10] = ["centos", "debian", "ubuntu", "arch", "windows", "macos", "pi", "android", "linux", "freebsd"];
                                if let Some(sys_info) = &o.sys_info {
                                    let os_r = format!("{} {}", sys_info.os_release.to_lowercase(), sys_info.os_name.to_lowercase());
                                    for s in OS_LIST.iter() {
                                        if os_r.contains(s) {
                                            if o.labels.is_empty() { write!(o.labels, "os={s}"); } else { write!(o.labels, ";os={s}"); }
                                            break;
                                        }
                                    }
                                }
                            }

                            if o.notify {
                                if latest_notify_ts + cfg.notify_interval < now {
                                    if o.online4 || o.online6 {
                                        notifier_tx.send((Event::Custom, stat.clone()));
                                    } else {
                                        o.disabled = true;
                                        notifier_tx.send((Event::NodeDown, stat.clone()));
                                        data_changed = true;
                                    }
                                    notified = true;
                                }
                            }
                            resp.servers.push(stat.as_ref().clone());
                        }
                        if notified { latest_notify_ts = now; }
                    }

                    resp.servers.sort_by(|a, b| {
                        if a.weight != b.weight { return a.weight.cmp(&b.weight).reverse(); }
                        if a.pos != b.pos { return a.pos.cmp(&b.pos); }
                        a.alias.cmp(&b.alias)
                    });

                    let max_server_ts = resp.servers.iter().map(|s| s.latest_ts).max().unwrap_or(0);
                    if data_changed || max_server_ts > last_serialized_ts {
                        if let Ok(mut o) = resp_json.lock() {
                            *o = serde_json::to_string(&resp).unwrap();
                        }
                        last_serialized_ts = max_server_ts;
                    }
                    if let Ok(mut o) = stats_data.lock() { *o = resp; }
                }
            }
        });

        thread::spawn(move || loop {
            while let Ok(msg) = notifier_rx.recv() {
                let (e, stat) = msg;
                let notifiers = &*notifies.lock().unwrap();
                for notifier in notifiers {
                    notifier.notify(&e, stat.borrow());
                }
            }
        });

        Ok(())
    }

    pub fn get_stats(&self) -> Arc<Mutex<StatsResp>> {
        self.stats_data.clone()
    }

    pub fn get_stats_json(&self) -> String {
        self.resp_json.lock().unwrap().to_string()
    }

    pub fn report(&self, data: serde_json::Value) -> Result<()> {
        lazy_static! {
            static ref SENDER: SyncSender<Cow<'static, HostStat>> = STAT_SENDER.get().unwrap().clone();
        }

        match serde_json::from_value(data) {
            Ok(stat) => {
                trace!("send stat => {:?} ", stat);
                SENDER.send(Cow::Owned(stat));
            }
            Err(err) => {
                error!("report error => {:?}", err);
            }
        };
        Ok(())
    }

    pub fn get_all_info(&self) -> Result<serde_json::Value> {
        let data = self.stats_data.lock().unwrap();
        let mut resp_json = serde_json::to_value(&*data)?;
        if let Some(srv_list) = resp_json["servers"].as_array_mut() {
            for (idx, stat) in data.servers.iter().enumerate() {
                if let Some(srv) = srv_list[idx].as_object_mut() {
                    srv.insert("ip_info".into(), serde_json::to_value(stat.ip_info.as_ref())?);
                    srv.insert("sys_info".into(), serde_json::to_value(stat.sys_info.as_ref())?);
                    if !stat.disks.is_empty() {
                        srv.insert("disks".into(), serde_json::to_value(&stat.disks)?);
                    }
                }
            }
        }
        Ok(resp_json)
    }
    
    pub fn get_stats_by_timerange(&self, start_time: i64, end_time: i64) -> Result<serde_json::Value> {
        let stats = self.db.get_stats_by_timerange(start_time, end_time)?;
        let mut servers_data = Vec::with_capacity(stats.len());
        
        for (host_name, records) in stats {
            if records.is_empty() { continue; }
            let latest = &records[records.len() - 1];
            
            let mut cpu_data = Vec::with_capacity(records.len());
            let mut memory_data = Vec::with_capacity(records.len());
            let mut network_in_data = Vec::with_capacity(records.len());
            let mut network_out_data = Vec::with_capacity(records.len());
            let mut disk_data_map: HashMap<String, Vec<DiskValue>> = HashMap::new();
            
            for record in &records {
                cpu_data.push(TimeValue { timestamp: record.timestamp, value: record.cpu });
                let mem_percent = if record.memory_total > 0 { (record.memory_used as f64 / record.memory_total as f64) * 100.0 } else { 0.0 };
                memory_data.push(MemValue { timestamp: record.timestamp, value: mem_percent, total: record.memory_total, used: record.memory_used });
                network_in_data.push(NetValue { timestamp: record.timestamp, value: record.network_in_speed, total: record.network_in });
                network_out_data.push(NetValue { timestamp: record.timestamp, value: record.network_out_speed, total: record.network_out });
                for disk in &record.disks {
                    let disk_percent = if disk.total > 0 { (disk.used as f64 / disk.total as f64) * 100.0 } else { 0.0 };
                    disk_data_map.entry(disk.mount_point.clone()).or_insert_with(|| Vec::with_capacity(records.len())).push(DiskValue { timestamp: record.timestamp, value: disk_percent, total: disk.total, used: disk.used });
                }
            }
            
            servers_data.push(serde_json::json!({
                "name": host_name, "alias": latest.alias, "online": latest.online, "data_points": records.len(),
                "cpu_history": cpu_data, "memory_history": memory_data, "network_in_history": network_in_data, "network_out_history": network_out_data, "disks_history": disk_data_map
            }));
        }
        
        Ok(serde_json::json!({ "updated": SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_secs(), "servers": servers_data }))
    }
}