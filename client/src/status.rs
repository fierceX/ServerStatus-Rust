#![allow(unused)]
use lazy_static::lazy_static;
use once_cell::sync::OnceCell;
use regex::Regex;
use std::collections::HashMap;
use std::collections::LinkedList;
use std::fs;
use std::fs::File;
use std::io::BufRead;
use std::io::BufReader;
use std::io::ErrorKind::ConnectionRefused;
use std::net::TcpStream;
use std::net::{Shutdown, ToSocketAddrs};
use std::process::Command;
use std::str;
use std::sync::Arc;
use std::sync::Mutex;
use std::thread;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use crate::vnstat;
use crate::Args;
use stat_common::server_status::{DiskInfo, StatRequest};

const SAMPLE_PERIOD: u64 = 1000; //ms
const TIMEOUT_MS: u64 = 1000;


static ONLINE_IPV4: u8 = 1;
static ONLINE_IPV6: u8 = 2;
pub fn get_network(args: &Args) -> (bool, bool) {
    let mut network: [bool; 2] = [(args.online & ONLINE_IPV4) != 0, (args.online & ONLINE_IPV6) != 0];
    if network.iter().any(|&x| x) {
        return network.into();
    }
    let addrs = vec![&args.ipv4_address, &args.ipv6_address];
    for (idx, probe_addr) in addrs.into_iter().enumerate() {
        let _ = probe_addr.to_socket_addrs().map(|mut iter| {
            if let Some(addr) = iter.next() {
                info!("{} => {}", probe_addr, addr);

                let r = TcpStream::connect_timeout(&addr, Duration::from_millis(TIMEOUT_MS)).map(|s| {
                    network[idx] = true;
                    s.shutdown(Shutdown::Both)
                });

                info!("{:?}", r);
            };
        });
    }

    network.into()
}


#[derive(Debug, Default)]
pub struct PingData {
    pub probe_uri: String,
    pub lost_rate: u32,
    pub ping_time: u32,
}

fn start_ping_collect_t(data: &Arc<Mutex<PingData>>) {
    let mut package_list: LinkedList<i32> = LinkedList::new();
    let mut package_lost: u32 = 0;
    let pt = &*data.lock().unwrap();
    let addr = pt
        .probe_uri
        .to_socket_addrs()
        .unwrap()
        .next()
        .expect("can't get addr info");
    info!("{} => {:?}", pt.probe_uri, addr);

    let ping_data = data.clone();
    thread::spawn(move || loop {
        if package_list.len() > 100 && package_list.pop_front().unwrap() == 0 {
            package_lost -= 1;
        }

        let instant = Instant::now();
        match TcpStream::connect_timeout(&addr, Duration::from_millis(TIMEOUT_MS)) {
            Ok(s) => {
                let _ = s.shutdown(Shutdown::Both);
                package_list.push_back(1);
            }
            Err(e) => {
                // error!("{:?}", e);
                if e.kind() == ConnectionRefused {
                    package_list.push_back(1);
                } else {
                    package_lost += 1;
                    package_list.push_back(0);
                }
            }
        }
        let time_cost_ms = instant.elapsed().as_millis();

        if let Ok(mut o) = ping_data.lock() {
            o.ping_time = time_cost_ms as u32;
            if package_list.len() > 30 {
                o.lost_rate = package_lost * 100 / package_list.len() as u32;
            }
        }

        thread::sleep(Duration::from_millis(SAMPLE_PERIOD));
    });
}

pub static G_PING_10010: OnceCell<Arc<Mutex<PingData>>> = OnceCell::new();
pub static G_PING_189: OnceCell<Arc<Mutex<PingData>>> = OnceCell::new();
pub static G_PING_10086: OnceCell<Arc<Mutex<PingData>>> = OnceCell::new();

pub fn start_all_ping_collect_t(args: &Args) {
    G_PING_10010
        .set(Arc::new(Mutex::new(PingData {
            probe_uri: args.cu_addr.to_owned(),
            lost_rate: 0,
            ping_time: 0,
        })))
        .unwrap();
    G_PING_189
        .set(Arc::new(Mutex::new(PingData {
            probe_uri: args.ct_addr.to_owned(),
            lost_rate: 0,
            ping_time: 0,
        })))
        .unwrap();
    G_PING_10086
        .set(Arc::new(Mutex::new(PingData {
            probe_uri: args.cm_addr.to_owned(),
            lost_rate: 0,
            ping_time: 0,
        })))
        .unwrap();

    if !args.disable_ping {
        start_ping_collect_t(G_PING_10010.get().unwrap());
        start_ping_collect_t(G_PING_189.get().unwrap());
        start_ping_collect_t(G_PING_10086.get().unwrap());
    }
}
