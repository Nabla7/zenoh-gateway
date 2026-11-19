//! A frontend on a slow, deep-buffered link (a phone over Tailscale): bulk streams must not make
//! control traffic (heartbeat replies, control answers) or each other's data wait in a growing queue.
//! A userspace UDP shaper sits between the Rust client and the server, and a signalling proxy
//! rewrites the SDP so ICE can only use it (`cargo test --features client --test backpressure`).

use std::time::Duration;
use tokio::time::Instant;
use zenoh_gateway::client::{Client, ClientOptions, Delivery, Message, SubscribeOptions};
use zenoh_gateway::zenoh;
#[path = "support/shaped_link.rs"]
mod shaped_link;
use shaped_link::{Direction, start_server, start_shaped_link};
use std::sync::{Arc, Mutex};

/// Puts `bytes` on `key` at `hz` until dropped.
fn publish(session: &zenoh::Session, key: &str, bytes: usize, hz: f64) -> tokio::task::JoinHandle<()> {
    let (session, key) = (session.clone(), key.to_owned());
    tokio::spawn(async move {
        let payload = vec![7u8; bytes];
        let mut ticker = tokio::time::interval(Duration::from_secs_f64(1.0 / hz));
        loop {
            ticker.tick().await;
            session.put(&key, payload.clone()).await.unwrap();
        }
    })
}

fn percentile(values: &[f64], q: f64) -> f64 {
    let mut sorted = values.to_vec();
    sorted.sort_by(f64::total_cmp);
    sorted.get(((q * sorted.len() as f64) as usize).min(sorted.len().saturating_sub(1))).copied().unwrap_or(f64::NAN)
}

fn now_ms() -> f64 {
    std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).unwrap().as_secs_f64() * 1000.0
}

/// What one run measured, per window of the run.
#[derive(Debug)]
struct Window {
    /// heartbeat round trips (what the Controller's drive hold reads)
    heartbeat_p50: f64,
    heartbeat_p95: f64,
    /// bulk messages: receive time minus the gateway's receive time of the sample
    bulk_age_p50: f64,
    bulk_age_p95: f64,
    /// the small `pose` stream's message age
    small_age_p95: f64,
    bulk_bytes_per_sec: f64,
}

/// A Controller-like page over a 1 MB/s link with a 2 s deep buffer: three point clouds (300 KB at 10 Hz) and a map
/// (1 MB at 1 Hz), all `latest`, want ~10 MB/s; a small pose stream at 20 Hz; heartbeats at 5 Hz. `consume_bytes_per_sec`
/// is how fast the page gets through bulk messages (a phone decoding clouds), None for at once.
async fn run(seconds: u64, consume_bytes_per_sec: Option<f64>) -> Vec<Window> {
    let (_running, session, server_url) = start_server().await;
    let down =
        Direction { bytes_per_sec: 1_000_000.0, queue_limit_bytes: 2_000_000, delay: Duration::from_millis(30), ..Default::default() };
    let up =
        Direction { bytes_per_sec: 10_000_000.0, queue_limit_bytes: 2_000_000, delay: Duration::from_millis(30), ..Default::default() };
    let (url, _queued) = start_shaped_link(server_url, down, up).await;
    let client =
        Client::connect(&url, ClientOptions { ice_servers: Some(vec![]), heartbeat_hz: 5.0, heartbeat_misses: 50, ..Default::default() })
            .await
            .expect("connect through the shaper");

    let started = Instant::now();
    let window_secs = 5.0;
    let windows = (seconds as f64 / window_secs).ceil() as usize;
    let window_of = move |at: Instant| ((at - started).as_secs_f64() / window_secs) as usize;
    type Samples = Arc<Mutex<Vec<Vec<f64>>>>;
    let new_samples = || -> Samples { Arc::new(Mutex::new(vec![Vec::new(); windows + 1])) };
    let (heartbeats, bulk_ages, small_ages, bulk_bytes) = (new_samples(), new_samples(), new_samples(), new_samples());

    let mut publishers = vec![publish(&session, "robot/pose", 200, 20.0), publish(&session, "robot/map", 1_000_000, 1.0)];
    for index in 0..3 {
        publishers.push(publish(&session, &format!("robot/cloud{index}"), 300_000, 10.0));
    }
    let latest = SubscribeOptions { delivery: Some(Delivery::Latest), ..Default::default() };
    let mut readers = Vec::new();
    for key in ["robot/cloud0", "robot/cloud1", "robot/cloud2", "robot/map", "robot/pose"] {
        let mut subscription = client.subscribe(key, latest.clone()).await.unwrap();
        let small = key == "robot/pose";
        let (ages, bytes) = if small { (small_ages.clone(), bulk_bytes.clone()) } else { (bulk_ages.clone(), bulk_bytes.clone()) };
        readers.push(tokio::spawn(async move {
            while let Some(message) = subscription.recv().await {
                let Message::Data(message) = message else { continue };
                let age = now_ms() - message.timestamp_ms;
                let window = window_of(Instant::now()).min(windows);
                ages.lock().unwrap()[window].push(age);
                if !small {
                    bytes.lock().unwrap()[window].push(message.bytes.len() as f64);
                    if let Some(rate) = consume_bytes_per_sec {
                        tokio::time::sleep(Duration::from_secs_f64(message.bytes.len() as f64 / rate)).await;
                    }
                }
            }
        }));
    }
    let sampler = {
        let (client, heartbeats) = (client.clone(), heartbeats.clone());
        tokio::spawn(async move {
            let mut last = None;
            let mut ticker = tokio::time::interval(Duration::from_millis(200));
            loop {
                ticker.tick().await;
                let rtt = client.rtt_ms();
                if rtt != last
                    && let Some(rtt) = rtt
                {
                    heartbeats.lock().unwrap()[window_of(Instant::now()).min(windows)].push(rtt);
                }
                last = rtt;
            }
        })
    };
    tokio::time::sleep(Duration::from_secs(seconds)).await;
    sampler.abort();
    for task in publishers.into_iter().chain(readers) {
        task.abort();
    }
    let stats = client.stats().await.unwrap_or_default();
    eprintln!("bandwidth: {}", stats["bandwidth"]);
    client.close().await;
    let (heartbeats, bulk_ages, small_ages, bulk_bytes) =
        (heartbeats.lock().unwrap(), bulk_ages.lock().unwrap(), small_ages.lock().unwrap(), bulk_bytes.lock().unwrap());
    (0..windows)
        .map(|window| Window {
            heartbeat_p50: percentile(&heartbeats[window], 0.5),
            heartbeat_p95: percentile(&heartbeats[window], 0.95),
            bulk_age_p50: percentile(&bulk_ages[window], 0.5),
            bulk_age_p95: percentile(&bulk_ages[window], 0.95),
            small_age_p95: percentile(&small_ages[window], 0.95),
            bulk_bytes_per_sec: bulk_bytes[window].iter().sum::<f64>() / window_secs,
        })
        .collect()
}

fn report(name: &str, windows: &[Window]) {
    eprintln!("{name}:");
    for (index, window) in windows.iter().enumerate() {
        eprintln!(
            "  {:>3}-{:<3}s heartbeat rtt p50 {:>6.0} p95 {:>6.0} ms | bulk age p50 {:>6.0} p95 {:>6.0} ms | pose age p95 {:>6.0} ms | bulk {:>5.0} KB/s",
            index * 5,
            index * 5 + 5,
            window.heartbeat_p50,
            window.heartbeat_p95,
            window.bulk_age_p50,
            window.bulk_age_p95,
            window.small_age_p95,
            window.bulk_bytes_per_sec / 1000.0
        );
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn a_slow_link_keeps_control_and_data_fresh() {
    let windows = run(20, None).await;
    report("1 MB/s link, 2 s buffer, ~10 MB/s wanted", &windows);
    let last = windows.last().unwrap();
    assert!(last.heartbeat_p95 < 500.0, "heartbeats wait behind bulk data: {last:?}");
    assert!(last.small_age_p95 < 500.0, "a small stream waits behind bulk data: {last:?}");
    assert!(last.bulk_age_p50 < 1000.0, "bulk data arrives stale: {last:?}");
    assert!(last.bulk_bytes_per_sec > 300_000.0, "the link carries too little: {last:?}");
}

#[tokio::test(flavor = "multi_thread")]
async fn a_slow_page_keeps_control_and_data_fresh() {
    let windows = run(20, Some(400_000.0)).await;
    report("page consumes 400 KB/s, 1 MB/s link", &windows);
    let last = windows.last().unwrap();
    assert!(last.heartbeat_p95 < 500.0, "heartbeats wait behind bulk data: {last:?}");
    assert!(last.bulk_age_p50 < 1000.0, "bulk data arrives stale: {last:?}");
}
