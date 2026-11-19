//! Shared loopback-only WebRTC network fixture; never routes to a real robot.
use serde_json::Value;
use std::net::SocketAddr;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;
use tokio::net::UdpSocket;
use tokio::sync::mpsc;
use tokio::time::Instant;
use zenoh_gateway::{RunningServer, Server, zenoh};
/// One direction of the shaped link: a drop-tail queue drained at `bytes_per_sec`, then `delay`.
#[derive(Clone, Copy, Default)]
pub(crate) struct Direction {
    pub(crate) bytes_per_sec: f64,
    pub(crate) queue_limit_bytes: usize,
    pub(crate) delay: Duration,
    pub(crate) loss_period: usize,
    pub(crate) loss_burst: usize,
    pub(crate) reorder_period: usize,
}

pub(crate) struct DirectionStats {
    pub(crate) queued: Arc<AtomicUsize>,
    pub(crate) dropped: Arc<AtomicUsize>,
    pub(crate) reordered: Arc<AtomicUsize>,
}
pub(crate) struct LinkStats {
    pub(crate) down: DirectionStats,
    pub(crate) up: DirectionStats,
}

/// Sends what is pushed to it (a packet, the socket to send it from, and where to) through a rate-limited FIFO and a
/// fixed delay; returns the push end and the bytes queued.
type Packet = (Vec<u8>, Arc<UdpSocket>, SocketAddr);

fn shaped_direction(direction: Direction) -> (mpsc::UnboundedSender<Packet>, DirectionStats) {
    let (push, mut queue) = mpsc::unbounded_channel::<Packet>();
    let (deliver, mut delivering) = mpsc::unbounded_channel::<(Instant, Packet)>();
    let queued = Arc::new(AtomicUsize::new(0));
    let dropped = Arc::new(AtomicUsize::new(0));
    let reordered = Arc::new(AtomicUsize::new(0));
    let drops = dropped.clone();
    let reorders = reordered.clone();
    let (accepted, served) = (queued.clone(), queued.clone());
    let (admit, mut admitted) = mpsc::unbounded_channel::<(Instant, Packet)>();
    // drop-tail admission
    tokio::spawn(async move {
        let mut count = 0;
        while let Some(packet) = queue.recv().await {
            count += 1;
            if (direction.loss_period > 0 && count % direction.loss_period < direction.loss_burst)
                || accepted.load(Ordering::Relaxed) + packet.0.len() > direction.queue_limit_bytes
            {
                drops.fetch_add(1, Ordering::Relaxed);
                continue;
            }
            accepted.fetch_add(packet.0.len(), Ordering::Relaxed);
            let _ = admit.send((Instant::now(), packet));
        }
    });
    // the bottleneck: each packet leaves when the one before it has, plus its own serialization time
    tokio::spawn(async move {
        let mut free_at = Instant::now();
        while let Some((arrived, packet)) = admitted.recv().await {
            let start = free_at.max(arrived);
            free_at = start + Duration::from_secs_f64(packet.0.len() as f64 / direction.bytes_per_sec);
            if free_at > Instant::now() {
                tokio::time::sleep_until(free_at).await;
            }
            served.fetch_sub(packet.0.len(), Ordering::Relaxed);
            let _ = deliver.send((free_at + direction.delay, packet));
        }
    });
    tokio::spawn(async move {
        let mut count = 0;
        while let Some((at, (bytes, socket, to))) = delivering.recv().await {
            tokio::time::sleep_until(at).await;
            count += 1;
            if direction.reorder_period > 0 && count % direction.reorder_period == 0 {
                reorders.fetch_add(1, Ordering::Relaxed);
                // Hold this packet independently so later packets overtake it.
                tokio::spawn(async move {
                    tokio::time::sleep(Duration::from_millis(40)).await;
                    let _ = socket.send_to(&bytes, to).await;
                });
            } else {
                let _ = socket.send_to(&bytes, to).await;
            }
        }
    });
    (push, DirectionStats { queued, dropped, reordered })
}

/// client <-> [client_side | one server-side socket per client address, as a NAT maps them] <-> `target` (the server's
/// ICE port); returns the client-side port. The client checks candidate pairs from each of its sockets, so answers must
/// go back to the socket that asked.
async fn start_shaper(target: SocketAddr, down: Direction, up: Direction) -> (u16, LinkStats) {
    let client_side = Arc::new(UdpSocket::bind("127.0.0.1:0").await.unwrap());
    let (to_client, down_stats) = shaped_direction(down);
    let (to_server, up_stats) = shaped_direction(up);
    let port = client_side.local_addr().unwrap().port();
    tokio::spawn(async move {
        let mut mapped: std::collections::HashMap<SocketAddr, Arc<UdpSocket>> = std::collections::HashMap::new();
        let mut buffer = vec![0u8; 65536];
        while let Ok((length, from)) = client_side.recv_from(&mut buffer).await {
            let server_side = match mapped.get(&from) {
                Some(socket) => socket.clone(),
                None => {
                    let socket = Arc::new(UdpSocket::bind("127.0.0.1:0").await.unwrap());
                    mapped.insert(from, socket.clone());
                    let (socket_in, client_side, to_client) = (socket.clone(), client_side.clone(), to_client.clone());
                    tokio::spawn(async move {
                        let mut buffer = vec![0u8; 65536];
                        while let Ok((length, _)) = socket_in.recv_from(&mut buffer).await {
                            let _ = to_client.send((buffer[..length].to_vec(), client_side.clone(), from));
                        }
                    });
                    socket
                }
            };
            let _ = to_server.send((buffer[..length].to_vec(), server_side, target));
        }
    });
    (port, LinkStats { down: down_stats, up: up_stats })
}

/// Signalling for `server_url` that routes the connection's ICE through a shaper: the offer loses its
/// candidates, the answer's are replaced by the shaper's port.
pub(crate) async fn start_shaped_link(server_url: String, down: Direction, up: Direction) -> (String, Arc<Mutex<Option<LinkStats>>>) {
    let down_queued: Arc<Mutex<Option<LinkStats>>> = Arc::default();
    let queued = down_queued.clone();
    let route = axum::routing::post(move |axum::Json(mut offer): axum::Json<Value>| {
        let (server_url, queued) = (server_url.clone(), queued.clone());
        async move {
            let without_candidates: Vec<&str> =
                offer["sdp"].as_str().unwrap().split("\r\n").filter(|line| !line.starts_with("a=candidate")).collect();
            offer["sdp"] = without_candidates.join("\r\n").into();
            let mut answer: Value =
                reqwest::Client::new().post(format!("{server_url}/offer")).json(&offer).send().await.unwrap().json().await.unwrap();
            let sdp = answer["sdp"].as_str().unwrap().to_owned();
            let loopback = sdp
                .split("\r\n")
                .find(|line| line.starts_with("a=candidate") && line.contains(" 127.0.0.1 "))
                .expect("a loopback candidate");
            let target_port: u16 = loopback.split(' ').nth(5).unwrap().parse().unwrap();
            let (port, down_queued) = start_shaper(SocketAddr::from(([127, 0, 0, 1], target_port)), down, up).await;
            *queued.lock().unwrap() = Some(down_queued);
            let mut replaced = false;
            let lines: Vec<String> = sdp
                .split("\r\n")
                .filter_map(|line| {
                    if !line.starts_with("a=candidate") {
                        return Some(line.to_owned());
                    }
                    (!std::mem::replace(&mut replaced, true)).then(|| format!("a=candidate:1 1 udp 2130706431 127.0.0.1 {port} typ host"))
                })
                .collect();
            answer["sdp"] = lines.join("\r\n").into();
            axum::Json(answer)
        }
    });
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let url = format!("http://{}", listener.local_addr().unwrap());
    tokio::spawn(async move { axum::serve(listener, axum::Router::new().route("/offer", route)).await.unwrap() });
    (url, down_queued)
}

fn isolated_config() -> zenoh::Config {
    let mut config = zenoh::Config::default();
    config.insert_json5("scouting/multicast/enabled", "false").unwrap();
    config.insert_json5("listen/endpoints", "[]").unwrap();
    config
}

pub(crate) async fn start_server() -> (RunningServer, zenoh::Session, String) {
    let session = zenoh::open(isolated_config()).await.unwrap();
    let server = Server::builder().session(session.clone()).build().await.unwrap();
    let running = server.bind("127.0.0.1:0").await.unwrap();
    let url = format!("http://{}", running.local_addr());
    (running, session, url)
}
