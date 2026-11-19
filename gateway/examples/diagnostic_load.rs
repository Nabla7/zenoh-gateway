//! Synthetic control + three camera streams through the actual gateway/WebRTC.
//! Run each diagnostics mode in a fresh process; excludes browser/XR rendering.
use anyhow::{Result, ensure};
use serde_json::{Value, json};
use std::{
    sync::{
        Arc,
        atomic::{AtomicU64, Ordering},
    },
    time::{Duration, Instant},
};
use tokio::{
    sync::Mutex,
    time::{MissedTickBehavior, interval},
};
use zenoh_gateway::client::{Client, ClientOptions, Delivery, Message, PublisherOptions, SubscribeOptions};
use zenoh_gateway::{Channel, DecodedFrame, EncodingOutput, EncodingSample, MessageEncoding, Server, VideoImage, diagnostics, zenoh};

struct Pattern;
impl MessageEncoding for Pattern {
    fn name(&self) -> &str {
        "diagnostic-pattern"
    }
    fn output(&self) -> EncodingOutput {
        EncodingOutput::Video
    }
    fn decode(&self, sample: &EncodingSample<'_>, _: Channel) -> Result<DecodedFrame> {
        let phase = sample.payload[0];
        let mut pixels = vec![0; 640 * 480 * 3];
        for y in 0..480 {
            for x in 0..640 {
                let i = (y * 640 + x) * 3;
                pixels[i] = (x as u8).wrapping_add(phase);
                pixels[i + 1] = y as u8;
                pixels[i + 2] = ((x / 32 + y / 32) % 2 * 100) as u8;
            }
        }
        Ok(DecodedFrame::Video(VideoImage::rgb8(640, 480, pixels)?))
    }
}
fn context(run: &str, stream: &str, sequence: u64) -> Value {
    let (origin_ns, clock_domain) = diagnostics::clock();
    json!({"schema":"dimos.trace/v1","run_id":run,"publisher":"synthetic-load","stream":stream,
        "sequence":sequence,"origin_ns":origin_ns,"clock_domain":clock_domain,"parents":[],"lineage":"complete"})
}
fn cpu_seconds() -> f64 {
    let mut usage = std::mem::MaybeUninit::<libc::rusage>::uninit();
    // SAFETY: getrusage initializes this correctly sized writable rusage.
    if unsafe { libc::getrusage(libc::RUSAGE_SELF, usage.as_mut_ptr()) } != 0 {
        return f64::NAN;
    }
    let usage = unsafe { usage.assume_init() };
    (usage.ru_utime.tv_sec + usage.ru_stime.tv_sec) as f64 + (usage.ru_utime.tv_usec + usage.ru_stime.tv_usec) as f64 / 1e6
}
#[tokio::main]
async fn main() -> Result<()> {
    let seconds = std::env::args().nth(1).unwrap_or_else(|| "20".into()).parse::<u64>()?;
    ensure!((5..=3600).contains(&seconds), "duration must be 5..3600 seconds");
    let mode = std::env::var("DIMOS_DIAGNOSTICS").unwrap_or_else(|_| "off".into());
    let tracing = mode != "off";
    let run = std::env::var("DIMOS_DIAGNOSTICS_RUN").unwrap_or_else(|_| "load".into());
    let mut config = zenoh::Config::default();
    config.insert_json5("scouting/multicast/enabled", "false").map_err(|e| anyhow::anyhow!(e))?;
    config.insert_json5("scouting/gossip/enabled", "false").map_err(|e| anyhow::anyhow!(e))?;
    config.insert_json5("listen/endpoints", "[]").map_err(|e| anyhow::anyhow!(e))?;
    let session = zenoh::open(config).await.map_err(|e| anyhow::anyhow!(e))?;
    let server = Server::builder().session(session.clone()).encoding(Pattern).build().await?;
    let running = server.bind("127.0.0.1:0").await?;
    let client =
        Client::connect(&format!("http://{}", running.local_addr()), ClientOptions { ice_servers: Some(vec![]), ..Default::default() })
            .await?;
    let mut tasks = tokio::task::JoinSet::new();
    let frames: Arc<Vec<AtomicU64>> = Arc::new((0..3).map(|_| AtomicU64::new(0)).collect());
    for camera in 0..3 {
        let key = format!("diagnostic-load/camera-{camera}");
        let mut subscription =
            client.subscribe(&key, SubscribeOptions { encoding: Some("diagnostic-pattern".into()), ..Default::default() }).await?;
        let counts = frames.clone();
        tasks.spawn(async move {
            while let Some(message) = subscription.recv().await {
                if matches!(message, Message::Video(_)) {
                    counts[camera].fetch_add(1, Ordering::Relaxed);
                }
            }
        });
        let (session, run) = (session.clone(), run.clone());
        tasks.spawn(async move {
            let mut ticker = interval(Duration::from_nanos(1_000_000_000 / 30));
            ticker.set_missed_tick_behavior(MissedTickBehavior::Skip);
            let mut sequence = 0;
            loop {
                ticker.tick().await;
                sequence += 1;
                let put = session.put(&key, vec![(sequence % 256) as u8]);
                if tracing {
                    let ctx = context(&run, &key, sequence);
                    diagnostics::event("camera.acquired", Some(&ctx), json!({"channel":key}));
                    let mut attachment = b"DIMOS_TRACE_V1\0".to_vec();
                    attachment.extend(serde_json::to_vec(&ctx).unwrap());
                    put.attachment(attachment).await.unwrap();
                } else {
                    put.await.unwrap();
                }
            }
        });
    }
    let key = "diagnostic-load/control";
    let receiver = session.declare_subscriber(key).await.map_err(|e| anyhow::anyhow!(e))?;
    let publisher = client
        .publish(
            key,
            PublisherOptions {
                trace_schema: tracing.then(|| "dimos.trace/v1".into()),
                delivery: Some(Delivery::Reliable),
                ..Default::default()
            },
        )
        .await?;
    let ages = Arc::new(Mutex::new(Vec::<f64>::new()));
    let measured = ages.clone();
    tasks.spawn(async move {
        while let Ok(sample) = receiver.recv_async().await {
            let payload = sample.payload().to_bytes();
            let sent = u64::from_le_bytes(payload.as_ref().try_into().unwrap());
            let age = diagnostics::clock().0.saturating_sub(sent) as f64 / 1e6;
            if tracing {
                diagnostics::event(
                    "synthetic.adapter_dispatch",
                    diagnostics::context(sample.attachment().map(|a| a.to_bytes()).as_deref()).as_ref(),
                    json!({"channel":key,"hardware_acknowledged":false}),
                );
            }
            measured.lock().await.push(age);
        }
    });
    tasks.spawn(async move {
        let mut sequence = 0;
        let mut ticker = interval(Duration::from_nanos(1_000_000_000 / 60));
        ticker.set_missed_tick_behavior(MissedTickBehavior::Skip);
        loop {
            ticker.tick().await;
            sequence += 1;
            let sent = diagnostics::clock().0;
            if tracing {
                let ctx = context(&run, key, sequence);
                diagnostics::event("operator.input", Some(&ctx), json!({"channel":key}));
                publisher.put_traced(sent.to_le_bytes(), &ctx).await.unwrap();
            } else {
                publisher.put(sent.to_le_bytes()).await.unwrap();
            }
        }
    });
    // Warm up actual codecs, ICE and the bandwidth estimator before measurement.
    tokio::time::sleep(Duration::from_secs(3)).await;
    ages.lock().await.clear();
    for count in frames.iter() {
        count.store(0, Ordering::Relaxed);
    }
    let began = Instant::now();
    let cpu = cpu_seconds();
    tokio::time::sleep(Duration::from_secs(seconds)).await;
    let cpu = cpu_seconds() - cpu;
    let elapsed = began.elapsed().as_secs_f64();
    let mut samples = ages.lock().await.clone();
    samples.sort_by(f64::total_cmp);
    let counts: Vec<u64> = frames.iter().map(|count| count.load(Ordering::Relaxed)).collect();
    tasks.shutdown().await;
    client.close().await;
    running.shutdown().await?;
    session.close().await.map_err(|e| anyhow::anyhow!(e))?;
    ensure!(samples.len() > seconds as usize * 50 && counts.iter().all(|n| *n > 10), "synthetic workload incomplete");
    let quantile = |q: f64| samples[(samples.len() as f64 * q).ceil() as usize - 1];
    // CPU percent is normalized to all online logical CPUs, not one core.
    let cores = unsafe { libc::sysconf(libc::_SC_NPROCESSORS_ONLN) }.max(1);
    println!(
        "{}",
        json!({"schema":"dimos.overhead/v1","mode":mode,"duration_s":elapsed,"cpu_seconds":cpu,"logical_cpus":cores,
        "process_host_cpu_percent":cpu/elapsed/cores as f64*100.0,"command_samples":samples.len(),"command_p95_ms":quantile(0.95),
        "command_p99_ms":quantile(0.99),"video_frames":counts,"camera_sources":3,"camera_source_hz":30,"input_hz":60,
        "scope":"actual native gateway, H264 encoder/pacer and WebRTC client; excludes browser decode, XR and robot controller"})
    );
    Ok(())
}
