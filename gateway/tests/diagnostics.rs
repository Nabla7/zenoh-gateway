//! Actual WebRTC path under independently controlled network delay.
#[path = "support/shaped_link.rs"]
mod shaped_link;
use serde_json::{Value, json};
use shaped_link::{Direction, start_server, start_shaped_link};
use std::time::Duration;
use zenoh_gateway::client::{Client, ClientOptions, Delivery, PublisherOptions};

#[tokio::test(flavor = "multi_thread")]
#[ignore = "run with DIMOS_DIAGNOSTICS=trace and an owned capture directory"]
async fn diagnostics_attributes_directional_network_delay_before_gateway_processing() {
    assert_eq!(std::env::var("DIMOS_DIAGNOSTICS").unwrap(), "trace");
    let root = std::path::PathBuf::from(std::env::var("DIMOS_DIAGNOSTICS_DIR").unwrap());
    let run = std::env::var("DIMOS_DIAGNOSTICS_RUN").unwrap();
    let (running, session, url) = start_server().await;
    let down =
        Direction { bytes_per_sec: 100_000_000.0, queue_limit_bytes: 1_000_000, delay: Duration::from_millis(2), ..Default::default() };
    let up = Direction { delay: Duration::from_millis(80), loss_period: 23, loss_burst: 2, reorder_period: 7, ..down };
    let (shaped, faults) = start_shaped_link(url, down, up).await;
    let client = Client::connect(&shaped, ClientOptions { ice_servers: Some(vec![]), ..Default::default() }).await.unwrap();
    let key = "diagnostics/shaped-control";
    let receiver = session.declare_subscriber(key).await.unwrap();
    let publisher = client
        .publish(
            key,
            PublisherOptions { trace_schema: Some("dimos.trace/v1".into()), delivery: Some(Delivery::Reliable), ..Default::default() },
        )
        .await
        .unwrap();
    for sequence in 1..=30 {
        let (at, domain) = zenoh_gateway::diagnostics::clock();
        let context = json!({"schema":"dimos.trace/v1","run_id":run,"publisher":"synthetic-operator","stream":key,"sequence":sequence,
            "origin_ns":at,"clock_domain":domain,"parents":[],"lineage":"complete"});
        zenoh_gateway::diagnostics::event("operator.input", Some(&context), json!({"channel":key}));
        publisher.put_traced(sequence.to_string().as_bytes(), &context).await.unwrap();
        let sample = tokio::time::timeout(Duration::from_secs(5), receiver.recv_async()).await.unwrap().unwrap();
        assert_eq!(sample.payload().to_bytes().as_ref(), sequence.to_string().as_bytes());
        zenoh_gateway::diagnostics::event(
            "synthetic.adapter_dispatch",
            Some(&context),
            json!({"channel":key,"hardware_acknowledged":false}),
        );
    }
    {
        let stats = faults.lock().unwrap();
        let stats = stats.as_ref().unwrap();
        assert!(stats.up.dropped.load(std::sync::atomic::Ordering::Relaxed) > 0);
        assert!(stats.up.reordered.load(std::sync::atomic::Ordering::Relaxed) > 0);
    }
    drop(publisher);
    client.close().await;
    running.shutdown().await.unwrap();
    let mut events: Vec<Value> = Vec::new();
    for process in std::fs::read_dir(root.join(run)).unwrap() {
        let path = process.unwrap().path().join("incident-gateway.jsonl");
        if path.is_file() {
            events.extend(
                std::fs::read_to_string(path)
                    .unwrap()
                    .lines()
                    .skip(1)
                    .map(|line| serde_json::from_str::<Value>(line).unwrap())
                    .filter(|e| e["context"]["stream"] == key),
            );
        }
    }
    for sequence in 1..=30 {
        let event = |stage: &str| events.iter().find(|e| e["event"] == stage && e["context"]["sequence"] == sequence).unwrap();
        let input = event("operator.input")["mono_ns"].as_u64().unwrap();
        let admitted = event("gateway.command_received")["mono_ns"].as_u64().unwrap();
        let dispatched = event("synthetic.adapter_dispatch")["mono_ns"].as_u64().unwrap();
        assert!(admitted - input >= 70_000_000, "injected upstream delay is missing");
        assert!(dispatched - admitted < admitted - input, "gateway processing must not be blamed for upstream delay");
    }
}
