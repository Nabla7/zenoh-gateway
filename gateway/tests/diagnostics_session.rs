//! The already-running gateway joins and leaves successive explicitly started host sessions.
use serde_json::{Value, json};
use std::{fs, path::Path, process::Command, time::{Duration, Instant}};

fn wait_for(mut predicate: impl FnMut() -> bool) {
    let deadline=Instant::now()+Duration::from_secs(10);
    while !predicate() {
        assert!(Instant::now()<deadline,"session did not become ready");
        std::thread::sleep(Duration::from_millis(20));
    }
}
fn reports(root: &Path, run: &str) -> Vec<Value> {
    fs::read_dir(root.join(run)).into_iter().flatten().flatten()
        .filter_map(|entry| fs::read(entry.path().join("status.json")).ok())
        .filter_map(|data|serde_json::from_slice(&data).ok()).collect()
}
#[test]
fn joins_and_switches_host_sessions() {
    if let Ok(root)=std::env::var("DIMOS_SESSION_TEST_CHILD") {
        zenoh_gateway::diagnostics::start_session_watch();
        let end=Instant::now()+Duration::from_secs(30);
        while Instant::now()<end && !Path::new(&root).join("finished").exists() {
            zenoh_gateway::diagnostics::event("test.gateway_frame",None,json!({}));
            std::thread::sleep(Duration::from_millis(20));
        }
        zenoh_gateway::diagnostics::flush();
        return;
    }
    let root=std::env::temp_dir().join(format!("dimos-session-test-{}",std::process::id()));
    fs::create_dir(&root).unwrap();
    let mut child=Command::new(std::env::current_exe().unwrap())
        .args(["--exact","joins_and_switches_host_sessions","--nocapture"])
        .env("DIMOS_SESSION_TEST_CHILD",&root).env("DIMOS_DIAGNOSTICS_DIR",&root)
        .env_remove("DIMOS_DIAGNOSTICS").env_remove("DIMOS_DIAGNOSTICS_RUN").spawn().unwrap();
    let result=std::panic::catch_unwind(|| {
        for run in ["first","second"] {
            let descriptor=json!({"schema":"dimos.diagnostics.session/v1","run_id":run,"mode":"trace","generation":run});
            fs::write(root.join("session.tmp"),descriptor.to_string()).unwrap();
            fs::rename(root.join("session.tmp"),root.join("session.json")).unwrap();
            wait_for(||reports(&root,run).iter().any(|r|r["state"]=="collecting" && r["last_events"]["test.gateway_frame"].is_u64()));
            zenoh_gateway::diagnostics::flush();
            fs::remove_file(root.join("session.json")).unwrap();
            wait_for(||reports(&root,run).iter().any(|r|r["state"]=="stopped"));
            let process=fs::read_dir(root.join(run)).unwrap().next().unwrap().unwrap().path();
            let data=fs::read_to_string(process.join("incident-gateway.jsonl")).unwrap();
            let rows:Vec<Value>=data.lines().map(|l|serde_json::from_str(l).unwrap()).collect();
            assert_eq!(rows[0]["run_id"],run);
            assert!(rows.iter().any(|e|e["event"]=="test.gateway_frame"));
        }
    });
    fs::write(root.join("finished"),"").unwrap();
    let status=child.wait().unwrap();
    fs::remove_dir_all(root).unwrap();
    assert!(status.success());
    if let Err(error)=result {std::panic::resume_unwind(error);}
}
