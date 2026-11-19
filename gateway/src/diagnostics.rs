//! Opt-in bounded flight recorder using the shared dimos.trace/v1 contract.
//! Payloads are never logged. Envelopes are used only on negotiated publisher channels.
use crate::trace_ring::TraceRing;
use serde_json::{Value, json};
use std::{
    fs,
    io::Write,
    path::PathBuf,
    sync::{
        OnceLock,
        atomic::{AtomicU64, Ordering},
        mpsc::{SyncSender, sync_channel},
    },
    time::{Duration, Instant, SystemTime, UNIX_EPOCH},
};

/// Shared trace contract version.
pub const SCHEMA: &str = "dimos.trace/v1";
const PREFIX: &[u8] = b"DIMOS_TRACE_V1\0";
const MAX_CONTEXT: usize = 16384;
static CLOCK: OnceLock<(Instant, String)> = OnceLock::new();
enum Record {
    Event(Value),
    Flush(SyncSender<()>),
}
static WRITER: OnceLock<Option<SyncSender<Record>>> = OnceLock::new();
static IDS: AtomicU64 = AtomicU64::new(1);
static DROPS: AtomicU64 = AtomicU64::new(0);

/// Local monotonic timestamp and its explicitly identified clock domain.
pub fn clock() -> (u64, String) {
    let (origin, domain) = CLOCK.get_or_init(|| {
        let private = format!("gateway-{}-{}/instant", std::process::id(), wall());
        #[cfg(target_os = "linux")]
        let domain = fs::read_to_string("/proc/sys/kernel/hostname")
            .ok()
            .zip(fs::read_to_string("/proc/sys/kernel/random/boot_id").ok())
            .map(|(host, boot)| format!("{}/{}/monotonic", host.trim(), boot.trim()))
            .unwrap_or(private);
        #[cfg(target_os = "macos")]
        let domain = (|| -> Option<String> {
            let boot = std::process::Command::new("/usr/sbin/sysctl").args(["-n", "kern.bootsessionuuid"]).output().ok()?;
            let host = std::process::Command::new("/bin/hostname").output().ok()?;
            if !boot.status.success() || !host.status.success() {
                return None;
            }
            Some(format!("{}/{}/monotonic", String::from_utf8(host.stdout).ok()?.trim(), String::from_utf8(boot.stdout).ok()?.trim()))
        })()
        .unwrap_or(private);
        #[cfg(not(any(target_os = "linux", target_os = "macos")))]
        let domain = private;
        (Instant::now(), domain)
    });
    #[cfg(any(target_os = "linux", target_os = "macos"))]
    if domain.ends_with("/monotonic") {
        let mut ts = libc::timespec { tv_sec: 0, tv_nsec: 0 };
        #[cfg(target_os = "linux")]
        let clock = libc::CLOCK_MONOTONIC;
        #[cfg(target_os = "macos")]
        let clock = libc::CLOCK_UPTIME_RAW;
        // SAFETY: ts is a correctly sized writable timespec.
        if unsafe { libc::clock_gettime(clock, &mut ts) } == 0 {
            return ((ts.tv_sec as u64) * 1_000_000_000 + ts.tv_nsec as u64, domain.clone());
        }
    }
    (origin.elapsed().as_nanos() as u64, format!("{domain}/private"))
}
fn wall() -> u128 {
    SystemTime::now().duration_since(UNIX_EPOCH).unwrap_or_default().as_nanos()
}

/// Decode only the opt-in DimOS attachment, leaving foreign attachments untouched.
pub fn context(data: Option<&[u8]>) -> Option<Value> {
    let raw = data?.strip_prefix(PREFIX)?;
    if raw.len() > MAX_CONTEXT {
        return None;
    }
    let value: Value = serde_json::from_slice(raw).ok()?;
    valid_context(&value).then_some(value)
}
/// Read identity from the original source sample carried by an encoded frame.
pub fn sample_context(meta: &crate::frame::SampleMeta) -> Option<Value> {
    context(meta.as_ref().and_then(|(_, bytes)| bytes.as_deref()))
}
fn valid_context(value: &Value) -> bool {
    value["schema"] == SCHEMA
        && ["run_id", "publisher", "stream", "clock_domain"]
            .iter()
            .all(|key| value[key].as_str().is_some_and(|s| !s.is_empty() && s.len() <= 1024))
        && value["sequence"].as_u64().is_some()
        && value["origin_ns"].as_u64().is_some()
        && value["parents"].as_array().is_some_and(|p| p.len() <= 32 && p.iter().all(|v| v.as_str().is_some_and(|s| s.len() <= 256)))
        && value["lineage"].as_str().is_some_and(|s| ["complete", "partial", "unknown"].contains(&s))
}

/// Dedicated channel framing: u32 context size | native attachment | original payload.
pub fn unwrap_put(bytes: &[u8]) -> Result<(Value, Vec<u8>, Vec<u8>), &'static str> {
    if bytes.len() < 4 {
        return Err("missing trace length");
    }
    let n = u32::from_le_bytes(bytes[..4].try_into().unwrap()) as usize;
    if n > MAX_CONTEXT + PREFIX.len() || n > bytes.len() - 4 {
        return Err("invalid trace size");
    }
    let attachment = bytes[4..4 + n].to_vec();
    let trace = context(Some(&attachment)).ok_or("invalid trace context")?;
    Ok((trace, attachment, bytes[4 + n..].to_vec()))
}

fn writer() -> Option<&'static SyncSender<Record>> {
    WRITER.get_or_init(|| {
        let mode=std::env::var("DIMOS_DIAGNOSTICS").ok()?;
        if mode!="trace" && mode!="metrics" {return None;}
        let run = std::env::var("DIMOS_DIAGNOSTICS_RUN").ok()?;
        if run.is_empty() || run.len() > 128 || !run.bytes().all(|c| c.is_ascii_alphanumeric() || c == b'-' || c == b'_') { return None; }
        let root = PathBuf::from(std::env::var("DIMOS_DIAGNOSTICS_DIR").ok()?);
        let reservation = Reservation::reserve(&root,32*1024*1024).ok()?;
        if reservation.bytes < 4*1024*1024 { return None; }
        let process=format!("gateway-{}-{}",std::process::id(),wall());
        let directory=root.join(&run).join(&process);
        let manifest=json!({"kind":"manifest","schema":"dimos.incident/v1","run_id":run,"process":process,"pid":std::process::id(),
            "wall_ns":wall() as u64,"mono_ns":clock().0,"mode":mode,"clock_domain":clock().1,"build":env!("CARGO_PKG_VERSION"),"host":clock().1.split('/').next().unwrap_or("unknown"),"coverage":"gateway application boundaries"});
        let (tx,rx)=sync_channel::<Record>(128);
        std::thread::Builder::new().name("gateway-diagnostics".into()).spawn(move || {
            if fs::create_dir_all(&directory).is_err(){return;}
            let ring_limit=(reservation.bytes*3/4).min(16*1024*1024);
            let mut ring=TraceRing::new(ring_limit);let started=Instant::now();
            let mut errors=0u64;
            let mut saved=Instant::now();let mut checked=Instant::now()-Duration::from_secs(1);
            let mut pending: Option<(Instant,String)>=None;
            let mut stages=serde_json::Map::new();
            loop {
                let now=Instant::now();let mut stop=false;let mut flush=false;let mut ack=None;
                match rx.recv_timeout(Duration::from_millis(100)) {
                    Ok(Record::Flush(reply))=>{flush=true;ack=Some(reply);}
                    Ok(Record::Event(mut event))=>{
                        event["process"]=manifest["process"].clone();
                        if let (Some(stage),Some(duration))=(event["event"].as_str(),event["duration_ns"].as_u64()){record_duration(&mut stages,stage.trim_end_matches(".end"),duration);}
                        if (event["outcome"]=="error" || event["duration_ns"].as_u64().is_some_and(|v|v>100_000_000)) && pending.is_none() {
                            pending=Some((now+Duration::from_secs(30),event["event"].as_str().unwrap_or("slow").to_string()));
                        }
                        if let Ok(mut row)=serde_json::to_vec(&event) {
                            row.push(b'\n');
                            if mode=="trace" && row.len()<=65536 {if ring.push(&row,now).is_err(){errors+=1;}}
                            else if mode=="trace" {DROPS.fetch_add(1,Ordering::Relaxed);}
                        }
                    }
                    Err(std::sync::mpsc::RecvTimeoutError::Disconnected)=>stop=true,
                    Err(std::sync::mpsc::RecvTimeoutError::Timeout)=>{}
                }
                let cutoff=pending.as_ref().map_or(now-Duration::from_secs(60),|(until,_)|*until-Duration::from_secs(90));
                if ring.trim(cutoff).is_err(){errors+=1;}
                if checked.elapsed()>=Duration::from_secs(1) {
                    if let Ok(raw)=fs::read(directory.join("control.json")) {
                        if raw.len()<=4096 {if let Ok(control)=serde_json::from_slice::<Value>(&raw){
                            stop=control["action"]=="stop";flush|=control["action"]=="flush";
                            if control["action"]=="mark" && pending.is_none(){pending=Some((now+Duration::from_secs(30),control["reason"].as_str().unwrap_or("manual").chars().take(512).collect()));}
                        }}
                        let _=fs::remove_file(directory.join("control.json"));
                    }
                }
                let due=pending.as_ref().is_some_and(|(until,_)|now>=*until);
                let health=json!({"observer_drops":DROPS.load(Ordering::Relaxed),"ring_evictions":ring.evictions,"io_errors":errors,"ring_bytes":ring.size(),"raw_ring_bytes":ring.raw_bytes,
                    "memory_reserved_bytes":reservation.bytes,"ring_limit_bytes":ring_limit,"requested_pre_seconds":60,"requested_post_seconds":30,
                    "stages":stages,"mode":mode,"retained_seconds":now.saturating_duration_since(started.max(cutoff).max(ring.capacity_lost_until.unwrap_or(started))).as_secs_f64(),"capture_window_complete":mode=="trace" && started<=cutoff && ring.capacity_lost_until.is_none_or(|at|at<=cutoff) && (pending.is_none() || due),"capacity_truncated":ring.capacity_lost_until.is_some_and(|at|at>cutoff)});
                if flush || stop || due || saved.elapsed()>=Duration::from_secs(5) {
                    let mut header=manifest.clone();header["health"]=health.clone();
                    header["captured_ns"]=clock().0.into();
                    header["reason"]=pending.as_ref().map_or("checkpoint",|(_,reason)|reason.as_str()).into();
                    let destination=directory.join(if due {format!("incident-gateway-{}.jsonl",wall())}else{"incident-gateway.jsonl".into()});
                    if persist_capture(&root,&destination,&header,&ring).is_err(){errors+=1;}
                    saved=Instant::now();if due{pending=None;}
                }
                if let Some(reply)=ack {let _=reply.send(());}
                if stop || checked.elapsed()>=Duration::from_secs(1) {
                    let mut status=manifest.clone();status["at_ns"]=(wall() as u64).into();status["state"]=if stop{"stopped"}else{"collecting"}.into();
                    status.as_object_mut().unwrap().extend(health.as_object().unwrap().clone());status["io_errors"]=errors.into();
                    if fs::write(directory.join("status.tmp"),status.to_string()).and_then(|_|fs::rename(directory.join("status.tmp"),directory.join("status.json"))).is_err(){errors+=1;}
                    checked=Instant::now();
                }
                if stop {break;}
            }
        }).ok()?;
        Some(tx)
    }).as_ref()
}

/// Flush queued evidence at graceful server shutdown, outside the async/control path.
/// Does not stop the process-wide collector when another server is still using it.
pub fn flush() {
    let Some(writer) = WRITER.get().and_then(|v| v.as_ref()) else {
        return;
    };
    let (tx, rx) = sync_channel(1);
    if writer.send(Record::Flush(tx)).is_ok() {
        let _ = rx.recv_timeout(Duration::from_secs(5));
    }
}

/// Enqueue bounded metadata without blocking the caller.
pub fn enabled() -> bool {
    writer().is_some()
}

/// Enqueue bounded metadata without blocking the caller.
pub fn event(name: &str, context: Option<&Value>, labels: Value) {
    let Some(writer) = writer() else {
        return;
    };
    let (at, domain) = clock();
    let mut value = labels;
    value["schema"] = SCHEMA.into();
    value["event"] = name.into();
    value["mono_ns"] = at.into();
    value["clock_domain"] = domain.into();
    value["context"] = context.cloned().unwrap_or(Value::Null);
    if writer.try_send(Record::Event(value)).is_err() {
        DROPS.fetch_add(1, Ordering::Relaxed);
    }
}
/// Local processing duration with the input sample identity.
pub struct Span {
    began: Instant,
    name: &'static str,
    context: Option<Value>,
    labels: Value,
}
impl Span {
    /// Begin a local span; dropping it records completion.
    pub fn new(name: &'static str, context: Option<Value>, mut labels: Value) -> Self {
        labels["span_id"] = IDS.fetch_add(1, Ordering::Relaxed).to_string().into();
        event(&format!("{name}.start"), context.as_ref(), labels.clone());
        Self { began: Instant::now(), name, context, labels }
    }
}
impl Drop for Span {
    fn drop(&mut self) {
        self.labels["duration_ns"] = (self.began.elapsed().as_nanos() as u64).into();
        event(&format!("{}.end", self.name), self.context.as_ref(), self.labels.clone());
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn negotiated_put_keeps_exact_payload_and_identity() {
        let c = json!({"schema":SCHEMA,"run_id":"run","publisher":"browser","stream":"hand","sequence":2,"origin_ns":9,"clock_domain":"browser/mono","parents":[],"lineage":"complete"});
        let mut attachment = PREFIX.to_vec();
        attachment.extend(serde_json::to_vec(&c).unwrap());
        let mut wire = (attachment.len() as u32).to_le_bytes().to_vec();
        wire.extend(&attachment);
        wire.extend([0, 1, 255, 0]);
        let (got, meta, payload) = unwrap_put(&wire).unwrap();
        assert_eq!(got, c);
        assert_eq!(meta, attachment);
        assert_eq!(payload, vec![0, 1, 255, 0]);
        assert!(unwrap_put(&[255; 12]).is_err());
    }
}

// Reservations use the same host-level file/lock contract as the Python collector.
struct Reservation {
    root: PathBuf,
    key: String,
    bytes: usize,
}
impl Reservation {
    fn reserve(root: &std::path::Path, requested: usize) -> std::io::Result<Self> {
        use std::os::fd::AsRawFd;
        fs::create_dir_all(root)?;
        let lock = fs::OpenOptions::new().create(true).append(true).open(root.join(".memory.lock"))?;
        // SAFETY: lock owns a valid file descriptor until the operation completes.
        if unsafe { libc::flock(lock.as_raw_fd(), libc::LOCK_EX) } != 0 {
            return Err(std::io::Error::last_os_error());
        }
        let path = root.join(".memory.json");
        let mut entries: serde_json::Map<String, serde_json::Value> = match fs::read(&path) {
            Ok(bytes) => serde_json::from_slice(&bytes).map_err(std::io::Error::other)?,
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => Default::default(),
            Err(error) => return Err(error),
        };
        entries.retain(|_, value| {
            let Some(pid) = value["pid"].as_i64() else {
                return true;
            };
            // SAFETY: signal 0 only tests existence/permissions; sends no signal.
            unsafe { libc::kill(pid as libc::pid_t, 0) == 0 || std::io::Error::last_os_error().raw_os_error() != Some(libc::ESRCH) }
        });
        let used: usize = entries.values().map(|v| v["bytes"].as_u64().unwrap_or(256 * 1024 * 1024) as usize).sum();
        let process_used: usize = entries
            .values()
            .filter(|v| v["pid"].as_u64() == Some(std::process::id() as u64))
            .map(|v| v["bytes"].as_u64().unwrap_or(64 * 1024 * 1024) as usize)
            .sum();
        let bytes = requested.min((256usize * 1024 * 1024).saturating_sub(used)).min((64usize * 1024 * 1024).saturating_sub(process_used));
        let key = format!("native-{}-{}", std::process::id(), SystemTime::now().duration_since(UNIX_EPOCH).unwrap_or_default().as_nanos());
        entries.insert(key.clone(), serde_json::json!({"pid":std::process::id(),"bytes":bytes}));
        fs::write(root.join(".memory.tmp"), serde_json::to_vec(&entries)?)?;
        fs::rename(root.join(".memory.tmp"), path)?;
        Ok(Self { root: root.to_path_buf(), key, bytes })
    }
}
impl Drop for Reservation {
    fn drop(&mut self) {
        use std::os::fd::AsRawFd;
        let Ok(lock) = fs::OpenOptions::new().create(true).append(true).open(self.root.join(".memory.lock")) else {
            return;
        };
        // SAFETY: lock owns the fd and flock does not transfer ownership.
        if unsafe { libc::flock(lock.as_raw_fd(), libc::LOCK_EX) } != 0 {
            return;
        }
        let path = self.root.join(".memory.json");
        if let Ok(raw) = fs::read(&path) {
            if let Ok(mut entries) = serde_json::from_slice::<serde_json::Map<String, serde_json::Value>>(&raw) {
                entries.remove(&self.key);
                if fs::write(self.root.join(".memory.tmp"), serde_json::to_vec(&entries).unwrap_or_default()).is_ok() {
                    let _ = fs::rename(self.root.join(".memory.tmp"), path);
                }
            }
        }
    }
}

// All collectors sharing a diagnostics root use this flock and 1 GiB disk budget.
fn persist_capture(
    root: &std::path::Path,
    destination: &std::path::Path,
    header: &serde_json::Value,
    ring: &TraceRing,
) -> std::io::Result<()> {
    use std::os::fd::AsRawFd;
    fs::create_dir_all(root)?;
    let lock = fs::OpenOptions::new().create(true).append(true).open(root.join(".retention.lock"))?;
    // SAFETY: lock owns this valid fd; flock does not transfer ownership.
    if unsafe { libc::flock(lock.as_raw_fd(), libc::LOCK_EX) } != 0 {
        return Err(std::io::Error::last_os_error());
    }
    let header = serde_json::to_vec(header)?;
    let incoming = header.len() as u64 + 1 + ring.raw_bytes as u64;
    const BUDGET: u64 = 1024 * 1024 * 1024;
    if incoming > BUDGET {
        return Err(std::io::Error::other("capture exceeds host disk budget"));
    }
    let mut files = Vec::new();
    let mut directories = vec![root.to_path_buf()];
    let mut total = 0u64;
    while let Some(directory) = directories.pop() {
        for entry in fs::read_dir(directory)? {
            let entry = entry?;
            let kind = entry.file_type()?;
            if kind.is_dir() {
                directories.push(entry.path());
            } else if kind.is_file()
                && entry.file_name().to_string_lossy().starts_with("incident-")
                && entry.path().extension().is_some_and(|s| s == "jsonl")
            {
                let meta = entry.metadata()?;
                total += meta.len();
                files.push((meta.modified()?, entry.path(), meta.len()));
            }
            if files.len() + directories.len() > 32768 {
                return Err(std::io::Error::other("capture directory inventory exceeds bound"));
            }
        }
    }
    files.sort_by_key(|(at, _, _)| *at);
    for (_, path, size) in files {
        if total + incoming <= BUDGET {
            break;
        }
        fs::remove_file(path)?;
        total = total.saturating_sub(size);
    }
    let temporary = destination.with_extension("tmp");
    let mut file = fs::File::create(&temporary)?;
    file.write_all(&header)?;
    file.write_all(b"\n")?;
    ring.write_to(&mut file)?;
    file.sync_data()?;
    fs::rename(temporary, destination)
}

fn record_duration(stages: &mut serde_json::Map<String, serde_json::Value>, stage: &str, duration: u64) {
    if stages.len() >= 512 && !stages.contains_key(stage) {
        return;
    }
    let metric =
        stages.entry(stage.to_string()).or_insert_with(|| serde_json::json!({"n":0,"sum_ns":0,"max_ns":0,"buckets_log2_ns":vec![0u64;64]}));
    metric["n"] = (metric["n"].as_u64().unwrap_or(0) + 1).into();
    metric["sum_ns"] = metric["sum_ns"].as_u64().unwrap_or(0).saturating_add(duration).into();
    metric["max_ns"] = metric["max_ns"].as_u64().unwrap_or(0).max(duration).into();
    let bucket = (64 - duration.leading_zeros()).min(63) as usize;
    metric["buckets_log2_ns"][bucket] = (metric["buckets_log2_ns"][bucket].as_u64().unwrap_or(0) + 1).into();
}
