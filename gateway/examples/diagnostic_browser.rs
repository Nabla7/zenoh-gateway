//! Isolated real gateway for browser/DimOS conformance. No hardware or discovery.
use anyhow::Result;
use serde_json::json;
use zenoh_gateway::{Channel, DecodedFrame, EncodingOutput, EncodingSample, MessageEncoding, Server, VideoImage, zenoh};

struct Pattern;
impl MessageEncoding for Pattern {
    fn name(&self) -> &str {
        "diagnostic-pattern"
    }
    fn output(&self) -> EncodingOutput {
        EncodingOutput::Video
    }
    fn decode(&self, sample: &EncodingSample<'_>, _: Channel) -> Result<DecodedFrame> {
        let phase = sample.payload.first().copied().unwrap_or(0);
        // Explicit fault input used only by this isolated qualification binary.
        if let Some(delay) = sample.payload.get(1).copied().filter(|d| *d > 0) {
            std::thread::sleep(std::time::Duration::from_millis(delay.into()));
        }
        let mut pixels = vec![0; 320 * 240 * 3];
        for (i, rgb) in pixels.chunks_exact_mut(3).enumerate() {
            rgb.copy_from_slice(&[(i % 320) as u8, (i / 320) as u8, phase]);
        }
        Ok(DecodedFrame::Video(VideoImage::rgb8(320, 240, pixels)?))
    }
}

#[tokio::main]
async fn main() -> Result<()> {
    let endpoint = std::env::args().nth(1).ok_or_else(|| anyhow::anyhow!("explicit loopback Zenoh endpoint required"))?;
    anyhow::ensure!(endpoint.starts_with("tcp/127.0.0.1:"), "only isolated loopback endpoints are permitted");
    let mut config = zenoh::Config::default();
    for (key, value) in [
        ("scouting/multicast/enabled", "false".to_owned()),
        ("scouting/gossip/enabled", "false".to_owned()),
        ("listen/endpoints", serde_json::to_string(&vec![endpoint])?),
    ] {
        config.insert_json5(key, &value).map_err(|e| anyhow::anyhow!(e))?;
    }
    let session = zenoh::open(config).await.map_err(|e| anyhow::anyhow!(e))?;
    let server = Server::builder().session(session).encoding(Pattern).build().await?;
    let running = server.bind("127.0.0.1:0").await?;
    println!("{}", json!({"url":format!("http://{}",running.local_addr())}));
    tokio::task::spawn_blocking(|| std::io::stdin().read_line(&mut String::new())).await??;
    running.shutdown().await?;
    Ok(())
}
