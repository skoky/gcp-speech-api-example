use std::env;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use anyhow::{Context, Result, anyhow, bail};
use base64::Engine;
use base64::engine::general_purpose::STANDARD as BASE64;
use cpal::traits::{DeviceTrait, HostTrait, StreamTrait};
use futures_util::{SinkExt, StreamExt};
use serde::Deserialize;
use serde_json::{Value, json};
use tokio::time::{interval, timeout};
use tokio_tungstenite::{
    connect_async,
    tungstenite::{Message, client::IntoClientRequest},
};

const WS_URL: &str = "wss://api.openai.com/v1/realtime?model=gpt-realtime";
const OPENAI_AUDIO_RATE: u32 = 24_000;
const SEND_INTERVAL_MS: u64 = 100;
const AUDIO_IDLE_FLUSH_MS: u64 = 1_200;
const SILENCE_RMS_THRESHOLD: f32 = 0.015;
const MIC_REPORT_INTERVAL_MS: u64 = 1_000;
const SESSION_TIMEOUT_SECS: u64 = 15;
const MAX_SPEECH_TURN_MS: u64 = 4_000;

#[derive(Debug, Deserialize)]
struct ServerEvent {
    #[serde(rename = "type")]
    event_type: String,
    #[serde(default)]
    delta: Option<String>,
    #[serde(default)]
    error: Option<ServerError>,
    #[serde(default)]
    session: Option<Value>,
}

#[derive(Debug, Deserialize)]
struct ServerError {
    #[serde(default)]
    message: Option<String>,
}

fn main() -> Result<()> {
    rustls::crypto::ring::default_provider()
        .install_default()
        .map_err(|_| anyhow!("failed to install rustls ring crypto provider"))?;

    let runtime = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .context("failed to create tokio runtime")?;

    runtime.block_on(async_main())
}

async fn async_main() -> Result<()> {
    let api_key = env::var("OPENAI_API_KEY")
        .context("set OPENAI_API_KEY to an OpenAI API key before running this example")?;

    let mut request = WS_URL
        .into_client_request()
        .context("failed to build websocket request")?;
    request.headers_mut().insert(
        "Authorization",
        format!("Bearer {api_key}")
            .parse()
            .context("invalid OPENAI_API_KEY header value")?,
    );

    let (ws_stream, _) = connect_async(request)
        .await
        .context("failed to connect to OpenAI Realtime API")?;
    let (mut ws_write, mut ws_read) = ws_stream.split();

    timeout(
        Duration::from_secs(SESSION_TIMEOUT_SECS),
        wait_for_session_created(&mut ws_read),
    )
    .await
    .context("timed out while waiting for OpenAI session.created")??;

    ws_write
        .send(Message::Text(build_session_update().to_string().into()))
        .await
        .context("failed to send session.update")?;

    timeout(
        Duration::from_secs(SESSION_TIMEOUT_SECS),
        wait_for_session_updated(&mut ws_read),
    )
    .await
    .context("timed out while waiting for OpenAI session.updated")??;

    let capture = start_microphone_capture()?;
    capture
        .stream
        .play()
        .context("failed to start microphone stream")?;

    let mut ticker = interval(Duration::from_millis(SEND_INTERVAL_MS));
    let mut last_non_silent_audio = Instant::now();
    let mut speech_turn_started_at: Option<Instant> = None;
    let mut last_mic_report = Instant::now();
    let mut awaiting_response = false;
    let mut saw_speech = false;
    let mut mic_peak_rms = 0.0f32;
    let mut last_output_text = String::new();

    loop {
        tokio::select! {
            _ = tokio::signal::ctrl_c() => break,
            _ = ticker.tick() => {
                let samples = drain_samples(&capture.samples)?;
                if !samples.is_empty() {
                    let rms = compute_rms(&samples);
                    mic_peak_rms = mic_peak_rms.max(rms);
                    if rms >= SILENCE_RMS_THRESHOLD {
                        last_non_silent_audio = Instant::now();
                        if speech_turn_started_at.is_none() {
                            speech_turn_started_at = Some(Instant::now());
                        }
                        saw_speech = true;
                    }

                    let pcm_bytes = resample_to_pcm16le(&samples, capture.sample_rate, OPENAI_AUDIO_RATE);
                    if !pcm_bytes.is_empty() {
                        let audio_message = json!({
                            "type": "input_audio_buffer.append",
                            "audio": BASE64.encode(pcm_bytes)
                        });
                        ws_write
                            .send(Message::Text(audio_message.to_string().into()))
                            .await
                            .context("failed to send audio chunk")?;
                    }
                }

                if last_mic_report.elapsed() >= Duration::from_millis(MIC_REPORT_INTERVAL_MS) {
                    mic_peak_rms = 0.0;
                    last_mic_report = Instant::now();
                }

                let silence_elapsed =
                    last_non_silent_audio.elapsed() >= Duration::from_millis(AUDIO_IDLE_FLUSH_MS);
                let max_turn_elapsed = speech_turn_started_at
                    .map(|started_at| started_at.elapsed() >= Duration::from_millis(MAX_SPEECH_TURN_MS))
                    .unwrap_or(false);

                if !awaiting_response
                    && saw_speech
                    && (silence_elapsed || max_turn_elapsed)
                {
                    ws_write
                        .send(Message::Text(
                            json!({"type": "input_audio_buffer.commit"})
                                .to_string()
                                .into(),
                        ))
                        .await
                        .context("failed to send input_audio_buffer.commit")?;
                    ws_write
                        .send(Message::Text(
                            json!({"type": "response.create"})
                                .to_string()
                                .into(),
                        ))
                        .await
                        .context("failed to send response.create")?;
                    awaiting_response = true;
                    saw_speech = false;
                    speech_turn_started_at = None;
                    last_output_text.clear();
                }
            }
            message = ws_read.next() => {
                match message {
                    Some(Ok(Message::Text(text))) => {
                        handle_server_event(&text, &mut awaiting_response, &mut last_output_text)?;
                    }
                    Some(Ok(Message::Close(frame))) => {
                        if let Some(frame) = frame {
                            bail!("OpenAI closed the websocket: code={} reason={}", frame.code, frame.reason);
                        }
                        bail!("OpenAI closed the websocket");
                    }
                    Some(Ok(_)) => {}
                    Some(Err(err)) => return Err(err).context("error while reading websocket message"),
                    None => bail!("websocket stream ended"),
                }
            }
        }
    }

    Ok(())
}

fn build_session_update() -> Value {
    json!({
        "type": "session.update",
        "session": {
            "type": "realtime",
            "instructions": "Output only the Czech translation of the spoken audio.",
            "output_modalities": ["text"],
            "audio": {
                "input": {
                    "format": {
                        "type": "audio/pcm",
                        "rate": OPENAI_AUDIO_RATE
                    },
                    "turn_detection": null
                }
            }
        }
    })
}

async fn wait_for_session_created(
    ws_read: &mut futures_util::stream::SplitStream<
        tokio_tungstenite::WebSocketStream<
            tokio_tungstenite::MaybeTlsStream<tokio::net::TcpStream>,
        >,
    >,
) -> Result<()> {
    while let Some(message) = ws_read.next().await {
        match message.context("failed while waiting for session.created")? {
            Message::Text(text) => {
                let event: ServerEvent = serde_json::from_str(&text)
                    .with_context(|| format!("invalid server event: {text}"))?;
                if event.event_type == "session.created" {
                    return Ok(());
                }
                if event.event_type == "error" {
                    bail!(
                        "OpenAI returned error before session.created: {}",
                        event
                            .error
                            .and_then(|err| err.message)
                            .unwrap_or(text.to_string())
                    );
                }
            }
            Message::Close(frame) => {
                if let Some(frame) = frame {
                    bail!(
                        "OpenAI closed the websocket before session.created: {} {}",
                        frame.code,
                        frame.reason
                    );
                }
                bail!("OpenAI closed the websocket before session.created");
            }
            _ => {}
        }
    }

    bail!("websocket closed before session.created")
}

async fn wait_for_session_updated(
    ws_read: &mut futures_util::stream::SplitStream<
        tokio_tungstenite::WebSocketStream<
            tokio_tungstenite::MaybeTlsStream<tokio::net::TcpStream>,
        >,
    >,
) -> Result<()> {
    while let Some(message) = ws_read.next().await {
        match message.context("failed while waiting for session.updated")? {
            Message::Text(text) => {
                let event: ServerEvent = serde_json::from_str(&text)
                    .with_context(|| format!("invalid server event: {text}"))?;
                match event.event_type.as_str() {
                    "session.updated" => return Ok(()),
                    "error" => bail!(
                        "OpenAI rejected session.update: {}",
                        event
                            .error
                            .and_then(|err| err.message)
                            .unwrap_or(text.to_string())
                    ),
                    _ => {}
                }
            }
            Message::Close(frame) => {
                if let Some(frame) = frame {
                    bail!(
                        "OpenAI closed the websocket before session.updated: {} {}",
                        frame.code,
                        frame.reason
                    );
                }
                bail!("OpenAI closed the websocket before session.updated");
            }
            _ => {}
        }
    }

    bail!("websocket closed before session.updated")
}

fn handle_server_event(
    text: &str,
    awaiting_response: &mut bool,
    last_output_text: &mut String,
) -> Result<()> {
    let event: ServerEvent = serde_json::from_str(text)
        .with_context(|| format!("failed to parse server event: {text}"))?;

    match event.event_type.as_str() {
        "response.output_text.delta" => {
            if let Some(delta) = event.delta {
                print_delta("", &delta, last_output_text);
            }
        }
        "response.output_text.done" => {
            println!();
        }
        "response.done" => {
            *awaiting_response = false;
            println!();
        }
        "response.created"
        | "response.output_item.added"
        | "response.content_part.added"
        | "input_audio_buffer.committed"
        | "conversation.item.created" => {}
        "error" => {
            bail!(
                "OpenAI returned an error: {}",
                event
                    .error
                    .and_then(|err| err.message)
                    .unwrap_or_else(|| text.to_string())
            );
        }
        _ => {
            let _ = event.session;
        }
    }

    Ok(())
}

fn print_delta(prefix: &str, delta: &str, full_text: &mut String) {
    if delta.is_empty() {
        return;
    }

    if !prefix.is_empty() && full_text.is_empty() {
        print!("{prefix} ");
    }
    print!("{delta}");
    full_text.push_str(delta);
}

struct CaptureState {
    stream: cpal::Stream,
    samples: Arc<Mutex<Vec<f32>>>,
    sample_rate: u32,
}

fn start_microphone_capture() -> Result<CaptureState> {
    let host = cpal::default_host();
    let device = host
        .default_input_device()
        .ok_or_else(|| anyhow!("no default microphone/input device found"))?;
    let supported = device
        .default_input_config()
        .context("failed to get default microphone config")?;

    let sample_rate = supported.sample_rate().0;
    let channels = supported.channels() as usize;
    let config: cpal::StreamConfig = supported.clone().into();
    let samples = Arc::new(Mutex::new(Vec::with_capacity(sample_rate as usize)));
    let capture_store = Arc::clone(&samples);

    let stream = match supported.sample_format() {
        cpal::SampleFormat::F32 => device.build_input_stream(
            &config,
            move |data: &[f32], _| push_input_data_f32(data, channels, &capture_store),
            move |err| eprintln!("microphone stream error: {err}"),
            None,
        ),
        cpal::SampleFormat::I16 => device.build_input_stream(
            &config,
            move |data: &[i16], _| push_input_data_i16(data, channels, &capture_store),
            move |err| eprintln!("microphone stream error: {err}"),
            None,
        ),
        cpal::SampleFormat::U16 => device.build_input_stream(
            &config,
            move |data: &[u16], _| push_input_data_u16(data, channels, &capture_store),
            move |err| eprintln!("microphone stream error: {err}"),
            None,
        ),
        other => bail!("unsupported microphone sample format: {other:?}"),
    }
    .context("failed to build microphone stream")?;

    Ok(CaptureState {
        stream,
        samples,
        sample_rate,
    })
}

fn push_input_data_f32(input: &[f32], channels: usize, samples: &Arc<Mutex<Vec<f32>>>) {
    push_frames(input, channels, samples, |sample| sample);
}

fn push_input_data_i16(input: &[i16], channels: usize, samples: &Arc<Mutex<Vec<f32>>>) {
    push_frames(input, channels, samples, |sample| {
        sample as f32 / i16::MAX as f32
    });
}

fn push_input_data_u16(input: &[u16], channels: usize, samples: &Arc<Mutex<Vec<f32>>>) {
    push_frames(input, channels, samples, |sample| {
        (sample as f32 / u16::MAX as f32) * 2.0 - 1.0
    });
}

fn push_frames<T>(
    input: &[T],
    channels: usize,
    samples: &Arc<Mutex<Vec<f32>>>,
    convert: impl Fn(T) -> f32,
) where
    T: Copy,
{
    if channels == 0 {
        return;
    }

    if let Ok(mut buffer) = samples.lock() {
        for frame in input.chunks(channels) {
            let sum: f32 = frame.iter().copied().map(&convert).sum();
            buffer.push((sum / channels as f32).clamp(-1.0, 1.0));
        }
    }
}

fn drain_samples(samples: &Arc<Mutex<Vec<f32>>>) -> Result<Vec<f32>> {
    let mut buffer = samples
        .lock()
        .map_err(|_| anyhow!("microphone sample buffer was poisoned"))?;
    Ok(std::mem::take(&mut *buffer))
}

fn resample_to_pcm16le(input: &[f32], input_rate: u32, output_rate: u32) -> Vec<u8> {
    if input.is_empty() {
        return Vec::new();
    }

    let output_len =
        ((input.len() as f64 * output_rate as f64) / input_rate as f64).round() as usize;
    let mut bytes = Vec::with_capacity(output_len * 2);

    for index in 0..output_len {
        let src_pos = index as f64 * input_rate as f64 / output_rate as f64;
        let left = src_pos.floor() as usize;
        let right = (left + 1).min(input.len() - 1);
        let frac = (src_pos - left as f64) as f32;
        let sample = input[left] * (1.0 - frac) + input[right] * frac;
        let pcm = (sample.clamp(-1.0, 1.0) * i16::MAX as f32) as i16;
        bytes.extend_from_slice(&pcm.to_le_bytes());
    }

    bytes
}

fn compute_rms(input: &[f32]) -> f32 {
    if input.is_empty() {
        return 0.0;
    }

    let sum_squares: f32 = input.iter().map(|sample| sample * sample).sum();
    (sum_squares / input.len() as f32).sqrt()
}
