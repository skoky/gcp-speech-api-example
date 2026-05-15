use std::env;
use std::io::{self, Write};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use anyhow::{Context, Result, anyhow, bail};
use cpal::traits::{DeviceTrait, HostTrait, StreamTrait};
use googleapis_tonic_google_cloud_speech_v2::google::cloud::speech::v2::{
    ExplicitDecodingConfig, PhraseSet, RecognitionConfig, SpeechAdaptation,
    StreamingRecognitionConfig, StreamingRecognitionFeatures, StreamingRecognizeRequest,
    explicit_decoding_config::AudioEncoding, phrase_set,
    recognition_config::DecodingConfig, speech_adaptation, speech_client::SpeechClient,
    streaming_recognize_request,
};
use tokio::sync::mpsc;
use tokio_stream::wrappers::ReceiverStream;
use tonic::metadata::MetadataValue;
use tonic::transport::{Channel, ClientTlsConfig};
use tonic::{Request, Status};

const GOOGLE_SCOPE: &str = "https://www.googleapis.com/auth/cloud-platform";
const DEFAULT_LOCATION: &str = "europe-west4";
const TARGET_SAMPLE_RATE: u32 = 16_000;
const TARGET_CHANNELS: u16 = 1;

#[tokio::main]
async fn main() -> Result<()> {
    rustls::crypto::ring::default_provider()
        .install_default()
        .map_err(|_| anyhow!("failed to install rustls ring provider"))?;

    let project_id = env::var("GOOGLE_CLOUD_PROJECT")
        .context("set GOOGLE_CLOUD_PROJECT to your Google Cloud project id")?;
    let location =
        env::var("GOOGLE_CLOUD_LOCATION").unwrap_or_else(|_| DEFAULT_LOCATION.to_owned());
    let endpoint =
        env::var("GOOGLE_CLOUD_SPEECH_ENDPOINT").unwrap_or_else(|_| speech_endpoint(&location));
    let bearer_token = read_bearer_token()
        .context("set GOOGLE_API_TOKEN or GOOGLE_ACCESS_TOKEN, or run `gcloud auth application-default print-access-token` and export the result")?;

    let recognizer = format!(
        "projects/{project_id}/locations/{location}/recognizers/_"
    );

    let channel = Channel::from_shared(endpoint.clone())
        .with_context(|| format!("invalid speech endpoint: {endpoint}"))?
        .tls_config(
            ClientTlsConfig::new()
                .domain_name(speech_domain(&location))
                .with_native_roots(),
        )
        .with_context(|| format!("failed to configure TLS for {endpoint}"))?
        .connect()
        .await
        .with_context(|| format!("failed to connect to Google Cloud Speech-to-Text via {endpoint}"))?;

    let auth_header: MetadataValue<_> = format!("Bearer {bearer_token}")
        .parse()
        .context("invalid bearer token")?;

    let mut client = SpeechClient::with_interceptor(channel, move |mut req: Request<()>| {
        req.metadata_mut()
            .insert("authorization", auth_header.clone());
        req.metadata_mut()
            .insert("x-goog-user-project", project_id.parse().map_err(|_| {
                Status::internal("failed to encode x-goog-user-project metadata")
            })?);
        Ok(req)
    });

    let (request_tx, request_rx) = mpsc::channel::<StreamingRecognizeRequest>(32);
    let response_task = tokio::spawn(async move {
        let mut last_partial = String::new();
        let response = client
            .streaming_recognize(ReceiverStream::new(request_rx))
            .await
            .context("streaming_recognize request failed")?;

        let mut inbound = response.into_inner();
        while let Some(message) = inbound
            .message()
            .await
            .context("failed to read streaming response")?
        {
            for result in message.results {
                if let Some(alternative) = result.alternatives.first() {
                    let transcript = alternative.transcript.trim();
                    if transcript.is_empty() {
                        continue;
                    }

                    if result.is_final {
                        if !last_partial.is_empty() {
                            print!("\r\x1b[2K");
                        }
                        println!("[final] {transcript}");
                        io::stdout().flush().context("failed to flush final transcript")?;
                        last_partial.clear();
                    } else if transcript != last_partial {
                        print!("\r\x1b[2K[partial] {transcript}");
                        io::stdout().flush().context("failed to flush partial transcript")?;
                        last_partial.clear();
                        last_partial.push_str(transcript);
                    }
                }
            }
        }

        Ok::<(), anyhow::Error>(())
    });

    request_tx
        .send(initial_request(recognizer.clone()))
        .await
        .context("failed to send initial streaming config")?;

    let mut capture = start_microphone_capture()?;
    capture
        .stream
        .play()
        .context("failed to start microphone capture")?;

    println!(
        "Streaming microphone audio to Google Speech-to-Text v2. Press Ctrl+C to stop."
    );
    println!("Using scope: {GOOGLE_SCOPE}");
    println!("Using endpoint: {endpoint}");
    println!("Using recognizer: {recognizer}");

    let mut response_task = response_task;
    loop {
        tokio::select! {
            task_result = &mut response_task => {
                task_result.context("response task join failed")??;
                break;
            }
            maybe_chunk = capture.rx.recv() => {
                let Some(chunk) = maybe_chunk else {
                    bail!("microphone capture channel closed unexpectedly");
                };

                let mono = downmix_to_mono(&chunk, capture.channels);
                let pcm16 = resample_f32_to_pcm16(&mono, capture.sample_rate, TARGET_SAMPLE_RATE);
                if pcm16.is_empty() {
                    continue;
                }

                request_tx
                    .send(audio_request(pcm16))
                    .await
                    .context("failed to send audio chunk because the speech stream closed")?;
            }
            signal = tokio::signal::ctrl_c() => {
                signal.context("failed waiting for Ctrl+C")?;
                break;
            }
        }
    }

    drop(request_tx);
    response_task.await.context("response task join failed")??;
    Ok(())
}

fn read_bearer_token() -> Result<String> {
    env::var("GOOGLE_API_TOKEN")
        .or_else(|_| env::var("GOOGLE_ACCESS_TOKEN"))
        .map(|token| token.trim().to_owned())
        .context("missing Google access token")
}

fn speech_endpoint(location: &str) -> String {
    if location == "global" {
        "https://speech.googleapis.com".to_owned()
    } else {
        format!("https://{location}-speech.googleapis.com")
    }
}

fn speech_domain(location: &str) -> String {
    if location == "global" {
        "speech.googleapis.com".to_owned()
    } else {
        format!("{location}-speech.googleapis.com")
    }
}

fn initial_request(recognizer: String) -> StreamingRecognizeRequest {
    let inline_phrases = PhraseSet {
        name: String::new(),
        uid: String::new(),
        phrases: vec![
            phrase_set::Phrase {
                value: "Dobrý den".to_owned(),
                boost: 15.0,
            },
            phrase_set::Phrase {
                value: "Česká republika".to_owned(),
                boost: 15.0,
            },
            phrase_set::Phrase {
                value: "umělá inteligence".to_owned(),
                boost: 15.0,
            },
            phrase_set::Phrase {
                value: "kolik je hodin".to_owned(),
                boost: 15.0,
            },
        ],
        boost: 15.0,
        display_name: String::new(),
        state: 0,
        create_time: None,
        update_time: None,
        delete_time: None,
        expire_time: None,
        etag: String::new(),
        reconciling: false,
        annotations: Default::default(),
        kms_key_name: String::new(),
        kms_key_version_name: String::new(),
    };

    let adaptation = SpeechAdaptation {
        phrase_sets: vec![speech_adaptation::AdaptationPhraseSet {
            value: Some(
                speech_adaptation::adaptation_phrase_set::Value::InlinePhraseSet(
                    inline_phrases,
                ),
            ),
        }],
        custom_classes: vec![],
    };

    let config = RecognitionConfig {
        model: "chirp_2".to_owned(),
        language_codes: vec!["cs-CZ".to_owned()],
        features: None,
        adaptation: Some(adaptation),
        transcript_normalization: None,
        translation_config: None,
        denoiser_config: None,
        decoding_config: Some(DecodingConfig::ExplicitDecodingConfig(
            ExplicitDecodingConfig {
                encoding: AudioEncoding::Linear16 as i32,
                sample_rate_hertz: TARGET_SAMPLE_RATE as i32,
                audio_channel_count: TARGET_CHANNELS as i32,
            },
        )),
    };

    let streaming_config = StreamingRecognitionConfig {
        config: Some(config),
        config_mask: None,
        streaming_features: Some(StreamingRecognitionFeatures {
            enable_voice_activity_events: false,
            interim_results: true,
            voice_activity_timeout: None,
        }),
    };

    StreamingRecognizeRequest {
        recognizer,
        streaming_request: Some(
            streaming_recognize_request::StreamingRequest::StreamingConfig(streaming_config),
        ),
    }
}

fn audio_request(audio: Vec<u8>) -> StreamingRecognizeRequest {
    StreamingRecognizeRequest {
        recognizer: String::new(),
        streaming_request: Some(streaming_recognize_request::StreamingRequest::Audio(
            audio,
        )),
    }
}

struct MicrophoneCapture {
    stream: cpal::Stream,
    rx: mpsc::Receiver<Vec<f32>>,
    sample_rate: u32,
    channels: u16,
}

fn start_microphone_capture() -> Result<MicrophoneCapture> {
    let host = cpal::default_host();
    let device = host
        .default_input_device()
        .context("no default microphone input device found")?;
    let supported_config = device
        .default_input_config()
        .context("failed to query default microphone config")?;

    let sample_rate = supported_config.sample_rate().0;
    let channels = supported_config.channels();
    let stream_config: cpal::StreamConfig = supported_config.clone().into();

    let (tx, rx) = mpsc::channel(32);
    let shared_tx = Arc::new(Mutex::new(tx));
    let error_callback = |err: cpal::StreamError| {
        let message = err.to_string();
        if message.contains("alsa::poll() spuriously returned") {
            return;
        }
        eprintln!("microphone stream error: {message}");
    };

    let stream = match supported_config.sample_format() {
        cpal::SampleFormat::F32 => build_input_stream::<f32>(
            &device,
            &stream_config,
            shared_tx,
            error_callback,
        )?,
        cpal::SampleFormat::I16 => build_input_stream::<i16>(
            &device,
            &stream_config,
            shared_tx,
            error_callback,
        )?,
        cpal::SampleFormat::U16 => build_input_stream::<u16>(
            &device,
            &stream_config,
            shared_tx,
            error_callback,
        )?,
        other => bail!("unsupported microphone sample format: {other:?}"),
    };

    Ok(MicrophoneCapture {
        stream,
        rx,
        sample_rate,
        channels,
    })
}

fn build_input_stream<T>(
    device: &cpal::Device,
    config: &cpal::StreamConfig,
    tx: Arc<Mutex<mpsc::Sender<Vec<f32>>>>,
    error_callback: impl FnMut(cpal::StreamError) + Send + 'static,
) -> Result<cpal::Stream>
where
    T: cpal::Sample + cpal::SizedSample,
    f32: cpal::FromSample<T>,
{
    let stream = device.build_input_stream(
        config,
        move |data: &[T], _| {
            let chunk: Vec<f32> = data.iter().map(|sample| sample.to_sample::<f32>()).collect();
            if let Ok(sender) = tx.lock() {
                let _ = sender.try_send(chunk);
            }
        },
        error_callback,
        Some(Duration::from_millis(100)),
    )?;

    Ok(stream)
}

fn downmix_to_mono(samples: &[f32], channels: u16) -> Vec<f32> {
    if channels <= 1 {
        return samples.to_vec();
    }

    let channels = channels as usize;
    samples
        .chunks_exact(channels)
        .map(|frame| frame.iter().sum::<f32>() / channels as f32)
        .collect()
}

fn resample_f32_to_pcm16(input: &[f32], from_hz: u32, to_hz: u32) -> Vec<u8> {
    if input.is_empty() {
        return Vec::new();
    }

    let mono = if from_hz == to_hz {
        input.to_vec()
    } else {
        let ratio = from_hz as f64 / to_hz as f64;
        let output_len = ((input.len() as f64) / ratio).ceil() as usize;
        let mut output = Vec::with_capacity(output_len);

        for i in 0..output_len {
            let src = i as f64 * ratio;
            let left = src.floor() as usize;
            let right = (left + 1).min(input.len().saturating_sub(1));
            let frac = (src - left as f64) as f32;
            let sample = input[left] * (1.0 - frac) + input[right] * frac;
            output.push(sample);
        }

        output
    };

    let mut bytes = Vec::with_capacity(mono.len() * 2);
    for sample in mono {
        let clamped = sample.clamp(-1.0, 1.0);
        let pcm = (clamped * i16::MAX as f32) as i16;
        bytes.extend_from_slice(&pcm.to_le_bytes());
    }
    bytes
}
