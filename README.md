# Google Cloud Setup For This Rust Speech-to-Text V2 Example

This project streams microphone audio to Google Cloud Speech-to-Text V2 with:

- model: `chirp_2`
- language: `cs-CZ`
- endpoint location: `europe-west4` by default

Rust is assumed to already be installed.

## 1. Create or choose a Google Cloud project

Pick a project ID and set it in your shell:

```bash
export GOOGLE_CLOUD_PROJECT="your-project-id"
gcloud config set project "$GOOGLE_CLOUD_PROJECT"
```

## 2. Enable the Speech-to-Text API

```bash
gcloud services enable speech.googleapis.com --project "$GOOGLE_CLOUD_PROJECT"
```

## 3. Authenticate locally with Application Default Credentials

For local development, sign in with your Google account:

```bash
gcloud auth application-default login
```

Verify that ADC can mint a token:

```bash
gcloud auth application-default print-access-token
```

## 4. Grant Speech-to-Text permissions

The identity behind your ADC token needs permission to call Speech-to-Text V2.

The minimal predefined role is:

- `roles/speech.client`

If you authenticated with your user account, grant that user:

```bash
gcloud projects add-iam-policy-binding "$GOOGLE_CLOUD_PROJECT" \
  --member="user:your-email@example.com" \
  --role="roles/speech.client"
```

If you use a service account instead, grant that service account:

```bash
gcloud projects add-iam-policy-binding "$GOOGLE_CLOUD_PROJECT" \
  --member="serviceAccount:your-service-account@${GOOGLE_CLOUD_PROJECT}.iam.gserviceaccount.com" \
  --role="roles/speech.client"
```

## 5. Export the variables this code expects

The current Rust example does **not** load ADC directly. It expects a bearer token in an env var, so export one from ADC:

```bash
export GOOGLE_API_TOKEN="$(gcloud auth application-default print-access-token)"
export GOOGLE_CLOUD_LOCATION="europe-west4"
```

Optional: override the endpoint explicitly. Normally you do not need this.

```bash
export GOOGLE_CLOUD_SPEECH_ENDPOINT="https://europe-west4-speech.googleapis.com"
```

## 6. Run the example

```bash
cargo run
```

Expected startup output is similar to:

```text
Streaming microphone audio to Google Speech-to-Text v2. Press Ctrl+C to stop.
Lowest live price tier: v2 standard recognition ($0.016/min as of 2026-05-15).
Using scope: https://www.googleapis.com/auth/cloud-platform
Using endpoint: https://europe-west4-speech.googleapis.com
Using recognizer: projects/your-project-id/locations/europe-west4/recognizers/_
```

## 7. Change language and uploaded phrase context

Both settings are in [src/main.rs](/home/skokanl/mywork/gemini-test/src/main.rs).

### Change the recognition language

In `RecognitionConfig`, update `language_codes`:

```rust
language_codes: vec!["cs-CZ".to_owned()],
```

Examples:

```rust
language_codes: vec!["en-US".to_owned()],
language_codes: vec!["de-DE".to_owned()],
language_codes: vec!["sk-SK".to_owned()],
```

### Change the uploaded phrase context

The example sends inline phrase hints through a `PhraseSet`. Update the items inside:

```rust
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
],
```

What to change:

- `value`: the phrase you want Google Speech-to-Text to favor
- `boost`: how strongly that phrase should be preferred

Practical guidance:

- keep phrases short and specific
- use phrases people are actually likely to say
- start with `boost: 10.0` to `20.0`
- too much boost can hurt recognition for normal speech

After changing language or phrase hints, just rerun:

```bash
cargo run
```

## 8. Common problems

### `Your default credentials were not found`

Run:

```bash
gcloud auth application-default login
```

### `Permission 'speech.recognizers.recognize' denied`

Your account or service account is missing Speech-to-Text IAM permissions. Grant:

```bash
roles/speech.client
```

### `The model "chirp_2" does not exist in the location named "global"`

This model is regional. Use:

```bash
export GOOGLE_CLOUD_LOCATION="europe-west4"
```

### TLS `UnknownIssuer`

This usually means your machine is behind a corporate proxy or custom CA. The code already uses native OS trust roots, so the missing trust is typically in the OS certificate store.

## Notes

- This example is for **streaming** recognition, not batch.
- For live streaming, the cheapest valid V2 tier is standard recognition pricing.
- Audio is sent as mono `LINEAR16` at `16000 Hz`.

## Sources

- Application Default Credentials setup: https://cloud.google.com/docs/authentication/provide-credentials-adc
- How ADC works: https://cloud.google.com/docs/authentication/application-default-credentials
- Speech-to-Text V2 IAM: https://cloud.google.com/speech-to-text/v2/docs/iam
- Speech IAM roles and permissions: https://cloud.google.com/iam/docs/roles-permissions/speech
- Chirp 2 regional availability: https://docs.cloud.google.com/speech-to-text/v2/docs/chirp_2-model
- Streaming recognition docs: https://cloud.google.com/speech-to-text/v2/docs/streaming-recognize
