# Sight

Wearable navigation aid for blind and low-vision users. Phone is worn camera-forward (lanyard, harness, or chest mount); audio comes through AirPods. The app fuses live camera, depth, and ambient sound into prioritized speech and spatial earcons.

## Ship it

```bash
cd sight
./deploy.sh
```

That's the whole thing. The script installs deps, logs you into Cloudflare, prompts for your Anthropic API key (stored as a Worker secret), and deploys. You get back a `https://sight.<your-subdomain>.workers.dev` URL that serves both the PWA and the `/describe` proxy. Open it on your phone in Safari (iOS) or Chrome (Android), tap the screen, and start walking.

Re-run `deploy.sh` any time to push updates — it's idempotent.

## Using it on the phone

1. Open the URL on your phone. **HTTPS is required** for camera + mic — Cloudflare gives you that automatically.
2. Tap the screen. Allow camera + mic permissions.
3. Wear the phone camera-forward; pop in AirPods.
4. **Tap anywhere** for a description; **long-press** to silence; **AirPods double-tap** triggers describe; **AirPods squeeze (Pro)** triggers describe.
5. On Android Chrome, you can also say **"describe", "read", "stop", "repeat"** out loud (iOS Safari has no `SpeechRecognition` API yet, so iPhone users use taps + AirPods).
6. **Add to Home Screen** to install as a PWA (full-screen, no Safari UI, faster cold start).

## Why three layers, not one

A single "send a frame to a vision model and read the answer" loop is too slow and too chatty for someone walking. Sight splits the work by latency budget:

| Layer | Latency | Frequency | Output |
|---|---|---|---|
| **Proximity** | ≤30 ms | 30–60 fps | Spatial earcons (panned ticks that speed up as obstacles get closer). Sound conveys direction faster than words. |
| **Salience** | ~200 ms | 5–10 fps | Short prioritized utterances ("step down", "person, left, 2 m") from on-device object/text/sound detection. Dedupe + interrupt rules. |
| **Narrative** | 1–3 s | On voice command / tap only | Rich Claude vision description, sent only when the user asks. |

Audio is a first-class input alongside video — the camera can't see behind you, but the mic can hear the bus.

## Layout

- **`proxy/`** — Cloudflare Worker. Holds the Anthropic API key. Serves the `web/` PWA as static assets at `/` and answers `POST /describe` for the narrative layer.
- **`web/`** — PWA: camera + mic + on-device object detection (TF.js COCO-SSD) + ambient-sound heuristics + Web Speech TTS + WebAudio spatial panning + service worker.
- **`ios/`** — Native SwiftUI scaffold for a future LiDAR-aware iOS app. Not needed for v1.

## Run locally

```bash
cd sight/proxy
npm install
echo "ANTHROPIC_API_KEY=sk-ant-..." > .dev.vars
npx wrangler dev
# Open http://localhost:8787 — but camera/mic require HTTPS, so on a real
# phone you need a tunnel: `cloudflared tunnel --url http://localhost:8787`
```

## Roadmap (next)

- Swap WebAudio FFT heuristic for real **YAMNet** sound classification (siren, dog bark, doorbell, etc.) via tfjs-tflite.
- iOS app: ARKit `smoothedSceneDepth` for true distance on Pro phones, `SNClassifySoundRequest` for first-class sound categories, background-audio so screen-off works.
- Read-the-sign mode that frames + OCR-ranks text in the central focus area.
- Crosswalk-light detector (red/green hand, walk/don't-walk countdown).
