# Sight

Wearable navigation aid for blind and low-vision users. Phone is worn camera-forward (lanyard, harness, or chest mount); audio comes through AirPods. The app fuses live camera, depth, and ambient sound into prioritized speech and spatial earcons.

## Why three layers, not one

A single "send a frame to a vision model and read the answer" loop is too slow and too chatty for someone walking. Sight splits the work by latency budget:

| Layer | Latency | Frequency | Output |
|---|---|---|---|
| **Proximity** | ≤30 ms | 30–60 fps | Spatial earcons (panned ticks that speed up as obstacles get closer). Sound conveys direction faster than words. |
| **Salience** | ~200 ms | 5–10 fps | Short prioritized utterances ("step down", "person, left, 2 m") from on-device object/text/sound detection. Dedupe + interrupt rules. |
| **Narrative** | 1–3 s | On voice command only | Rich Claude vision description, called only when the user asks ("describe", "what's around me", "read that"). |

Audio is a first-class input alongside video — the camera can't see behind you, but the mic can hear the bus.

## Pieces

- **`proxy/`** — Cloudflare Worker. Holds the Anthropic API key, exposes `POST /describe`. Both the web and iOS clients call it.
- **`web/`** — PWA. Works on any modern phone (iOS Safari, Android Chrome). Camera + mic + on-device object detection (TF.js COCO-SSD) + on-device sound classification (YAMNet) + Web Speech TTS + WebAudio spatial panning.
- **`ios/`** — Native SwiftUI app. Adds LiDAR depth (when present), `SNClassifySoundRequest`, `AVSpeechSynthesizer` priority queue, background audio so the screen can be off.

## Quick start

```bash
# Proxy
cd proxy && npm install && npx wrangler dev    # local at http://localhost:8787

# Web app
cd web && python3 -m http.server 8080          # then open http://<your-ip>:8080 on your phone
                                                # (camera + mic require HTTPS on real phones; use a tunnel like cloudflared)

# iOS
open ios/Sight.xcodeproj                        # requires Xcode + Apple Developer account
```

## Status

Early scaffold. Proxy and web app are end-to-end runnable; iOS is a skeleton that compiles into a working baseline but is not feature-complete relative to the web build yet.
