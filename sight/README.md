# Sight

Wearable navigation aid for blind users. Phone is worn camera-forward (lanyard, harness, chest mount); audio comes through AirPods. The app fuses live camera, ambient sound, and on-demand AI vision into prioritized speech and spatial earcons.

**Voice-first.** The blind user never needs to find a button or read a screen. The app talks to them, listens for AirPods double-taps, and reads any tap-anywhere gesture.

**Two priority use cases:** not hitting things while walking, and crossing roads safely.

## Ship it

```bash
cd sight
./deploy.sh
```

That installs deps, logs into Cloudflare, prompts for your Anthropic API key (stored as a Worker secret, never in the repo), and deploys. You get back a single URL like `https://sight.<your-subdomain>.workers.dev` that serves both the PWA and the AI proxy.

## How a blind user uses it

1. Someone (or Siri) opens the URL on their iPhone in Safari.
2. App says: **"Sight is ready. I will warn you about people and vehicles automatically."** Then: instructions.
3. They wear the phone camera-forward and put in AirPods.
4. **Single tap anywhere on the screen** → describes what's ahead
5. **Double tap anywhere** → starts **crossing mode** (auto-exits after 90 s, or double-tap again to exit)
6. **Hold the screen** → re-reads the instructions
7. **AirPods double-tap** → describes (same as single tap)
8. They don't need to do anything to get hazard warnings — the app speaks "car left, approaching" or "person ahead" automatically the moment it sees one.

On Android Chrome, voice commands also work: say "describe", "crossing", "stop", "help".

## How it stays safe and fast

Three layers, each tuned to a different latency budget:

| Layer | Where | Latency | What it does |
|---|---|---|---|
| **Proximity earcons** | WebAudio | ~20 ms | Ticks pan left/right and speed up as the closest in-path object gets closer. Sound conveys direction faster than words. |
| **Salience** | TF.js COCO-SSD on-device, ~5 fps | ~200 ms | Tracks objects frame-to-frame (`tracker.js`). Anything **looming** (bbox area growing >20 % in the last second) or sitting **in your path** (centre-bottom of the frame) gets HAZARD priority. Speaks short utterances like "car left, approaching" with dedupe + interruption rules. |
| **Narrative** | Claude vision via the Worker, on-demand | 1–3 s | Single-tap → Opus describes the scene (40 words, hazards first). Double-tap → **crossing mode** flips the model to **Haiku** with a traffic-only system prompt that returns 5-word verdicts ("wait", "go", "car left", "almost across") every 1.5 s while you cross. |

Audio is a first-class input: the Worker passes recent on-device sound classifier hits and any ambient speech transcript to Claude in the same prompt as the image. The camera can't see behind you, but the mic can hear the bus.

## Layout

- **`proxy/`** — Cloudflare Worker. Holds the Anthropic key. Serves `web/` at `/` and answers `POST /describe` with `mode: "describe" | "read" | "crossing"`.
- **`web/`** — PWA. Camera + mic + on-device detection + tracker + spatial audio + voice coach. The blind user only needs this URL.
- **`ios/`** — Native SwiftUI scaffold for a future LiDAR-aware iOS app. Not needed for v1.

## Run locally

```bash
cd sight/proxy
npm install
echo "ANTHROPIC_API_KEY=sk-ant-..." > .dev.vars
npx wrangler dev
# Camera + mic require HTTPS on real phones. Tunnel with cloudflared:
#   cloudflared tunnel --url http://localhost:8787
```

## Roadmap (next)

- Replace WebAudio FFT heuristic with **YAMNet** for real sound categories (siren, dog bark, doorbell, beeping crosswalk signal).
- iOS app: ARKit `smoothedSceneDepth` for true distance on Pro phones, `SNClassifySoundRequest` for first-class sound categories, screen-off background-audio mode.
- Detect crosswalk signals visually (red hand vs walking person) so crossing mode doesn't depend on a round-trip to Claude.
- Curb / step detection — the most underserved hazard.
