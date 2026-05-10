// On-device object detection at ~5 fps using TF.js COCO-SSD (loaded from CDN).
// Emits events: detected objects with normalized bbox, derived pan + closeness.
import { say, Priority } from "./speech.js";
import { setProximity, tick } from "./earcons.js";
import { settings } from "./store.js";

const HAZARD_CLASSES = new Set(["car", "truck", "bus", "motorcycle", "bicycle", "train"]);
const PERSON_CLASSES = new Set(["person"]);
const STATIC_OBSTACLE = new Set(["chair", "bench", "fire hydrant", "stop sign", "parking meter", "potted plant"]);

let model = null;
let video = null;
let running = false;

export async function startVision(videoEl) {
  video = videoEl;
  await loadDeps();
  if (!model) model = await window.cocoSsd.load({ base: "lite_mobilenet_v2" });
  if (running) return;
  running = true;
  loop();
}

export function stopVision() {
  running = false;
}

async function loadDeps() {
  if (window.cocoSsd) return;
  await loadScript("https://cdn.jsdelivr.net/npm/@tensorflow/tfjs@4.20.0/dist/tf.min.js");
  await loadScript("https://cdn.jsdelivr.net/npm/@tensorflow-models/coco-ssd@2.2.3/dist/coco-ssd.min.js");
}

function loadScript(src) {
  return new Promise((resolve, reject) => {
    const s = document.createElement("script");
    s.src = src; s.onload = resolve; s.onerror = reject;
    document.head.appendChild(s);
  });
}

async function loop() {
  while (running) {
    const t0 = performance.now();
    if (video.readyState >= 2) {
      try {
        const preds = await model.detect(video, 8, 0.55);
        process(preds);
      } catch (e) { console.warn("detect", e); }
    }
    const dt = performance.now() - t0;
    await sleep(Math.max(0, 180 - dt)); // ~5 fps
  }
}

function process(preds) {
  if (!preds.length) { setProximity(0, 0); return; }
  const w = video.videoWidth, h = video.videoHeight;
  let nearest = null;
  for (const p of preds) {
    const [x, y, bw, bh] = p.bbox;
    const cx = (x + bw / 2) / w;       // 0..1
    const bottom = (y + bh) / h;        // ground-contact heuristic: closer = lower in frame
    const area = (bw * bh) / (w * h);
    const closeness = Math.min(1, Math.max(area * 4, bottom > 0.55 ? (bottom - 0.55) * 2 : 0));
    const pan = (cx - 0.5) * 2;         // -1..1
    const item = { ...p, cx, bottom, area, closeness, pan };
    if (!nearest || item.closeness > nearest.closeness) nearest = item;

    if (HAZARD_CLASSES.has(p.class) && closeness > 0.25) {
      const dir = pan < -0.25 ? "left" : pan > 0.25 ? "right" : "ahead";
      say(`${p.class} ${dir}`, Priority.HAZARD, 2500);
      tick(pan, Math.max(0.7, closeness));
    } else if (PERSON_CLASSES.has(p.class) && closeness > 0.4) {
      const dir = pan < -0.25 ? "left" : pan > 0.25 ? "right" : "ahead";
      say(`person ${dir}`, Priority.SALIENT, 3500);
    } else if (STATIC_OBSTACLE.has(p.class) && closeness > 0.55) {
      say(`${p.class} ahead`, Priority.SALIENT, 5000);
    }
  }
  if (settings.earcons && nearest) setProximity(nearest.pan, nearest.closeness);
  else setProximity(0, 0);
}

function sleep(ms) { return new Promise((r) => setTimeout(r, ms)); }
