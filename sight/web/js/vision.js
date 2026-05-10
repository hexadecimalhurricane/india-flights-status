// Object detection + path-zone hazard logic + looming escalation.
// COCO-SSD at ~5 fps, fed through tracker.js for per-object motion.
import { say, Priority } from "./speech.js";
import { setProximity } from "./earcons.js";
import { settings } from "./store.js";
import { update as updateTracker, reset as resetTracker } from "./tracker.js";

const VEHICLE_CLASSES = new Set(["car", "truck", "bus", "motorcycle", "bicycle", "train"]);
const PERSON_CLASSES = new Set(["person"]);
const STATIC_OBSTACLE = new Set(["chair", "bench", "fire hydrant", "stop sign", "parking meter", "potted plant", "traffic light"]);

// Walking path zone: central horizontal third, bottom 60% of frame.
// Anything inside this rectangle is "in your path" and gets priority bumped.
const PATH_X = [0.30, 0.70];
const PATH_Y = [0.40, 1.00];

let model = null;
let video = null;
let running = false;
let mode = "default"; // "default" | "crossing"

export function setMode(m) { mode = m; resetTracker(); }
export function getMode() { return mode; }

export async function startVision(videoEl) {
  video = videoEl;
  await loadDeps();
  if (!model) model = await window.cocoSsd.load({ base: "lite_mobilenet_v2" });
  if (running) return;
  running = true;
  loop();
}

export function stopVision() { running = false; }

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
    if (video.readyState >= 2 && video.videoWidth > 0) {
      try {
        const preds = await model.detect(video, 10, 0.50);
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
  const tracked = updateTracker(preds, w, h);

  let nearestForEarcon = null;
  let topUtterance = null; // pick the single most urgent thing per frame to speak

  for (const t of tracked) {
    const inPath = t.cx >= PATH_X[0] && t.cx <= PATH_X[1] && t.bottom >= PATH_Y[0];
    const closeness = clamp01(Math.max(t.area * 4, t.bottom > 0.55 ? (t.bottom - 0.55) * 2 : 0));
    const looming = t.looming || 1;
    const dir = t.cx < 0.40 ? "left" : t.cx > 0.60 ? "right" : "ahead";
    const isVehicle = VEHICLE_CLASSES.has(t.class);
    const isPerson = PERSON_CLASSES.has(t.class);
    const isObstacle = STATIC_OBSTACLE.has(t.class);

    // Earcon target: whatever object has the highest combined urgency.
    const urgency = closeness * (inPath ? 1.6 : 1.0) * (looming > 1.15 ? 1.4 : 1.0);
    if (!nearestForEarcon || urgency > nearestForEarcon._urgency) {
      nearestForEarcon = { ...t, _urgency: urgency, _pan: (t.cx - 0.5) * 2 };
    }

    // Speech rules
    let phrase = null, prio = Priority.AMBIENT, cd = 5000;

    if (isVehicle && (looming > 1.20 || (inPath && closeness > 0.20))) {
      phrase = `${t.class} ${dir}, approaching`;
      prio = Priority.HAZARD;
      cd = 1800;
    } else if (isVehicle && closeness > 0.30) {
      phrase = `${t.class} ${dir}`;
      prio = Priority.HAZARD;
      cd = 2500;
    } else if (isPerson && inPath && (looming > 1.20 || closeness > 0.45)) {
      phrase = `person ${dir}`;
      prio = Priority.HAZARD;
      cd = 2500;
    } else if (isPerson && closeness > 0.55) {
      phrase = `person ${dir}`;
      prio = Priority.SALIENT;
      cd = 4000;
    } else if (isObstacle && inPath && closeness > 0.45) {
      phrase = `${t.class} ahead`;
      prio = Priority.SALIENT;
      cd = 5000;
    }

    if (phrase) {
      const score = prio * 1000 + closeness * 100 + (looming - 1) * 50;
      if (!topUtterance || score > topUtterance.score) topUtterance = { phrase, prio, cd, score };
    }
  }

  // In crossing mode the narrative layer (Haiku) owns the speech channel; vision only feeds earcons.
  if (mode !== "crossing" && topUtterance) say(topUtterance.phrase, topUtterance.prio, topUtterance.cd);

  if (settings.earcons && nearestForEarcon) {
    const closeness = clamp01(Math.max(nearestForEarcon.area * 4, nearestForEarcon.bottom > 0.55 ? (nearestForEarcon.bottom - 0.55) * 2 : 0));
    setProximity(nearestForEarcon._pan, closeness);
  } else {
    setProximity(0, 0);
  }
}

function clamp01(v) { return Math.max(0, Math.min(1, v)); }
function sleep(ms) { return new Promise((r) => setTimeout(r, ms)); }
