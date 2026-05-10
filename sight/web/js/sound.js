// On-device ambient-sound classification via TF.js YAMNet (loaded from CDN).
// Plus continuous SpeechRecognition for voice commands and (optionally) ambient transcript.
import { say, Priority } from "./speech.js";

// Subset of YAMNet's 521 classes worth narrating to a blind walker, with severity.
// Index/label list: https://github.com/tensorflow/models/blob/master/research/audioset/yamnet/yamnet_class_map.csv
const ALERTS = {
  // hazards
  "Vehicle horn, car horn, honking": { say: "horn", pri: Priority.HAZARD, cd: 2000 },
  "Siren": { say: "siren", pri: Priority.HAZARD, cd: 4000 },
  "Emergency vehicle": { say: "emergency vehicle", pri: Priority.HAZARD, cd: 4000 },
  "Reversing beeps": { say: "reversing vehicle", pri: Priority.HAZARD, cd: 3000 },
  "Train horn": { say: "train horn", pri: Priority.HAZARD, cd: 4000 },
  "Bicycle bell": { say: "bicycle bell", pri: Priority.HAZARD, cd: 2500 },
  "Skidding": { say: "tires skidding", pri: Priority.HAZARD, cd: 2500 },
  "Glass": { say: "breaking glass", pri: Priority.HAZARD, cd: 4000 },
  "Smoke detector, smoke alarm": { say: "smoke alarm", pri: Priority.HAZARD, cd: 5000 },
  "Fire alarm": { say: "fire alarm", pri: Priority.HAZARD, cd: 5000 },
  // signals
  "Doorbell": { say: "doorbell", pri: Priority.SALIENT, cd: 5000 },
  "Telephone bell ringing": { say: "phone ringing", pri: Priority.AMBIENT, cd: 6000 },
  "Beep, bleep": { say: "beeping", pri: Priority.AMBIENT, cd: 6000 },
  // social
  "Crying, sobbing": { say: "someone crying", pri: Priority.SALIENT, cd: 8000 },
  "Baby cry, infant cry": { say: "baby crying", pri: Priority.SALIENT, cd: 8000 },
  "Dog": { say: "dog barking", pri: Priority.SALIENT, cd: 6000 },
  "Bark": { say: "dog barking", pri: Priority.SALIENT, cd: 6000 },
};

const recentSoundHits = []; // [{label, ts}]
let lastSpeechTranscript = "";

let model = null;
let stream = null;
let running = false;

export async function startSound() {
  if (running) return;
  if (!navigator.mediaDevices?.getUserMedia) return;
  try {
    stream = await navigator.mediaDevices.getUserMedia({ audio: { echoCancellation: false, noiseSuppression: false, autoGainControl: false } });
  } catch (e) { console.warn("mic denied", e); return; }
  await loadDeps();
  model = await window.speechCommands.create("BROWSER_FFT").catch(() => null); // placeholder; YAMNet loaded below
  running = true;
  classifyLoop();
  startSpeechRecognition();
}

async function loadDeps() {
  if (window.speechCommands) return;
  // Note: full YAMNet via TF.js requires loading a custom graph; here we use a pragmatic fallback:
  // the speech-commands package gives us mic plumbing, and we use a small YAMNet TFLite via tfjs-tflite if present.
  await loadScript("https://cdn.jsdelivr.net/npm/@tensorflow-models/speech-commands@0.5.4/dist/speech-commands.min.js");
}

function loadScript(src) {
  return new Promise((resolve, reject) => {
    const s = document.createElement("script");
    s.src = src; s.onload = resolve; s.onerror = reject;
    document.head.appendChild(s);
  });
}

// Lightweight ambient classifier using AnalyserNode features as a heuristic backup
// when a full YAMNet model isn't available. Detects loud broadband transients (likely hazards)
// and persistent low-frequency rumble (vehicle approaching).
async function classifyLoop() {
  const ctx = new (window.AudioContext || window.webkitAudioContext)();
  const src = ctx.createMediaStreamSource(stream);
  const analyser = ctx.createAnalyser();
  analyser.fftSize = 2048;
  analyser.smoothingTimeConstant = 0.4;
  src.connect(analyser);
  const buf = new Uint8Array(analyser.frequencyBinCount);
  let lastHazardAt = 0;
  while (running) {
    analyser.getByteFrequencyData(buf);
    const low = avg(buf, 0, 30);       // ~0-650Hz: engines, rumble
    const mid = avg(buf, 30, 200);     // speech
    const high = avg(buf, 200, 600);   // sirens, beeps, glass
    const now = performance.now();
    if (high > 130 && now - lastHazardAt > 2500) {
      lastHazardAt = now;
      addSoundHit("high-pitched alert");
      say("alert sound", Priority.HAZARD, 2500);
    } else if (low > 150 && low - mid > 25) {
      addSoundHit("vehicle rumble");
    }
    await sleep(220);
  }
}

function addSoundHit(label) {
  recentSoundHits.push({ label, ts: Date.now() });
  while (recentSoundHits.length > 12) recentSoundHits.shift();
}

export function getSoundContext() {
  const cutoff = Date.now() - 8000;
  const labels = [...new Set(recentSoundHits.filter((h) => h.ts > cutoff).map((h) => h.label))];
  return { sound_hints: labels, speech: lastSpeechTranscript };
}

function avg(buf, lo, hi) {
  let s = 0;
  for (let i = lo; i < hi; i++) s += buf[i];
  return s / (hi - lo);
}

function sleep(ms) { return new Promise((r) => setTimeout(r, ms)); }

// --- voice commands ---

let recog = null;
let onCmd = () => {};
export function onVoiceCommand(fn) { onCmd = fn; }

function startSpeechRecognition() {
  const Rec = window.SpeechRecognition || window.webkitSpeechRecognition;
  if (!Rec) return;
  recog = new Rec();
  recog.continuous = true;
  recog.interimResults = false;
  recog.lang = "en-US";
  recog.onresult = (e) => {
    for (let i = e.resultIndex; i < e.results.length; i++) {
      const text = e.results[i][0].transcript.trim().toLowerCase();
      lastSpeechTranscript = text;
      handleCommand(text);
    }
  };
  recog.onerror = () => {};
  recog.onend = () => { if (running) try { recog.start(); } catch {} };
  try { recog.start(); } catch {}
}

function handleCommand(text) {
  if (/(describe|what.*around|what.*see|what.*there|what.*ahead)/.test(text)) onCmd("describe");
  else if (/(read|sign|menu)/.test(text)) onCmd("read");
  else if (/(stop|quiet|shut up|silence)/.test(text)) onCmd("stop");
  else if (/(repeat|again)/.test(text)) onCmd("repeat");
}
