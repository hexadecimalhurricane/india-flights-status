// Ambient-sound awareness via WebAudio FFT heuristics + voice commands via SpeechRecognition.
// (YAMNet via tfjs-tflite is the next upgrade — captures real categories like "siren", "dog bark".)
import { say, Priority } from "./speech.js";

const recentSoundHits = []; // [{label, ts}]
let lastSpeechTranscript = "";
let stream = null;
let running = false;

export async function startSound() {
  if (running) return;
  if (!navigator.mediaDevices?.getUserMedia) return;
  try {
    stream = await navigator.mediaDevices.getUserMedia({
      audio: { echoCancellation: false, noiseSuppression: false, autoGainControl: false },
    });
  } catch (e) {
    console.warn("mic denied", e);
    return;
  }
  running = true;
  classifyLoop();
  startSpeechRecognition();
}

// Heuristic ambient classifier: loud broadband transients (sirens, alarms, horns) and
// persistent low-frequency rumble (vehicles). Replace with YAMNet for real labels.
async function classifyLoop() {
  const ctx = new (window.AudioContext || window.webkitAudioContext)();
  const src = ctx.createMediaStreamSource(stream);
  const analyser = ctx.createAnalyser();
  analyser.fftSize = 2048;
  analyser.smoothingTimeConstant = 0.4;
  src.connect(analyser);
  const buf = new Uint8Array(analyser.frequencyBinCount);
  let lastHazardAt = 0;
  let lastVehicleAt = 0;
  while (running) {
    analyser.getByteFrequencyData(buf);
    const low = avg(buf, 0, 30);     // ~0-650 Hz: engines, rumble
    const mid = avg(buf, 30, 200);   // speech band
    const high = avg(buf, 200, 600); // sirens, beeps, glass
    const now = performance.now();
    if (high > 130 && now - lastHazardAt > 2500) {
      lastHazardAt = now;
      addSoundHit("high-pitched alert");
      say("alert sound", Priority.HAZARD, 2500);
    } else if (low > 150 && low - mid > 25 && now - lastVehicleAt > 6000) {
      lastVehicleAt = now;
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
export function voiceCommandsAvailable() {
  return !!(window.SpeechRecognition || window.webkitSpeechRecognition);
}

function startSpeechRecognition() {
  const Rec = window.SpeechRecognition || window.webkitSpeechRecognition;
  if (!Rec) return; // iOS Safari has no SpeechRecognition; UI must rely on the button + AirPods media keys.
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
  if (/(crossing|cross the|crossing the)/.test(text)) onCmd("crossing");
  else if (/^help|instructions|how do i/.test(text)) onCmd("help");
  else if (/(describe|what.*around|what.*see|what.*there|what.*ahead)/.test(text)) onCmd("describe");
  else if (/(read|sign|menu)/.test(text)) onCmd("read");
  else if (/(stop|quiet|shut up|silence)/.test(text)) onCmd("stop");
  else if (/(repeat|again)/.test(text)) onCmd("repeat");
}
