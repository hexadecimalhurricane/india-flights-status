import { settings, saveSettings } from "./store.js";
import { listVoices, pickVoice, say, shutUp, Priority } from "./speech.js";
import { initAudio, resumeAudio } from "./earcons.js";
import { startVision } from "./vision.js";
import { startSound, onVoiceCommand } from "./sound.js";
import { describe, getLastNarrative } from "./narrative.js";
import { bindVideo, isCrossing, toggleCrossing, speakWelcome, speakHelp } from "./coach.js";

const $ = (id) => document.getElementById(id);
const video = $("cam");
const status = $("status");

let started = false;

// Gesture state
const DOUBLE_TAP_MS = 350;
const LONG_PRESS_MS = 700;
let pendingTap = null;
let longPressTimer = null;
let pressedAt = 0;

async function start() {
  if (started) return;
  started = true;
  status.textContent = "Starting…";

  initAudio();
  resumeAudio();
  startSilentAudioLoop();

  try {
    const stream = await navigator.mediaDevices.getUserMedia({
      video: { facingMode: { ideal: "environment" }, width: { ideal: 1280 }, height: { ideal: 720 } },
      audio: false,
    });
    video.srcObject = stream;
    await video.play();
  } catch (e) {
    status.textContent = "Camera permission denied. Reload and allow camera access.";
    say("Camera permission was denied. Please reload and allow access.", Priority.NARRATIVE, 0);
    console.warn(e);
    return;
  }

  await populateVoices();
  bindVideo(video);

  status.textContent = "Listening";
  speakWelcome();

  startVision(video).catch((e) => console.warn("vision", e));
  startSound().catch((e) => console.warn("sound", e));

  onVoiceCommand((cmd) => {
    if (cmd === "describe") doDescribe();
    else if (cmd === "read") describeRead();
    else if (cmd === "stop") shutUp();
    else if (cmd === "repeat") { const t = getLastNarrative(); if (t) say(t, Priority.NARRATIVE, 0); }
    else if (cmd === "crossing" || cmd === "cross") toggleCrossing();
    else if (cmd === "help") speakHelp();
  });

  if ("mediaSession" in navigator) {
    navigator.mediaSession.metadata = new MediaMetadata({ title: "Sight", artist: "Surroundings", album: "Sight" });
    navigator.mediaSession.setActionHandler("play", doDescribe);
    navigator.mediaSession.setActionHandler("pause", () => shutUp());
    navigator.mediaSession.setActionHandler("nexttrack", describeRead);
    navigator.mediaSession.setActionHandler("previoustrack", () => { const t = getLastNarrative(); if (t) say(t, Priority.NARRATIVE, 0); });
    navigator.mediaSession.playbackState = "playing";
  }

  if ("serviceWorker" in navigator) {
    try { await navigator.serviceWorker.register("./sw.js"); } catch {}
  }
}

function doDescribe() {
  if (isCrossing()) { toggleCrossing(); return; } // tapping during crossing exits crossing
  describe(video, "Describe what's ahead of me, hazards first.");
}

function describeRead() {
  describe(video, "Read any signs, prices, or large text in view, top to bottom.");
}

// --- gesture state machine ---
//
// single tap          → describe (or exit crossing if active)
// double tap          → toggle crossing mode
// long press (700ms)  → speak help
//
// We resolve single vs double on a short timer.

document.body.addEventListener("pointerdown", (e) => {
  if (e.target.closest("button, input, select, label, #settings")) return;
  pressedAt = performance.now();
  longPressTimer = setTimeout(() => {
    longPressTimer = null;
    if (!started) start();
    else speakHelp();
  }, LONG_PRESS_MS);
});

document.body.addEventListener("pointerup", (e) => {
  if (e.target.closest("button, input, select, label, #settings")) return;
  if (!longPressTimer) return; // long-press already fired
  clearTimeout(longPressTimer);
  longPressTimer = null;

  const heldMs = performance.now() - pressedAt;
  if (heldMs > LONG_PRESS_MS) return; // shouldn't happen, defensive

  if (!started) { start(); return; }

  if (pendingTap) {
    clearTimeout(pendingTap.id);
    pendingTap = null;
    toggleCrossing(); // double tap
    return;
  }
  pendingTap = { id: setTimeout(() => { pendingTap = null; doDescribe(); }, DOUBLE_TAP_MS) };
});

document.body.addEventListener("pointercancel", () => {
  if (longPressTimer) { clearTimeout(longPressTimer); longPressTimer = null; }
});

// --- silent audio loop so iOS routes AirPods double-tap to us ---
function startSilentAudioLoop() {
  try {
    const a = document.createElement("audio");
    a.loop = true; a.preload = "auto";
    a.src = "data:audio/wav;base64,UklGRiQAAABXQVZFZm10IBAAAAABAAEAQB8AAEAfAAABAAgAZGF0YQAAAAA=";
    a.play().catch(() => {});
  } catch {}
}

async function populateVoices() {
  const sel = $("voice");
  const voices = await listVoices();
  sel.innerHTML = "";
  for (const v of voices) {
    const o = document.createElement("option");
    o.value = v.voiceURI; o.textContent = `${v.name} (${v.lang})`;
    sel.appendChild(o);
  }
  if (settings.voiceURI) sel.value = settings.voiceURI;
  await pickVoice(sel.value);
}

// Sighted-helper settings panel — hidden gear icon, doesn't affect voice flow
$("settingsBtn").addEventListener("click", (e) => {
  e.stopPropagation();
  $("proxyUrl").value = settings.proxyUrl;
  $("clientToken").value = settings.clientToken;
  $("rate").value = settings.rate;
  $("earcons").checked = settings.earcons;
  $("ambient").checked = settings.ambient;
  $("settings").hidden = false;
});
$("closeSettings").addEventListener("click", async () => {
  settings.proxyUrl = $("proxyUrl").value.trim();
  settings.clientToken = $("clientToken").value.trim();
  settings.voiceURI = $("voice").value;
  settings.rate = parseFloat($("rate").value);
  settings.earcons = $("earcons").checked;
  settings.ambient = $("ambient").checked;
  saveSettings();
  await pickVoice(settings.voiceURI);
  $("settings").hidden = true;
});

document.addEventListener("visibilitychange", () => { if (!document.hidden) resumeAudio(); });
