import { settings, saveSettings } from "./store.js";
import { listVoices, pickVoice, say, shutUp, Priority } from "./speech.js";
import { initAudio, resumeAudio } from "./earcons.js";
import { startVision } from "./vision.js";
import { startSound, onVoiceCommand, voiceCommandsAvailable } from "./sound.js";
import { describe, getLastNarrative } from "./narrative.js";

const $ = (id) => document.getElementById(id);
const video = $("cam");
const status = $("status");
const describeBtn = $("describeBtn");

let started = false;
let longPressTimer = null;

async function start() {
  if (started) return;
  started = true;
  status.textContent = "Starting…";

  initAudio();
  resumeAudio();
  startSilentAudioLoop(); // keeps iOS media session alive so AirPods double-tap reaches us

  try {
    const stream = await navigator.mediaDevices.getUserMedia({
      video: { facingMode: { ideal: "environment" }, width: { ideal: 1280 }, height: { ideal: 720 } },
      audio: false,
    });
    video.srcObject = stream;
    await video.play();
  } catch (e) {
    status.textContent = "Camera permission denied. Reload and allow camera access.";
    console.warn(e);
    return;
  }

  await populateVoices();

  const voiceHint = voiceCommandsAvailable() ? "Say 'describe' or tap." : "Tap anywhere to describe.";
  status.textContent = voiceHint;
  say("Sight ready. " + voiceHint, Priority.NARRATIVE, 0);

  startVision(video).catch((e) => console.warn("vision", e));
  startSound().catch((e) => console.warn("sound", e));

  onVoiceCommand((cmd) => {
    if (cmd === "describe") describe(video, "Describe what's ahead of me, hazards first.");
    else if (cmd === "read") describe(video, "Read any signs, prices, or large text in view, top to bottom.");
    else if (cmd === "stop") shutUp();
    else if (cmd === "repeat") { const t = getLastNarrative(); if (t) say(t, Priority.NARRATIVE, 0); }
  });

  // Whole screen = describe; long-press = stop. Settings/describe buttons stop propagation.
  document.body.addEventListener("pointerdown", onPointerDown);
  document.body.addEventListener("pointerup", onPointerUp);
  document.body.addEventListener("pointercancel", onPointerUp);

  // AirPods double-tap / hardware media keys
  if ("mediaSession" in navigator) {
    navigator.mediaSession.metadata = new MediaMetadata({ title: "Sight", artist: "Surroundings", album: "Sight" });
    navigator.mediaSession.setActionHandler("play", () => describe(video, "Describe what's ahead of me, hazards first."));
    navigator.mediaSession.setActionHandler("pause", () => shutUp());
    navigator.mediaSession.setActionHandler("nexttrack", () => describe(video, "Read any signs in view."));
    navigator.mediaSession.setActionHandler("previoustrack", () => { const t = getLastNarrative(); if (t) say(t, Priority.NARRATIVE, 0); });
    navigator.mediaSession.playbackState = "playing";
  }

  if ("serviceWorker" in navigator) {
    try { await navigator.serviceWorker.register("./sw.js"); } catch {}
  }
}

function onPointerDown(e) {
  if (e.target.closest("button, input, select, label, #settings")) return;
  longPressTimer = setTimeout(() => {
    longPressTimer = null;
    shutUp();
    say("Stopped.", Priority.NARRATIVE, 0);
  }, 600);
}

function onPointerUp(e) {
  if (e.target.closest("button, input, select, label, #settings")) return;
  if (longPressTimer) {
    clearTimeout(longPressTimer);
    longPressTimer = null;
    if (!started) { start(); return; }
    describe(video, "Describe what's ahead of me, hazards first.");
  }
}

// A silent looping buffer keeps the iOS media session in "playing" state,
// which is what causes AirPods double-tap (play/pause) to be routed here
// instead of to Music / nothing.
function startSilentAudioLoop() {
  try {
    const a = document.createElement("audio");
    a.loop = true;
    a.preload = "auto";
    // 1-second silent WAV (8 kHz mono)
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

describeBtn.addEventListener("click", (e) => {
  e.stopPropagation();
  if (!started) { start(); return; }
  describe(video, "Describe what's ahead of me, hazards first.");
});

// Settings panel
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
