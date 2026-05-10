import { settings, saveSettings } from "./store.js";
import { listVoices, pickVoice, say, shutUp, Priority } from "./speech.js";
import { initAudio, resumeAudio } from "./earcons.js";
import { startVision } from "./vision.js";
import { startSound, onVoiceCommand } from "./sound.js";
import { describe, getLastNarrative } from "./narrative.js";

const $ = (id) => document.getElementById(id);
const video = $("cam");
const status = $("status");
const describeBtn = $("describeBtn");

let started = false;

async function start() {
  if (started) return;
  started = true;
  status.textContent = "Starting…";

  initAudio();
  resumeAudio();

  try {
    const stream = await navigator.mediaDevices.getUserMedia({
      video: { facingMode: { ideal: "environment" }, width: { ideal: 1280 }, height: { ideal: 720 } },
      audio: false,
    });
    video.srcObject = stream;
    await video.play();
  } catch (e) {
    status.textContent = "Camera permission denied.";
    console.warn(e);
    return;
  }

  await populateVoices();

  status.textContent = "Listening…";
  say("Sight ready.", Priority.NARRATIVE, 0);

  startVision(video).catch((e) => console.warn("vision", e));
  startSound().catch((e) => console.warn("sound", e));

  onVoiceCommand((cmd) => {
    if (cmd === "describe") describe(video, "Describe what's ahead of me, hazards first.");
    else if (cmd === "read") describe(video, "Read any signs, prices, or large text in view, top to bottom.");
    else if (cmd === "stop") shutUp();
    else if (cmd === "repeat") { const t = getLastNarrative(); if (t) say(t, Priority.NARRATIVE, 0); }
  });

  if ("serviceWorker" in navigator) {
    try { await navigator.serviceWorker.register("./sw.js"); } catch {}
  }
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

document.addEventListener("click", start, { once: false });
describeBtn.addEventListener("click", (e) => {
  e.stopPropagation();
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

// AirPods/headset media keys + page lifecycle
navigator.mediaSession?.setActionHandler?.("play", () => describe(video));
navigator.mediaSession?.setActionHandler?.("pause", shutUp);
document.addEventListener("visibilitychange", () => { if (!document.hidden) resumeAudio(); });
