// Narrative layer: capture frame(s) + sound context, POST to proxy, speak the answer.
import { say, Priority } from "./speech.js";
import { settings } from "./store.js";
import { getSoundContext } from "./sound.js";

let lastNarrative = "";

export function getLastNarrative() { return lastNarrative; }

export async function describe(video, question) {
  say("One moment.", Priority.NARRATIVE, 0);
  const text = await callProxy(video, { question, mode: "describe" });
  if (text) { lastNarrative = text; say(text, Priority.NARRATIVE, 0); }
  else say("No description available.", Priority.NARRATIVE, 0);
}

// Crossing mode and read mode use describeMode; the caller decides how to speak the result.
export async function describeMode(video, mode, question) {
  return await callProxy(video, { question, mode });
}

async function callProxy(video, { question, mode }) {
  const base = (settings.proxyUrl || location.origin).replace(/\/$/, "");
  const frames = [grabFrame(video, mode === "crossing" ? 480 : 640)];
  const ctx = getSoundContext();
  try {
    const res = await fetch(base + "/describe", {
      method: "POST",
      headers: {
        "content-type": "application/json",
        ...(settings.clientToken ? { authorization: `Bearer ${settings.clientToken}` } : {}),
      },
      body: JSON.stringify({
        images: frames,
        question,
        mode,
        sound_hints: ctx.sound_hints,
        speech: ctx.speech,
      }),
    });
    if (!res.ok) return "";
    const data = await res.json();
    return (data.text || "").trim();
  } catch (e) {
    console.warn(e);
    return "";
  }
}

function grabFrame(video, maxW = 640) {
  const w = video.videoWidth, h = video.videoHeight;
  const scale = Math.min(1, maxW / w);
  const cw = Math.round(w * scale), ch = Math.round(h * scale);
  const c = document.createElement("canvas");
  c.width = cw; c.height = ch;
  c.getContext("2d").drawImage(video, 0, 0, cw, ch);
  const dataUrl = c.toDataURL("image/jpeg", 0.78);
  return dataUrl.split(",", 2)[1];
}
