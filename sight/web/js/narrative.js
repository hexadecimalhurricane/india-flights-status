// Narrative layer: capture frame(s) + sound context, POST to proxy, speak the answer.
import { say, Priority } from "./speech.js";
import { settings } from "./store.js";
import { getSoundContext } from "./sound.js";

let lastNarrative = "";

export function getLastNarrative() { return lastNarrative; }

export async function describe(video, question) {
  // Default to the same origin (works when the Worker serves both the PWA and /describe).
  const base = (settings.proxyUrl || location.origin).replace(/\/$/, "");
  say("One moment.", Priority.NARRATIVE, 0);
  const frames = [grabFrame(video, 640)];
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
        sound_hints: ctx.sound_hints,
        speech: ctx.speech,
      }),
    });
    if (!res.ok) {
      say("Description failed.", Priority.NARRATIVE, 0);
      return;
    }
    const data = await res.json();
    lastNarrative = data.text || "";
    if (lastNarrative) say(lastNarrative, Priority.NARRATIVE, 0);
    else say("No description available.", Priority.NARRATIVE, 0);
  } catch (e) {
    console.warn(e);
    say("Network error.", Priority.NARRATIVE, 0);
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
