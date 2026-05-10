// Priority-queued, deduplicated speech.
// Higher priority utterances interrupt lower; recently-spoken phrases are suppressed for cooldownMs.
import { settings } from "./store.js";

export const Priority = { HAZARD: 3, SALIENT: 2, NARRATIVE: 1, AMBIENT: 0 };

const recent = new Map(); // text -> ts
const queue = [];
let speakingPriority = -1;
let voice = null;

export function listVoices() {
  return new Promise((resolve) => {
    const v = speechSynthesis.getVoices();
    if (v.length) return resolve(v);
    speechSynthesis.onvoiceschanged = () => resolve(speechSynthesis.getVoices());
  });
}

export async function pickVoice(uri) {
  const voices = await listVoices();
  voice = voices.find((v) => v.voiceURI === uri)
       || voices.find((v) => v.lang?.startsWith("en") && /Samantha|Karen|Daniel|Google US/i.test(v.name))
       || voices.find((v) => v.lang?.startsWith("en"))
       || voices[0]
       || null;
  return voice;
}

export function say(text, priority = Priority.SALIENT, cooldownMs = 4000) {
  if (!text) return;
  const now = performance.now();
  const last = recent.get(text);
  if (last && now - last < cooldownMs) return;
  recent.set(text, now);

  if (priority > speakingPriority) {
    speechSynthesis.cancel();
    queue.length = 0;
    queue.push({ text, priority });
    drain();
    return;
  }
  queue.push({ text, priority });
  if (!speechSynthesis.speaking) drain();
}

function drain() {
  const next = queue.shift();
  if (!next) { speakingPriority = -1; return; }
  speakingPriority = next.priority;
  const u = new SpeechSynthesisUtterance(next.text);
  if (voice) u.voice = voice;
  u.rate = settings.rate;
  u.onend = u.onerror = () => { speakingPriority = -1; drain(); };
  speechSynthesis.speak(u);
}

export function shutUp() {
  queue.length = 0;
  speechSynthesis.cancel();
  speakingPriority = -1;
}
