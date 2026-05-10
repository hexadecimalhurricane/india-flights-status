// Spatial earcons via WebAudio.
// Each obstacle gets a short tick; pan reflects horizontal position in frame,
// pitch + tick rate reflect distance proxy (object area / vertical position).
let ctx = null;
let master = null;

export function initAudio() {
  if (ctx) return ctx;
  ctx = new (window.AudioContext || window.webkitAudioContext)();
  master = ctx.createGain();
  master.gain.value = 0.18;
  master.connect(ctx.destination);
  return ctx;
}

export function resumeAudio() {
  if (ctx && ctx.state === "suspended") ctx.resume();
}

// pan: -1 (hard left) to +1 (hard right)
// closeness: 0 (far) to 1 (very close)
export function tick(pan = 0, closeness = 0.3) {
  if (!ctx) return;
  const t = ctx.currentTime;
  const osc = ctx.createOscillator();
  osc.type = "triangle";
  osc.frequency.value = 380 + closeness * 600;

  const env = ctx.createGain();
  env.gain.setValueAtTime(0.0001, t);
  env.gain.exponentialRampToValueAtTime(0.6, t + 0.005);
  env.gain.exponentialRampToValueAtTime(0.0001, t + 0.09 + closeness * 0.06);

  const panner = ctx.createStereoPanner();
  panner.pan.value = Math.max(-1, Math.min(1, pan));

  osc.connect(env).connect(panner).connect(master);
  osc.start(t);
  osc.stop(t + 0.18);
}

// Continuous "headlight" — emits ticks at a rate that scales with closeness
let beat = null;
export function setProximity(pan, closeness) {
  if (!ctx) return;
  const intervalMs = Math.max(90, 700 - closeness * 600);
  if (beat) { clearInterval(beat.id); beat = null; }
  if (closeness <= 0.05) return;
  beat = { id: setInterval(() => tick(pan, closeness), intervalMs) };
}

export function clearProximity() {
  if (beat) { clearInterval(beat.id); beat = null; }
}
