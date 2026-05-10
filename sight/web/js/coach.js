// Voice coach: spoken onboarding, help, and the crossing-mode finite state machine.
// Crossing mode: every CROSSING_INTERVAL_MS, ship a frame to the proxy with mode="crossing"
// and speak the (haiku) verdict. Auto-exits after CROSSING_TIMEOUT_MS or on user toggle.
import { say, shutUp, Priority } from "./speech.js";
import { setMode as setVisionMode } from "./vision.js";
import { describeMode } from "./narrative.js";

const CROSSING_INTERVAL_MS = 1500;
const CROSSING_TIMEOUT_MS = 90_000;

let crossing = false;
let crossingTimer = null;
let crossingDeadline = 0;
let videoEl = null;

export function bindVideo(v) { videoEl = v; }

export function isCrossing() { return crossing; }

const HELP = [
  "Sight is listening.",
  "Single tap to describe what's ahead.",
  "Double tap to start crossing the road mode.",
  "Hold the screen for these instructions.",
  "AirPods double tap also describes.",
].join(" ");

export function speakWelcome() {
  say("Sight is ready. I will warn you about people and vehicles automatically.", Priority.NARRATIVE, 0);
  setTimeout(() => say(HELP, Priority.NARRATIVE, 0), 3500);
}

export function speakHelp() {
  shutUp();
  say(HELP, Priority.NARRATIVE, 0);
}

export function toggleCrossing() {
  if (crossing) endCrossing("Crossing mode off.");
  else startCrossing();
}

function startCrossing() {
  if (!videoEl) return;
  crossing = true;
  setVisionMode("crossing");
  crossingDeadline = performance.now() + CROSSING_TIMEOUT_MS;
  shutUp();
  say("Crossing mode. Listening for traffic.", Priority.HAZARD, 0);
  loop();
}

function endCrossing(announce) {
  crossing = false;
  setVisionMode("default");
  if (crossingTimer) { clearTimeout(crossingTimer); crossingTimer = null; }
  if (announce) say(announce, Priority.NARRATIVE, 0);
}

async function loop() {
  if (!crossing) return;
  if (performance.now() > crossingDeadline) {
    endCrossing("Crossing mode timed out.");
    return;
  }
  try {
    const text = await describeMode(videoEl, "crossing");
    if (crossing && text) {
      // Crossing verdicts are top-priority. Each one preempts the previous.
      say(text.toLowerCase(), Priority.HAZARD, 600, { interrupt: true });
    }
  } catch (e) {
    console.warn("crossing", e);
  }
  if (crossing) crossingTimer = setTimeout(loop, CROSSING_INTERVAL_MS);
}
