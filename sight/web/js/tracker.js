// Lightweight IoU tracker. Matches detections frame-to-frame so we can compute
// per-object motion: area growth (looming = approaching), horizontal drift, age.
// No dependencies. ~50 lines.

let nextId = 1;
const tracks = new Map(); // id -> {id, class, bbox, area, prevArea, areaHistory, lastSeen, firstSeen, vx}
const MAX_AGE_MS = 800;
const IOU_MATCH = 0.25;

export function update(preds, frameW, frameH, now = performance.now()) {
  // Mark all unseen
  const unmatched = new Set(tracks.keys());
  const out = [];

  for (const p of preds) {
    const [x, y, bw, bh] = p.bbox;
    const det = {
      class: p.class,
      score: p.score,
      bbox: [x, y, bw, bh],
      cx: (x + bw / 2) / frameW,
      cy: (y + bh / 2) / frameH,
      bottom: (y + bh) / frameH,
      area: (bw * bh) / (frameW * frameH),
    };

    // Find best IoU match of same class
    let bestId = null, bestIoU = 0;
    for (const id of unmatched) {
      const t = tracks.get(id);
      if (t.class !== det.class) continue;
      const iou = boxIoU(t.bbox, det.bbox);
      if (iou > bestIoU) { bestIoU = iou; bestId = id; }
    }

    let track;
    if (bestId !== null && bestIoU >= IOU_MATCH) {
      track = tracks.get(bestId);
      track.prevArea = track.area;
      track.areaHistory.push({ area: det.area, t: now });
      while (track.areaHistory.length > 0 && now - track.areaHistory[0].t > 1000) track.areaHistory.shift();
      track.vx = (det.cx - track.cx) * 1000 / Math.max(1, now - track.lastSeen); // dx/sec, normalized
      Object.assign(track, det);
      track.lastSeen = now;
      unmatched.delete(bestId);
    } else {
      track = {
        id: nextId++,
        ...det,
        prevArea: det.area,
        areaHistory: [{ area: det.area, t: now }],
        firstSeen: now,
        lastSeen: now,
        vx: 0,
      };
      tracks.set(track.id, track);
    }

    track.looming = computeLooming(track);
    out.push(track);
  }

  // Drop stale tracks
  for (const id of unmatched) {
    const t = tracks.get(id);
    if (now - t.lastSeen > MAX_AGE_MS) tracks.delete(id);
  }

  return out;
}

// Looming = area growth over last ~1s. >1 means growing.
function computeLooming(t) {
  if (t.areaHistory.length < 2) return 1;
  const oldest = t.areaHistory[0];
  const newest = t.areaHistory[t.areaHistory.length - 1];
  if (newest.t === oldest.t) return 1;
  return Math.max(0.01, newest.area) / Math.max(0.001, oldest.area);
}

function boxIoU(a, b) {
  const [ax, ay, aw, ah] = a;
  const [bx, by, bw, bh] = b;
  const x1 = Math.max(ax, bx), y1 = Math.max(ay, by);
  const x2 = Math.min(ax + aw, bx + bw), y2 = Math.min(ay + ah, by + bh);
  const inter = Math.max(0, x2 - x1) * Math.max(0, y2 - y1);
  const union = aw * ah + bw * bh - inter;
  return union > 0 ? inter / union : 0;
}

export function reset() { tracks.clear(); nextId = 1; }
