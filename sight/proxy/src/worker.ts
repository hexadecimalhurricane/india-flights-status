interface Env {
  ANTHROPIC_API_KEY: string;
  CLIENT_TOKEN?: string;
  MODEL: string;
  MAX_TOKENS: string;
  ASSETS: { fetch: (req: Request) => Promise<Response> };
}

interface DescribeRequest {
  images: string[];        // base64 JPEG/PNG, no data: prefix
  question?: string;       // optional user question; defaults to general scene description
  sound_hints?: string[];  // recent on-device audio classifier hits, e.g. ["siren", "footsteps"]
  speech?: string;         // recent ambient speech transcript, optional
  heading_deg?: number;    // compass heading if available
}

const SYSTEM_PROMPT = `You are the eyes of a blind person walking through the world. They are wearing AirPods and a phone with a forward-facing camera. Speak directly to them in second person.

Rules:
- Hazards first. If there is anything dangerous in the path (steps, traffic, low overhang, wet floor, dropoff, oncoming person/bike), say it first, in five words or fewer.
- Then spatial layout: what is ahead, left, right. Use clock positions or "ahead / left / right" — never "in the image".
- Distances in meters or steps when you can estimate them. Use the depth cues you can see (object size, ground contact point).
- Read any text that is large, signage-like, or clearly directed at the user (street signs, store names, exit signs, menu items). Skip ambient text like license plates.
- If the user provided sound hints or a speech transcript, integrate them — they describe things the camera can't see.
- No filler. No "I can see", no "in this image", no "it looks like", no "the photo shows". Talk like a confident friend.
- Total response under 60 words unless the user explicitly asked for detail.`;

const corsHeaders = {
  "Access-Control-Allow-Origin": "*",
  "Access-Control-Allow-Methods": "POST, OPTIONS",
  "Access-Control-Allow-Headers": "Content-Type, Authorization",
  "Access-Control-Max-Age": "86400",
};

export default {
  async fetch(req: Request, env: Env): Promise<Response> {
    if (req.method === "OPTIONS") return new Response(null, { headers: corsHeaders });

    const url = new URL(req.url);
    if (url.pathname === "/health") return json({ ok: true });
    if (url.pathname !== "/describe") return env.ASSETS.fetch(req);
    if (req.method !== "POST") return json({ error: "method not allowed" }, 405);

    if (env.CLIENT_TOKEN) {
      const auth = req.headers.get("authorization") ?? "";
      if (auth !== `Bearer ${env.CLIENT_TOKEN}`) return json({ error: "unauthorized" }, 401);
    }

    let body: DescribeRequest;
    try {
      body = await req.json();
    } catch {
      return json({ error: "bad json" }, 400);
    }

    if (!Array.isArray(body.images) || body.images.length === 0) {
      return json({ error: "images[] required" }, 400);
    }
    if (body.images.length > 4) {
      return json({ error: "max 4 images" }, 400);
    }

    const userContent: Array<Record<string, unknown>> = body.images.map((b64) => ({
      type: "image",
      source: { type: "base64", media_type: detectMediaType(b64), data: b64 },
    }));

    const contextLines: string[] = [];
    if (body.sound_hints?.length) contextLines.push(`Recent sounds detected: ${body.sound_hints.join(", ")}.`);
    if (body.speech) contextLines.push(`Ambient speech heard: "${body.speech.slice(0, 200)}".`);
    if (typeof body.heading_deg === "number") contextLines.push(`Compass heading: ${Math.round(body.heading_deg)}°.`);
    const question = body.question?.trim() || "Describe what's ahead of me, hazards first.";
    userContent.push({ type: "text", text: contextLines.length ? `${contextLines.join(" ")}\n\n${question}` : question });

    const upstream = await fetch("https://api.anthropic.com/v1/messages", {
      method: "POST",
      headers: {
        "content-type": "application/json",
        "x-api-key": env.ANTHROPIC_API_KEY,
        "anthropic-version": "2023-06-01",
      },
      body: JSON.stringify({
        model: env.MODEL,
        max_tokens: Number(env.MAX_TOKENS) || 400,
        system: SYSTEM_PROMPT,
        messages: [{ role: "user", content: userContent }],
      }),
    });

    if (!upstream.ok) {
      const errText = await upstream.text();
      return json({ error: "upstream", status: upstream.status, detail: errText.slice(0, 500) }, 502);
    }

    const data = (await upstream.json()) as { content?: Array<{ type: string; text?: string }> };
    const text = (data.content ?? []).filter((b) => b.type === "text").map((b) => b.text ?? "").join("").trim();
    return json({ text });
  },
};

function detectMediaType(b64: string): string {
  if (b64.startsWith("/9j/")) return "image/jpeg";
  if (b64.startsWith("iVBOR")) return "image/png";
  if (b64.startsWith("UklGR")) return "image/webp";
  return "image/jpeg";
}

function json(obj: unknown, status = 200): Response {
  return new Response(JSON.stringify(obj), {
    status,
    headers: { "content-type": "application/json", ...corsHeaders },
  });
}
