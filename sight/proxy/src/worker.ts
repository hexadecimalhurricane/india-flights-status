interface Env {
  ANTHROPIC_API_KEY: string;
  CLIENT_TOKEN?: string;
  MODEL: string;
  MAX_TOKENS: string;
  ASSETS: { fetch: (req: Request) => Promise<Response> };
}

type Mode = "describe" | "read" | "crossing";

interface DescribeRequest {
  images: string[];
  question?: string;
  sound_hints?: string[];
  speech?: string;
  heading_deg?: number;
  mode?: Mode;
}

const PROMPT_DESCRIBE = `You are the eyes of a blind person walking. They are wearing AirPods and a phone with a forward-facing camera. Speak directly in second person.

Rules:
- Hazards first. Anything dangerous (steps, traffic, low overhang, dropoff, oncoming person/bike): say it first in five words or fewer.
- Then spatial layout. Use "ahead / left / right" or clock positions. Never say "in the image".
- Distances in meters or steps when possible. Use object size and ground contact for depth cues.
- Read text only if it is large or signage-like. Skip license plates and ambient text.
- If sound hints or ambient speech are provided, integrate them — they describe what the camera can't see.
- No filler. No "I can see", no "the photo shows". Talk like a confident friend.
- Total response under 40 words.`;

const PROMPT_READ = `You are reading text aloud for a blind person looking at a sign, menu, or document. Read what is large and clearly intended to be read, top to bottom, left to right. Skip ambient text (license plates, distant signs not relevant). If multiple things are visible, pick the most prominent. Plain text only, no commentary, under 50 words.`;

const PROMPT_CROSSING = `You are watching a road for a blind pedestrian who is about to cross or is crossing. The user has activated road-crossing mode and needs ultra-fast, ultra-short verdicts. Use sound hints — they may hear vehicles you can't see.

OUTPUT FORMAT — pick exactly one phrase, no extra words, no punctuation:
- "wait" — there is traffic moving across their path or signal is red
- "go" — clear and safe to cross now, signal is walk/green
- "car left" / "car right" / "car ahead" — vehicle approaching from that direction
- "bike left" / "bike right" — cyclist approaching
- "person crossing" — someone else is in the crosswalk
- "almost across" — they are at or past midpoint with clear path
- "back up" — they are stepping off a curb into traffic
- "no crosswalk visible" — they appear off course
- "clear" — no traffic, no signal, safe road

Maximum five words. No "I see", no "looks like", no explanation.`;

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

    if (!Array.isArray(body.images) || body.images.length === 0) return json({ error: "images[] required" }, 400);
    if (body.images.length > 4) return json({ error: "max 4 images" }, 400);

    const mode: Mode = body.mode === "crossing" || body.mode === "read" ? body.mode : "describe";
    const { model, system, maxTokens } = profileFor(mode, env);

    const userContent: Array<Record<string, unknown>> = body.images.map((b64) => ({
      type: "image",
      source: { type: "base64", media_type: detectMediaType(b64), data: b64 },
    }));

    const contextLines: string[] = [];
    if (body.sound_hints?.length) contextLines.push(`Recent sounds: ${body.sound_hints.join(", ")}.`);
    if (body.speech) contextLines.push(`Ambient speech: "${body.speech.slice(0, 160)}".`);
    if (typeof body.heading_deg === "number") contextLines.push(`Heading: ${Math.round(body.heading_deg)}°.`);

    const defaultQ = mode === "crossing"
      ? "Verdict for this exact instant of crossing the road."
      : mode === "read"
        ? "Read the visible text for me."
        : "Describe what's ahead of me, hazards first.";
    const question = body.question?.trim() || defaultQ;
    userContent.push({ type: "text", text: contextLines.length ? `${contextLines.join(" ")}\n\n${question}` : question });

    const upstream = await fetch("https://api.anthropic.com/v1/messages", {
      method: "POST",
      headers: {
        "content-type": "application/json",
        "x-api-key": env.ANTHROPIC_API_KEY,
        "anthropic-version": "2023-06-01",
      },
      body: JSON.stringify({
        model,
        max_tokens: maxTokens,
        system,
        messages: [{ role: "user", content: userContent }],
      }),
    });

    if (!upstream.ok) {
      const errText = await upstream.text();
      return json({ error: "upstream", status: upstream.status, detail: errText.slice(0, 500) }, 502);
    }

    const data = (await upstream.json()) as { content?: Array<{ type: string; text?: string }> };
    const text = (data.content ?? []).filter((b) => b.type === "text").map((b) => b.text ?? "").join("").trim();
    return json({ text, mode });
  },
};

function profileFor(mode: Mode, env: Env): { model: string; system: string; maxTokens: number } {
  if (mode === "crossing") {
    // Sub-second latency matters more than verbosity. Haiku is the right model.
    return { model: "claude-haiku-4-5", system: PROMPT_CROSSING, maxTokens: 30 };
  }
  if (mode === "read") {
    return { model: env.MODEL, system: PROMPT_READ, maxTokens: 200 };
  }
  return { model: env.MODEL, system: PROMPT_DESCRIBE, maxTokens: Number(env.MAX_TOKENS) || 200 };
}

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
