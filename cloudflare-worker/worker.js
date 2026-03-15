/**
 * INFS — India Flight Status
 * Cloudflare Worker: proxies all traffic to the Python FastAPI backend on Render.
 *
 * Deploy:
 *   npm install -g wrangler
 *   wrangler login
 *   wrangler deploy
 *
 * Set the BACKEND_URL secret after deploying:
 *   wrangler secret put BACKEND_URL
 *   (paste your Render URL, e.g. https://mefs.onrender.com)
 */

export default {
  async fetch(request, env) {
    const backendUrl = env.BACKEND_URL;

    if (!backendUrl) {
      return new Response(
        JSON.stringify({ error: "BACKEND_URL not configured. Set it via: wrangler secret put BACKEND_URL" }),
        { status: 503, headers: { "Content-Type": "application/json" } }
      );
    }

    const url = new URL(request.url);
    const targetUrl = new URL(url.pathname + url.search, backendUrl);

    // Forward the request to the FastAPI backend
    const proxyRequest = new Request(targetUrl.toString(), {
      method: request.method,
      headers: (() => {
        const h = new Headers(request.headers);
        // Identify the true origin host to the backend
        h.set("X-Forwarded-Host", url.hostname);
        h.set("X-Forwarded-Proto", url.protocol.replace(":", ""));
        return h;
      })(),
      body: ["GET", "HEAD"].includes(request.method) ? undefined : request.body,
      redirect: "follow",
    });

    try {
      const response = await fetch(proxyRequest);

      // Pass response through, adding CORS headers for API calls
      const responseHeaders = new Headers(response.headers);
      responseHeaders.set("X-Powered-By", "INFS / Cloudflare Workers");

      return new Response(response.body, {
        status: response.status,
        statusText: response.statusText,
        headers: responseHeaders,
      });
    } catch (err) {
      // Backend is waking up (Render free tier spins down after 15 min inactivity)
      if (url.pathname === "/" || url.pathname === "") {
        return new Response(WAKE_HTML, {
          status: 503,
          headers: { "Content-Type": "text/html; charset=utf-8" },
        });
      }
      return new Response(
        JSON.stringify({ error: "Backend unavailable, retrying shortly.", detail: err.message }),
        { status: 503, headers: { "Content-Type": "application/json" } }
      );
    }
  },
};

// Shown while Render's free tier is waking up (~30 seconds on cold start)
const WAKE_HTML = `<!DOCTYPE html>
<html lang="en">
<head>
  <meta charset="UTF-8">
  <meta http-equiv="refresh" content="15">
  <meta name="viewport" content="width=device-width, initial-scale=1.0">
  <title>INFS – Starting up…</title>
  <style>
    body { background: #0a0f1a; color: #e2e8f0; font-family: 'Courier New', monospace;
           display: flex; align-items: center; justify-content: center; height: 100vh; margin: 0; }
    .box { text-align: center; padding: 2rem; }
    h1 { font-size: 2rem; color: #facc15; margin-bottom: 0.5rem; letter-spacing: 0.15em; }
    p  { color: #94a3b8; margin: 0.5rem 0; }
    .spinner { display: inline-block; width: 2rem; height: 2rem; border: 3px solid #334155;
               border-top-color: #facc15; border-radius: 50%; animation: spin 1s linear infinite;
               margin: 1.5rem auto; }
    @keyframes spin { to { transform: rotate(360deg); } }
  </style>
</head>
<body>
  <div class="box">
    <h1>✈ INFS</h1>
    <p>India Flight Status</p>
    <div class="spinner"></div>
    <p>Server is waking up — refreshing automatically…</p>
    <p style="font-size:0.75rem; color:#475569; margin-top:1rem;">
      Free-tier cold start takes ~30 seconds. Page will reload automatically.
    </p>
  </div>
</body>
</html>`;
