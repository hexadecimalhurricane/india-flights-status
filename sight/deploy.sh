#!/usr/bin/env bash
# Ship Sight (web + proxy) as one Cloudflare Worker. Idempotent — safe to re-run.
# First-time prerequisites: node 18+, an Anthropic API key, a free Cloudflare account.
set -euo pipefail

cd "$(dirname "$0")/proxy"

if [ ! -d node_modules ]; then
  echo "==> Installing dependencies"
  npm install
fi

echo "==> Logging in to Cloudflare (skipped if already logged in)"
npx wrangler whoami >/dev/null 2>&1 || npx wrangler login

if ! npx wrangler secret list 2>/dev/null | grep -q ANTHROPIC_API_KEY; then
  echo "==> Setting ANTHROPIC_API_KEY (paste your key when prompted)"
  npx wrangler secret put ANTHROPIC_API_KEY
fi

echo "==> Deploying"
npx wrangler deploy

echo
echo "Done. Open the URL above on your phone in Safari/Chrome."
echo "On iPhone you'll need to 'Add to Home Screen' for the PWA install."
