/**
 * Cloudflare Worker — apex to www redirect with WebSocket pass-through.
 *
 * The domain datafeed.fun redirects to www.datafeed.fun for regular HTTP
 * traffic.  However, a blanket 301 redirect breaks WebSocket upgrade requests
 * (the client gets "HTTP 301" instead of "101 Switching Protocols").
 *
 * This Worker checks for the Upgrade: websocket header and bypasses the
 * redirect so that WebSocket connections succeed.
 *
 * ── Deploy ──
 * 1. Cloudflare Dashboard → Workers & Pages → Create Worker.
 * 2. Paste this script.
 * 3. Add a route: datafeed.fun/*
 * 4. Remove any existing Page Rule that does the same redirect — this Worker
 *    replaces it.
 * 5. (Optional) Add a second route: www.datafeed.fun/* → no-op (just fetch).
 */

export default {
  async fetch(request) {
    const url = new URL(request.url);

    // Only apply the redirect on the apex domain.
    if (url.hostname !== "datafeed.fun") {
      // www.datafeed.fun – forward unchanged.
      return fetch(request);
    }

    // Detect WebSocket upgrade requests.
    const upgrade = request.headers.get("Upgrade") || "";
    if (upgrade.toLowerCase() === "websocket") {
      // Pass WebSocket upgrades straight through to the origin.
      return fetch(request);
    }

    // Regular HTTP request – redirect apex → www (preserve path & query).
    return Response.redirect(
      `https://www.datafeed.fun${url.pathname}${url.search}`,
      301,
    );
  },
};
