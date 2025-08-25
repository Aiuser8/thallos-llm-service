// pages/api/ui-query.js
// Force Node runtime (Edge won't expose process.env reliably for pg/openai)
export const config = { runtime: 'nodejs' };

export default async function handler(req, res) {
  if (req.method !== 'POST') {
    res.status(405).json({ ok: false, error: 'Use POST' });
    return;
  }

  try {
    // Build absolute URL to our own /api/query
    const base =
      process.env.VERCEL_URL
        ? `https://${process.env.VERCEL_URL}`
        : (req.headers.host ? `https://${req.headers.host}` : '');

    // Forward to /api/query with the service key injected
    const r = await fetch(`${base}/api/query`, {
      method: 'POST',
      headers: {
        'content-type': 'application/json',
        'x-service-key': process.env.SERVICE_API_KEY || ''  // << important
      },
      body: JSON.stringify(req.body || {})
    });

    // Try to pass through JSON; if not JSON, surface a helpful snippet
    const text = await r.text();
    try {
      const data = JSON.parse(text);
      res.status(r.status).setHeader('content-type', 'application/json').end(JSON.stringify(data));
    } catch {
      res.status(500).json({
        ok: false,
        error: 'Upstream returned non-JSON',
        status: r.status,
        body_snippet: text.slice(0, 220)
      });
    }
  } catch (err) {
    res.status(500).json({ ok: false, error: String(err?.message || err) });
  }
}