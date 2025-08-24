// Force Node runtime (pg won't work on Edge)
export const config = { runtime: 'nodejs' };

export default async function handler(req, res) {
  if (req.method !== 'POST') {
    res.status(405).json({ ok: false, error: 'Use POST' });
    return;
  }

  try {
    // Build the same-origin base URL for both prod & local dev
    const host = req.headers.host || process.env.VERCEL_URL;
    const base =
      host?.startsWith('http') ? host :
      process.env.VERCEL_URL ? `https://${process.env.VERCEL_URL}` :
      `http://${host}`;

    const r = await fetch(`${base}/api/query`, {
      method: 'POST',
      headers: {
        'content-type': 'application/json',
        'x-service-key': process.env.SERVICE_API_KEY || '',
      },
      body: JSON.stringify(req.body || {}),
    });

    const data = await r.json().catch(() => ({ ok: false, error: 'Non-JSON response from /api/query' }));
    res.status(r.ok ? 200 : (r.status || 500)).json(data);
  } catch (err) {
    res.status(500).json({ ok: false, error: err?.message || String(err) });
  }
}