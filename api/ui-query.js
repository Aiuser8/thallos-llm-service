// Force Node runtime (pg won't run on Edge)
export const config = { runtime: 'nodejs' };

export default async function handler(req, res) {
  if (req.method !== 'POST') {
    res.status(405).json({ ok: false, error: 'Use POST' });
    return;
  }
  try {
    const base = process.env.VERCEL_URL ? `https://${process.env.VERCEL_URL}` : '';
    const r = await fetch(`${base}/api/query`, {
      method: 'POST',
      headers: {
        'content-type': 'application/json',
        'x-service-key': process.env.SERVICE_API_KEY || ''
      },
      body: JSON.stringify(req.body || {})
    });
    const data = await r.json().catch(() => ({ ok:false, error:'Bad JSON from /api/query'}));
    res.status(r.ok ? 200 : (r.status || 500)).json(data);
  } catch (err) {
    res.status(500).json({ ok: false, error: String(err?.message || err) });
  }
}
