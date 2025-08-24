// pages/api/ui-query.js
export const config = { runtime: 'nodejs' };

export default async function handler(req, res) {
  if (req.method !== 'POST') {
    res.status(405).json({ ok: false, error: 'Method not allowed' });
    return;
  }
  try {
    const r = await fetch(`${process.env.VERCEL_URL ? 'https://' + process.env.VERCEL_URL : ''}/api/query`, {
      method: 'POST',
      headers: {
        'content-type': 'application/json',
        'x-service-key': process.env.SERVICE_API_KEY || ''
      },
      body: JSON.stringify(req.body || {})
    });

    // pass JSON through; if backend returned HTML/text, surface it as an error
    const ct = r.headers.get('content-type') || '';
    if (!ct.includes('application/json')) {
      const text = await r.text();
      res.status(r.status || 500).json({ ok: false, error: text.slice(0, 500) });
      return;
    }
    const data = await r.json();
    res.status(r.status || 200).json(data);
  } catch (err) {
    res.status(500).json({ ok: false, error: err.message || String(err) });
  }
}