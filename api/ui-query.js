// pages/api/ui-query.js
export const config = { runtime: 'nodejs' }; // pg won't run on Edge

export default async function handler(req, res) {
  if (req.method !== 'POST') {
    res.status(405).json({ ok: false, error: 'Use POST' });
    return;
  }

  try {
    // Use an absolute base to avoid env weirdness
    const base =
      process.env.NEXT_PUBLIC_BASE_URL ||
      (process.env.VERCEL_URL ? `https://${process.env.VERCEL_URL}` : 'https://thallos-llm-service.vercel.app');

    const r = await fetch(`${base}/api/query`, {
      method: 'POST',
      headers: {
        'content-type': 'application/json',
        'x-service-key': process.env.SERVICE_API_KEY || ''
      },
      body: JSON.stringify(req.body || {})
    });

    const data = await r.json().catch(() => ({}));
    res.status(r.ok ? 200 : (r.status || 500)).json(data);
  } catch (err) {
    res.status(500).json({ ok: false, error: err.message || String(err) });
  }
}