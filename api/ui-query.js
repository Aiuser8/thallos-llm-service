// api/ui-query.js — root "api" folder Serverless Function
// Force Node runtime (pg won't run on Edge)
export const config = { runtime: 'nodejs' };

export default async function handler(req, res) {
  if (req.method !== 'POST') {
    res.status(405).json({ ok: false, error: 'Use POST' });
    return;
  }

  try {
    // Use explicit base to avoid VERCEL_URL edge cases
    const base =
      process.env.INTERNAL_BASE_URL ||
      (process.env.VERCEL_URL ? `https://${process.env.VERCEL_URL}` : 'https://thallos-llm-service.vercel.app');

    const bodyString = typeof req.body === 'string'
      ? req.body
      : JSON.stringify(req.body || {});

    const r = await fetch(`${base}/api/query`, {
      method: 'POST',
      headers: {
        'content-type': 'application/json',
        'x-service-key': process.env.SERVICE_API_KEY || ''
      },
      body: bodyString
    });

    const text = await r.text();
    let data;
    try {
      data = JSON.parse(text);
    } catch {
      // Bubble up real details so we can see what's wrong
      res.status(r.status || 500).json({
        ok: false,
        error: 'Upstream returned non-JSON',
        status: r.status,
        body_snippet: text.slice(0, 300)
      });
      return;
    }

    res.status(r.ok ? 200 : (r.status || 500)).json(data);
  } catch (err) {
    res.status(500).json({ ok: false, error: String(err?.message || err) });
  }
}