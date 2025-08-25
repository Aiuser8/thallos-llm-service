export const config = { runtime: 'nodejs' };

export default async function handler(req, res) {
  if (req.method !== 'POST') {
    res.status(405).json({ ok: false, error: 'Use POST' });
    return;
  }
  try {
    const base =
      process.env.VERCEL_URL
        ? `https://${process.env.VERCEL_URL}`
        : (req.headers.host ? `https://${req.headers.host}` : '');

    const r = await fetch(`${base}/api/query`, {
      method: 'POST',
      headers: {
        'content-type': 'application/json',
        'x-service-key': process.env.SERVICE_API_KEY || ''   // <- injects key
      },
      body: JSON.stringify(req.body || {})
    });

    // return upstream JSON or a helpful error if non‑JSON
    const text = await r.text();
    let data;
    try { data = JSON.parse(text); }
    catch {
      res.status(r.status || 500).json({
        ok: false,
        error: 'Upstream returned non-JSON',
        status: r.status,
        body_snippet: text.slice(0, 200)
      });
      return;
    }
    res.status(r.ok ? 200 : (r.status || 500)).json(data);
  } catch (err) {
    res.status(500).json({ ok: false, error: String(err?.message || err) });
  }
}