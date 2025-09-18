// Minimal connectivity check — always returns JSON
export const config = { runtime: 'nodejs18.x' };

function json(res, status, body) {
  res.statusCode = status;
  res.setHeader('content-type', 'application/json; charset=utf-8');
  res.setHeader('cache-control', 'no-store');
  res.end(JSON.stringify(body));
}

export default async function handler(req, res) {
  if (req.method !== 'POST') return json(res, 405, { error: 'Method not allowed', allowed: ['POST'] });
  return json(res, 200, { ok: true, echo: 'connected' });
}
