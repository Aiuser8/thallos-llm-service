// pages/api/query.js
export const config = { runtime: 'nodejs' };

export default async function handler(req, res) {
  if (req.method !== 'POST') {
    res.status(405).json({ ok: false, error: 'Use POST' });
    return;
  }
  res.status(200).json({
    ok: true,
    echo: req.body ?? null,
    env: {
      has_DB: !!process.env.DATABASE_URL,
      has_OPENAI: !!process.env.OPENAI_API_KEY,
      has_SERVICE: !!process.env.SERVICE_API_KEY,
    },
    where: 'pages/api/query.js'
  });
}