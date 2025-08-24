import { Pool } from 'pg';
import OpenAI from 'openai';

const pool = new Pool({
  connectionString: process.env.DATABASE_URL, // should be the pooler URL (port 6543)
  ssl: { rejectUnauthorized: false },
});

export default async function handler(req, res) {
  try {
    // 1) require service key (simple auth)
    const headerKey = req.headers['x-service-key'];
    if (!headerKey || headerKey !== process.env.SERVICE_API_KEY) {
      return res.status(401).json({ ok: false, error: 'unauthorized' });
    }

    // 2) DB check
    const client = await pool.connect();
    const dbNow = await client.query('SELECT NOW() as now');
    const sample = await client.query(
      `SELECT COUNT(*)::int AS n FROM public.aave_v1 WHERE protocol='aave' LIMIT 1`
    );
    client.release();

    // 3) OpenAI check (super light)
    const openai = new OpenAI({ apiKey: process.env.OPENAI_API_KEY });
    // a cheap, quick call
    await openai.chat.completions.create({
      model: 'gpt-4o-mini', // or your chosen model
      messages: [{ role: 'user', content: 'ping' }],
      max_tokens: 5,
    });

    return res.status(200).json({
      ok: true,
      db_time: dbNow.rows[0].now,
      rows_in_aave_v1: sample.rows[0].n,
    });
  } catch (err) {
    return res.status(500).json({ ok: false, error: String(err?.message || err) });
  }
}
