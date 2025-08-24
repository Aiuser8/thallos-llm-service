// api/query.js
import { ask } from '../src/llm/core.js';

const SERVICE_API_KEY = process.env.SERVICE_API_KEY || '';

export default async function handler(req, res) {
  try {
    if (req.method !== 'POST') {
      return res.status(405).json({ ok: false, error: 'Use POST' });
    }

    // Optional: lightweight auth for non-browser calls
    const key = req.headers['x-service-key'];
    const isBrowser = (req.headers['sec-fetch-mode'] === 'cors' || req.headers['sec-fetch-site']);
    if (!isBrowser && SERVICE_API_KEY && key !== SERVICE_API_KEY) {
      return res.status(401).json({ ok: false, error: 'Unauthorized' });
    }

    const { question } = req.body || {};
    if (!question || typeof question !== 'string') {
      return res.status(400).json({ ok: false, error: 'Missing "question" string' });
    }

    const result = await ask(question);
    if (result.error) return res.status(400).json({ ok: false, ...result });

    return res.status(200).json({ ok: true, ...result });
  } catch (e) {
    return res.status(500).json({ ok: false, error: e.message || String(e) });
  }
}
