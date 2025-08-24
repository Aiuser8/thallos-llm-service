// pages/index.jsx
import { useState } from 'react';

export default function Home() {
  const [question, setQuestion] = useState('Over the last 7 days, what were the min, max, and average USDC utilization?');
  const [loading, setLoading] = useState(false);
  const [answer, setAnswer] = useState('');
  const [sql, setSql] = useState('');
  const [rows, setRows] = useState([]);

  async function submit(e) {
    e.preventDefault();
    setLoading(true);
    setAnswer('');
    setSql('');
    setRows([]);
    try {
      const resp = await fetch('/api/query', {
        method: 'POST',
        headers: { 'content-type': 'application/json' },
        body: JSON.stringify({ question }),
      });
      const data = await resp.json();
      if (!data.ok) throw new Error(data.error || 'Query failed');

      setAnswer(data.answer || '');
      setSql(data.sql || '');
      setRows(Array.isArray(data.rows) ? data.rows : []);
    } catch (err) {
      setAnswer(`Error: ${err.message}`);
    } finally {
      setLoading(false);
    }
  }

  return (
    <div style={{ maxWidth: 900, margin: '40px auto', fontFamily: 'system-ui, -apple-system, Segoe UI, Roboto, Arial' }}>
      <h1 style={{ marginBottom: 8 }}>Thallos LLM Query</h1>
      <p style={{ color: '#555', marginTop: 0 }}>
        Same logic as your CLI: model → SQL → guarded execution on Supabase → summary.
      </p>
      <form onSubmit={submit} style={{ display: 'flex', gap: 12, marginBottom: 16 }}>
        <textarea
          value={question}
          onChange={(e) => setQuestion(e.target.value)}
          rows={3}
          style={{ flex: 1, padding: 12, borderRadius: 8, border: '1px solid #ccc' }}
          placeholder="Ask a complex question…"
        />
        <button
          type="submit"
          disabled={loading}
          style={{
            minWidth: 160,
            border: 0, borderRadius: 8,
            background: '#111', color: '#fff',
            padding: '12px 16px', cursor: 'pointer', height: 44
          }}
        >
          {loading ? 'Running…' : 'Ask'}
        </button>
      </form>

      {answer && (
        <div style={{ padding: 12, background: '#f6f6f6', borderRadius: 8, marginBottom: 16 }}>
          <strong>Answer</strong>
          <div style={{ marginTop: 8 }}>{answer}</div>
        </div>
      )}

      {sql && (
        <div style={{ padding: 12, background: '#fafafa', borderRadius: 8, marginBottom: 16 }}>
          <strong>SQL</strong>
          <pre style={{ whiteSpace: 'pre-wrap', marginTop: 8 }}>{sql}</pre>
        </div>
      )}

      {rows?.length > 0 && (
        <div style={{ padding: 12, background: '#fafafa', borderRadius: 8 }}>
          <strong>Rows (first 10)</strong>
          <pre style={{ whiteSpace: 'pre-wrap', marginTop: 8 }}>
            {JSON.stringify(rows.slice(0, 10), null, 2)}
          </pre>
        </div>
      )}
    </div>
  );
}
