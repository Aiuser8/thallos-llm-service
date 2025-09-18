// lib/schema.js
import { createRequire } from 'module';
const require = createRequire(import.meta.url);
const pkg = require('../package.json');

export async function fetchSchema(pool) {
  const client = await pool.connect();
  try {
    const wantedFQ = Object.keys(pkg.tables || {});
    if (wantedFQ.length === 0) {
      return { tables: new Set(), colsByTable: new Map(), doc: '' };
    }

    const colsRes = await client.query(
      `
      SELECT
        table_schema,
        table_name,
        column_name,
        data_type,
        ordinal_position
      FROM information_schema.columns
      WHERE (table_schema || '.' || table_name) = ANY($1::text[])
      ORDER BY table_schema, table_name, ordinal_position
      `,
      [wantedFQ]
    );

    const tables = new Set();
    const colsByTable = new Map();

    for (const r of colsRes.rows) {
      const fqtn = `${r.table_schema}.${r.table_name}`;
      tables.add(fqtn);
      if (!colsByTable.has(fqtn)) colsByTable.set(fqtn, new Set());
      colsByTable.get(fqtn).add(r.column_name.toLowerCase());
    }

    const docParts = [];
    for (const fqtn of wantedFQ) {
      const meta = pkg.tables[fqtn] || {};
      const desc = meta.description || '';
      const pk = Array.isArray(meta.primary_key) ? meta.primary_key : [];
      const liveCols = Array.from(colsByTable.get(fqtn) || []).sort();

      docParts.push(`${fqtn} — ${desc}`.trim());
      docParts.push('  columns:');
      if (liveCols.length) {
        for (const col of liveCols) {
          const explain = meta.columns?.[col] || '';
          docParts.push(`    - ${col}${explain ? `: ${explain}` : ''}`);
        }
      } else if (meta.columns) {
        for (const [col, explain] of Object.entries(meta.columns)) {
          docParts.push(`    - ${col}: ${explain}`);
        }
      }
      if (pk.length) docParts.push(`  primary_key: [${pk.join(', ')}]`);
      docParts.push('');
    }

    const doc = docParts.join('\n');
    return { tables, colsByTable, doc };
  } finally {
    client.release();
  }
}