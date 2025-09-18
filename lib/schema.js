// lib/schema.js
import { Pool } from 'pg';
import pkg from '../package.json' assert { type: 'json' };

/**
 * Fetch a live schema doc for the tables listed under package.json → "tables".
 * Returns:
 *  - tables: Set<string> of fully-qualified table names ("schema.table")
 *  - colsByTable: Map<string, Set<string>> of lowercase column names per fqtn
 *  - doc: human-readable whitelist doc for prompts
 */
export async function fetchSchema(pool /** @type {Pool} */) {
  const client = await pool.connect();
  try {
    // Pull FQ table names (e.g., "clean.protocol_chain_tvl_daily") from package.json
    const wantedFQ = Object.keys(pkg.tables || {});
    if (wantedFQ.length === 0) {
      return { tables: new Set(), colsByTable: new Map(), doc: '' };
    }

    // Query information_schema for exactly those schema-qualified names
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

    // Build structures keyed by fully-qualified table name
    const tables = new Set();
    const colsByTable = new Map(); // fqtn -> Set<column_name>

    for (const r of colsRes.rows) {
      const fqtn = `${r.table_schema}.${r.table_name}`;
      tables.add(fqtn);
      if (!colsByTable.has(fqtn)) colsByTable.set(fqtn, new Set());
      colsByTable.get(fqtn).add(r.column_name.toLowerCase());
    }

    // Human-readable whitelist doc built from package.json + live columns
    const docParts = [];
    for (const fqtn of wantedFQ) {
      const meta = pkg.tables[fqtn] || {};
      const desc = meta.description || '';
      const pk = Array.isArray(meta.primary_key) ? meta.primary_key : [];
      const liveCols = Array.from(colsByTable.get(fqtn) || []).sort();

      docParts.push(`${fqtn} — ${desc}`.trim());
      if (liveCols.length) {
        docParts.push('  columns:');
        for (const col of liveCols) {
          const explain = meta.columns?.[col] || ''; // show your curated description when available
          docParts.push(`    - ${col}${explain ? `: ${explain}` : ''}`);
        }
      } else if (meta.columns) {
        // Fallback to meta if table exists but we couldn't read columns for some reason
        docParts.push('  columns:');
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