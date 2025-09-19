// lib/schema.js
// Builds the planner/guard schema doc from a JSON registry file.
// Optionally enriches with live columns from information_schema if a pool is provided.

import fs from 'fs/promises';
import path from 'path';
import { fileURLToPath } from 'url';

const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);

// Default path: ../config/llm_table_registry.json (relative to this file)
const DEFAULT_REGISTRY_PATH = path.resolve(__dirname, '../config/llm_table_registry.json');

async function loadRegistry() {
  const registryPath = process.env.LLM_TABLE_REGISTRY_PATH || DEFAULT_REGISTRY_PATH;
  const raw = await fs.readFile(registryPath, 'utf8');
  const json = JSON.parse(raw);
  if (!json || typeof json !== 'object') {
    throw new Error('Invalid table registry JSON: root must be an object');
  }
  return json;
}

export async function fetchSchema(pool = null) {
  // 1) Load the static registry
  const registry = await loadRegistry();
  const wantedFQ = Object.keys(registry);
  if (wantedFQ.length === 0) {
    return { tables: new Set(), colsByTable: new Map(), doc: '' };
  }

  // 2) Seed tables + cols from registry (descriptions come from registry)
  const tables = new Set(wantedFQ);
  const colsByTable = new Map();
  for (const [fqtn, meta] of Object.entries(registry)) {
    const cols = Object.keys(meta.columns || {});
    colsByTable.set(fqtn, new Set(cols.map(c => c.toLowerCase())));
  }

  // 3) (Optional) Enrich/verify with live DB columns if a pool is provided
  if (pool) {
    const client = await pool.connect();
    try {
      const colsRes = await client.query(
        `
        SELECT
          table_schema,
          table_name,
          column_name,
          ordinal_position
        FROM information_schema.columns
        WHERE (table_schema || '.' || table_name) = ANY($1::text[])
        ORDER BY table_schema, table_name, ordinal_position
        `,
        [wantedFQ]
      );

      // Overwrite/augment colsByTable with live columns for better accuracy
      for (const r of colsRes.rows) {
        const fqtn = `${r.table_schema}.${r.table_name}`;
        if (!colsByTable.has(fqtn)) colsByTable.set(fqtn, new Set());
        colsByTable.get(fqtn).add(r.column_name.toLowerCase());
      }
    } finally {
      client.release();
    }
  }

  // 4) Build LLM-readable doc text
  const docParts = [];
  for (const fqtn of wantedFQ) {
    const meta = registry[fqtn] || {};
    const desc = meta.description || '';
    const pk = Array.isArray(meta.primary_key) ? meta.primary_key : [];

    // Prefer live columns if present; otherwise fall back to registry declaration
    const liveCols = Array.from(colsByTable.get(fqtn) || []).sort();
    const declaredCols = meta.columns || {};

    docParts.push(`TABLE ${fqtn}`);
    if (desc) docParts.push(desc);
    if (pk.length) docParts.push(`Primary key: (${pk.join(', ')})`);
    docParts.push('Columns:');

    if (liveCols.length) {
      for (const col of liveCols) {
        const explain = declaredCols[col] || declaredCols[col?.toLowerCase?.()] || '';
        docParts.push(`- ${col}${explain ? `: ${explain}` : ''}`);
      }
    } else {
      // No live columns available (e.g., pool omitted) → use declared columns
      for (const [col, explain] of Object.entries(declaredCols)) {
        docParts.push(`- ${col}: ${explain}`);
      }
    }

    docParts.push(''); // blank line between tables
  }

  const doc = docParts.join('\n');
  return { tables, colsByTable, doc };
}
