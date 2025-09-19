// lib/guard.js
// Read-only SQL guard with registry enforcement.
// - Allows only a single SELECT / WITH ... SELECT
// - Blocks DDL/DML/admin keywords and comments
// - Enforces/clamps LIMIT
// - Validates that all referenced tables exist in the registry (tables: Set<"schema.table">)
//
// Signature matches api/query.js usage:
//   guardSql(sql, tables, colsByTable, maxLimit = 500)

function ensureLimit(sql, maxLimit = 500) {
  let hadLimit = false;
  const clamped = String(sql).replace(/\blimit\s+(\d+)\b/ig, (_m, n) => {
    hadLimit = true;
    const v = Math.min(parseInt(n, 10) || maxLimit, maxLimit);
    return `LIMIT ${v}`;
  });
  if (hadLimit) return clamped;
  return `${String(sql).trim()}\nLIMIT ${maxLimit}`;
}

// Very lightweight table reference extractor.
// It looks for schema-qualified names like "clean.table" anywhere in the query.
// NOTE: This won’t catch unqualified table names; require the planner to schema-qualify.
function extractSchemaTables(sql) {
  const out = new Set();
  const re = /\b([a-z_][\w$]*)\.([a-z_][\w$]*)\b/gi; // schema.table
  let m;
  while ((m = re.exec(sql)) !== null) {
    out.add(`${m[1]}.${m[2]}`);
  }
  return Array.from(out);
}

export function guardSql(sql, tables, colsByTable, maxLimit = 500) {
  let s = String(sql || '').trim();
  if (!s) throw new Error('Empty SQL.');

  // Only allow one statement
  if (s.includes(';')) {
    // Allow a single trailing semicolon but no multiple statements
    if (!/;\s*$/.test(s)) throw new Error('Multiple SQL statements are not allowed.');
    s = s.replace(/;+\s*$/g, '').trim();
  }

  const sUpper = s.toUpperCase();

  // Only SELECT / WITH ... SELECT
  if (!sUpper.startsWith('SELECT') && !sUpper.startsWith('WITH')) {
    throw new Error('Only SELECT (or WITH ... SELECT) statements are allowed.');
  }

  // Block destructive/admin keywords and risky commands
  if (/\b(UPDATE|INSERT|DELETE|DROP|ALTER|TRUNCATE|CREATE|GRANT|REVOKE|COPY|VACUUM|ANALYZE|REFRESH|CLUSTER|REINDEX|CALL|DO)\b/i.test(s)) {
    throw new Error('Destructive/DDL/admin SQL keywords are not allowed.');
  }

  // No comments
  if (/(--|\/\*)/.test(s)) {
    throw new Error('SQL comments are not allowed.');
  }

  // Registry-based table allow-list (if provided)
  // Expect `tables` to be a Set of "schema.table".
  if (tables && typeof tables.has === 'function') {
    const used = extractSchemaTables(s);
    for (const t of used) {
      if (!tables.has(t)) {
        throw new Error(`Table not allowed: ${t}`);
      }
    }
  }

  // (Optional) Column checks could be added here using colsByTable,
  // but robust validation requires a full SQL parser to resolve aliases.
  // We rely on the LLM planner + registry doc to keep column usage sane.

  // Enforce/clamp LIMIT
  s = ensureLimit(s, maxLimit);

  return s;
}