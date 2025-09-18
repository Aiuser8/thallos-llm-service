// lib/guard.js

function ensureLimit(sql, maxLimit = 500) {
  // If there is a LIMIT, clamp it. Otherwise append one.
  let hadLimit = false;
  const clamped = sql.replace(/\blimit\s+(\d+)\b/ig, (_m, n) => {
    hadLimit = true;
    const v = Math.min(parseInt(n, 10) || maxLimit, maxLimit);
    return `LIMIT ${v}`;
  });
  if (hadLimit) return clamped;
  return `${sql.trim()}\nLIMIT ${maxLimit}`;
}

/** Very light table reference extraction: finds schema.table after FROM/JOIN */
function extractReferencedFQTables(sql) {
  const refs = new Set();
  const re = /\b(?:from|join)\s+([a-zA-Z0-9_]+)\.([a-zA-Z0-9_]+)\b/ig;
  let m;
  while ((m = re.exec(sql)) !== null) {
    refs.add(`${m[1].toLowerCase()}.${m[2].toLowerCase()}`);
  }
  return refs;
}

export function guardSql(sql, whitelistTables, _whitelistColsByTable, maxLimit = 500) {
  let s = String(sql || '').trim();
  if (!s) throw new Error('Empty SQL.');

  // single statement only
  if (s.includes(';')) {
    // Allow a trailing semicolon only
    if (!/;s*$/.test(s)) throw new Error('Multiple SQL statements are not allowed.');
    s = s.replace(/;+\s*$/g, '').trim();
  }

  const sUpper = s.toUpperCase();

  // Must begin with SELECT or WITH
  if (!sUpper.startsWith('SELECT') && !sUpper.startsWith('WITH')) {
    throw new Error('Only SELECT (or WITH ... SELECT) statements are allowed.');
  }

  // Disallow destructive / admin keywords anywhere
  if (/\b(UPDATE|INSERT|DELETE|DROP|ALTER|TRUNCATE|CREATE|GRANT|REVOKE|COPY|VACUUM|ANALYZE)\b/i.test(s)) {
    throw new Error('Destructive/DDL SQL keywords are not allowed.');
  }

  // No comments
  if (/(--|\/\*)/.test(s)) {
    throw new Error('SQL comments are not allowed.');
  }

  // Disallow obvious unsafe schemas
  if (/\b(?:pg_catalog|pg_toast|information_schema)\./i.test(s)) {
    throw new Error('Access to system schemas is not allowed.');
  }

  // Enforce schema/table whitelist (expects Set of "schema.table")
  const refs = extractReferencedFQTables(s);
  for (const fq of refs) {
    if (!whitelistTables?.has?.(fq)) {
      throw new Error(`Table not allowed: ${fq}`);
    }
  }

  // Enforce/Clamp LIMIT
  s = ensureLimit(s, maxLimit);

  return s;
}