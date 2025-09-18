// lib/guard.js

function ensureLimit(sql, maxLimit = 500) {
  let hadLimit = false;
  const clamped = sql.replace(/\blimit\s+(\d+)\b/ig, (_m, n) => {
    hadLimit = true;
    const v = Math.min(parseInt(n, 10) || maxLimit, maxLimit);
    return `LIMIT ${v}`;
  });
  if (hadLimit) return clamped;
  return `${sql.trim()}\nLIMIT ${maxLimit}`;
}

export function guardSql(sql, _ignored1, _ignored2, maxLimit = 500) {
  let s = String(sql || '').trim();
  if (!s) throw new Error('Empty SQL.');

  // Only allow one statement
  if (s.includes(';')) {
    if (!/;\s*$/.test(s)) throw new Error('Multiple SQL statements are not allowed.');
    s = s.replace(/;+\s*$/g, '').trim();
  }

  const sUpper = s.toUpperCase();
  if (!sUpper.startsWith('SELECT') && !sUpper.startsWith('WITH')) {
    throw new Error('Only SELECT (or WITH ... SELECT) statements are allowed.');
  }

  // Block destructive/admin keywords
  if (/\b(UPDATE|INSERT|DELETE|DROP|ALTER|TRUNCATE|CREATE|GRANT|REVOKE|COPY|VACUUM|ANALYZE)\b/i.test(s)) {
    throw new Error('Destructive/DDL SQL keywords are not allowed.');
  }

  // No comments
  if (/(--|\/\*)/.test(s)) {
    throw new Error('SQL comments are not allowed.');
  }

  // Enforce/clamp LIMIT
  s = ensureLimit(s, maxLimit);

  return s;
}