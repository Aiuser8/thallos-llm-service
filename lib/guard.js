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

/**
 * Guard a query but DO NOT restrict which tables can be used.
 * - Single statement only (SELECT / WITH … SELECT)
 * - No comments
 * - No DDL/DML / admin keywords
 * - Clamp LIMIT
 *
 * @param {string} sql
 * @param {Set<string>} _ignoredAllowedTables  (ignored)
 * @param {Map<string, Set<string>>} _ignoredColsByTable (ignored)
 * @param {number} maxLimit
 * @returns {string} safe SQL
 */
export function guardSql(sql, _ignoredAllowedTables = new Set(), _ignoredColsByTable = new Map(), maxLimit = 500) {
  let s = String(sql || '').trim();
  if (!s) throw new Error('Empty SQL.');

  // Only one statement (semicolon only allowed at very end)
  if (s.includes(';')) {
    if (!/;\s*$/.test(s)) throw new Error('Multiple SQL statements are not allowed.');
    s = s.replace(/;+\s*$/g, '').trim();
  }

  // Only SELECT / WITH
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