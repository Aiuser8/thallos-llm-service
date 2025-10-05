// lib/db.js - Shared database connection pool
import pg from "pg";

// Create and cache the database connection pool
let dbPool = null;

export function getDbPool() {
  if (!dbPool) {
    const url = new URL(process.env.DATABASE_URL);
    dbPool = new pg.Pool({
      host: url.hostname, // e.g., aws-1-us-east-2.pooler.supabase.com
      port: Number(url.port || 5432),
      user: decodeURIComponent(url.username),
      password: decodeURIComponent(url.password),
      database: url.pathname.replace(/^\//, ""), // "postgres"
      // Force TLS but relax CA validation to avoid SELF_SIGNED_CERT_IN_CHAIN in Vercel
      ssl: { rejectUnauthorized: false },
      // Improved connection pool configuration for high-volume usage and stability
      max: 10, // Reduced from 20 to avoid Supabase connection limits
      min: 2, // Keep minimum connections open for faster responses
      idleTimeoutMillis: 20000, // Reduce idle timeout to 20 seconds
      connectionTimeoutMillis: 5000, // Increase connection timeout to 5 seconds
      query_timeout: 30000, // 30 second query timeout
      keepAlive: true, // Keep connections alive to prevent EADDRNOTAVAIL errors
      application_name: 'thallos-llm-service' // Help identify connections in Supabase logs
    });
  }
  return dbPool;
}
