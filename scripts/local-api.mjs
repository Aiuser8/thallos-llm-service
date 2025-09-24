import 'dotenv/config';                    // load .env locally
import http from 'http';
import url from 'url';
import handler from '../api/query.js';     // your existing API handler

const PORT = process.env.PORT || 3000;

const server = http.createServer(async (req, res) => {
  const { pathname } = url.parse(req.url, true);

  if (req.method === 'POST' && pathname === '/api/query') {
    let raw = '';
    req.on('data', (c) => (raw += c));
    req.on('end', async () => {
      try { req.body = raw ? JSON.parse(raw) : {}; } catch { req.body = {}; }
      
      // Create Vercel-compatible request/response objects
      const vercelReq = {
        ...req,
        url: req.url,
        method: req.method,
        headers: req.headers,
        body: req.body
      };
      
      const vercelRes = {
        setHeader: (name, value) => res.setHeader(name, value),
        status: (code) => { res.statusCode = code; return vercelRes; },
        json: (data) => {
          res.setHeader('content-type', 'application/json');
          res.end(JSON.stringify(data));
        },
        end: (data) => res.end(data)
      };
      
      try { 
        await handler(vercelReq, vercelRes); 
      }
      catch (e) {
        res.statusCode = 500;
        res.setHeader('content-type','application/json');
        res.end(JSON.stringify({ ok:false, error:e.message }));
      }
    });
    return;
  }

  res.statusCode = 404;
  res.setHeader('content-type','application/json');
  res.end(JSON.stringify({ ok:false, error:'not found' }));
});

server.listen(PORT, () =>
  console.log(`Local API on http://localhost:${PORT}`)
);
