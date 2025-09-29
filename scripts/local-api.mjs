import 'dotenv/config';                    // load .env locally
import http from 'http';
import url from 'url';
import fs from 'fs';
import path from 'path';
import handler from '../api/query.js';     // your existing API handler

const PORT = process.env.PORT || 3000;

const server = http.createServer(async (req, res) => {
  const { pathname } = url.parse(req.url, true);

  // Handle API requests
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

  // Serve static files
  if (req.method === 'GET') {
    let filePath;
    if (pathname === '/') {
      filePath = path.join(process.cwd(), 'public', 'index.html');
    } else {
      filePath = path.join(process.cwd(), 'public', pathname);
    }

    try {
      const data = fs.readFileSync(filePath);
      const ext = path.extname(filePath).toLowerCase();
      
      let contentType = 'text/plain';
      if (ext === '.html') contentType = 'text/html';
      else if (ext === '.css') contentType = 'text/css';
      else if (ext === '.js') contentType = 'application/javascript';
      else if (ext === '.json') contentType = 'application/json';
      
      res.statusCode = 200;
      res.setHeader('content-type', contentType);
      res.end(data);
      return;
    } catch (err) {
      // File not found, fall through to 404
    }
  }

  res.statusCode = 404;
  res.setHeader('content-type','application/json');
  res.end(JSON.stringify({ ok:false, error:'not found' }));
});

server.listen(PORT, () =>
  console.log(`Local API on http://localhost:${PORT}`)
);
