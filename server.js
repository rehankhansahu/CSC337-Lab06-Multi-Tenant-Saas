/**
 * Dual-Mode Production Server (Express + Native HTTP Fallback)
 * CSC337: Advanced Web Technologies - Lab Assignment 06
 * Topic: Multi-Tenant SaaS, Kafka Event-Driven Architecture, DB Performance, Serverless
 */

const http = require('http');
const fs = require('fs');
const path = require('path');
const url = require('url');
const config = require('./src/config');
const { getConsumerMetrics } = require('./src/worker/kafkaConsumer');
const { dispatchApi } = require('./src/routes/api');

let appInstance = null;

// =========================================================================
// MODE 1: PRODUCTION EXPRESS SERVER (Used when dependencies are installed)
// =========================================================================
try {
  const express = require('express');
  const cors = require('cors');
  const app = express();

  app.use(cors());
  app.use(express.json());
  app.use(express.urlencoded({ extended: true }));

  // Static Frontend Assets
  app.use(express.static(path.join(__dirname, 'public')));

  // Mount API
  const apiRoutes = require('./src/routes/api');
  app.use('/api/v1', apiRoutes);

  // SPA navigation fallback
  app.get('*', (req, res) => {
    if (req.path.startsWith('/api')) {
      return res.status(404).json({ error: 'NotFound', message: `API '${req.path}' not found.` });
    }
    res.sendFile(path.join(__dirname, 'public', 'index.html'));
  });

  appInstance = app;
} catch (e) {
  // Express not installed in local environment; fallback to Native HTTP Server
  appInstance = null;
}

// =========================================================================
// MODE 2: NATIVE HTTP SERVER (Zero-dependency instant local execution)
// =========================================================================
function nativeHttpHandler(req, res) {
  // Global CORS headers
  res.setHeader('Access-Control-Allow-Origin', '*');
  res.setHeader('Access-Control-Allow-Methods', 'GET, POST, PUT, DELETE, OPTIONS');
  res.setHeader('Access-Control-Allow-Headers', 'Content-Type, x-tenant-id, Authorization');

  if (req.method === 'OPTIONS') {
    res.statusCode = 204;
    return res.end();
  }

  const parsedUrl = url.parse(req.url, true);
  const pathname = parsedUrl.pathname;

  // Route API Requests
  if (pathname.startsWith('/api/v1')) {
    let bodyData = '';
    req.on('data', chunk => { bodyData += chunk; });
    req.on('end', () => {
      let body = {};
      if (bodyData) {
        try { body = JSON.parse(bodyData); } catch (e) { /* ignore parse error */ }
      }
      dispatchApi(req, res, pathname, parsedUrl.query, body);
    });
    return;
  }

  // Serve Static Frontend Files from public/
  let filePath = path.join(__dirname, 'public', pathname === '/' ? 'index.html' : pathname);
  
  if (!fs.existsSync(filePath) || fs.statSync(filePath).isDirectory()) {
    filePath = path.join(__dirname, 'public', 'index.html');
  }

  const ext = path.extname(filePath).toLowerCase();
  const mimeTypes = {
    '.html': 'text/html',
    '.js': 'application/javascript',
    '.css': 'text/css',
    '.json': 'application/json',
    '.svg': 'image/svg+xml',
    '.png': 'image/png'
  };

  fs.readFile(filePath, (err, content) => {
    if (err) {
      res.writeHead(500, { 'Content-Type': 'text/plain', 'Connection': 'close' });
      return res.end('Error loading asset');
    }
    res.writeHead(200, {
      'Content-Type': mimeTypes[ext] || 'text/plain',
      'Content-Length': Buffer.byteLength(content),
      'Connection': 'close'
    });
    res.end(content);
  });
}

// Start Server if executed directly
if (require.main === module) {
  const PORT = config.port || 5000;
  
  if (appInstance) {
    appInstance.listen(PORT, () => printServerBanner(PORT, 'Express.js Production'));
  } else {
    const server = http.createServer(nativeHttpHandler);
    server.on('checkContinue', (req, res) => {
      res.writeContinue();
      nativeHttpHandler(req, res);
    });
    server.listen(PORT, () => printServerBanner(PORT, 'High-Performance Native Node.js'));
  }
}

function printServerBanner(port, engine) {
  console.log(`================================================================`);
  console.log(`🚀 CSC337 Lab 06 Multi-Tenant Kafka SaaS Platform`);
  console.log(`⚡ Engine: ${engine}`);
  console.log(`📡 URL: http://localhost:${port}`);
  console.log(`🏢 Active Tenants: ${Object.keys(config.tenants).join(', ')}`);
  console.log(`📊 Kafka Consumer Group: ${getConsumerMetrics().consumerGroupId}`);
  console.log(`================================================================`);
}

module.exports = appInstance || nativeHttpHandler;
