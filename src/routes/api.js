/**
 * REST API Route Handlers (Hybrid Express + Native HTTP)
 * Implements:
 * - Multi-Tenant SaaS & Data Scoping
 * - Apache Kafka Order Producer & Ingestion
 * - N+1 Query Resolution & EXPLAIN ANALYZE
 * - Connection Pooling
 * - Serverless Cold-Start Worker
 * - OAuth 2.0 / OpenID Connect (Google & GitHub)
 * - Bcrypt Password Verification
 * - Access Token + Refresh Token Rotation with httpOnly Cookies
 * - RBAC (SuperAdmin, Manager, Employee)
 * - Rate Limiting & Helmet Security Headers
 */

const config = require('../config');
const { resolveTenantId, enforceTenantBoundary } = require('../middleware/tenantMiddleware');
const { createOrder, getOrdersByTenant, getOrderById, getProductsByTenant } = require('../services/orderService');
const { benchmarkNPlusOne, getExplainAnalyzeReport } = require('../services/performanceService');
const { getConnectionPoolStats, seedData } = require('../db/connectionPool');
const { getConsumerMetrics } = require('../worker/kafkaConsumer');
const { dispatchServerlessInvoice, resetServerlessContainer, getInvocationHistory } = require('../services/serverlessWorker');
const {
  userDatabase,
  verifyPassword,
  issueTokenPair,
  rotateRefreshToken,
  handleOAuthLogin,
  verifyRBAC,
  checkRateLimit
} = require('../services/authService');

// Core API Controller Functions
async function handleHealth(tenantId) {
  return {
    status: 'ONLINE',
    service: 'Enterprise Multi-Tenant Kafka SaaS Platform',
    timestamp: new Date().toISOString(),
    activeTenant: config.tenants[tenantId] || { id: tenantId },
    connectionPool: getConnectionPoolStats(),
    kafkaConsumer: getConsumerMetrics().status,
    security: {
      passwordHashing: 'Bcrypt ($2a$12$ rounds)',
      oauthProviders: ['Google (OpenID Connect)', 'GitHub OAuth 2.0'],
      tokenPolicy: 'Access Token (15m) + Refresh Token Rotation (httpOnly Secure)',
      rbacRoles: ['SuperAdmin', 'Manager', 'Employee'],
      rateLimiting: '30 requests/minute active',
      headers: 'Helmet standards (nosniff, DENY, HSTS, strict CORS)'
    }
  };
}

function handleTenants(tenantId) {
  return {
    activeTenantId: tenantId,
    tenants: Object.values(config.tenants)
  };
}

function handleCatalog(tenantId) {
  const products = getProductsByTenant(tenantId);
  const tenant = config.tenants[tenantId] || { name: tenantId };
  return {
    tenantId,
    tenantName: tenant.name,
    count: products.length,
    products
  };
}

async function handleCreateOrder(tenantId, body) {
  const result = await createOrder(tenantId, body || {});
  return {
    status: 'Accepted',
    statusCode: 202,
    message: `Order accepted and enqueued to Kafka topic [${result.kafka.topic}]`,
    order: result.order,
    items: result.items,
    kafka: {
      topic: result.kafka.topic,
      partition: result.kafka.partition,
      offset: result.kafka.offset,
      messageKey: result.kafka.key,
      mode: result.kafka.mode,
      guarantee: 'Strict FIFO Ordering guaranteed on designated tenant partition'
    }
  };
}

function handleGetOrders(tenantId, limit = 50) {
  const orders = getOrdersByTenant(tenantId, limit);
  return {
    tenantId,
    count: orders.length,
    orders
  };
}

function handleGetOrderById(tenantId, orderId) {
  const order = getOrderById(tenantId, orderId);
  return {
    tenantId,
    order
  };
}

async function handleNPlusOne(tenantId, limit = 50) {
  return await benchmarkNPlusOne(tenantId, limit);
}

function handleExplainAnalyze(tenantId) {
  return getExplainAnalyzeReport(tenantId);
}

function handleConnectionPool() {
  return getConnectionPoolStats();
}

function handleKafkaEvents() {
  return {
    consumerGroup: getConsumerMetrics(),
    recentEvents: seedData.kafkaLogs.slice(0, 30)
  };
}

async function handleServerlessInvoice(tenantId, body) {
  return await dispatchServerlessInvoice({
    orderId: body.orderId || `ORD-${Date.now()}`,
    tenantId: tenantId,
    customerEmail: body.customerEmail || `finance@${tenantId}.com`,
    totalAmount: body.totalAmount || 499.00,
    triggerSource: body.triggerSource || 'MANUAL_HTTP_INVOCATION'
  });
}

function handleServerlessReset() {
  return resetServerlessContainer();
}

function handleServerlessTelemetry() {
  return getInvocationHistory();
}

// Security Response Helper with Helmet Headers
function sendJson(res, statusCode, data, tenantId, extraHeaders = {}) {
  const payload = JSON.stringify(data);
  const headers = {
    'Content-Type': 'application/json',
    'Content-Length': Buffer.byteLength(payload),
    'X-Resolved-Tenant-Id': tenantId || 'tenant_acme',
    'Connection': 'close',
    // Helmet Security Standards
    'X-Content-Type-Options': 'nosniff',
    'X-Frame-Options': 'DENY',
    'X-XSS-Protection': '1; mode=block',
    'Strict-Transport-Security': 'max-age=31536000; includeSubDomains',
    ...extraHeaders
  };
  res.writeHead(statusCode, headers);
  res.end(payload);
}

// -------------------------------------------------------------
// Native HTTP Dispatcher (Runs with zero npm dependencies)
// -------------------------------------------------------------
async function dispatchApi(req, res, pathname, query, body) {
  const tenantId = resolveTenantId(req);
  const clientIp = req.socket?.remoteAddress || req.headers['x-forwarded-for'] || '127.0.0.1';

  // Rate Limiting Check
  const rateLimit = checkRateLimit(clientIp);
  const rateHeaders = {
    'X-RateLimit-Limit': rateLimit.limit.toString(),
    'X-RateLimit-Remaining': rateLimit.remaining.toString(),
    'X-RateLimit-Reset': rateLimit.resetSeconds.toString()
  };

  if (!rateLimit.allowed) {
    return sendJson(res, 429, {
      status: 'RateLimitExceeded',
      statusCode: 429,
      error: 'Too Many Requests: Rate limit exceeded. Try again in ' + rateLimit.resetSeconds + ' seconds.',
      rateLimit
    }, tenantId, rateHeaders);
  }

  try {
    // --- HEALTH & METADATA ---
    if (pathname === '/api/v1/health' && req.method === 'GET') {
      const data = await handleHealth(tenantId);
      return sendJson(res, 200, data, tenantId, rateHeaders);
    }

    if (pathname === '/api/v1/tenants' && req.method === 'GET') {
      const data = handleTenants(tenantId);
      return sendJson(res, 200, data, tenantId, rateHeaders);
    }

    if (pathname === '/api/v1/catalog' && req.method === 'GET') {
      const data = handleCatalog(tenantId);
      return sendJson(res, 200, data, tenantId, rateHeaders);
    }

    // --- ORDERS & KAFKA ---
    if (pathname === '/api/v1/orders' && req.method === 'POST') {
      const data = await handleCreateOrder(tenantId, body);
      return sendJson(res, 202, data, tenantId, rateHeaders);
    }

    if (pathname === '/api/v1/orders' && req.method === 'GET') {
      const limit = parseInt(query.limit || '50', 10);
      const data = handleGetOrders(tenantId, limit);
      return sendJson(res, 200, data, tenantId, rateHeaders);
    }

    if (pathname.startsWith('/api/v1/orders/') && req.method === 'GET') {
      const orderId = pathname.split('/').pop();
      try {
        const data = handleGetOrderById(tenantId, orderId);
        return sendJson(res, 200, data, tenantId, rateHeaders);
      } catch (err) {
        const code = err.status || 500;
        return sendJson(res, code, {
          status: 'AccessDenied',
          statusCode: code,
          error: err.message,
          code: err.code || 'RESOURCE_FETCH_ERROR'
        }, tenantId, rateHeaders);
      }
    }

    // --- DATABASE PERFORMANCE & EXPLAIN ANALYZE ---
    if (pathname === '/api/v1/performance/n-plus-one' && req.method === 'GET') {
      const limit = parseInt(query.limit || '50', 10);
      const data = await handleNPlusOne(tenantId, limit);
      return sendJson(res, 200, data, tenantId, rateHeaders);
    }

    if (pathname === '/api/v1/performance/explain-analyze' && req.method === 'GET') {
      const data = handleExplainAnalyze(tenantId);
      return sendJson(res, 200, data, tenantId, rateHeaders);
    }

    if (pathname === '/api/v1/performance/connection-pool' && req.method === 'GET') {
      const data = handleConnectionPool();
      return sendJson(res, 200, data, tenantId, rateHeaders);
    }

    // --- KAFKA EVENTS ---
    if (pathname === '/api/v1/kafka/events' && req.method === 'GET') {
      const data = handleKafkaEvents();
      return sendJson(res, 200, data, tenantId, rateHeaders);
    }

    // --- SERVERLESS WORKER ---
    if (pathname === '/api/v1/serverless/invoice-dispatch' && req.method === 'POST') {
      const data = await handleServerlessInvoice(tenantId, body);
      return sendJson(res, 200, data, tenantId, rateHeaders);
    }

    if (pathname === '/api/v1/serverless/reset-container' && req.method === 'POST') {
      const data = handleServerlessReset();
      return sendJson(res, 200, data, tenantId, rateHeaders);
    }

    if (pathname === '/api/v1/serverless/telemetry' && req.method === 'GET') {
      const data = handleServerlessTelemetry();
      return sendJson(res, 200, data, tenantId, rateHeaders);
    }

    // =========================================================
    // AUTHENTICATION, OAUTH 2.0, BCRYPT, TOKEN ROTATION & RBAC
    // =========================================================

    // 1. Password Verification with Bcrypt
    if (pathname === '/api/v1/auth/login' && req.method === 'POST') {
      const { email, password } = body || {};
      const user = userDatabase.find(u => u.email === email && u.tenant_id === tenantId);

      if (!user) {
        return sendJson(res, 401, {
          status: 'AuthenticationFailed',
          error: 'Invalid credentials or user not registered under this tenant.'
        }, tenantId, rateHeaders);
      }

      const isValid = verifyPassword(password, user.passwordHash, user.email);
      if (!isValid) {
        return sendJson(res, 401, {
          status: 'AuthenticationFailed',
          error: 'Invalid password. Passwords are cryptographically verified against Bcrypt hashes.'
        }, tenantId, rateHeaders);
      }

      const tokens = issueTokenPair(user);
      return sendJson(res, 200, {
        status: 'Authenticated',
        message: 'Bcrypt password hash matched successfully.',
        passwordProtection: 'Bcrypt Salted & Hashed ($2a$12$)',
        ...tokens
      }, tenantId, {
        ...rateHeaders,
        'Set-Cookie': `refreshToken=${tokens.refreshToken}; HttpOnly; Secure; SameSite=Strict; Path=/api/v1/auth; Max-Age=604800`
      });
    }

    // 2. OAuth 2.0 / OpenID Connect Social Login (Google & GitHub)
    if (pathname === '/api/v1/auth/oauth' && req.method === 'POST') {
      const { provider } = body || {};
      const oauthResult = handleOAuthLogin(provider || 'google', tenantId);
      return sendJson(res, 200, oauthResult, tenantId, {
        ...rateHeaders,
        'Set-Cookie': `refreshToken=${oauthResult.refreshToken}; HttpOnly; Secure; SameSite=Strict; Path=/api/v1/auth; Max-Age=604800`
      });
    }

    // 3. Access Token + Refresh Token Rotation with httpOnly Cookies
    if (pathname === '/api/v1/auth/refresh' && req.method === 'POST') {
      const providedToken = body.refreshToken || 'rt_sample_rotation_seed';
      try {
        const rotationResult = rotateRefreshToken(providedToken);
        return sendJson(res, 200, rotationResult, tenantId, {
          ...rateHeaders,
          'Set-Cookie': `refreshToken=${rotationResult.newRefreshToken}; HttpOnly; Secure; SameSite=Strict; Path=/api/v1/auth; Max-Age=604800`
        });
      } catch (err) {
        return sendJson(res, err.status || 401, {
          status: 'TokenRotationFailed',
          error: err.message,
          code: err.code
        }, tenantId, rateHeaders);
      }
    }

    // 4. RBAC Verification & Rejection Test
    if (pathname === '/api/v1/auth/rbac-test' && req.method === 'GET') {
      const userRole = query.role || 'Employee';
      const requiredRole = query.requiredRole || 'SuperAdmin';
      
      try {
        verifyRBAC(userRole, requiredRole);
        return sendJson(res, 200, {
          status: 'Authorized',
          message: `Access Granted. Role '${userRole}' has sufficient privileges for '${requiredRole}' action.`,
          userRole,
          requiredRole
        }, tenantId, rateHeaders);
      } catch (err) {
        return sendJson(res, 403, {
          status: 'Forbidden',
          statusCode: 403,
          code: err.code,
          error: err.message,
          userRole,
          requiredRole,
          explanation: `Demonstrates strict Role-Based Access Control: '${userRole}' cannot perform actions restricted to '${requiredRole}'.`
        }, tenantId, rateHeaders);
      }
    }

    // 5. User Store & Bcrypt Audit List
    if (pathname === '/api/v1/auth/users' && req.method === 'GET') {
      const sanitized = userDatabase
        .filter(u => u.tenant_id === tenantId)
        .map(u => ({
          id: u.id,
          name: u.name,
          email: u.email,
          role: u.role,
          tenant_id: u.tenant_id,
          passwordStorage: 'Bcrypt Hash (Plain-text passwords never stored)',
          storedHashSample: u.passwordHash.slice(0, 28) + '...'
        }));
      return sendJson(res, 200, { users: sanitized }, tenantId, rateHeaders);
    }

    // Unmatched API endpoint
    return sendJson(res, 404, {
      error: 'NotFound',
      message: `Endpoint '${pathname}' was not found.`,
      documentation: '/api/v1/health'
    }, tenantId, rateHeaders);

  } catch (err) {
    return sendJson(res, 500, {
      status: 'Error',
      error: err.message
    }, tenantId, rateHeaders);
  }
}

// -------------------------------------------------------------
// Optional Express Router (Active when express is installed)
// -------------------------------------------------------------
let expressRouter = null;
try {
  const express = require('express');
  expressRouter = express.Router();
  const { tenantResolutionMiddleware } = require('../middleware/tenantMiddleware');

  expressRouter.use(tenantResolutionMiddleware);

  expressRouter.get('/health', async (req, res) => res.json(await handleHealth(req.tenant.id)));
  expressRouter.get('/tenants', (req, res) => res.json(handleTenants(req.tenant.id)));
  expressRouter.get('/catalog', (req, res) => res.json(handleCatalog(req.tenant.id)));
  expressRouter.post('/orders', async (req, res) => {
    try {
      const data = await handleCreateOrder(req.tenant.id, req.body);
      res.status(202).json(data);
    } catch (e) {
      res.status(400).json({ status: 'Rejected', error: e.message });
    }
  });
  expressRouter.get('/orders', (req, res) => res.json(handleGetOrders(req.tenant.id, parseInt(req.query.limit || '50', 10))));
  expressRouter.get('/orders/:id', (req, res) => {
    try {
      res.json(handleGetOrderById(req.tenant.id, req.params.id));
    } catch (e) {
      res.status(e.status || 500).json({ status: 'AccessDenied', error: e.message, code: e.code });
    }
  });
  expressRouter.get('/performance/n-plus-one', async (req, res) => res.json(await handleNPlusOne(req.tenant.id, parseInt(req.query.limit || '50', 10))));
  expressRouter.get('/performance/explain-analyze', (req, res) => res.json(handleExplainAnalyze(req.tenant.id)));
  expressRouter.get('/performance/connection-pool', (req, res) => res.json(handleConnectionPool()));
  expressRouter.get('/kafka/events', (req, res) => res.json(handleKafkaEvents()));
  expressRouter.post('/serverless/invoice-dispatch', async (req, res) => res.json(await handleServerlessInvoice(req.tenant.id, req.body)));
  expressRouter.post('/serverless/reset-container', (req, res) => res.json(handleServerlessReset()));
  expressRouter.get('/serverless/telemetry', (req, res) => res.json(handleServerlessTelemetry()));
} catch (e) {
  // Express fallback
}

module.exports = expressRouter || dispatchApi;
module.exports.dispatchApi = dispatchApi;
