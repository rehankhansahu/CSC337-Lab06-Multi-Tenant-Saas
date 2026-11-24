/**
 * REST API Route Handlers (Hybrid Express + Native HTTP)
 * CSC337 Lab Assignment 06:
 * Multi-Tenant SaaS, Kafka Event-Driven Architecture, DB Performance, Serverless
 */

const config = require('../config');
const { resolveTenantId, enforceTenantBoundary } = require('../middleware/tenantMiddleware');
const { createOrder, getOrdersByTenant, getOrderById, getProductsByTenant } = require('../services/orderService');
const { benchmarkNPlusOne, getExplainAnalyzeReport } = require('../services/performanceService');
const { getConnectionPoolStats, seedData } = require('../db/connectionPool');
const { getConsumerMetrics } = require('../worker/kafkaConsumer');
const { dispatchServerlessInvoice, resetServerlessContainer, getInvocationHistory } = require('../services/serverlessWorker');

// Core API Controller Functions (Pure functions shared across Express & Native HTTP)
async function handleHealth(tenantId) {
  return {
    status: 'ONLINE',
    service: 'Enterprise Multi-Tenant Kafka SaaS Platform',
    timestamp: new Date().toISOString(),
    activeTenant: config.tenants[tenantId] || { id: tenantId },
    connectionPool: getConnectionPoolStats(),
    kafkaConsumer: getConsumerMetrics().status
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

function sendJson(res, statusCode, data, tenantId) {
  const payload = JSON.stringify(data);
  res.writeHead(statusCode, {
    'Content-Type': 'application/json',
    'Content-Length': Buffer.byteLength(payload),
    'X-Resolved-Tenant-Id': tenantId || 'tenant_acme',
    'Connection': 'close'
  });
  res.end(payload);
}

// -------------------------------------------------------------
// Native HTTP Dispatcher (Runs with zero npm dependencies)
// -------------------------------------------------------------
async function dispatchApi(req, res, pathname, query, body) {
  const tenantId = resolveTenantId(req);

  try {
    if (pathname === '/api/v1/health' && req.method === 'GET') {
      const data = await handleHealth(tenantId);
      return sendJson(res, 200, data, tenantId);
    }

    if (pathname === '/api/v1/tenants' && req.method === 'GET') {
      const data = handleTenants(tenantId);
      return sendJson(res, 200, data, tenantId);
    }

    if (pathname === '/api/v1/catalog' && req.method === 'GET') {
      const data = handleCatalog(tenantId);
      return sendJson(res, 200, data, tenantId);
    }

    if (pathname === '/api/v1/orders' && req.method === 'POST') {
      const data = await handleCreateOrder(tenantId, body);
      return sendJson(res, 202, data, tenantId); // HTTP 202 Accepted
    }

    if (pathname === '/api/v1/orders' && req.method === 'GET') {
      const limit = parseInt(query.limit || '50', 10);
      const data = handleGetOrders(tenantId, limit);
      return sendJson(res, 200, data, tenantId);
    }

    if (pathname.startsWith('/api/v1/orders/') && req.method === 'GET') {
      const orderId = pathname.split('/').pop();
      try {
        const data = handleGetOrderById(tenantId, orderId);
        return sendJson(res, 200, data, tenantId);
      } catch (err) {
        const code = err.status || 500;
        return sendJson(res, code, {
          status: 'AccessDenied',
          statusCode: code,
          error: err.message,
          code: err.code || 'RESOURCE_FETCH_ERROR'
        }, tenantId);
      }
    }

    if (pathname === '/api/v1/performance/n-plus-one' && req.method === 'GET') {
      const limit = parseInt(query.limit || '50', 10);
      const data = await handleNPlusOne(tenantId, limit);
      return sendJson(res, 200, data, tenantId);
    }

    if (pathname === '/api/v1/performance/explain-analyze' && req.method === 'GET') {
      const data = handleExplainAnalyze(tenantId);
      return sendJson(res, 200, data, tenantId);
    }

    if (pathname === '/api/v1/performance/connection-pool' && req.method === 'GET') {
      const data = handleConnectionPool();
      return sendJson(res, 200, data, tenantId);
    }

    if (pathname === '/api/v1/kafka/events' && req.method === 'GET') {
      const data = handleKafkaEvents();
      return sendJson(res, 200, data, tenantId);
    }

    if (pathname === '/api/v1/serverless/invoice-dispatch' && req.method === 'POST') {
      const data = await handleServerlessInvoice(tenantId, body);
      return sendJson(res, 200, data, tenantId);
    }

    if (pathname === '/api/v1/serverless/reset-container' && req.method === 'POST') {
      const data = handleServerlessReset();
      return sendJson(res, 200, data, tenantId);
    }

    if (pathname === '/api/v1/serverless/telemetry' && req.method === 'GET') {
      const data = handleServerlessTelemetry();
      return sendJson(res, 200, data, tenantId);
    }

    // Unmatched API endpoint
    return sendJson(res, 404, {
      error: 'NotFound',
      message: `Endpoint '${pathname}' was not found.`,
      documentation: '/api/v1/health'
    }, tenantId);

  } catch (err) {
    return sendJson(res, 500, {
      status: 'Error',
      error: err.message
    }, tenantId);
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
  // Express not yet installed locally; native HTTP dispatcher handles all requests seamlessly
}

module.exports = expressRouter || dispatchApi;
module.exports.dispatchApi = dispatchApi;
