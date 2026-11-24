/**
 * Multi-Tenant Order Service
 * CSC337 Lecture 18: Multi-Tenancy & Kafka Event-Driven Architecture
 */

const { seedData } = require('../db/connectionPool');
const { publishOrderCreatedEvent } = require('../kafka/kafkaClient');
const { enforceTenantBoundary } = require('../middleware/tenantMiddleware');

/**
 * Places an asynchronous order with immediate 202 Accepted response
 * Emits OrderCreated event to Kafka topic 'tenant.orders' keyed by tenant_id
 */
async function createOrder(tenantId, orderInput) {
  const customerEmail = orderInput.customer_email || `buyer@${tenantId}.com`;
  const rawItems = orderInput.items || [];

  if (!rawItems || rawItems.length === 0) {
    throw new Error('Order must contain at least one item');
  }

  const orderId = `ord_${tenantId === 'tenant_acme' ? 'acme' : 'glx'}_${Date.now()}`;
  let totalAmount = 0;
  const processedItems = [];

  // Build items and verify tenant ownership of products
  for (const item of rawItems) {
    const product = seedData.products.find(p => p.id === item.product_id && p.tenant_id === tenantId);
    if (!product) {
      throw new Error(`Product ID '${item.product_id}' not found in catalog for tenant '${tenantId}'`);
    }

    const qty = parseInt(item.quantity || 1, 10);
    const lineTotal = product.price * qty;
    totalAmount += lineTotal;

    const orderItem = {
      id: `item_${Date.now()}_${Math.random().toString(36).substr(2, 4)}`,
      order_id: orderId,
      tenant_id: tenantId,
      product_id: product.id,
      product_name: product.name,
      sku: product.sku,
      quantity: qty,
      unit_price: product.price,
      total_price: parseFloat(lineTotal.toFixed(2))
    };

    processedItems.push(orderItem);
    seedData.orderItems.push(orderItem);
  }

  // Create Order in PENDING_ENQUEUED state
  const newOrder = {
    id: orderId,
    tenant_id: tenantId,
    order_number: `ORD-${Date.now()}-${tenantId.toUpperCase().slice(-4)}`,
    customer_email: customerEmail,
    status: 'PENDING_ENQUEUED',
    total_amount: parseFloat(totalAmount.toFixed(2)),
    created_at: new Date().toISOString(),
    updated_at: new Date().toISOString()
  };

  seedData.orders.unshift(newOrder);

  // Publish to Kafka Broker immediately
  const kafkaDispatch = await publishOrderCreatedEvent({
    ...newOrder,
    items: processedItems
  }, tenantId);

  return {
    order: newOrder,
    items: processedItems,
    kafka: kafkaDispatch
  };
}

/**
 * Retrieve tenant-scoped orders
 */
function getOrdersByTenant(tenantId, limit = 50) {
  // Strict tenant scoping
  return seedData.orders
    .filter(o => o.tenant_id === tenantId)
    .slice(0, limit);
}

/**
 * Retrieve single order by ID with tenant boundary validation
 */
function getOrderById(tenantId, orderId) {
  const order = seedData.orders.find(o => o.id === orderId);
  if (!order) {
    const err = new Error(`Order '${orderId}' not found.`);
    err.status = 404;
    throw err;
  }

  // Enforce Tenant Isolation Boundary (Lecture 18 requirement)
  enforceTenantBoundary(tenantId, order.tenant_id);

  const items = seedData.orderItems.filter(i => i.order_id === order.id && i.tenant_id === tenantId);
  return {
    ...order,
    items
  };
}

/**
 * Get tenant product catalog with live inventory counts
 */
function getProductsByTenant(tenantId) {
  return seedData.products.filter(p => p.tenant_id === tenantId);
}

module.exports = {
  createOrder,
  getOrdersByTenant,
  getOrderById,
  getProductsByTenant
};
