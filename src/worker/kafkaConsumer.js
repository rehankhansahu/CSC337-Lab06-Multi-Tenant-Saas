/**
 * Kafka Consumer Group Background Service
 * CSC337 Lecture 18: Asynchronous Event Processing & Offset Management
 * 
 * Subscribes to topic 'tenant.orders' under consumer group 'orders-consumer-group'.
 * Consumes OrderCreated events, deducts inventory, transitions order state,
 * logs committed offsets, and triggers async serverless invoice dispatch.
 */

const config = require('../config');
const { virtualKafkaBus } = require('../kafka/kafkaClient');
const { seedData } = require('../db/connectionPool');
const { dispatchServerlessInvoice } = require('../services/serverlessWorker');

const consumerMetrics = {
  consumerGroupId: config.kafka.groupId,
  topic: config.kafka.topic,
  totalEventsProcessed: 0,
  lastProcessedOffset: null,
  lastProcessedPartition: null,
  lastProcessedKey: null,
  activeWorkers: 1,
  status: 'ACTIVE_LISTENING',
  committedOffsets: {}
};

/**
 * Core event processing logic executed upon consuming an OrderCreated message
 */
async function processOrderCreatedEvent(event) {
  try {
    const { orderId, tenantId, payload } = event;
    console.log(`[Kafka Consumer Group: ${config.kafka.groupId}] Consuming Offset #${event.offset} (Partition ${event.partition}) for Tenant [${event.key}]`);

    // 1. Mark event in audit log as PROCESSING
    const auditLog = seedData.kafkaLogs.find(l => l.id === event.id);
    if (auditLog) {
      auditLog.status = 'PROCESSING';
    }

    // 2. Find order in repository and update status
    const order = seedData.orders.find(o => o.id === orderId);
    if (order) {
      order.status = 'PROCESSING';
      order.updated_at = new Date().toISOString();
    }

    // 3. Deduct inventory for each product in order
    if (payload && payload.items && payload.items.length > 0) {
      payload.items.forEach(item => {
        const product = seedData.products.find(p => p.id === item.product_id && p.tenant_id === tenantId);
        if (product) {
          const qty = parseInt(item.quantity || 1, 10);
          product.stock = Math.max(0, product.stock - qty);
          console.log(`[Inventory Deducted]: Product ${product.sku} new stock: ${product.stock}`);
        }
      });
    }

    // Simulate async business processing delay (e.g., payment verification / fraud check)
    await new Promise(resolve => setTimeout(resolve, 400));

    // 4. Mark order as COMPLETED
    if (order) {
      order.status = 'COMPLETED';
      order.updated_at = new Date().toISOString();
    }

    // 5. Commit Offset
    consumerMetrics.totalEventsProcessed++;
    consumerMetrics.lastProcessedOffset = event.offset;
    consumerMetrics.lastProcessedPartition = event.partition;
    consumerMetrics.lastProcessedKey = event.key;
    consumerMetrics.committedOffsets[event.partition] = event.offset;

    if (auditLog) {
      auditLog.status = 'COMPLETED';
      auditLog.processedAt = new Date().toISOString();
      auditLog.committedOffset = event.offset;
    }

    // 6. Trigger Serverless Async Worker (Invoice dispatch / Cold start tracking)
    setImmediate(async () => {
      try {
        await dispatchServerlessInvoice({
          orderId: order ? order.id : event.orderId,
          tenantId: tenantId,
          customerEmail: payload ? payload.customerEmail : 'client@enterprise.com',
          totalAmount: order ? order.total_amount : 500,
          triggerSource: 'KAFKA_CONSUMER_EVENT'
        });
      } catch (err) {
        console.warn('[Serverless Worker Trigger Error]:', err.message);
      }
    });

  } catch (error) {
    console.error(`[Kafka Consumer Error on Offset ${event.offset}]:`, error.message);
    const auditLog = seedData.kafkaLogs.find(l => l.id === event.id);
    if (auditLog) {
      auditLog.status = 'FAILED';
      auditLog.error = error.message;
    }
  }
}

// Bind to event bus
virtualKafkaBus.on('OrderCreated', async (event) => {
  await processOrderCreatedEvent(event);
});

function getConsumerMetrics() {
  return {
    ...consumerMetrics,
    timestamp: new Date().toISOString(),
    inventoryState: seedData.products.map(p => ({
      sku: p.sku,
      tenantId: p.tenant_id,
      name: p.name,
      stock: p.stock
    }))
  };
}

module.exports = {
  processOrderCreatedEvent,
  getConsumerMetrics,
  consumerMetrics
};
