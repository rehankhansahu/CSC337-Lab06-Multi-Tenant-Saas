/**
 * Standalone Background Kafka Consumer Worker Process
 * Run with: npm run worker
 * Can be deployed as a background worker on Render, Docker, or Kubernetes.
 */

const { getConsumerMetrics } = require('./kafkaConsumer');
const config = require('../config');

console.log('================================================================');
console.log('👷 Kafka Consumer Worker Process Started');
console.log(`📡 Topic: ${config.kafka.topic}`);
console.log(`👥 Consumer Group: ${config.kafka.groupId}`);
console.log('⚡ Status: Listening for asynchronous OrderCreated events...');
console.log('================================================================');

// Keep process alive and periodically log telemetry
setInterval(() => {
  const metrics = getConsumerMetrics();
  console.log(`[Worker Heartbeat]: Processed ${metrics.totalEventsProcessed} events. Last Offset: ${metrics.lastProcessedOffset || 'None'}`);
}, 10000);
