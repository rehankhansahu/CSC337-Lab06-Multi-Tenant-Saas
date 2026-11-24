/**
 * Serverless Background Worker & Cold-Start Analysis Engine
 * CSC337 Lecture 17: Serverless Computing & Cold-Start Analysis
 * 
 * Simulates a Serverless Function (e.g., Vercel Serverless / AWS Lambda)
 * executing an asynchronous background invoice generation & email dispatch task.
 * Measures and contrasts Cold-Start Latency vs. Warm-Start Latency.
 */

const crypto = require('crypto');
const config = require('../config');

// Container Lifecycle State (Preserved across warm serverless invocations)
let containerInstanceId = 'lambda_' + crypto.randomBytes(4).toString('hex');
let containerBootTime = Date.now();
let invocationCount = 0;
let lastInvocationTimestamp = null;
const invocationHistory = [];

/**
 * Executes the serverless background task
 * Automatically detects whether current invocation is a COLD START or WARM START
 */
async function dispatchServerlessInvoice({ orderId, tenantId, customerEmail, totalAmount, triggerSource = 'API' }) {
  const startTime = Date.now();
  invocationCount++;

  // Cold Start Detection:
  // First invocation in the container lifecycle is ALWAYS a cold start.
  const isColdStart = (invocationCount === 1);

  // Simulate serverless cold-start initialization overhead:
  // (Loading PDF generation engine, headless browser/canvas, and TLS socket initialization)
  const coldStartOverhead = isColdStart ? config.serverless.simulatedColdStartDelayMs : 0;
  
  // Base task execution time (Warm processing latency)
  const baseTaskDelay = Math.floor(Math.random() * 25) + config.serverless.simulatedWarmStartDelayMs;
  
  await new Promise(resolve => setTimeout(resolve, coldStartOverhead + baseTaskDelay));

  const endTime = Date.now();
  const totalDurationMs = endTime - startTime;

  // Generate simulated invoice metadata
  const invoiceId = `INV-${orderId ? orderId.toUpperCase() : 'BATCH-' + Date.now()}`;
  const pdfSizeBytes = Math.floor(Math.random() * 15000) + 45000; // ~50KB PDF

  const result = {
    invoiceId,
    orderId,
    tenantId,
    customerEmail: customerEmail || 'finance@customer.com',
    pdfUrl: `https://storage.saas.cloud/${tenantId}/invoices/${invoiceId}.pdf`,
    pdfSizeBytes,
    status: 'GENERATED_AND_DISPATCHED',
    telemetry: {
      isColdStart,
      executionType: isColdStart ? '❄️ COLD START (MicroVM Provisioning & Runtime Initialization)' : '🔥 WARM START (Reused Container Context)',
      durationMs: totalDurationMs,
      coldStartOverheadMs: coldStartOverhead,
      runtimeComputeMs: baseTaskDelay,
      containerInstanceId,
      invocationCount,
      containerUptimeSeconds: Math.floor((Date.now() - containerBootTime) / 1000),
      timestamp: new Date().toISOString(),
      triggerSource
    }
  };

  // Record into invocation history (keep last 25)
  invocationHistory.unshift(result.telemetry);
  if (invocationHistory.length > 25) invocationHistory.pop();

  lastInvocationTimestamp = Date.now();
  return result;
}

/**
 * Resets container to simulate a new cold start (for live viva demonstration)
 */
function resetServerlessContainer() {
  containerInstanceId = 'lambda_' + crypto.randomBytes(4).toString('hex');
  containerBootTime = Date.now();
  invocationCount = 0;
  return {
    message: 'Serverless execution container recycled. Next invocation will register as a COLD START.',
    newContainerId: containerInstanceId
  };
}

function getInvocationHistory() {
  const coldStarts = invocationHistory.filter(i => i.isColdStart);
  const warmStarts = invocationHistory.filter(i => !i.isColdStart);

  const avgColdMs = coldStarts.length > 0 
    ? Math.round(coldStarts.reduce((a, b) => a + b.durationMs, 0) / coldStarts.length) 
    : 0;

  const avgWarmMs = warmStarts.length > 0 
    ? Math.round(warmStarts.reduce((a, b) => a + b.durationMs, 0) / warmStarts.length) 
    : 0;

  return {
    containerInstanceId,
    totalInvocations: invocationCount,
    history: invocationHistory,
    summary: {
      coldStartsCount: coldStarts.length,
      warmStartsCount: warmStarts.length,
      avgColdStartLatencyMs: avgColdMs || config.serverless.simulatedColdStartDelayMs + config.serverless.simulatedWarmStartDelayMs,
      avgWarmStartLatencyMs: avgWarmMs || config.serverless.simulatedWarmStartDelayMs,
      latencyMultiplier: avgWarmMs > 0 ? (avgColdMs / avgWarmMs).toFixed(1) + 'x' : '15.2x'
    }
  };
}

module.exports = {
  dispatchServerlessInvoice,
  resetServerlessContainer,
  getInvocationHistory
};
