/**
 * Database Performance Tuning & Optimization Service
 * CSC337 Lecture 17: Query Optimization, N+1 Resolution & EXPLAIN ANALYZE
 */

const { seedData } = require('../db/connectionPool');

/**
 * Executes and compares Un-Optimized (N+1) vs Optimized (Single JOIN) Queries
 */
async function benchmarkNPlusOne(tenantId, limit = 50) {
  // ==========================================
  // 1. UN-OPTIMIZED N+1 QUERY PATH
  // ==========================================
  const unoptimizedStart = process.hrtime.bigint();
  let unoptimizedQueryCount = 0;

  // Query 1: Fetch Orders
  unoptimizedQueryCount++;
  const tenantOrders = seedData.orders
    .filter(o => o.tenant_id === tenantId)
    .slice(0, limit);

  // N Queries: Loop through each order and fetch its items individually
  const unoptimizedResult = [];
  for (const order of tenantOrders) {
    unoptimizedQueryCount++;
    // Simulate discrete DB round-trip latency (e.g. 2-4ms per query)
    await new Promise(r => setTimeout(r, 2));
    const items = seedData.orderItems.filter(item => item.order_id === order.id && item.tenant_id === tenantId);
    unoptimizedResult.push({
      ...order,
      items
    });
  }

  const unoptimizedEnd = process.hrtime.bigint();
  const unoptimizedDurationMs = Number(unoptimizedEnd - unoptimizedStart) / 1000000;

  // ==========================================
  // 2. OPTIMIZED SINGLE-QUERY JOIN PATH
  // ==========================================
  const optimizedStart = process.hrtime.bigint();
  let optimizedQueryCount = 1;

  // Single JOIN: Eager-load orders with all child items in a single query pass
  // Simulates:
  // SELECT o.*, json_agg(oi.*) AS items FROM orders o LEFT JOIN order_items oi ON o.id = oi.order_id
  // WHERE o.tenant_id = $1 GROUP BY o.id LIMIT 50;
  await new Promise(r => setTimeout(r, 5)); // Single roundtrip DB latency

  const itemsByOrderId = {};
  seedData.orderItems.forEach(item => {
    if (item.tenant_id === tenantId) {
      if (!itemsByOrderId[item.order_id]) itemsByOrderId[item.order_id] = [];
      itemsByOrderId[item.order_id].push(item);
    }
  });

  const optimizedResult = tenantOrders.map(order => ({
    ...order,
    items: itemsByOrderId[order.id] || []
  }));

  const optimizedEnd = process.hrtime.bigint();
  const optimizedDurationMs = Number(optimizedEnd - optimizedStart) / 1000000;

  const speedupFactor = (unoptimizedDurationMs / Math.max(0.1, optimizedDurationMs)).toFixed(1);
  const queryReductionPercent = Math.round(((unoptimizedQueryCount - optimizedQueryCount) / unoptimizedQueryCount) * 100);

  return {
    tenantId,
    sampleSize: tenantOrders.length,
    unoptimized: {
      strategy: 'N+1 Loop Query (Anti-Pattern)',
      queriesExecuted: unoptimizedQueryCount,
      sqlBreakdown: [
        `1x: SELECT * FROM orders WHERE tenant_id = '${tenantId}' LIMIT ${limit};`,
        `${tenantOrders.length}x: SELECT * FROM order_items WHERE order_id = ? AND tenant_id = '${tenantId}';`
      ],
      durationMs: parseFloat(unoptimizedDurationMs.toFixed(2)),
      networkRoundtrips: unoptimizedQueryCount,
      risk: 'Connection Pool Exhaustion & Excessive Database Thrashing'
    },
    optimized: {
      strategy: 'Single SQL JOIN / Eager Loading (Optimized)',
      queriesExecuted: optimizedQueryCount,
      sqlBreakdown: [
        `1x: SELECT o.id, o.order_number, o.status, o.total_amount, o.created_at, ` +
        `json_agg(json_build_object('id', oi.id, 'sku', oi.sku, 'qty', oi.quantity, 'price', oi.unit_price)) AS items ` +
        `FROM orders o LEFT JOIN order_items oi ON o.id = oi.order_id ` +
        `WHERE o.tenant_id = '${tenantId}' GROUP BY o.id ORDER BY o.created_at DESC LIMIT ${limit};`
      ],
      durationMs: parseFloat(optimizedDurationMs.toFixed(2)),
      networkRoundtrips: 1,
      benefit: 'Single Network Roundtrip & Constant Connection Usage'
    },
    comparison: {
      speedup: `${speedupFactor}x Faster`,
      queriesReduced: `${queryReductionPercent}% Reduction (${unoptimizedQueryCount} queries -> 1 query)`,
      latencySavedMs: parseFloat((unoptimizedDurationMs - optimizedDurationMs).toFixed(2))
    }
  };
}

/**
 * Returns PostgreSQL EXPLAIN ANALYZE comparison before vs after composite indexing
 */
function getExplainAnalyzeReport(tenantId) {
  const querySql = `SELECT * FROM orders WHERE tenant_id = '${tenantId}' AND created_at >= '2026-01-01' ORDER BY created_at DESC;`;

  const beforeIndexPlan = [
    `Sort  (cost=1845.82..1846.95 rows=450 width=88) (actual time=44.120..44.250 ms rows=50 loops=1)`,
    `  Sort Key: created_at DESC`,
    `  Sort Method: top-N heapsort  Memory: 38kB`,
    `  ->  Seq Scan on orders  (cost=0.00..1825.00 rows=450 width=88) (actual time=0.045..41.850 ms rows=50 loops=1)`,
    `        Filter: ((tenant_id = '${tenantId}'::text) AND (created_at >= '2026-01-01 00:00:00'::timestamp without time zone))`,
    `        Rows Removed by Filter: 24950`,
    `Planning Time: 0.185 ms`,
    `Execution Time: 44.380 ms`
  ];

  const afterIndexPlan = [
    `Index Scan Backward using idx_orders_tenant_created on orders  (cost=0.29..14.85 rows=50 width=88) (actual time=0.025..0.380 ms rows=50 loops=1)`,
    `  Index Cond: ((tenant_id = '${tenantId}'::text) AND (created_at >= '2026-01-01 00:00:00'::timestamp without time zone))`,
    `Planning Time: 0.082 ms`,
    `Execution Time: 0.412 ms`
  ];

  return {
    querySql,
    indexDefinition: `CREATE INDEX idx_orders_tenant_created ON orders (tenant_id, created_at DESC);`,
    compositeIndexFields: ['tenant_id', 'created_at DESC'],
    tenantId,
    before: {
      indexUsed: 'None (Unindexed / Full Table Scan)',
      scanType: 'Seq Scan (Sequential Table Scan)',
      cost: '0.00..1845.82',
      rowsRemovedByFilter: 24950,
      planningTimeMs: 0.185,
      executionTimeMs: 44.38,
      planText: beforeIndexPlan.join('\n'),
      verdict: 'Inefficient: Scans every single row in table and discards 24,950 un-matching tenant rows in memory.'
    },
    after: {
      indexUsed: 'idx_orders_tenant_created',
      scanType: 'Index Scan Backward (B-Tree Composite Index)',
      cost: '0.29..14.85',
      rowsRemovedByFilter: 0,
      planningTimeMs: 0.082,
      executionTimeMs: 0.412,
      planText: afterIndexPlan.join('\n'),
      verdict: 'Optimal: Directly seeks the tenant slice and reads ordered records in microsecond O(log N) time.'
    },
    benchmarkSummary: {
      executionTimeReductionPercent: '99.07%',
      speedupMultiplier: '107.7x Faster',
      latencyBeforeMs: 44.38,
      latencyAfterMs: 0.412,
      rowsInspectedBefore: 25000,
      rowsInspectedAfter: 50
    }
  };
}

module.exports = {
  benchmarkNPlusOne,
  getExplainAnalyzeReport
};
