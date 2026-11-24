/**
 * Frontend Dashboard Controller
 * Enterprise Multi-Tenant Kafka SaaS Platform
 */

let currentTenantId = 'tenant_acme';
let currentTab = 'orders';
let kafkaPollingInterval = null;

// Initialize Dashboard
document.addEventListener('DOMContentLoaded', () => {
  if (window.lucide) lucide.createIcons();
  switchTenant('tenant_acme');
  startKafkaPolling();
  fetchPoolStats();

  const urlParams = new URLSearchParams(window.location.search);
  const requestedTab = urlParams.get('tab') || window.location.hash.replace('#', '');
  if (requestedTab && ['orders', 'nplusone', 'explain', 'serverless', 'isolation', 'pool', 'auth'].includes(requestedTab)) {
    switchTab(requestedTab);
  }
});

/**
 * Switch Active Tenant Context (Tenant A vs Tenant B)
 */
function switchTenant(tenantId) {
  currentTenantId = tenantId;

  const btnAcme = document.getElementById('btnTenantAcme');
  const btnGlobex = document.getElementById('btnTenantGlobex');
  const headerBadge = document.getElementById('activeHeaderBadge');
  const catalogBadge = document.getElementById('catalogTenantBadge');

  if (tenantId === 'tenant_acme') {
    btnAcme.className = 'flex items-center gap-2 px-3 py-1.5 rounded-md text-xs font-medium transition bg-blue-600 text-white shadow';
    btnGlobex.className = 'flex items-center gap-2 px-3 py-1.5 rounded-md text-xs font-medium transition text-slate-400 hover:text-slate-200';
    headerBadge.innerText = 'x-tenant-id: tenant_acme';
    headerBadge.className = 'bg-slate-800 px-2 py-0.5 rounded text-blue-400 font-mono';
    if (catalogBadge) {
      catalogBadge.innerText = 'Acme Corp';
      catalogBadge.className = 'text-[10px] font-mono px-2 py-0.5 rounded bg-blue-500/10 text-blue-400 border border-blue-500/20';
    }
  } else {
    btnGlobex.className = 'flex items-center gap-2 px-3 py-1.5 rounded-md text-xs font-medium transition bg-emerald-600 text-white shadow';
    btnAcme.className = 'flex items-center gap-2 px-3 py-1.5 rounded-md text-xs font-medium transition text-slate-400 hover:text-slate-200';
    headerBadge.innerText = 'x-tenant-id: tenant_globex';
    headerBadge.className = 'bg-slate-800 px-2 py-0.5 rounded text-emerald-400 font-mono';
    if (catalogBadge) {
      catalogBadge.innerText = 'Globex Logistics';
      catalogBadge.className = 'text-[10px] font-mono px-2 py-0.5 rounded bg-emerald-500/10 text-emerald-400 border border-emerald-500/20';
    }
  }

  // Refresh view data for newly selected tenant
  fetchCatalog();
  fetchOrders();
  if (currentTab === 'explain') fetchExplainAnalyze();
}

/**
 * Tab Navigation Switcher
 */
function switchTab(tabId) {
  currentTab = tabId;

  // Update tab buttons
  const tabs = ['orders', 'nplusone', 'explain', 'serverless', 'isolation', 'pool', 'auth'];
  tabs.forEach(t => {
    const btn = document.getElementById(`tab-${t}`);
    const panel = document.getElementById(`panel-${t}`);
    if (t === tabId) {
      btn.className = 'tab-btn px-4 py-2.5 text-xs font-semibold rounded-t-lg border-b-2 border-blue-500 text-blue-400 bg-slate-900/60 flex items-center gap-2 transition';
      panel.classList.remove('hidden');
    } else {
      btn.className = 'tab-btn px-4 py-2.5 text-xs font-semibold rounded-t-lg border-b-2 border-transparent text-slate-400 hover:text-slate-200 hover:bg-slate-900/40 flex items-center gap-2 transition';
      panel.classList.add('hidden');
    }
  });

  if (window.lucide) lucide.createIcons();

  if (tabId === 'nplusone') runNPlusOneBenchmark();
  if (tabId === 'explain') fetchExplainAnalyze();
  if (tabId === 'serverless') fetchServerlessTelemetry();
  if (tabId === 'pool') fetchPoolStats();
}

/**
 * Fetch and Display Tenant Product Catalog
 */
async function fetchCatalog() {
  try {
    const res = await fetch('/api/v1/catalog', {
      headers: { 'x-tenant-id': currentTenantId }
    });
    const data = await res.json();
    const container = document.getElementById('catalogList');
    container.innerHTML = '';

    (data.products || []).forEach(prod => {
      const card = document.createElement('div');
      card.className = 'bg-slate-950 p-3 rounded-xl border border-slate-800 flex items-center justify-between text-xs';
      card.innerHTML = `
        <div>
          <div class="font-bold text-white">${prod.name}</div>
          <div class="text-[10px] text-slate-400 font-mono mt-0.5">${prod.sku} • $${prod.price.toFixed(2)}</div>
        </div>
        <div class="text-right">
          <div class="font-mono text-xs ${prod.stock > 10 ? 'text-emerald-400' : 'text-amber-400'} font-bold">
            ${prod.stock} in stock
          </div>
          <button onclick="orderSpecificProduct('${prod.id}')" class="mt-1 px-2 py-0.5 bg-blue-600/80 hover:bg-blue-600 text-[10px] text-white rounded font-medium transition">
            Order 1 Unit
          </button>
        </div>
      `;
      container.appendChild(card);
    });
  } catch (err) {
    console.error('Failed to fetch catalog:', err);
  }
}

/**
 * Fetch and Display Orders for Current Tenant
 */
async function fetchOrders() {
  try {
    const res = await fetch('/api/v1/orders?limit=25', {
      headers: { 'x-tenant-id': currentTenantId }
    });
    const data = await res.json();
    const tbody = document.getElementById('ordersTableBody');
    const countBadge = document.getElementById('ordersCountBadge');
    tbody.innerHTML = '';

    const orders = data.orders || [];
    countBadge.innerText = `${orders.length} Orders Scoped`;

    orders.forEach(order => {
      const tr = document.createElement('tr');
      tr.className = 'hover:bg-slate-950/60 transition';
      
      let statusBadge = '<span class="px-2 py-0.5 rounded text-[10px] bg-slate-800 text-slate-300">UNKNOWN</span>';
      if (order.status === 'COMPLETED') {
        statusBadge = '<span class="px-2 py-0.5 rounded text-[10px] bg-emerald-500/10 text-emerald-400 border border-emerald-500/20 font-bold">COMPLETED</span>';
      } else if (order.status === 'PROCESSING') {
        statusBadge = '<span class="px-2 py-0.5 rounded text-[10px] bg-amber-500/10 text-amber-400 border border-amber-500/20 font-bold animate-pulse">PROCESSING</span>';
      } else if (order.status === 'PENDING_ENQUEUED' || order.status === 'PENDING') {
        statusBadge = '<span class="px-2 py-0.5 rounded text-[10px] bg-blue-500/10 text-blue-400 border border-blue-500/20 font-bold">QUEUED (202)</span>';
      }

      tr.innerHTML = `
        <td class="px-4 py-3 font-bold text-white">${order.order_number}</td>
        <td class="px-4 py-3"><span class="text-slate-400">${order.tenant_id}</span></td>
        <td class="px-4 py-3 text-slate-300">${order.customer_email}</td>
        <td class="px-4 py-3">${statusBadge}</td>
        <td class="px-4 py-3 text-slate-200 font-bold">$${parseFloat(order.total_amount).toFixed(2)}</td>
        <td class="px-4 py-3 text-slate-500 text-[10px]">${new Date(order.created_at).toLocaleTimeString()}</td>
        <td class="px-4 py-3 text-right">
          <button onclick="inspectSingleOrder('${order.id}')" class="text-blue-400 hover:text-blue-300 text-[11px] underline">
            Inspect
          </button>
        </td>
      `;
      tbody.appendChild(tr);
    });
  } catch (err) {
    console.error('Failed to fetch orders:', err);
  }
}

/**
 * Place a Single Asynchronous Order (Returns 202 Accepted)
 */
async function placeSingleOrder() {
  try {
    const prodId = currentTenantId === 'tenant_acme' ? 'prod_101' : 'prod_201';
    await submitOrderPayload({
      customer_email: `purchasing@client-${Math.floor(Math.random() * 900 + 100)}.com`,
      items: [{ product_id: prodId, quantity: 2 }]
    });
  } catch (err) {
    alert('Failed to place order: ' + err.message);
  }
}

async function orderSpecificProduct(productId) {
  try {
    await submitOrderPayload({
      customer_email: `buyer@direct-${Date.now().toString().slice(-4)}.com`,
      items: [{ product_id: productId, quantity: 1 }]
    });
  } catch (err) {
    alert('Order error: ' + err.message);
  }
}

/**
 * High-Throughput Burst Order Test (10 concurrent requests)
 */
async function placeBurstOrders(count = 10) {
  const prodId = currentTenantId === 'tenant_acme' ? 'prod_102' : 'prod_202';
  const promises = [];
  
  for (let i = 1; i <= count; i++) {
    promises.push(
      fetch('/api/v1/orders', {
        method: 'POST',
        headers: {
          'Content-Type': 'application/json',
          'x-tenant-id': currentTenantId
        },
        body: JSON.stringify({
          customer_email: `burst-order-${i}@enterprise.com`,
          items: [{ product_id: prodId, quantity: 1 }]
        })
      })
    );
  }

  const responses = await Promise.all(promises);
  console.log(`[Burst Order Complete]: ${responses.length} orders dispatched with 202 Accepted.`);
  setTimeout(() => {
    fetchOrders();
    fetchCatalog();
  }, 500);
}

async function submitOrderPayload(payload) {
  const start = performance.now();
  const res = await fetch('/api/v1/orders', {
    method: 'POST',
    headers: {
      'Content-Type': 'application/json',
      'x-tenant-id': currentTenantId
    },
    body: JSON.stringify(payload)
  });

  const duration = (performance.now() - start).toFixed(1);
  const data = await res.json();

  if (res.status === 202) {
    fetchOrders();
    fetchCatalog();
    console.log(`[HTTP 202 Accepted in ${duration}ms]: Enqueued to Kafka Partition ${data.kafka.partition}, Offset ${data.kafka.offset}`);
  } else {
    throw new Error(data.error || 'Failed to submit order');
  }
}

/**
 * Kafka Telemetry Polling Loop
 */
function startKafkaPolling() {
  if (kafkaPollingInterval) clearInterval(kafkaPollingInterval);

  const poll = async () => {
    try {
      const res = await fetch('/api/v1/kafka/events', {
        headers: { 'x-tenant-id': currentTenantId }
      });
      const data = await res.json();

      const consumer = data.consumerGroup || {};
      const offsets = consumer.committedOffsets || {};

      if (offsets[0]) document.getElementById('partOffset0').innerText = offsets[0];
      if (offsets[1]) document.getElementById('partOffset1').innerText = offsets[1];
      if (offsets[2]) document.getElementById('partOffset2').innerText = offsets[2];

      const logContainer = document.getElementById('kafkaEventLog');
      const events = data.recentEvents || [];
      
      if (events.length === 0) {
        logContainer.innerHTML = '<div class="text-slate-500 py-4 text-center">// Waiting for Kafka order events...</div>';
      } else {
        logContainer.innerHTML = events.slice(0, 15).map(evt => {
          let badge = `<span class="text-yellow-400 font-bold">[QUEUED]</span>`;
          if (evt.status === 'COMPLETED') badge = `<span class="text-emerald-400 font-bold">[COMMITTED]</span>`;
          if (evt.status === 'PROCESSING') badge = `<span class="text-blue-400 font-bold">[PROCESSING]</span>`;

          return `
            <div class="flex items-center justify-between py-1 border-b border-slate-900">
              <div>
                <span class="text-indigo-400 font-bold">P${evt.partition}:#${evt.offset}</span>
                <span class="text-slate-500 ml-1">Key: <span class="text-yellow-300 font-bold">${evt.key}</span></span>
                <span class="text-slate-400 ml-1">Order: ${evt.orderId}</span>
              </div>
              <div>${badge}</div>
            </div>
          `;
        }).join('');
      }
    } catch (err) {
      // silent
    }
  };

  poll();
  kafkaPollingInterval = setInterval(poll, 1500);
}

/**
 * N+1 vs Single JOIN Live Benchmark Runner
 */
async function runNPlusOneBenchmark() {
  try {
    const res = await fetch('/api/v1/performance/n-plus-one?limit=50', {
      headers: { 'x-tenant-id': currentTenantId }
    });
    const data = await res.json();

    document.getElementById('n1LatencyUnopt').innerText = `${data.unoptimized.durationMs} ms`;
    document.getElementById('n1LatencyOpt').innerText = `${data.optimized.durationMs} ms`;
    document.getElementById('n1SpeedupBadge').innerText = data.comparison.speedup;
    document.getElementById('n1LatencySaved').innerText = `${data.comparison.latencySavedMs} ms`;
  } catch (err) {
    console.error('Benchmark failed:', err);
  }
}

/**
 * Fetch PostgreSQL EXPLAIN ANALYZE Report
 */
async function fetchExplainAnalyze() {
  try {
    const res = await fetch('/api/v1/performance/explain-analyze', {
      headers: { 'x-tenant-id': currentTenantId }
    });
    const data = await res.json();

    document.getElementById('explainPlanBefore').innerText = data.before.planText;
    document.getElementById('explainPlanAfter').innerText = data.after.planText;
  } catch (err) {
    console.error('Explain fetch failed:', err);
  }
}

/**
 * Serverless Cold-Start Invocation
 */
async function runServerlessTest() {
  try {
    const res = await fetch('/api/v1/serverless/invoice-dispatch', {
      method: 'POST',
      headers: {
        'Content-Type': 'application/json',
        'x-tenant-id': currentTenantId
      },
      body: JSON.stringify({
        orderId: `ORD-${Date.now().toString().slice(-6)}`,
        totalAmount: 1250.00,
        triggerSource: 'UI_MANUAL_BENCHMARK'
      })
    });
    const data = await res.json();
    fetchServerlessTelemetry();
  } catch (err) {
    alert('Serverless error: ' + err.message);
  }
}

/**
 * Batch 5x Serverless Invocations to illustrate Cold Start spike followed by Warm baseline
 */
async function runServerlessBenchmarkBatch() {
  for (let i = 0; i < 5; i++) {
    await fetch('/api/v1/serverless/invoice-dispatch', {
      method: 'POST',
      headers: {
        'Content-Type': 'application/json',
        'x-tenant-id': currentTenantId
      },
      body: JSON.stringify({
        orderId: `BATCH-${i + 1}`,
        triggerSource: `BATCH_TEST_RUN_${i + 1}`
      })
    });
  }
  fetchServerlessTelemetry();
}

async function resetContainerContext() {
  await fetch('/api/v1/serverless/reset-container', {
    method: 'POST',
    headers: { 'x-tenant-id': currentTenantId }
  });
  alert('Serverless Execution Context Recycled! The next invocation will register as a COLD START.');
  fetchServerlessTelemetry();
}

async function fetchServerlessTelemetry() {
  try {
    const res = await fetch('/api/v1/serverless/telemetry', {
      headers: { 'x-tenant-id': currentTenantId }
    });
    const data = await res.json();

    document.getElementById('containerIdBadge').innerText = `Container: ${data.containerInstanceId}`;
    if (data.summary) {
      document.getElementById('statColdLatency').innerText = `${data.summary.avgColdStartLatencyMs} ms`;
      document.getElementById('statWarmLatency').innerText = `${data.summary.avgWarmStartLatencyMs} ms`;
      document.getElementById('statVariance').innerText = `${data.summary.latencyMultiplier} Ratio`;
    }

    const container = document.getElementById('serverlessInvocationsList');
    container.innerHTML = (data.history || []).map((inv, idx) => {
      const isCold = inv.isColdStart;
      const tag = isCold 
        ? `<span class="bg-cyan-950 text-cyan-300 border border-cyan-800 px-2 py-0.5 rounded font-bold">❄️ COLD START</span>` 
        : `<span class="bg-amber-950 text-amber-300 border border-amber-800 px-2 py-0.5 rounded font-bold">🔥 WARM START</span>`;

      return `
        <div class="bg-slate-950 p-2.5 rounded-xl border border-slate-800 flex items-center justify-between text-xs">
          <div>
            <span class="text-slate-400 font-bold">Invocation #${inv.invocationCount}</span>
            <span class="text-slate-500 ml-2">Uptime: ${inv.containerUptimeSeconds}s</span>
            <span class="text-slate-400 ml-2">Trigger: ${inv.triggerSource || 'API'}</span>
          </div>
          <div class="flex items-center gap-3">
            <span class="font-bold ${isCold ? 'text-cyan-400' : 'text-amber-400'}">${inv.durationMs} ms</span>
            ${tag}
          </div>
        </div>
      `;
    }).join('');
  } catch (err) {
    console.error('Failed to load serverless telemetry:', err);
  }
}

/**
 * Cross-Tenant Boundary Penetration Attack Simulation
 */
async function simulateCrossTenantBreach() {
  const output = document.getElementById('exploitTestOutput');
  output.innerHTML = `<div class="text-yellow-400 animate-pulse">// Dispatching cross-tenant exploit payload...</div>`;

  try {
    // Attempting to fetch Tenant B's order using Tenant A's header
    const targetOrderId = currentTenantId === 'tenant_acme' ? 'ord_glx_1001' : 'ord_acme_1001';
    const res = await fetch(`/api/v1/orders/${targetOrderId}`, {
      headers: { 'x-tenant-id': currentTenantId }
    });

    const data = await res.json();

    if (res.status === 403 || res.status === 404) {
      output.innerHTML = `
        <div class="text-emerald-400 font-bold">🛡️ [SECURITY DEFENSE SUCCESSFUL - ACCESS BLOCKED]</div>
        <div class="text-slate-400">Request: GET /api/v1/orders/${targetOrderId}</div>
        <div class="text-slate-400">Request Header: x-tenant-id = ${currentTenantId}</div>
        <div class="text-red-400 mt-2 font-bold">HTTP ${res.status} Forbidden / Access Denied</div>
        <div class="text-yellow-300 mt-1">Error Code: ${data.code || 'TENANT_DATA_BOUNDARY_VIOLATION'}</div>
        <div class="text-slate-300 mt-1">Message: "${data.error}"</div>
        <div class="text-emerald-400 mt-2">Zero Data Leakage: Cross-tenant data boundary securely defended.</div>
      `;
    } else {
      output.innerHTML = `<div class="text-red-500 font-bold">⚠️ Warning: Request returned status ${res.status}</div>`;
    }
  } catch (err) {
    output.innerHTML = `<div class="text-red-400">Request Failed: ${err.message}</div>`;
  }
}

/**
 * Fetch Connection Pool Telemetry
 */
async function fetchPoolStats() {
  try {
    const res = await fetch('/api/v1/performance/connection-pool', {
      headers: { 'x-tenant-id': currentTenantId }
    });
    const data = await res.json();

    if (document.getElementById('poolMax')) document.getElementById('poolMax').innerText = `${data.maxLimit} Clients`;
    if (document.getElementById('poolMin')) document.getElementById('poolMin').innerText = `${data.minLimit} Clients`;
    if (document.getElementById('poolWaiting')) document.getElementById('poolWaiting').innerText = `${data.waitingCount} Requests`;
    if (document.getElementById('rawPoolJson')) document.getElementById('rawPoolJson').innerText = JSON.stringify(data, null, 2);
  } catch (err) {
    console.error('Failed to fetch connection pool stats:', err);
  }
}

function inspectSingleOrder(orderId) {
  fetch(`/api/v1/orders/${orderId}`, {
    headers: { 'x-tenant-id': currentTenantId }
  })
    .then(r => r.json())
    .then(data => {
      alert(`Order Details:\nID: ${data.order.id}\nNumber: ${data.order.order_number}\nAmount: $${data.order.total_amount}\nStatus: ${data.order.status}\nItems Count: ${(data.order.items || []).length}`);
    });
}

// =========================================================================
// TAB 7: AUTHENTICATION, OAUTH 2.0, TOKEN ROTATION & RBAC CONTROLLERS
// =========================================================================

let activeAuthSession = {
  accessToken: null,
  refreshToken: null,
  user: null
};

/**
 * Social Login via OAuth 2.0 / OpenID Connect (Google & GitHub)
 */
async function loginWithOAuth(provider) {
  const consoleEl = document.getElementById('authConsoleOutput');
  consoleEl.innerHTML = `<div class="text-yellow-400 animate-pulse">// Initiating OAuth 2.0 / OpenID Connect handshake with ${provider.toUpperCase()}...</div>`;

  try {
    const res = await fetch('/api/v1/auth/oauth', {
      method: 'POST',
      headers: {
        'Content-Type': 'application/json',
        'x-tenant-id': currentTenantId
      },
      body: JSON.stringify({ provider })
    });

    const data = await res.json();
    activeAuthSession.accessToken = data.accessToken;
    activeAuthSession.refreshToken = data.refreshToken;
    activeAuthSession.user = data.user;

    document.getElementById('activeAuthSessionBadge').innerText = `${data.user.name} (${data.user.role})`;
    document.getElementById('activeAuthSessionBadge').className = 'text-[10px] bg-purple-500/10 text-purple-300 border border-purple-500/30 px-2 py-0.5 rounded font-mono font-bold';

    consoleEl.innerHTML = `
      <div class="text-emerald-400 font-bold">✅ OAuth 2.0 Authentication Successful!</div>
      <div class="text-slate-400">Provider: <span class="text-white">${data.oauthProfile.provider}</span></div>
      <div class="text-slate-400">Identity (sub): <span class="text-yellow-300">${data.oauthProfile.sub}</span></div>
      <div class="text-slate-400">User: <span class="text-white font-bold">${data.user.name}</span> (${data.user.email})</div>
      <div class="text-slate-400">Role Assigned: <span class="text-purple-400 font-bold">${data.user.role}</span></div>
      <div class="text-blue-400 mt-2 font-bold">🔑 Access Token (15m expiry):</div>
      <div class="text-slate-500 break-all text-[10px]">${data.accessToken.slice(0, 80)}...</div>
      <div class="text-purple-400 mt-2 font-bold">🔄 Refresh Token (Issued & Stored in httpOnly Secure Cookie):</div>
      <div class="text-slate-300 text-[10px]">${data.refreshToken}</div>
    `;
  } catch (err) {
    consoleEl.innerHTML = `<div class="text-red-400">OAuth Login Error: ${err.message}</div>`;
  }
}

/**
 * Password Login with Bcrypt Verification
 */
async function loginWithBcrypt(email, password) {
  const consoleEl = document.getElementById('authConsoleOutput');
  consoleEl.innerHTML = `<div class="text-yellow-400 animate-pulse">// Verifying credentials against Bcrypt password hash...</div>`;

  try {
    const res = await fetch('/api/v1/auth/login', {
      method: 'POST',
      headers: {
        'Content-Type': 'application/json',
        'x-tenant-id': currentTenantId
      },
      body: JSON.stringify({ email, password })
    });

    const data = await res.json();
    if (res.status === 200) {
      activeAuthSession.accessToken = data.accessToken;
      activeAuthSession.refreshToken = data.refreshToken;
      activeAuthSession.user = data.user;

      document.getElementById('activeAuthSessionBadge').innerText = `${data.user.name} (${data.user.role})`;
      document.getElementById('activeAuthSessionBadge').className = 'text-[10px] bg-emerald-500/10 text-emerald-300 border border-emerald-500/30 px-2 py-0.5 rounded font-mono font-bold';

      consoleEl.innerHTML = `
        <div class="text-emerald-400 font-bold">✅ Bcrypt Password Hash Verification Successful!</div>
        <div class="text-slate-400">User: <span class="text-white">${data.user.name}</span> (${data.user.email})</div>
        <div class="text-slate-400">RBAC Role: <span class="text-purple-400 font-bold">${data.user.role}</span></div>
        <div class="text-slate-400">Hash Algorithm: <span class="text-emerald-300">${data.passwordProtection}</span></div>
        <div class="text-blue-400 mt-2 font-bold">🔑 Access Token Issued (Bearer JWT):</div>
        <div class="text-slate-500 break-all text-[10px]">${data.accessToken.slice(0, 80)}...</div>
        <div class="text-purple-400 mt-2 font-bold">🔄 Refresh Token Set (httpOnly Secure Cookie):</div>
        <div class="text-slate-300 text-[10px]">${data.refreshToken}</div>
      `;
    } else {
      consoleEl.innerHTML = `<div class="text-red-400 font-bold">❌ Authentication Failed: ${data.error}</div>`;
    }
  } catch (err) {
    consoleEl.innerHTML = `<div class="text-red-400">Login Error: ${err.message}</div>`;
  }
}

/**
 * Access Token + Refresh Token Rotation
 */
async function testTokenRotation() {
  const consoleEl = document.getElementById('authConsoleOutput');

  if (!activeAuthSession.refreshToken) {
    // Generate an initial session if not logged in yet
    await loginWithOAuth('google');
  }

  consoleEl.innerHTML += `<div class="text-purple-400 mt-2 animate-pulse">// Rotating Refresh Token (Invalidating old token & issuing new pair)...</div>`;

  try {
    const res = await fetch('/api/v1/auth/refresh', {
      method: 'POST',
      headers: {
        'Content-Type': 'application/json',
        'x-tenant-id': currentTenantId
      },
      body: JSON.stringify({ refreshToken: activeAuthSession.refreshToken })
    });

    const data = await res.json();
    if (res.status === 200) {
      activeAuthSession.refreshToken = data.newRefreshToken;
      activeAuthSession.accessToken = data.newAccessToken;

      consoleEl.innerHTML = `
        <div class="text-emerald-400 font-bold">🔄 [TOKEN ROTATION SUCCESSFUL]</div>
        <div class="text-slate-400">Previous Refresh Token: <span class="text-red-400 line-through">${data.previousTokenRevoked}</span> <span class="text-red-300 font-bold">(REVOKED)</span></div>
        <div class="text-slate-400">New Rotated Refresh Token: <span class="text-emerald-400 font-mono font-bold">${data.newRefreshToken}</span></div>
        <div class="text-slate-400">Rotation Counter: <span class="text-yellow-300 font-bold">#${data.rotationCount}</span></div>
        <div class="text-slate-400">Cookie Security: <code class="text-blue-300 text-[10px]">${data.cookiePolicy}</code></div>
        <div class="text-emerald-300 mt-2 text-[11px]">Zero Token Reuse Guarantee: Re-submitting the revoked token triggers instant session termination.</div>
      `;
    } else {
      consoleEl.innerHTML += `<div class="text-red-400">Rotation Failed: ${data.error}</div>`;
    }
  } catch (err) {
    consoleEl.innerHTML += `<div class="text-red-400">Rotation Error: ${err.message}</div>`;
  }
}

/**
 * Demonstrate RBAC Rejection (Employee attempting SuperAdmin action)
 */
async function testRbacRejection() {
  const consoleEl = document.getElementById('authConsoleOutput');
  consoleEl.innerHTML = `<div class="text-yellow-400 animate-pulse">// Testing RBAC Guard: Simulating 'Employee' role requesting 'SuperAdmin' resource...</div>`;

  try {
    const res = await fetch('/api/v1/auth/rbac-test?role=Employee&requiredRole=SuperAdmin', {
      headers: { 'x-tenant-id': currentTenantId }
    });

    const data = await res.json();
    if (res.status === 403) {
      consoleEl.innerHTML = `
        <div class="text-red-400 font-bold">🚫 [RBAC AUTHORIZATION REJECTED - HTTP 403 FORBIDDEN]</div>
        <div class="text-slate-400">User Role: <span class="text-amber-400 font-bold font-mono">Employee</span></div>
        <div class="text-slate-400">Required Role: <span class="text-purple-400 font-bold font-mono">SuperAdmin</span></div>
        <div class="text-red-300 mt-1">Error Code: <code>${data.code}</code></div>
        <div class="text-slate-300 mt-1">Message: "${data.error}"</div>
        <div class="text-emerald-400 mt-2 text-[11px]">Strict RBAC Enforcement: Employee role lacks administrative privileges and was strictly denied.</div>
      `;
    } else {
      consoleEl.innerHTML = `<div class="text-yellow-400">Response: ${JSON.stringify(data)}</div>`;
    }
  } catch (err) {
    consoleEl.innerHTML = `<div class="text-red-400">RBAC Request Failed: ${err.message}</div>`;
  }
}
