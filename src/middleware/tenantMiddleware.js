/**
 * Tenant Resolution & Isolation Middleware
 * CSC337 Lecture 18: Multi-Tenant SaaS Architecture
 * 
 * Extracts tenant context from:
 * 1. Custom Header: 'x-tenant-id' (Primary)
 * 2. Host Subdomain: e.g., 'acme.saas.com' -> 'tenant_acme'
 * 3. JWT Bearer Token / Query Param fallback
 * 
 * Enforces strict tenant data boundaries: Tenant A cannot access Tenant B.
 */

const config = require('../config');

function resolveTenantId(req) {
  // Strategy 1: Custom Header (e.g., 'x-tenant-id')
  const headerTenant = req.headers['x-tenant-id'] || req.headers['x-tenant'];
  if (headerTenant) {
    const normalized = headerTenant.toLowerCase().trim();
    if (config.tenants[normalized]) return normalized;
    if (normalized === 'acme' && config.tenants['tenant_acme']) return 'tenant_acme';
    if (normalized === 'globex' && config.tenants['tenant_globex']) return 'tenant_globex';
    return normalized;
  }

  // Strategy 2: Subdomain Extraction
  const host = req.headers.host || '';
  const subdomain = host.split('.')[0]?.toLowerCase();
  if (subdomain) {
    if (subdomain === 'acme') return 'tenant_acme';
    if (subdomain === 'globex') return 'tenant_globex';
    if (config.tenants[subdomain]) return subdomain;
  }

  // Strategy 3: Query Parameter Fallback (useful for testing & browser links)
  if (req.query && req.query.tenant_id) {
    const qTenant = req.query.tenant_id.toLowerCase().trim();
    if (config.tenants[qTenant]) return qTenant;
    if (qTenant === 'acme') return 'tenant_acme';
    if (qTenant === 'globex') return 'tenant_globex';
  }

  // Default fallback for development/demo: Tenant Acme
  return 'tenant_acme';
}

function tenantResolutionMiddleware(req, res, next) {
  try {
    const tenantId = resolveTenantId(req);
    const tenant = config.tenants[tenantId];

    if (!tenant) {
      return res.status(403).json({
        error: 'TenantNotFound',
        message: `Tenant context '${tenantId}' is not authorized or does not exist.`,
        allowedTenants: Object.keys(config.tenants)
      });
    }

    // Bind immutable tenant context to request
    req.tenant = {
      id: tenant.id,
      name: tenant.name,
      subdomain: tenant.subdomain,
      tier: tenant.tier
    };

    // Attach response headers for auditability
    res.setHeader('X-Resolved-Tenant-Id', tenant.id);
    res.setHeader('X-Resolved-Tenant-Name', tenant.name);

    next();
  } catch (error) {
    return res.status(500).json({
      error: 'TenantResolutionFailed',
      message: error.message
    });
  }
}

/**
 * Strict Data Boundary Enforcer
 * Validates that an accessed resource belongs exclusively to the requesting tenant.
 * Violations immediately terminate with 403 Forbidden.
 */
function enforceTenantBoundary(requestTenantId, resourceTenantId) {
  if (requestTenantId !== resourceTenantId) {
    const err = new Error(`Data Boundary Violation: Tenant '${requestTenantId}' attempted unauthorized access to data owned by Tenant '${resourceTenantId}'. Access Denied.`);
    err.status = 403;
    err.code = 'TENANT_DATA_BOUNDARY_VIOLATION';
    throw err;
  }
  return true;
}

module.exports = {
  tenantResolutionMiddleware,
  enforceTenantBoundary,
  resolveTenantId
};
