/**
 * Application Configuration
 * CSC337: Advanced Web Technologies - Lab Assignment 06
 * Topic: Database Performance Tuning, Serverless, Event-Driven Architecture with Kafka & Multi-Tenant SaaS
 */

try {
  require('dotenv').config();
} catch (e) {
  // dotenv optional when environment variables are supplied via host/CLI
}

module.exports = {
  port: process.env.PORT || 5000,
  nodeEnv: process.env.NODE_ENV || 'development',
  
  // Multi-Tenancy Configuration
  tenants: {
    'tenant_acme': {
      id: 'tenant_acme',
      name: 'Acme Corporation',
      subdomain: 'acme',
      tier: 'Enterprise',
      color: '#3b82f6',
      badge: 'Acme Corp'
    },
    'tenant_globex': {
      id: 'tenant_globex',
      name: 'Globex Logistics',
      subdomain: 'globex',
      tier: 'Professional',
      color: '#10b981',
      badge: 'Globex Logistics'
    }
  },

  // PostgreSQL Connection Pool Configuration
  db: {
    connectionString: process.env.DATABASE_URL || '',
    host: process.env.PGHOST || 'localhost',
    port: parseInt(process.env.PGPORT || '5432', 10),
    user: process.env.PGUSER || 'postgres',
    password: process.env.PGPASSWORD || 'postgres',
    database: process.env.PGDATABASE || 'saas_db',
    // Connection pool limits (Lecture 17 requirement)
    max: parseInt(process.env.PG_POOL_MAX || '10', 10), // Maximum pool size
    min: parseInt(process.env.PG_POOL_MIN || '2', 10),  // Minimum idle connections
    idleTimeoutMillis: 30000, // Close idle clients after 30 seconds
    connectionTimeoutMillis: 3000 // Fast fail after 3s if pool exhausted
  },

  // Apache Kafka Configuration (Upstash / Aiven / Local)
  kafka: {
    brokers: (process.env.KAFKA_BROKERS || 'localhost:9092').split(','),
    clientId: process.env.KAFKA_CLIENT_ID || 'saas-order-service',
    groupId: process.env.KAFKA_GROUP_ID || 'orders-consumer-group',
    topic: process.env.KAFKA_TOPIC || 'tenant.orders',
    // Upstash Kafka REST credentials (Optional Serverless Kafka)
    upstashRestUrl: process.env.UPSTASH_KAFKA_REST_URL || '',
    upstashRestToken: process.env.UPSTASH_KAFKA_REST_TOKEN || '',
    // SASL Authentication (Required for Cloud Kafka such as Upstash / Aiven / Confluent)
    username: process.env.KAFKA_USERNAME || '',
    password: process.env.KAFKA_PASSWORD || '',
    ssl: process.env.KAFKA_SSL === 'true' || Boolean(process.env.KAFKA_USERNAME)
  },

  // Serverless Cold-Start Simulation Settings
  serverless: {
    simulatedColdStartDelayMs: parseInt(process.env.COLD_START_DELAY_MS || '580', 10),
    simulatedWarmStartDelayMs: parseInt(process.env.WARM_START_DELAY_MS || '35', 10)
  }
};
