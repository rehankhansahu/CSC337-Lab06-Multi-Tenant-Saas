/**
 * Vercel Serverless Function Entrypoint
 * Deploys the entire Multi-Tenant API and Dashboard directly to Vercel
 */

const app = require('../server');

module.exports = app;
