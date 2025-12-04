import Bottleneck from "bottleneck";
import { logger } from "../config/logger.js";

/**
 * Global Bottleneck rate limiter for Zendesk API calls
 * 
 * Configuration:
 * - maxConcurrent: 1 (only one request at a time)
 * - minTime: 200ms (5 requests per second max)
 * 
 * To adjust rate limiting:
 * - Increase minTime to slow down (e.g., 500ms = 2 req/sec)
 * - Decrease minTime to speed up (e.g., 100ms = 10 req/sec)
 * - Adjust maxConcurrent if Zendesk allows parallel requests
 * 
 * Environment variables can override defaults:
 * - ZENDESK_RATE_LIMIT_MIN_TIME: minimum time between requests in ms (default: 200)
 * - ZENDESK_RATE_LIMIT_MAX_CONCURRENT: max concurrent requests (default: 1)
 */
const minTime = Number(process.env.ZENDESK_RATE_LIMIT_MIN_TIME) || 200;
const maxConcurrent = Number(process.env.ZENDESK_RATE_LIMIT_MAX_CONCURRENT) || 1;

export const zendeskLimiter = new Bottleneck({
  maxConcurrent,
  minTime,
});
