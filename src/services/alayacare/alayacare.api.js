import axios from "axios";
import { config } from "../../config/index.js";
import { logger } from "../../config/logger.js";
import { withRetry } from "../../utils/retry.js";

const basicAuth = Buffer.from(
  `${config.alayacare.publicKey}:${config.alayacare.privateKey}`
).toString("base64");

export const DETAIL_CONCURRENCY = Number(process.env.ALAYACARE_DETAIL_CONCURRENCY) || 10;
const DETAIL_RETRIES = Number(process.env.ALAYACARE_DETAIL_RETRIES) || 3;
const DETAIL_RETRY_DELAY = Number(process.env.ALAYACARE_DETAIL_RETRY_DELAY) || 1000;

export const alayaClient = axios.create({
  baseURL: config.alayacare.baseUrl,
  headers: {
    Authorization: `Basic ${basicAuth}`,
  },
});

export function requestWithRetry(fn) {
  return withRetry(fn, DETAIL_RETRIES, DETAIL_RETRY_DELAY);
}

export async function fetchClientDetail(id) {
  const { data } = await requestWithRetry(() => alayaClient.get(`/patients/clients/${id}`));
  return data;
}

export async function fetchCaregiverDetail(id) {
  try {
    const res = await requestWithRetry(() => alayaClient.get(`/employees/employees/${id}`));
    return res.data;
  } catch (err) {
    logger.warn(`Failed to fetch detail for caregiver ${id}: ${err.message}`);
    return null;
  }
}

logger.info("📡 AlayaCare API client initialized");

