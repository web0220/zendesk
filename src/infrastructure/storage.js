import fs from "fs";
import path from "path";
import { logger } from "../config/logger.js";
import AWS from "aws-sdk";

// Detect environment
const isProd = process.env.NODE_ENV === "production";
const LOG_DIR = path.resolve("logs");

/**
 * Ensure local log directory exists
 */
function ensureLocalDir() {
  if (!fs.existsSync(LOG_DIR)) fs.mkdirSync(LOG_DIR);
}

/**
 * Save run log locally
 */
async function saveLocal(data, fileName) {
  ensureLocalDir();
  const filePath = path.join(LOG_DIR, fileName);
  await fs.promises.writeFile(filePath, JSON.stringify(data, null, 2), "utf-8");
  logger.info(`🪵 Log saved locally: ${filePath}`);
  return filePath;
}

/**
 * Save run log to S3 (for production)
 */
async function saveToS3(data, fileName) {
  const bucket = process.env.AWS_LOG_BUCKET;
  const region = process.env.AWS_REGION || "us-east-1";

  if (!bucket) {
    logger.warn("⚠️ No AWS_LOG_BUCKET configured — falling back to local logs.");
    return saveLocal(data, fileName);
  }

  const s3 = new AWS.S3({ region });
  const params = {
    Bucket: bucket,
    Key: `logs/${fileName}`,
    Body: JSON.stringify(data, null, 2),
    ContentType: "application/json",
  };

  try {
    await s3.putObject(params).promise();
    logger.info(`☁️ Log uploaded to S3: s3://${bucket}/logs/${fileName}`);
    return `s3://${bucket}/logs/${fileName}`;
  } catch (err) {
    logger.error("S3 upload failed:", err.message);
    return saveLocal(data, fileName);
  }
}

/**
 * Save a run result (auto-switch between local/S3)
 */
export async function saveRunResult(data, status = "success") {
  const fileName = `run_${new Date().toISOString().replace(/[:.]/g, "-")}_${status}.json`;
  const log = { timestamp: new Date().toISOString(), status, ...data };

  if (isProd) {
    return saveToS3(log, fileName);
  } else {
    return saveLocal(log, fileName);
  }
}
