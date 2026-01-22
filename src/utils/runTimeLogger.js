import fs from "fs";
import path from "path";
import { fileURLToPath } from "url";

const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);

/**
 * Logs run times to a daily log file
 * Creates one log file per day, appends to it throughout the day
 * 
 * @param {Date} startTime - Job start time
 * @param {Date} endTime - Job end time
 * @param {string} status - 'success' or 'error'
 * @param {Error|null} error - Error object if status is 'error'
 */
export function logRunTime(startTime, endTime, status = "success", error = null) {
  try {
    // Create logs directory if it doesn't exist
    const logsDir = path.join(__dirname, "../../logs");
    if (!fs.existsSync(logsDir)) {
      fs.mkdirSync(logsDir, { recursive: true });
    }

    // Create log file name with date format: run_times_MM-DD-YYYY.log
    const date = new Date();
    const dateStr = `${String(date.getMonth() + 1).padStart(2, "0")}-${String(date.getDate()).padStart(2, "0")}-${date.getFullYear()}`;
    const logFileName = `run_times_${dateStr}.log`;
    const logFilePath = path.join(logsDir, logFileName);

    // Calculate duration
    const durationMs = endTime - startTime;
    const durationSeconds = (durationMs / 1000).toFixed(2);
    const durationMinutes = Math.floor(durationMs / 60000);
    const durationSecs = ((durationMs % 60000) / 1000).toFixed(2);
    const durationStr = durationMinutes > 0 
      ? `${durationMinutes}m ${durationSecs}s`
      : `${durationSeconds}s`;

    // Format times
    const startTimeStr = startTime.toISOString();
    const endTimeStr = endTime.toISOString();

    // Create log entry
    let logEntry = `[${startTimeStr}] START\n`;
    logEntry += `[${endTimeStr}] END - Duration: ${durationStr} - Status: ${status.toUpperCase()}\n`;
    
    if (error) {
      logEntry += `  Error: ${error.message}\n`;
    }
    
    logEntry += `━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n`;

    // Append to log file
    fs.appendFileSync(logFilePath, logEntry, "utf8");
  } catch (err) {
    // Fallback to console if file writing fails
    console.error("Failed to write run time log:", err);
  }
}

