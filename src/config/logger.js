import fs from "fs";
import path from "path";
import { fileURLToPath } from "url";

const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);
const isProd = process.env.NODE_ENV === "production";
const isDev = process.env.NODE_ENV === "development" || process.env.ENABLE_LOG_FILE === "true";

// Only create log files in dev mode
let logStream = null;
let logFilePath = null;

if (isDev) {
  // Create logs directory if it doesn't exist
  const logsDir = path.join(__dirname, "../../logs");
  if (!fs.existsSync(logsDir)) {
    fs.mkdirSync(logsDir, { recursive: true });
  }

  // Create log file with timestamp
  const timestamp = new Date().toISOString().replace(/[:.]/g, "-");
  const logFileName = `sync_${timestamp}.log`;
  logFilePath = path.join(logsDir, logFileName);

  // Create write stream for log file
  try {
    logStream = fs.createWriteStream(logFilePath, { flags: "a" });
    console.log(`📝 Logging to file: ${logFilePath}`);
  } catch (err) {
    console.error("Failed to create log file:", err);
  }
}

// Helper function to format log message
function formatLogMessage(level, args) {
  const timestamp = new Date().toISOString();
  const message = args
    .map((arg) => {
      if (typeof arg === "object") {
        try {
          return JSON.stringify(arg, null, 2);
        } catch {
          return String(arg);
        }
      }
      return String(arg);
    })
    .join(" ");
  return `[${timestamp}] [${level}] ${message}\n`;
}

// Helper function to write to both console and file
function writeLog(level, consoleMethod, args) {
  // Write to console
  consoleMethod(`[${level}]`, ...args);

  // Write to file if stream is available
  if (logStream) {
    try {
      const logMessage = formatLogMessage(level, args);
      logStream.write(logMessage);
    } catch (err) {
      console.error("Failed to write to log file:", err);
    }
  }
}

export const logger = {
  info: (...args) => {
    writeLog("INFO", console.log, args);
  },
  warn: (...args) => {
    writeLog("WARN", console.warn, args);
  },
  error: (...args) => {
    writeLog("ERROR", console.error, args);
  },
  debug: (...args) => {
    if (!isProd) {
      writeLog("DEBUG", console.log, args);
    }
  },
  // Method to close log file stream (call on exit)
  close: () => {
    if (logStream) {
      logStream.end();
      logStream = null;
    }
  },
  // Get log file path for reference
  getLogPath: () => logFilePath,
};
