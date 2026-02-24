import fs from "fs";
import path from "path";
import { fileURLToPath } from "url";

const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);

const LOG_FILE_NAME = "clients_no_demographics_email.log";

function getLogFilePath() {
  const logsDir = path.join(__dirname, "../../logs");
  if (!fs.existsSync(logsDir)) {
    fs.mkdirSync(logsDir, { recursive: true });
  }
  return path.join(logsDir, LOG_FILE_NAME);
}

/**
 * Appends a client record to the "no demographics.email" log file.
 * Records ac_id and name (one line per client, tab-separated).
 * Writes a header line when the file is created.
 *
 * @param {string} acId - AlayaCare client id
 * @param {string} name - Client name (e.g. first + last from demographics)
 */
export function logClientNoDemographicsEmail(acId, name) {
  try {
    const logFilePath = getLogFilePath();
    const isNewFile = !fs.existsSync(logFilePath);
    const safeName = (name ?? "")
      .replace(/\t/g, " ")
      .replace(/\r?\n/g, " ")
      .trim() || "—";
    const line = `${acId}\t${safeName}\n`;
    if (isNewFile) {
      fs.writeFileSync(logFilePath, `ac_id\tname\n${line}`, "utf8");
    } else {
      fs.appendFileSync(logFilePath, line, "utf8");
    }
  } catch (err) {
    console.error("Failed to write clients_no_demographics_email log:", err);
  }
}
