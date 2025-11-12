const isProd = process.env.NODE_ENV === "production";

export const logger = {
  info: (...args) => {
    // keep it simple for now
    console.log("[INFO]", ...args);
  },
  warn: (...args) => {
    console.warn("[WARN]", ...args);
  },
  error: (...args) => {
    console.error("[ERROR]", ...args);
  },
  debug: (...args) => {
    if (!isProd) {
      console.log("[DEBUG]", ...args);
    }
  },
};
