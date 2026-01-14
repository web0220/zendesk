/**
 * Date and timezone utility functions
 */

/**
 * Get current date components in EST timezone
 * @returns {Object} Object with year, month (0-indexed), day
 */
export function getCurrentDateInEST() {
  const now = new Date();
  const formatter = new Intl.DateTimeFormat("en-US", {
    timeZone: "America/New_York",
    year: "numeric",
    month: "2-digit",
    day: "2-digit",
  });
  
  const parts = formatter.formatToParts(now);
  return {
    year: parseInt(parts.find((p) => p.type === "year").value),
    month: parseInt(parts.find((p) => p.type === "month").value) - 1, // 0-indexed
    day: parseInt(parts.find((p) => p.type === "day").value),
  };
}

/**
 * Get the UTC offset for a specific date in America/New_York timezone
 * Returns offset in minutes (EST = -300, EDT = -240)
 * @param {Date} date - Date to check
 * @returns {number} Offset in minutes
 */
export function getESTOffsetMinutes(date) {
  // Create two formatters: one for EST and one for UTC
  const estFormatter = new Intl.DateTimeFormat("en-US", {
    timeZone: "America/New_York",
    hour: "2-digit",
    hour12: false,
    timeZoneName: "longOffset",
  });
  
  const utcFormatter = new Intl.DateTimeFormat("en-US", {
    timeZone: "UTC",
    hour: "2-digit",
    hour12: false,
  });
  
  const estParts = estFormatter.formatToParts(date);
  const utcParts = utcFormatter.formatToParts(date);
  
  const estHour = parseInt(estParts.find((p) => p.type === "hour").value);
  const utcHour = parseInt(utcParts.find((p) => p.type === "hour").value);
  
  // Calculate offset (EST is UTC-5, EDT is UTC-4)
  let offset = (estHour - utcHour) * 60;
  
  // Handle day boundary crossing
  if (offset > 720) offset -= 1440;
  if (offset < -720) offset += 1440;
  
  return offset;
}

