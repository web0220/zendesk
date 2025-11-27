import { fetchClients, fetchCaregivers } from "../src/services/alayacare/fetch.js";
import { logger } from "../src/config/logger.js";
import { writeFileSync } from "fs";
import { join } from "path";

/**
 * Escape CSV field value (handle commas, quotes, newlines)
 */
function escapeCsvField(value) {
  if (value === null || value === undefined) return "";
  const str = String(value);
  // If contains comma, quote, or newline, wrap in quotes and escape quotes
  if (str.includes(",") || str.includes('"') || str.includes("\n")) {
    return `"${str.replace(/"/g, '""')}"`;
  }
  return str;
}

/**
 * Convert array of objects to CSV string
 */
function arrayToCsv(data, headers) {
  const rows = [headers.map(escapeCsvField).join(",")];
  
  for (const row of data) {
    const values = headers.map(header => row[header] ?? "");
    rows.push(values.map(escapeCsvField).join(","));
  }
  
  return rows.join("\n");
}

/**
 * Get phone number from user object (tries multiple fields)
 */
function getPhoneNumber(user) {
  return user.phone_main || user.phone || user.demographics?.phone_main || user.demographics?.phone || "";
}

/**
 * Get full name from user object
 */
function getName(user) {
  const firstName = user.first_name || user.demographics?.first_name || "";
  const lastName = user.last_name || user.demographics?.last_name || "";
  return `${firstName} ${lastName}`.trim() || "";
}

/**
 * Check if user has an email address
 */
function hasEmail(user) {
  const email = user.email || user.demographics?.email || "";
  return email && email.trim().length > 0;
}

/**
 * Main function to list users without email addresses
 */
async function listUsersWithoutEmail() {
  try {
    logger.info("🔍 Starting to fetch users from AlayaCare...");
    logger.info("=".repeat(70));

    // Fetch all active clients and caregivers
    logger.info("\n📥 Fetching active clients...");
    const clients = await fetchClients({
      fetchAll: true,
      includeDetails: true,
      status: "active",
    });
    logger.info(`✅ Fetched ${clients.length} active clients`);

    logger.info("\n📥 Fetching active caregivers...");
    const caregivers = await fetchCaregivers({
      fetchAll: true,
      includeDetails: true,
      status: "active",
    });
    logger.info(`✅ Fetched ${caregivers.length} active caregivers`);

    // Combine all users
    const allUsers = [
      ...clients.map(c => ({ ...c, type: "client" })),
      ...caregivers.map(cg => ({ ...cg, type: "caregiver" }))
    ];

    logger.info(`\n📊 Total active users: ${allUsers.length}`);

    // Filter users without email addresses
    const usersWithoutEmail = allUsers.filter(user => !hasEmail(user));

    logger.info(`\n📋 Active users without email addresses: ${usersWithoutEmail.length}`);

    if (usersWithoutEmail.length === 0) {
      logger.info("✅ All users have email addresses!");
      return;
    }

    // Prepare data for CSV
    const csvData = usersWithoutEmail.map(user => ({
      name: getName(user),
      phone_number: getPhoneNumber(user),
      ac_id: user.id || "",
      id: user.id || "",
    }));

    // Generate CSV
    const headers = ["name", "phone_number", "ac_id", "id"];
    const csvContent = arrayToCsv(csvData, headers);

    // Save to file
    const timestamp = new Date().toISOString().replace(/[:.]/g, "-").slice(0, -5);
    const filename = `users-without-email_${timestamp}.csv`;
    const filepath = join(process.cwd(), "data", filename);

    writeFileSync(filepath, csvContent, "utf8");

    logger.info(`\n✅ CSV file saved: ${filepath}`);
    logger.info(`   Total records: ${csvData.length}`);

    // Show sample of first 10 records
    logger.info("\n📄 Sample records (first 10):");
    csvData.slice(0, 10).forEach((record, i) => {
      logger.info(`   ${i + 1}. ${record.name} | Phone: ${record.phone_number || "N/A"} | AC ID: ${record.ac_id}`);
    });
    if (csvData.length > 10) {
      logger.info(`   ... and ${csvData.length - 10} more records`);
    }

    logger.info("\n" + "=".repeat(70));
    logger.info("✅ Script completed successfully!");
    logger.info("=".repeat(70));

  } catch (error) {
    logger.error("\n❌ Error:", error.message);
    if (error.response) {
      logger.error("API Response Status:", error.response.status);
      logger.error("API Response Data:", JSON.stringify(error.response.data, null, 2));
    }
    logger.error("Stack trace:", error.stack);
    process.exit(1);
  }
}

listUsersWithoutEmail();

