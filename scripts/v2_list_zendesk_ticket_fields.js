import { config } from "../src/config/index.js";
import { logger } from "../src/config/logger.js";
import { getZendeskClient, callZendesk } from "../src/services/zendesk/zendesk.api.js";
import { zendeskLimiter } from "../src/utils/rateLimiters/zendesk.js";

/**
 * Fetch all ticket fields from Zendesk and display them
 */
async function listTicketFields() {
  try {
    logger.info("🔍 Fetching all ticket fields from Zendesk...");

    const fields = await callZendesk(async () => {
      const res = await zendeskLimiter.schedule(() =>
        getZendeskClient().get("/ticket_fields.json")
      );
      return res.data?.ticket_fields || [];
    });

    logger.info(`✅ Found ${fields.length} ticket fields\n`);

    // Group fields by type
    const fieldsByType = {};
    fields.forEach((field) => {
      const type = field.type || "unknown";
      if (!fieldsByType[type]) {
        fieldsByType[type] = [];
      }
      fieldsByType[type].push(field);
    });

    // Display all fields
    console.log("═══════════════════════════════════════════════════════════");
    console.log("📋 ALL TICKET FIELDS");
    console.log("═══════════════════════════════════════════════════════════\n");

    // Sort by type, then by title
    const sortedTypes = Object.keys(fieldsByType).sort();
    
    for (const type of sortedTypes) {
      console.log(`\n📌 Type: ${type.toUpperCase()}`);
      console.log("─".repeat(60));

      const typeFields = fieldsByType[type].sort((a, b) =>
        (a.title || "").localeCompare(b.title || "")
      );

      for (const field of typeFields) {
        console.log(`\n  ID: ${field.id}`);
        console.log(`  Title: ${field.title || "N/A"}`);
        console.log(`  Key: ${field.key || "N/A"}`);
        console.log(`  Type: ${field.type || "N/A"}`);
        console.log(`  Active: ${field.active ? "Yes" : "No"}`);
        console.log(`  Required: ${field.required ? "Yes" : "No"}`);
        console.log(`  System: ${field.system ? "Yes" : "No"}`);

        // For dropdown/tagger fields, show options
        if (field.custom_field_options && field.custom_field_options.length > 0) {
          console.log(`  Options (${field.custom_field_options.length}):`);
          field.custom_field_options.forEach((option) => {
            console.log(`    - ${option.value} (ID: ${option.id})`);
          });
        }

        // Check if this is the Coordination Contact Category field
        const titleLower = (field.title || "").toLowerCase();
        const keyLower = (field.key || "").toLowerCase();
        if (
          titleLower.includes("coordination") &&
          (titleLower.includes("contact") || titleLower.includes("category"))
        ) {
          console.log(`  ⭐ THIS MIGHT BE THE FIELD YOU'RE LOOKING FOR! ⭐`);
        }
      }
    }

    // Search specifically for Coordination Contact Category
    console.log("\n\n═══════════════════════════════════════════════════════════");
    console.log("🔍 SEARCHING FOR 'COORDINATION CONTACT CATEGORY'");
    console.log("═══════════════════════════════════════════════════════════\n");

    const matchingFields = fields.filter((field) => {
      const title = (field.title || "").toLowerCase();
      const key = (field.key || "").toLowerCase();
      return (
        (title.includes("coordination") &&
          (title.includes("contact") || title.includes("category"))) ||
        key.includes("coordination") ||
        key.includes("contact_category")
      );
    });

    if (matchingFields.length === 0) {
      console.log("❌ No fields found matching 'Coordination Contact Category'");
      console.log("\n💡 Try searching for fields containing:");
      console.log("   - 'coordination'");
      console.log("   - 'contact'");
      console.log("   - 'category'");
      console.log("   - 'check in'");
    } else {
      console.log(`✅ Found ${matchingFields.length} matching field(s):\n`);
      matchingFields.forEach((field) => {
        console.log(`\n📋 Field: ${field.title}`);
        console.log(`   ID: ${field.id}`);
        console.log(`   Key: ${field.key || "N/A"}`);
        console.log(`   Type: ${field.type}`);
        console.log(`   Active: ${field.active ? "Yes" : "No"}`);

        if (field.custom_field_options && field.custom_field_options.length > 0) {
          console.log(`\n   Available Options:`);
          field.custom_field_options.forEach((option) => {
            console.log(`     • "${option.value}" (ID: ${option.id})`);
          });
          console.log(
            `\n   💡 Use this field ID in your .env: ZENDESK_CONTACT_CATEGORY_FIELD_ID=${field.id}`
          );
        } else {
          console.log(`   ⚠️  No options found for this field`);
        }
      });
    }

    // Also search for fields with "check in" related terms
    console.log("\n\n═══════════════════════════════════════════════════════════");
    console.log("🔍 SEARCHING FOR FIELDS WITH 'CHECK IN' RELATED TERMS");
    console.log("═══════════════════════════════════════════════════════════\n");

    const checkInFields = fields.filter((field) => {
      const title = (field.title || "").toLowerCase();
      const key = (field.key || "").toLowerCase();
      return (
        title.includes("check") ||
        title.includes("contact") ||
        key.includes("check") ||
        key.includes("contact")
      );
    });

    if (checkInFields.length > 0) {
      console.log(`Found ${checkInFields.length} field(s) with check-in related terms:\n`);
      checkInFields.forEach((field) => {
        console.log(`  • ${field.title} (ID: ${field.id}, Type: ${field.type})`);
        if (field.custom_field_options && field.custom_field_options.length > 0) {
          console.log(`    Options: ${field.custom_field_options.map((o) => o.value).join(", ")}`);
        }
      });
    }

    console.log("\n\n═══════════════════════════════════════════════════════════");
    console.log("📝 SUMMARY");
    console.log("═══════════════════════════════════════════════════════════");
    console.log(`Total fields: ${fields.length}`);
    console.log(`Active fields: ${fields.filter((f) => f.active).length}`);
    console.log(`Custom fields: ${fields.filter((f) => !f.system).length}`);
    console.log(`Fields with options: ${fields.filter((f) => f.custom_field_options && f.custom_field_options.length > 0).length}`);

  } catch (error) {
    logger.error("❌ Failed to fetch ticket fields:", error.response?.data || error.message);
    throw error;
  }
}

// Run the script
listTicketFields()
  .then(() => {
    logger.info("✅ Script completed successfully");
    process.exit(0);
  })
  .catch((error) => {
    logger.error("❌ Script failed:", error);
    process.exit(1);
  });
