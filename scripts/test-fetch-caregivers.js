import { fetchCaregiverDetail } from "../src/modules/alayacare/service.js";
import { mapCaregiverToZendesk } from "../src/modules/alayacare/mapper.js";
import { logger } from "../src/config/logger.js";

async function testFetchEmployee() {
  const employeeId = Number(process.argv[2]) || 108;

  try {
    logger.info(`🧪 Fetching employee detail for ID: ${employeeId}\n`);

    const employee = await fetchCaregiverDetail(employeeId);

    if (!employee) {
      logger.error(`❌ Employee ${employeeId} not found`);
      process.exit(1);
    }

    logger.info("✅ Fetched Employee Data:");
    logger.info(JSON.stringify(employee, null, 2));

    const mapped = mapCaregiverToZendesk(employee);

    if (!mapped) {
      logger.error("❌ mapCaregiverToZendesk returned null");
      process.exit(1);
    }

    logger.info("\n🗺️  Mapped for Zendesk:");
    logger.info(JSON.stringify(mapped, null, 2));

    logger.info("\n✅ Test complete!");
  } catch (error) {
    logger.error("❌ Test failed:", error.message);
    if (error.response) {
      logger.error("API Response Status:", error.response.status);
      logger.error("API Response Data:", JSON.stringify(error.response.data, null, 2));
    }
    process.exit(1);
  }
}

testFetchEmployee();

