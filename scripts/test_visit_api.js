import axios from "axios";
import { config } from "../src/config/index.js";
import { logger } from "../src/config/logger.js";

const basicAuth = Buffer.from(
  `${config.alayacare.publicKey}:${config.alayacare.privateKey}`
).toString("base64");

// Test different axios configurations
async function testVisitAPI() {
  const testEmployeeId = 5224; // Example from PRD
  const startAt = "2026-01-20T00:00:00";
  const endAt = "2026-01-31T00:00:00";

  const params = {
    alayacare_employee_id: testEmployeeId,
    start_at: startAt,
    end_at: endAt,
    status: "scheduled",
    cancelled: false,
  };

  console.log("═══════════════════════════════════════════════════════════");
  console.log("🧪 Testing AlayaCare Visit API");
  console.log("═══════════════════════════════════════════════════════════");
  console.log(`Base URL: ${config.alayacare.baseUrl}`);
  console.log(`Endpoint: /ext/api/v2/scheduler/visit`);
  console.log(`Params:`, params);
  console.log("");

  // Test 1: Minimal headers (only Authorization)
  console.log("Test 1: Minimal headers (only Authorization)");
  try {
    const client1 = axios.create({
      baseURL: config.alayacare.baseUrl,
      headers: {
        Authorization: `Basic ${basicAuth}`,
      },
    });

    // Remove Content-Type and Accept if they exist
    client1.interceptors.request.use((config) => {
      if (config.headers) {
        delete config.headers['Content-Type'];
        delete config.headers['content-type'];
        delete config.headers['Accept'];
        delete config.headers['accept'];
      }
      return config;
    });

    const res1 = await client1.get("/ext/api/v2/scheduler/visit", { params });
    console.log("✅ SUCCESS!");
    console.log(`Status: ${res1.status}`);
    console.log(`Items count: ${res1.data?.items?.length || 0}`);
    console.log(`Total pages: ${res1.data?.total_pages || 0}`);
    if (res1.data?.items?.length > 0) {
      console.log("Sample item:", JSON.stringify(res1.data.items[0], null, 2));
    }
  } catch (error) {
    console.log("❌ FAILED!");
    console.log(`Status: ${error.response?.status || 'N/A'}`);
    console.log(`Message: ${error.message}`);
    if (error.response?.data) {
      console.log(`Response data:`, JSON.stringify(error.response.data, null, 2));
    }
    if (error.config?.headers) {
      console.log(`Request headers sent:`, error.config.headers);
    }
  }
  console.log("");

  // Test 2: No headers at all (just Authorization in request)
  console.log("Test 2: Explicitly set only Authorization in request config");
  try {
    const client2 = axios.create({
      baseURL: config.alayacare.baseUrl,
    });

    const res2 = await client2.get("/ext/api/v2/scheduler/visit", {
      params,
      headers: {
        Authorization: `Basic ${basicAuth}`,
      },
    });
    console.log("✅ SUCCESS!");
    console.log(`Status: ${res2.status}`);
    console.log(`Items count: ${res2.data?.items?.length || 0}`);
  } catch (error) {
    console.log("❌ FAILED!");
    console.log(`Status: ${error.response?.status || 'N/A'}`);
    console.log(`Message: ${error.message}`);
    if (error.response?.data) {
      console.log(`Response data:`, JSON.stringify(error.response.data, null, 2));
    }
    if (error.config?.headers) {
      console.log(`Request headers sent:`, error.config.headers);
    }
  }
  console.log("");

  // Test 3: Using fetch (native Node.js fetch if available, or node-fetch)
  console.log("Test 3: Using native fetch (if available)");
  try {
    const url = new URL("/ext/api/v2/scheduler/visit", config.alayacare.baseUrl);
    Object.keys(params).forEach(key => {
      if (params[key] !== null && params[key] !== undefined) {
        url.searchParams.append(key, params[key]);
      }
    });

    const fetchResponse = await fetch(url.toString(), {
      method: 'GET',
      headers: {
        'Authorization': `Basic ${basicAuth}`,
        // Explicitly do NOT set Content-Type or Accept
      },
    });

    if (fetchResponse.ok) {
      const data = await fetchResponse.json();
      console.log("✅ SUCCESS!");
      console.log(`Status: ${fetchResponse.status}`);
      console.log(`Items count: ${data?.items?.length || 0}`);
    } else {
      console.log("❌ FAILED!");
      console.log(`Status: ${fetchResponse.status}`);
      const text = await fetchResponse.text();
      console.log(`Response: ${text}`);
    }
  } catch (error) {
    console.log("❌ FAILED!");
    console.log(`Error: ${error.message}`);
    if (error.name === 'TypeError' && error.message.includes('fetch')) {
      console.log("Note: Native fetch not available. Install node-fetch or use Node.js 18+");
    }
  }
  console.log("");

  // Test 4: Check what headers axios is actually sending
  console.log("Test 4: Inspecting actual request headers");
  try {
    const client4 = axios.create({
      baseURL: config.alayacare.baseUrl,
      headers: {
        Authorization: `Basic ${basicAuth}`,
      },
    });

    // Add interceptor to log actual headers being sent
    client4.interceptors.request.use((config) => {
      console.log("Headers being sent:", JSON.stringify(config.headers, null, 2));
      // Remove Content-Type and Accept
      if (config.headers) {
        delete config.headers['Content-Type'];
        delete config.headers['content-type'];
        delete config.headers['Accept'];
        delete config.headers['accept'];
      }
      console.log("Headers after removal:", JSON.stringify(config.headers, null, 2));
      return config;
    });

    const res4 = await client4.get("/ext/api/v2/scheduler/visit", { params });
    console.log("✅ SUCCESS!");
    console.log(`Status: ${res4.status}`);
  } catch (error) {
    console.log("❌ FAILED!");
    console.log(`Status: ${error.response?.status || 'N/A'}`);
    if (error.config?.headers) {
      console.log("Final headers that were sent:", JSON.stringify(error.config.headers, null, 2));
    }
  }
}

// Run the test
testVisitAPI()
  .then(() => {
    console.log("\n✅ Test completed");
    process.exit(0);
  })
  .catch((error) => {
    console.error("\n❌ Test script error:", error);
    process.exit(1);
  });

