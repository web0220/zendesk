import dotenv from "dotenv";
import { fileURLToPath } from "url";
import path from "path";

const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);
const envPath = path.resolve(__dirname, "../../.env");

dotenv.config({ path: envPath });

export const config = {
  env: process.env.NODE_ENV || "development",
  alayacare: {
    baseUrl: process.env.ALAYACARE_BASE_URL,
    publicKey: process.env.ALAYACARE_PUBLIC_KEY,
    privateKey: process.env.ALAYACARE_PRIVATE_KEY,
  },
  zendesk: {
    subdomain: process.env.ZENDESK_SUBDOMAIN,
    email: process.env.ZENDESK_EMAIL,
    token: process.env.ZENDESK_API_TOKEN,
  },
  aws: {
    region: process.env.AWS_REGION || "us-east-1",
    logBucket: process.env.AWS_LOG_BUCKET || null,
  },
};
