import snowflake from "snowflake-sdk";
import dotenv from "dotenv";
import fs from "fs";
dotenv.config();

const privateKey = fs.readFileSync("./config/snowflake_key.p8", "utf8");

const connection = snowflake.createConnection({
  account: process.env.SNOWFLAKE_ACCOUNT,
  username: process.env.SNOWFLAKE_USER,
  privateKey: privateKey,
  authenticator: "SNOWFLAKE_JWT",
  warehouse: process.env.SNOWFLAKE_WAREHOUSE,
  database: process.env.SNOWFLAKE_DATABASE,
  schema: process.env.SNOWFLAKE_SCHEMA
});

// Track connection state
let isConnected = false;
let connectionPromise = null;

// Connect and track state
connectionPromise = new Promise((resolve, reject) => {
  connection.connect((err, conn) => {
    if (err) {
      console.error("❌ Snowflake connection failed:", err);
      reject(err);
    } else {
      console.log("✅ Connected to Snowflake");
      isConnected = true;
      resolve(conn);
    }
  });
});

// Helper to ensure connection before queries
export const ensureConnected = async () => {
  if (!isConnected) {
    await connectionPromise;
  }
  return connection;
};

export default connection;
