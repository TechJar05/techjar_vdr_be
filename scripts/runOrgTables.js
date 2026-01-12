// Script to create ORGANIZATIONS and PAYMENTS tables in Snowflake
import snowflake from "snowflake-sdk";
import dotenv from "dotenv";
import fs from "fs";
import path from "path";
import { fileURLToPath } from "url";

const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);

dotenv.config({ path: path.join(__dirname, "../.env") });

const privateKey = fs.readFileSync(path.join(__dirname, "../config/snowflake_key.p8"), "utf8");

const connection = snowflake.createConnection({
  account: process.env.SNOWFLAKE_ACCOUNT,
  username: process.env.SNOWFLAKE_USER,
  privateKey: privateKey,
  authenticator: "SNOWFLAKE_JWT",
  warehouse: process.env.SNOWFLAKE_WAREHOUSE,
  database: process.env.SNOWFLAKE_DATABASE,
  schema: process.env.SNOWFLAKE_SCHEMA,
});

// SQL statements to create tables
const createOrganizationsTable = `
CREATE TABLE IF NOT EXISTS ORGANIZATIONS (
    ID VARCHAR(36) PRIMARY KEY,
    ORGANIZATION_NAME VARCHAR(255) NOT NULL,
    EMAIL VARCHAR(255) NOT NULL UNIQUE,
    PASSWORD VARCHAR(255) NOT NULL,
    PHONE VARCHAR(50),
    WEBSITE VARCHAR(255),
    ADDRESS VARCHAR(500),
    HAS_ACTIVE_PLAN BOOLEAN DEFAULT FALSE,
    PLAN_TYPE VARCHAR(50),
    PLAN_START_DATE TIMESTAMP_NTZ,
    PLAN_END_DATE TIMESTAMP_NTZ,
    CREATED_AT TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    UPDATED_AT TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
)
`;

const createPaymentsTable = `
CREATE TABLE IF NOT EXISTS PAYMENTS (
    ID VARCHAR(36) PRIMARY KEY,
    ORGANIZATION_ID VARCHAR(36) NOT NULL,
    RAZORPAY_ORDER_ID VARCHAR(255) NOT NULL,
    RAZORPAY_PAYMENT_ID VARCHAR(255),
    RAZORPAY_SIGNATURE VARCHAR(255),
    AMOUNT NUMBER(10,2) NOT NULL,
    CURRENCY VARCHAR(10) DEFAULT 'INR',
    PLAN_TYPE VARCHAR(50) NOT NULL,
    PLAN_DURATION_MONTHS INTEGER NOT NULL,
    STATUS VARCHAR(20) DEFAULT 'pending',
    CREATED_AT TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    UPDATED_AT TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
)
`;

// Helper to execute SQL
const execSql = (sql) =>
  new Promise((resolve, reject) => {
    connection.execute({
      sqlText: sql,
      complete: (err, stmt, rows) => {
        if (err) return reject(err);
        resolve({ stmt, rows });
      },
    });
  });

// Main execution
async function createTables() {
  console.log("Connecting to Snowflake...");

  connection.connect(async (err, conn) => {
    if (err) {
      console.error("Failed to connect to Snowflake:", err.message);
      process.exit(1);
    }

    console.log("Connected to Snowflake successfully!");

    try {
      // Create ORGANIZATIONS table
      console.log("\nCreating ORGANIZATIONS table...");
      await execSql(createOrganizationsTable);
      console.log("ORGANIZATIONS table created successfully!");

      // Create PAYMENTS table
      console.log("\nCreating PAYMENTS table...");
      await execSql(createPaymentsTable);
      console.log("PAYMENTS table created successfully!");

      // Verify tables exist
      console.log("\nVerifying tables...");
      const { rows } = await execSql(`
        SELECT TABLE_NAME
        FROM INFORMATION_SCHEMA.TABLES
        WHERE TABLE_SCHEMA = '${process.env.SNOWFLAKE_SCHEMA}'
        AND TABLE_NAME IN ('ORGANIZATIONS', 'PAYMENTS')
      `);

      console.log("\nTables found in database:");
      rows.forEach(row => console.log(`  - ${row.TABLE_NAME}`));

      console.log("\n✅ All tables created successfully!");
      process.exit(0);
    } catch (error) {
      console.error("\n❌ Error creating tables:", error.message);
      process.exit(1);
    }
  });
}

createTables();
