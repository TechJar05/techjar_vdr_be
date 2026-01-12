// backend/scripts/runMigration.js
// Run with: node scripts/runMigration.js

import snowflake from "snowflake-sdk";
import fs from "fs";
import dotenv from "dotenv";

dotenv.config();

const privateKey = fs.readFileSync("./config/snowflake_key.p8", "utf8");

const connection = snowflake.createConnection({
  account: process.env.SNOWFLAKE_ACCOUNT,
  username: process.env.SNOWFLAKE_USER,
  privateKey: privateKey,
  authenticator: "SNOWFLAKE_JWT",
  warehouse: process.env.SNOWFLAKE_WAREHOUSE,
  database: process.env.SNOWFLAKE_DATABASE,
  schema: process.env.SNOWFLAKE_SCHEMA,
});

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

async function runMigration() {
  console.log("Connecting to Snowflake...");

  await new Promise((resolve, reject) => {
    connection.connect((err) => {
      if (err) reject(err);
      else resolve();
    });
  });

  console.log("✅ Connected to Snowflake");

  // Drop existing tables (PAYMENTS first due to foreign key)
  console.log("Dropping existing PAYMENTS table if exists...");
  await execSql(`DROP TABLE IF EXISTS PAYMENTS`);

  console.log("Dropping existing ORGANIZATIONS table if exists...");
  await execSql(`DROP TABLE IF EXISTS ORGANIZATIONS`);

  // Create ORGANIZATIONS table
  console.log("Creating ORGANIZATIONS table...");
  await execSql(`
    CREATE TABLE ORGANIZATIONS (
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
  `);
  console.log("✅ ORGANIZATIONS table created");

  // Create PAYMENTS table
  console.log("Creating PAYMENTS table...");
  await execSql(`
    CREATE TABLE PAYMENTS (
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
      UPDATED_AT TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
      FOREIGN KEY (ORGANIZATION_ID) REFERENCES ORGANIZATIONS(ID)
    )
  `);
  console.log("✅ PAYMENTS table created");

  console.log("\n🎉 Migration completed successfully!");
  process.exit(0);
}

runMigration().catch((err) => {
  console.error("❌ Migration failed:", err.message);
  process.exit(1);
});
