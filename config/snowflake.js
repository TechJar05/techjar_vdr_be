import snowflake from "snowflake-sdk";
import dotenv from "dotenv";
import fs from "fs";
dotenv.config();

const privateKey = fs.readFileSync("./config/snowflake_key.p8", "utf8");

// Track connection state
let isConnected = false;
let connectionPromise = null;
let reconnectAttempts = 0;
const MAX_RECONNECT_ATTEMPTS = 5;
const RECONNECT_DELAY_MS = 2000;

// Connection wrapper - holds the actual connection reference
const connectionWrapper = {
  _conn: null,
  execute(options) {
    if (!this._conn) {
      const originalComplete = options.complete;
      options.complete = (err, stmt, rows) => {
        if (err) {
          originalComplete(new Error("Snowflake connection not initialized"), null, null);
        } else {
          originalComplete(err, stmt, rows);
        }
      };
      // Try to ensure connection and retry
      ensureConnected().then(() => {
        this._conn.execute(options);
      }).catch(connErr => {
        options.complete(connErr, null, null);
      });
      return;
    }
    this._conn.execute(options);
  },
  isUp() {
    return this._conn && this._conn.isUp();
  }
};

// Create a new Snowflake connection
const createConnection = () => {
  return snowflake.createConnection({
    account: process.env.SNOWFLAKE_ACCOUNT,
    username: process.env.SNOWFLAKE_USER,
    privateKey: privateKey,
    authenticator: "SNOWFLAKE_JWT",
    warehouse: process.env.SNOWFLAKE_WAREHOUSE,
    database: process.env.SNOWFLAKE_DATABASE,
    schema: process.env.SNOWFLAKE_SCHEMA,
    clientSessionKeepAlive: true,
    clientSessionKeepAliveHeartbeatFrequency: 3600
  });
};

// Connect with retry logic
const connectWithRetry = () => {
  return new Promise((resolve, reject) => {
    const conn = createConnection();

    conn.connect((err, connResult) => {
      if (err) {
        console.error("❌ Snowflake connection failed:", err.message);
        isConnected = false;

        if (reconnectAttempts < MAX_RECONNECT_ATTEMPTS) {
          reconnectAttempts++;
          console.log(`🔄 Retrying connection (${reconnectAttempts}/${MAX_RECONNECT_ATTEMPTS}) in ${RECONNECT_DELAY_MS}ms...`);
          setTimeout(() => {
            connectWithRetry().then(resolve).catch(reject);
          }, RECONNECT_DELAY_MS * reconnectAttempts);
        } else {
          reject(new Error(`Failed to connect after ${MAX_RECONNECT_ATTEMPTS} attempts: ${err.message}`));
        }
      } else {
        console.log("✅ Connected to Snowflake");
        connectionWrapper._conn = conn;
        isConnected = true;
        reconnectAttempts = 0;
        resolve(conn);
      }
    });
  });
};

// Initialize connection
connectionPromise = connectWithRetry();

// Reconnect function for when connection is lost
const reconnect = async () => {
  console.log("🔄 Attempting to reconnect to Snowflake...");
  isConnected = false;
  reconnectAttempts = 0;
  connectionPromise = connectWithRetry();
  return connectionPromise;
};

// Helper to ensure connection before queries
export const ensureConnected = async () => {
  if (!isConnected) {
    await connectionPromise;
  }

  // Verify connection is actually valid
  if (connectionWrapper._conn && !connectionWrapper._conn.isUp()) {
    console.log("⚠️ Connection is down, reconnecting...");
    await reconnect();
  }

  return connectionWrapper;
};

// Export reconnect for manual use if needed
export { reconnect };

// Export the wrapper - it proxies calls to the actual connection
export default connectionWrapper;
