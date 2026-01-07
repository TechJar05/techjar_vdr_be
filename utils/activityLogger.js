import connection from "../config/snowflake.js";
import { v4 as uuidv4 } from "uuid";

let userLogTableEnsured = false;

export const ensureUserLogTable = () =>
  new Promise((resolve, reject) => {
    if (userLogTableEnsured) return resolve();

    const sql = `
      CREATE TABLE IF NOT EXISTS USER_LOGS (
        ID STRING NOT NULL,
        USER_EMAIL STRING,
        USER_NAME STRING,
        ROLE STRING,
        ACTION STRING,
        DESCRIPTION STRING,
        RESOURCE_ID STRING,
        RESOURCE_TYPE STRING,
        IP_ADDRESS STRING,
        USER_AGENT STRING,
        META STRING,
        CREATED_AT TIMESTAMP_LTZ DEFAULT CURRENT_TIMESTAMP(),
        PRIMARY KEY (ID)
      )
    `;

    connection.execute({
      sqlText: sql,
      complete: (err) => {
        if (err) {
          try { err.message = `${err.message} | SQL: ${sql}`; } catch (e) {}
          return reject(err);
        }
        userLogTableEnsured = true;
        resolve();
      },
    });
  });

const exec = (sqlText, binds = []) =>
  new Promise((resolve, reject) => {
    connection.execute({
      sqlText,
      binds,
      complete: (err) => {
        if (err) {
          try { err.message = `${err.message} | SQL: ${sqlText}`; } catch (e) {}
          return reject(err);
        }
        resolve();
      },
    });
  });

export const logActivity = async ({
  userEmail,
  userName,
  role,
  action,
  description,
  resourceId,
  resourceType,
  ipAddress,
  userAgent,
  meta,
} = {}) => {
  await ensureUserLogTable();

  const id = uuidv4();
  const metadata =
    typeof meta === "object" && meta !== null ? JSON.stringify(meta) : meta || "";

  const sql = `
    INSERT INTO USER_LOGS (
      ID,
      USER_EMAIL,
      USER_NAME,
      ROLE,
      ACTION,
      DESCRIPTION,
      RESOURCE_ID,
      RESOURCE_TYPE,
      IP_ADDRESS,
      USER_AGENT,
      META,
      CREATED_AT
    )
    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, CURRENT_TIMESTAMP())
  `;

  await exec(sql, [
    id,
    userEmail || null,
    userName || null,
    role || null,
    action || null,
    description || null,
    resourceId || null,
    resourceType || null,
    ipAddress || null,
    userAgent || null,
    metadata || null,
  ]);

  return id;
};

const extractIp = (req = {}) => {
  const forwarded = req.headers?.["x-forwarded-for"];
  if (forwarded && typeof forwarded === "string") {
    return forwarded.split(",")[0].trim();
  }
  return req.ip || req.connection?.remoteAddress || "";
};

export const logWithRequest = async (req, details = {}) => {
  const meta = details.meta || details.metadata || undefined;
  return logActivity({
    userEmail: details.userEmail || req?.user?.email || req?.body?.email || req?.query?.email,
    userName:
      details.userName ||
      req?.user?.name ||
      req?.body?.name ||
      (req?.user?.email ? req.user.email.split("@")[0] : ""),
    role: details.role || req?.user?.role,
    action: details.action || details.event || "activity",
    description: details.description || "",
    resourceId: details.resourceId,
    resourceType: details.resourceType,
    ipAddress: details.ipAddress || extractIp(req),
    userAgent: details.userAgent || req?.headers?.["user-agent"] || "",
    meta,
  });
};

export const safeLogWithRequest = (req, details = {}) =>
  logWithRequest(req, details).catch((err) =>
    console.error("[activityLogger] failed to write log:", err?.message || err)
  );

export default {
  logActivity,
  logWithRequest,
  safeLogWithRequest,
};

