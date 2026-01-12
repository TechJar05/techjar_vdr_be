// backend/controllers/orgController.js
// Isolated controller for organization registration, login, and payment handling

import bcrypt from "bcryptjs";
import jwt from "jsonwebtoken";
import { v4 as uuidv4 } from "uuid";
import Razorpay from "razorpay";
import crypto from "crypto";
import dotenv from "dotenv";
import connection, { ensureConnected } from "../config/snowflake.js";

// Load environment variables
dotenv.config();

// Initialize Razorpay instance (lazy initialization to ensure env vars are loaded)
let razorpay = null;
const getRazorpay = () => {
  if (!razorpay) {
    razorpay = new Razorpay({
      key_id: process.env.RAZORPAY_KEY_ID,
      key_secret: process.env.RAZORPAY_KEY_SECRET,
    });
  }
  return razorpay;
};

// Helper: Execute Snowflake SQL (Promise wrapper)
const execSql = async (sql, binds = []) => {
  await ensureConnected();
  return new Promise((resolve, reject) => {
    connection.execute({
      sqlText: sql,
      binds,
      complete: (err, stmt, rows) => {
        if (err) {
          err.message = `${err.message} | SQL: ${sql}`;
          return reject(err);
        }
        resolve({ stmt, rows });
      },
    });
  });
};

// Helper: Escape single quotes for SQL
const esc = (s = "") => String(s).replace(/'/g, "''");

// Generate JWT token for organization
const generateOrgToken = (orgData) => {
  return jwt.sign(
    {
      id: orgData.ID,
      email: orgData.EMAIL,
      organizationName: orgData.ORGANIZATION_NAME,
      type: "organization",
    },
    process.env.JWT_SECRET,
    { expiresIn: "24h" }
  );
};

// Plan duration mapping (in months)
const PLAN_DURATIONS = {
  monthly: 1,
  quarterly: 3,
  yearly: 12,
};

// =====================================================
// POST /org/register - Register a new organization
// =====================================================
export const registerOrganization = async (req, res) => {
  const { organizationName, email, password, phone, website, address } = req.body;

  // Validate required fields
  if (!organizationName || !email || !password) {
    return res.status(400).json({
      error: "Organization name, email, and password are required",
    });
  }

  // Validate email format
  const emailRegex = /^[^\s@]+@[^\s@]+\.[^\s@]+$/;
  if (!emailRegex.test(email)) {
    return res.status(400).json({ error: "Invalid email format" });
  }

  // Validate password strength
  if (password.length < 6) {
    return res.status(400).json({
      error: "Password must be at least 6 characters long",
    });
  }

  try {
    // Check if organization with this email already exists
    const checkSql = `SELECT ID FROM ORGANIZATIONS WHERE EMAIL = '${esc(email.toLowerCase())}'`;
    const { rows: existing } = await execSql(checkSql);

    if (existing && existing.length > 0) {
      return res.status(409).json({
        error: "An organization with this email already exists",
      });
    }

    // Hash the password
    const salt = await bcrypt.genSalt(10);
    const hashedPassword = await bcrypt.hash(password, salt);

    // Generate unique ID
    const orgId = uuidv4();

    // Insert organization into database
    const insertSql = `
      INSERT INTO ORGANIZATIONS (
        ID, ORGANIZATION_NAME, EMAIL, PASSWORD, PHONE, WEBSITE, ADDRESS,
        HAS_ACTIVE_PLAN, CREATED_AT, UPDATED_AT
      ) VALUES (
        '${esc(orgId)}',
        '${esc(organizationName)}',
        '${esc(email.toLowerCase())}',
        '${esc(hashedPassword)}',
        '${esc(phone || "")}',
        '${esc(website || "")}',
        '${esc(address || "")}',
        FALSE,
        CURRENT_TIMESTAMP(),
        CURRENT_TIMESTAMP()
      )
    `;

    await execSql(insertSql);

    res.status(201).json({
      message: "Organization registered successfully",
      organization: {
        id: orgId,
        organizationName,
        email: email.toLowerCase(),
        hasActivePlan: false,
      },
    });
  } catch (err) {
    console.error("Organization registration error:", err.message);
    console.error("Full error:", err);
    res.status(500).json({ error: "Failed to register organization", details: err.message });
  }
};

// =====================================================
// POST /org/login - Organization login
// =====================================================
export const loginOrganization = async (req, res) => {
  const { email, password } = req.body;

  // Validate required fields
  if (!email || !password) {
    return res.status(400).json({ error: "Email and password are required" });
  }

  try {
    // Find organization by email
    const sql = `
      SELECT ID, ORGANIZATION_NAME, EMAIL, PASSWORD, HAS_ACTIVE_PLAN,
             PLAN_TYPE, PLAN_START_DATE, PLAN_END_DATE
      FROM ORGANIZATIONS
      WHERE EMAIL = '${esc(email.toLowerCase())}'
    `;
    const { rows } = await execSql(sql);

    if (!rows || rows.length === 0) {
      return res.status(401).json({ error: "Invalid email or password" });
    }

    const org = rows[0];

    // Verify password
    const isMatch = await bcrypt.compare(password, org.PASSWORD);
    if (!isMatch) {
      return res.status(401).json({ error: "Invalid email or password" });
    }

    // Check plan status
    let planStatus = "not_purchased";
    let hasActivePlan = org.HAS_ACTIVE_PLAN;

    if (org.HAS_ACTIVE_PLAN && org.PLAN_END_DATE) {
      const now = new Date();
      const planEndDate = new Date(org.PLAN_END_DATE);

      if (now > planEndDate) {
        // Plan has expired - update the database
        planStatus = "expired";
        hasActivePlan = false;

        const updateSql = `
          UPDATE ORGANIZATIONS
          SET HAS_ACTIVE_PLAN = FALSE, UPDATED_AT = CURRENT_TIMESTAMP()
          WHERE ID = '${esc(org.ID)}'
        `;
        await execSql(updateSql);
      } else {
        planStatus = "active";
      }
    }

    // If plan is not active or expired, return appropriate response
    if (planStatus !== "active") {
      return res.status(200).json({
        requiresPlan: true,
        planStatus,
        organization: {
          id: org.ID,
          organizationName: org.ORGANIZATION_NAME,
          email: org.EMAIL,
          hasActivePlan: false,
          planExpiry: org.PLAN_END_DATE,
        },
        message:
          planStatus === "expired"
            ? "Plan expired. Please purchase again."
            : "Please purchase a plan to continue.",
      });
    }

    // Generate JWT token for active plan
    const token = generateOrgToken(org);

    res.status(200).json({
      message: "Login successful",
      token,
      organization: {
        id: org.ID,
        organizationName: org.ORGANIZATION_NAME,
        email: org.EMAIL,
        hasActivePlan: true,
        planType: org.PLAN_TYPE,
        planStartDate: org.PLAN_START_DATE,
        planEndDate: org.PLAN_END_DATE,
      },
    });
  } catch (err) {
    console.error("Organization login error:", err);
    res.status(500).json({ error: "Failed to login" });
  }
};

// =====================================================
// POST /org/payment/create-order - Create Razorpay order
// =====================================================
export const createPaymentOrder = async (req, res) => {
  const { organizationId, planType, amount } = req.body;

  // Validate required fields
  if (!organizationId || !planType || !amount) {
    return res.status(400).json({
      error: "Organization ID, plan type, and amount are required",
    });
  }

  // Validate plan type
  if (!PLAN_DURATIONS[planType]) {
    return res.status(400).json({
      error: "Invalid plan type. Must be monthly, quarterly, or yearly",
    });
  }

  try {
    // Verify organization exists
    const checkSql = `SELECT ID FROM ORGANIZATIONS WHERE ID = '${esc(organizationId)}'`;
    const { rows } = await execSql(checkSql);

    if (!rows || rows.length === 0) {
      return res.status(404).json({ error: "Organization not found" });
    }

    // Create Razorpay order
    // Receipt must be max 40 chars - use short ID + timestamp suffix
    const shortId = organizationId.substring(0, 8);
    const timestamp = Date.now().toString().slice(-10);
    const options = {
      amount: Math.round(amount * 100), // Razorpay expects amount in paise
      currency: "INR",
      receipt: `org_${shortId}_${timestamp}`,
      notes: {
        organizationId,
        planType,
      },
    };

    const order = await getRazorpay().orders.create(options);

    // Store payment record in database
    const paymentId = uuidv4();
    const insertSql = `
      INSERT INTO PAYMENTS (
        ID, ORGANIZATION_ID, RAZORPAY_ORDER_ID, AMOUNT, CURRENCY,
        PLAN_TYPE, PLAN_DURATION_MONTHS, STATUS, CREATED_AT, UPDATED_AT
      ) VALUES (
        '${esc(paymentId)}',
        '${esc(organizationId)}',
        '${esc(order.id)}',
        ${amount},
        'INR',
        '${esc(planType)}',
        ${PLAN_DURATIONS[planType]},
        'pending',
        CURRENT_TIMESTAMP(),
        CURRENT_TIMESTAMP()
      )
    `;

    await execSql(insertSql);

    res.status(200).json({
      orderId: order.id,
      amount: order.amount,
      currency: order.currency,
      keyId: process.env.RAZORPAY_KEY_ID,
    });
  } catch (err) {
    console.error("Create payment order error:", err.message);
    console.error("Full error:", err);
    res.status(500).json({ error: "Failed to create payment order", details: err.message });
  }
};

// =====================================================
// POST /org/payment/verify - Verify payment and activate plan
// =====================================================
export const verifyPayment = async (req, res) => {
  const {
    razorpay_order_id,
    razorpay_payment_id,
    razorpay_signature,
    organizationId,
  } = req.body;

  // Validate required fields
  if (!razorpay_order_id || !razorpay_payment_id || !razorpay_signature || !organizationId) {
    return res.status(400).json({ error: "All payment details are required" });
  }

  try {
    // Verify Razorpay signature
    const body = razorpay_order_id + "|" + razorpay_payment_id;
    const expectedSignature = crypto
      .createHmac("sha256", process.env.RAZORPAY_KEY_SECRET)
      .update(body.toString())
      .digest("hex");

    if (expectedSignature !== razorpay_signature) {
      // Update payment status to failed
      await execSql(`
        UPDATE PAYMENTS
        SET STATUS = 'failed', UPDATED_AT = CURRENT_TIMESTAMP()
        WHERE RAZORPAY_ORDER_ID = '${esc(razorpay_order_id)}'
      `);

      return res.status(400).json({ error: "Invalid payment signature" });
    }

    // Get payment record to determine plan details
    const paymentSql = `
      SELECT ID, PLAN_TYPE, PLAN_DURATION_MONTHS, AMOUNT
      FROM PAYMENTS
      WHERE RAZORPAY_ORDER_ID = '${esc(razorpay_order_id)}'
    `;
    const { rows: paymentRows } = await execSql(paymentSql);

    if (!paymentRows || paymentRows.length === 0) {
      return res.status(404).json({ error: "Payment record not found" });
    }

    const payment = paymentRows[0];

    // Calculate plan dates
    const planStartDate = new Date();
    const planEndDate = new Date();
    planEndDate.setMonth(planEndDate.getMonth() + payment.PLAN_DURATION_MONTHS);

    // Update payment record
    await execSql(`
      UPDATE PAYMENTS
      SET
        RAZORPAY_PAYMENT_ID = '${esc(razorpay_payment_id)}',
        RAZORPAY_SIGNATURE = '${esc(razorpay_signature)}',
        STATUS = 'success',
        UPDATED_AT = CURRENT_TIMESTAMP()
      WHERE RAZORPAY_ORDER_ID = '${esc(razorpay_order_id)}'
    `);

    // Activate organization plan
    await execSql(`
      UPDATE ORGANIZATIONS
      SET
        HAS_ACTIVE_PLAN = TRUE,
        PLAN_TYPE = '${esc(payment.PLAN_TYPE)}',
        PLAN_START_DATE = '${planStartDate.toISOString()}',
        PLAN_END_DATE = '${planEndDate.toISOString()}',
        UPDATED_AT = CURRENT_TIMESTAMP()
      WHERE ID = '${esc(organizationId)}'
    `);

    // Fetch updated organization data
    const orgSql = `
      SELECT ID, ORGANIZATION_NAME, EMAIL, HAS_ACTIVE_PLAN,
             PLAN_TYPE, PLAN_START_DATE, PLAN_END_DATE
      FROM ORGANIZATIONS
      WHERE ID = '${esc(organizationId)}'
    `;
    const { rows: orgRows } = await execSql(orgSql);
    const org = orgRows[0];

    res.status(200).json({
      message: "Payment verified successfully. Plan activated!",
      organization: {
        id: org.ID,
        organizationName: org.ORGANIZATION_NAME,
        email: org.EMAIL,
        hasActivePlan: true,
        planType: org.PLAN_TYPE,
        planStartDate: org.PLAN_START_DATE,
        planEndDate: org.PLAN_END_DATE,
      },
    });
  } catch (err) {
    console.error("Payment verification error:", err);
    res.status(500).json({ error: "Failed to verify payment" });
  }
};