// backend/controllers/adminController.js
// Superadmin controller for Razorpay payment management

import Razorpay from "razorpay";
import dotenv from "dotenv";
import jwt from "jsonwebtoken";
import connection, { ensureConnected } from "../config/snowflake.js";

dotenv.config();

// Initialize Razorpay instance
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

// Helper: Execute Snowflake SQL
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

// =====================================================
// POST /superadmin/login - Superadmin login
// =====================================================
export const superadminLogin = async (req, res) => {
  const { email, password } = req.body;

  if (!email || !password) {
    return res.status(400).json({ error: "Email and password are required" });
  }

  try {
    // Check superadmin credentials from environment
    const superadminEmail = process.env.SUPERADMIN_EMAIL || "superadmin@vdr.com";
    const superadminPassword = process.env.SUPERADMIN_PASSWORD || "superadmin123";

    if (email.toLowerCase() !== superadminEmail.toLowerCase()) {
      return res.status(401).json({ error: "Invalid credentials" });
    }

    if (password !== superadminPassword) {
      return res.status(401).json({ error: "Invalid credentials" });
    }

    // Generate JWT token
    const token = jwt.sign(
      { email: superadminEmail, role: "superadmin", type: "superadmin" },
      process.env.JWT_SECRET,
      { expiresIn: "24h" }
    );

    res.status(200).json({
      message: "Login successful",
      token,
      admin: {
        id: "superadmin-1",
        email: superadminEmail,
        name: "Super Admin",
        role: "superadmin",
      },
    });
  } catch (err) {
    console.error("Superadmin login error:", err);
    res.status(500).json({ error: "Failed to login" });
  }
};

// =====================================================
// GET /superadmin/payments - Fetch all Razorpay payments
// =====================================================
export const getPayments = async (req, res) => {
  try {
    const { skip = 0, count = 100, from, to, status } = req.query;

    const options = {
      skip: parseInt(skip),
      count: Math.min(parseInt(count), 100),
    };

    if (from) options.from = parseInt(from);
    if (to) options.to = parseInt(to);

    const payments = await getRazorpay().payments.all(options);

    // Filter by status if provided
    let filteredItems = payments.items;
    if (status && status !== "all") {
      filteredItems = payments.items.filter((p) => p.status === status);
    }

    res.status(200).json({
      entity: "collection",
      count: filteredItems.length,
      items: filteredItems,
    });
  } catch (err) {
    console.error("Fetch payments error:", err);
    res.status(500).json({ error: "Failed to fetch payments", details: err.message });
  }
};

// =====================================================
// GET /superadmin/payments/:id - Fetch single payment
// =====================================================
export const getPaymentById = async (req, res) => {
  try {
    const { id } = req.params;
    const payment = await getRazorpay().payments.fetch(id);
    res.status(200).json(payment);
  } catch (err) {
    console.error("Fetch payment error:", err);
    res.status(500).json({ error: "Failed to fetch payment", details: err.message });
  }
};

// =====================================================
// GET /superadmin/dashboard/stats - Dashboard statistics
// =====================================================
export const getDashboardStats = async (req, res) => {
  try {
    // Fetch recent payments from Razorpay
    const payments = await getRazorpay().payments.all({ count: 100 });

    // Calculate stats
    const captured = payments.items.filter((p) => p.status === "captured");
    const failed = payments.items.filter((p) => p.status === "failed");
    const pending = payments.items.filter(
      (p) => p.status === "created" || p.status === "authorized"
    );

    const totalRevenue = captured.reduce((sum, p) => sum + p.amount, 0);
    const pendingAmount = pending.reduce((sum, p) => sum + p.amount, 0);

    // Get organization and user counts from database
    let totalUsers = 0;
    let totalOrganizations = 0;

    try {
      const userResult = await execSql("SELECT COUNT(*) as COUNT FROM USERS");
      totalUsers = userResult.rows[0]?.COUNT || 0;
    } catch (e) {
      console.log("Could not fetch user count:", e.message);
    }

    try {
      const orgResult = await execSql("SELECT COUNT(*) as COUNT FROM ORGANIZATIONS");
      totalOrganizations = orgResult.rows[0]?.COUNT || 0;
    } catch (e) {
      console.log("Could not fetch organization count:", e.message);
    }

    res.status(200).json({
      totalUsers,
      totalOrganizations,
      totalRevenue,
      pendingPayments: pendingAmount,
      successfulPayments: captured.length,
      failedPayments: failed.length,
      razorpayMode: process.env.RAZORPAY_MODE || "test",
    });
  } catch (err) {
    console.error("Dashboard stats error:", err);
    res.status(500).json({ error: "Failed to fetch dashboard stats", details: err.message });
  }
};

// =====================================================
// GET /superadmin/dashboard/revenue - Revenue over time
// =====================================================
export const getRevenueData = async (req, res) => {
  try {
    const { period = "daily" } = req.query;

    const now = Date.now();
    const fromTime =
      period === "daily"
        ? Math.floor((now - 30 * 24 * 60 * 60 * 1000) / 1000)
        : Math.floor((now - 365 * 24 * 60 * 60 * 1000) / 1000);

    const payments = await getRazorpay().payments.all({
      from: fromTime,
      count: 100,
    });

    // Group by date
    const revenueMap = new Map();

    payments.items
      .filter((p) => p.status === "captured")
      .forEach((payment) => {
        const date = new Date(payment.created_at * 1000);
        let key;

        if (period === "daily") {
          key = date.toISOString().split("T")[0];
        } else {
          const months = ["Jan", "Feb", "Mar", "Apr", "May", "Jun", "Jul", "Aug", "Sep", "Oct", "Nov", "Dec"];
          key = months[date.getMonth()];
        }

        revenueMap.set(key, (revenueMap.get(key) || 0) + payment.amount);
      });

    const revenueData = Array.from(revenueMap.entries())
      .map(([date, amount]) => ({ date, amount }))
      .sort((a, b) => a.date.localeCompare(b.date));

    res.status(200).json(revenueData);
  } catch (err) {
    console.error("Revenue data error:", err);
    res.status(500).json({ error: "Failed to fetch revenue data", details: err.message });
  }
};

// =====================================================
// POST /superadmin/payments/:id/refund - Refund a payment
// =====================================================
export const refundPayment = async (req, res) => {
  try {
    const { id } = req.params;
    const { amount } = req.body;

    const refundOptions = {};
    if (amount) {
      refundOptions.amount = Math.round(amount * 100);
    }

    const refund = await getRazorpay().payments.refund(id, refundOptions);
    const payment = await getRazorpay().payments.fetch(id);

    res.status(200).json(payment);
  } catch (err) {
    console.error("Refund error:", err);
    res.status(500).json({ error: "Failed to process refund", details: err.message });
  }
};
