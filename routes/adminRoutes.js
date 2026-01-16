// backend/routes/adminRoutes.js
// Superadmin routes for Razorpay payment management

import express from "express";
import {
  superadminLogin,
  getPayments,
  getPaymentById,
  getDashboardStats,
  getRevenueData,
  refundPayment,
} from "../controllers/adminController.js";
import { protect } from "../middleware/authMiddleware.js";

const router = express.Router();

// Public routes
router.post("/login", superadminLogin);

// Protected routes (require authentication)
router.get("/payments", protect, getPayments);
router.get("/payments/:id", protect, getPaymentById);
router.post("/payments/:id/refund", protect, refundPayment);
router.get("/dashboard/stats", protect, getDashboardStats);
router.get("/dashboard/revenue", protect, getRevenueData);

export default router;
