// backend/routes/orgRoutes.js
// Isolated routes for organization registration, login, and payment

import express from "express";
import {
  registerOrganization,
  loginOrganization,
  createPaymentOrder,
  verifyPayment,
} from "../controllers/orgController.js";

const router = express.Router();

// Organization Registration
// POST /org/register
router.post("/register", registerOrganization);

// Organization Login
// POST /org/login
router.post("/login", loginOrganization);

// Create Razorpay Payment Order
// POST /org/payment/create-order
router.post("/payment/create-order", createPaymentOrder);

// Verify Payment and Activate Plan
// POST /org/payment/verify
router.post("/payment/verify", verifyPayment);

export default router;
