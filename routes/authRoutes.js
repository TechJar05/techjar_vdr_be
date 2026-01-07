// backend/routes/authRoutes.js
import express from "express";
import { requestOTP, verifyOTP, registerUser, requestPasswordReset, resetPasswordWithOTP } from "../controllers/authController.js";

const router = express.Router();

// OTP routes
router.post("/request-otp", requestOTP);
router.post("/verify-otp", verifyOTP);

// Open registration route (no auth required)
router.post("/register", registerUser);

// Password reset routes (no auth required)
router.post("/forgot-password", requestPasswordReset);
router.post("/reset-password", resetPasswordWithOTP);

export default router;
