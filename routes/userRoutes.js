import express from "express";
import {
  listUsers,
  updateUser,
  deleteUser,
  getCurrentUser,
} from "../controllers/userController.js";
import { protect } from "../middleware/authMiddleware.js";

const router = express.Router();

// ✅ Get current profile
router.get("/me", protect, getCurrentUser);

// ✅ Get all users
router.get("/", listUsers);

// ✅ Update user
router.put("/:email", updateUser);

// ✅ Delete user
router.delete("/:email", deleteUser);

export default router;
