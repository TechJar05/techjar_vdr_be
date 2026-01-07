// backend/routes/settingsRoutes.js
import express from "express";
import multer from "multer";
import {
  getProfile,
  updateProfile,
  uploadLogo,
  resetPassword,
  changeEmail,
  getTags,
  createTag,
  updateTag,
  deleteTag,
} from "../controllers/settingsController.js";
import { protect } from "../middleware/authMiddleware.js";

const router = express.Router();
const upload = multer(); // in-memory buffer

// Profile routes
router.get("/profile", protect, getProfile);
router.put("/profile", protect, updateProfile);
router.post("/profile/logo", protect, upload.single("logo"), uploadLogo);
router.post("/profile/change-email", protect, changeEmail);

// Password reset
router.post("/reset-password", protect, resetPassword);

// Tags routes
router.get("/tags", protect, getTags);
router.post("/tags", protect, createTag);
router.put("/tags/:id", protect, updateTag);
router.delete("/tags/:id", protect, deleteTag);

export default router;

