import express from "express";
import { protect } from "../middleware/authMiddleware.js";
import {
  getFilesReport,
  getFileShareReport,
  getFileActivity,
} from "../controllers/reportController.js";

const router = express.Router();

// Get all files report (with views, downloads, shares, etc.)
router.get("/files", protect, getFilesReport);

// Get file share report (list files that are shared with share counts)
router.get("/file-share", protect, getFileShareReport);

// Get activity for a specific file
router.get("/file/:fileId/activity", protect, getFileActivity);

export default router;
