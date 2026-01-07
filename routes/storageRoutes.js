import express from "express";
import { protect } from "../middleware/authMiddleware.js";
import {
  getUserStorage,
  addToStorage,
  addFolderToStorage,
  listStorageFiles,
  removeFromStorage,
} from "../controllers/storageController.js";

const router = express.Router();

// Get user's storage info
router.get("/", protect, getUserStorage);

// List files in user's storage
router.get("/files", protect, listStorageFiles);

// Add file to storage (download/save)
router.post("/add", protect, addToStorage);

// Add folder with all contents to storage
router.post("/add-folder", protect, addFolderToStorage);

// Remove file from storage
  router.delete("/:storageRef", protect, removeFromStorage);

export default router;
