
// backend/routes/fileRoutes.js
import express from "express";
import multer from "multer";
import {
  createFolder,
  listFolders,
  openFolder,
  renameFolder,
  deleteFolder,
  uploadFile,
  listFiles,
  deleteFile,
  addComment,
  downloadFile,
} from "../controllers/fileController.js";
import { protect } from "../middleware/authMiddleware.js";
import { isAdmin } from "../middleware/roleMiddleware.js";
import { viewFile } from "../controllers/fileController.js";

const router = express.Router();
const upload = multer(); // in-memory buffer

// Folders
router.post("/folder", protect, createFolder);
router.get("/folders", protect, listFolders);
router.get("/folder/:id", protect, openFolder);
router.put("/folder/:id", protect, isAdmin, renameFolder);
router.delete("/folder/:id", protect, isAdmin, deleteFolder);

// Files
router.post("/upload/:folderId", protect, upload.single("file"), uploadFile);
router.get("/files/:folderId", protect, listFiles);
router.delete("/file/:id", protect, deleteFile);
// 👇 This must exist
router.get("/view/:id", viewFile);
// Download
router.get("/download/:id", protect, downloadFile);

// Comments
router.post("/file/:id/comment", protect, addComment);

// Export router at the END
export default router;
