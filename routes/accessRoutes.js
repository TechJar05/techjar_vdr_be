import express from "express";
import {
  requestAccess,
  listAccessRequests,
  updateAccessStatus,
  checkUserAccess,
  revokeAllAccess,
  revokeSpecificAccess,
} from "../controllers/accessController.js";
import { protect } from "../middleware/authMiddleware.js";

const router = express.Router();

// User requests access to file/folder
router.post("/request", protect, requestAccess);

// Admin lists all access requests
router.get("/requests", protect, listAccessRequests);

// Admin approves/rejects request OR revokes specific access
router.put("/requests/:id", protect, (req, res, next) => {
  // Route based on action in body
  if (req.body.action === "revoke") {
    return revokeSpecificAccess(req, res);
  }
  // Otherwise, it's an approve/reject action
  return updateAccessStatus(req, res);
});

// Admin revokes all access to an item
router.delete("/requests/:id", protect, revokeAllAccess);

// Check if user has access
router.get("/check", protect, checkUserAccess);

export default router;
