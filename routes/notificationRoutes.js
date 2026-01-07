import express from "express";
import { protect } from "../middleware/authMiddleware.js";
import notificationController from "../controllers/notificationController.js";

const router = express.Router();

router.get("/", protect, notificationController.listNotifications);
router.put("/:id/read", protect, notificationController.markRead);
router.delete("/:id", protect, notificationController.deleteNotification);
router.delete("/", protect, notificationController.deleteNotification);

export default router;
