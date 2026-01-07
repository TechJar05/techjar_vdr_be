import express from "express";
import { listLogs } from "../controllers/logController.js";
import { protect } from "../middleware/authMiddleware.js";
import { isAdmin } from "../middleware/roleMiddleware.js";

const router = express.Router();

router.get("/", protect, isAdmin, listLogs);

export default router;

