
// backend/routes/groupRoutes.js
import express from "express";
import { createGroup, listGroups, deleteGroup } from "../controllers/groupController.js";
import { protect } from "../middleware/authMiddleware.js";
import { isAdmin } from "../middleware/roleMiddleware.js";

const router = express.Router();

router.post("/", protect, isAdmin, createGroup);
router.get("/", protect, listGroups);
router.delete("/:id", protect, isAdmin, deleteGroup);

export default router;
