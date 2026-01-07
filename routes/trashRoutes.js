import express from "express";
import { listTrash, restoreItem, permanentlyDeleteItem } from "../controllers/trashController.js";
import { protect } from "../middleware/authMiddleware.js";
import { isAdmin } from "../middleware/roleMiddleware.js";

const router = express.Router();

router.get("/", protect, isAdmin, listTrash);
router.post("/restore/:id", protect, isAdmin, restoreItem);
router.delete("/:id", protect, isAdmin, permanentlyDeleteItem);

export default router;
