import express from "express";
import { listFavorites, addFavorite, removeFavorite } from "../controllers/favoriteController.js";
import { protect } from "../middleware/authMiddleware.js";

const router = express.Router();

router.use(protect);

router.get("/", listFavorites);
router.post("/", addFavorite);
router.delete("/:itemId", removeFavorite);

export default router;

