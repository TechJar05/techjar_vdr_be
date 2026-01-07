// backend/server.js
import express from "express";
import dotenv from "dotenv";
import cors from "cors";
import helmet from "helmet";

import authRoutes from "./routes/authRoutes.js";
import userRoutes from "./routes/userRoutes.js";
import fileRoutes from "./routes/fileRoutes.js";
import groupRoutes from "./routes/groupRoutes.js";
import accessRoutes from "./routes/accessRoutes.js";
import trashRoutes from "./routes/trashRoutes.js";
import favoriteRoutes from "./routes/favoriteRoutes.js";
import notificationRoutes from "./routes/notificationRoutes.js";
import storageRoutes from "./routes/storageRoutes.js";
import reportRoutes from "./routes/reportRoutes.js";
import settingsRoutes from "./routes/settingsRoutes.js";
import logRoutes from "./routes/logRoutes.js";

dotenv.config();
const app = express();
app.set("trust proxy", true);

// Middleware
app.use(helmet());
app.use(
  cors({
    origin: ["http://localhost:3000", "https://yourfrontend.com"],
    credentials: true,
  })
);
app.use(express.json({ limit: "10mb" }));
app.use(express.urlencoded({ extended: true, limit: "10mb" }));

// Routes
app.use("/api/auth", authRoutes);
app.use("/api/users", userRoutes);
app.use("/api/files", fileRoutes);
app.use("/api/groups", groupRoutes);
app.use("/api/access", accessRoutes);
app.use("/api/trash", trashRoutes);
app.use("/api/favorites", favoriteRoutes);
app.use("/api/notifications", notificationRoutes);
app.use("/api/storage", storageRoutes);
app.use("/api/reports", reportRoutes);
app.use("/api/settings", settingsRoutes);
app.use("/api/logs", logRoutes);

// Health check
app.get("/", (req, res) => res.send("✅ VDR Backend Running"));

// 404 fallback
app.use((req, res) => res.status(404).json({ message: "Route not found" }));

// Global error handler
app.use((err, req, res, next) => {
  console.error("💥 Error:", err.stack);
  res.status(500).json({ error: err.message || "Internal Server Error" });
});

const PORT = process.env.PORT || 5000;
app.listen(PORT, () => console.log(`🚀 Server running on port ${PORT}`));
