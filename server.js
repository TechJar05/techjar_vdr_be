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
import orgRoutes from "./routes/orgRoutes.js";
import adminRoutes from "./routes/adminRoutes.js";

dotenv.config();
const app = express();
app.set("trust proxy", true);

// CORS configuration - must come before helmet
const corsOptions = {
  origin: ["https://vdr.tjdem.online", "http://localhost:3000", "http://localhost:5173", "http://localhost:5174"],
  credentials: true,
  methods: ["GET", "POST", "PUT", "DELETE", "PATCH", "OPTIONS"],
  allowedHeaders: ["Content-Type", "Authorization", "X-Requested-With"],
};

// Handle preflight requests
app.options("*", cors(corsOptions));

// Apply CORS
app.use(cors(corsOptions));

// Helmet with CORS-compatible settings
app.use(
  helmet({
    crossOriginResourcePolicy: { policy: "cross-origin" },
    crossOriginOpenerPolicy: { policy: "same-origin-allow-popups" },
  })
);
app.use(express.json({ limit: "10mb" }));
app.use(express.urlencoded({ extended: true, limit: "10mb" }));

// Routes (Nginx strips /api prefix, so use paths without /api)
app.use("/auth", authRoutes);
app.use("/users", userRoutes);
app.use("/files", fileRoutes);
app.use("/groups", groupRoutes);
app.use("/access", accessRoutes);
app.use("/trash", trashRoutes);
app.use("/favorites", favoriteRoutes);
app.use("/notifications", notificationRoutes);
app.use("/storage", storageRoutes);
app.use("/reports", reportRoutes);
app.use("/settings", settingsRoutes);
app.use("/logs", logRoutes);
app.use("/org", orgRoutes);
app.use("/superadmin", adminRoutes);

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
