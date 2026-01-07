// backend/middleware/authMiddleware.js
import jwt from "jsonwebtoken";

export const protect = (req, res, next) => {
  const authHeader = req.headers.authorization || "";
  const token = authHeader.split(" ")[1];

  if (!token) {
    return res.status(401).json({ message: "Unauthorized: token missing" });
  }

  jwt.verify(token, process.env.JWT_SECRET, (err, decoded) => {
    if (err) {
      // log to help debugging: TokenExpiredError, JsonWebTokenError, etc.
      console.error("JWT verify error:", err.name, err.message);

      if (err.name === "TokenExpiredError") {
        return res.status(401).json({ message: "Token expired" });
      }

      // invalid signature, malformed token, etc.
      return res.status(403).json({ message: "Invalid token" });
    }

    req.user = decoded;
    next();
  });
};
