// backend/middleware/orgAuthMiddleware.js
// Middleware for organization JWT authentication

import jwt from "jsonwebtoken";

/**
 * Middleware to protect organization routes
 * Verifies JWT token and attaches organization data to request
 */
export const protectOrg = (req, res, next) => {
  const authHeader = req.headers.authorization;

  if (!authHeader || !authHeader.startsWith("Bearer ")) {
    return res.status(401).json({
      error: "Not authorized. No token provided.",
    });
  }

  const token = authHeader.split(" ")[1];

  try {
    const decoded = jwt.verify(token, process.env.JWT_SECRET);

    // Verify this is an organization token
    if (decoded.type !== "organization") {
      return res.status(403).json({
        error: "Invalid token type. Organization token required.",
      });
    }

    // Attach organization data to request
    req.organization = {
      id: decoded.id,
      email: decoded.email,
      organizationName: decoded.organizationName,
    };

    next();
  } catch (err) {
    console.error("Organization auth error:", err.message);

    if (err.name === "TokenExpiredError") {
      return res.status(401).json({ error: "Token expired. Please login again." });
    }

    return res.status(403).json({ error: "Invalid token." });
  }
};

export default protectOrg;
