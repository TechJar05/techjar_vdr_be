import { generateOTP } from "../utils/generateOTP.js";
import { sendMail } from "../utils/sendMail.js";
import { generateToken } from "../utils/generateToken.js";
import connection from "../config/snowflake.js";
import bcrypt from "bcryptjs";
import { safeLogWithRequest } from "../utils/activityLogger.js";

global.otpStore = {}; // Temporary in-memory OTP storage
global.passwordResetStore = {}; // Temporary in-memory password reset OTP storage

// Request OTP
export const requestOTP = (req, res) => {
  const { email } = req.body;
  if (!email) return res.status(400).json({ message: "Email is required" });

  const otp = generateOTP();
  global.otpStore[email] = { otp, expiry: Date.now() + 5 * 60 * 1000 }; // OTP valid for 5 min
  
  // Send email asynchronously (don't block response)
  sendMail(email, "VDR OTP Login", `Your OTP is: ${otp}`).catch(err => {
    console.error("[requestOTP] Email send failed:", err);
  });
  
  safeLogWithRequest(req, {
    action: "request_otp",
    description: `OTP requested for ${email}`,
    resourceId: email,
    resourceType: "user",
  });
  res.json({ message: "OTP sent to email" });
};

// Verify OTP and issue JWT
export const verifyOTP = (req, res) => {
  const { email, otp } = req.body;
  if (!email || !otp) return res.status(400).json({ message: "Email and OTP required" });

  const record = global.otpStore[email];
  if (!record || record.otp !== otp || Date.now() > record.expiry) {
    return res.status(400).json({ message: "Invalid or expired OTP" });
  }

  const query = `SELECT * FROM USERS WHERE EMAIL='${email}'`;
  connection.execute({
    sqlText: query,
    complete: (err, stmt, rows) => {
      if (err || rows.length === 0) return res.status(404).json({ message: "User not found" });

      const token = generateToken({
        email,
        role: rows[0].ROLE,
        name: rows[0].NAME || "",
      });
      delete global.otpStore[email];
      safeLogWithRequest(req, {
        action: "login",
        description: `User logged in`,
        resourceId: email,
        resourceType: "user",
      });
      res.json({ token, role: rows[0].ROLE, });
    }
  });
};

// Register new user
export const registerUser = (req, res) => {
  const { name, email, password, role } = req.body;

  if (!name || !email || !password || !role) {
    return res.status(400).json({ message: "All fields are required" });
  }

  // Hash password
  const hashedPassword = bcrypt.hashSync(password, 10);

  // Insert into USERS table
  const query = `
    INSERT INTO USERS (NAME, EMAIL, PASSWORD, ROLE)
    VALUES ('${name}', '${email}', '${hashedPassword}', '${role}')
  `;

  connection.execute({
    sqlText: query,
    complete: (err) => {
      if (err) return res.status(500).json({ message: "Error registering user", error: err.message });
      
      // Optional: send confirmation email asynchronously (don't block response)
      sendMail(email, "VDR Registration", `Hello ${name}, your account has been created.`).catch(err => {
        console.error("[registerUser] Email send failed:", err);
      });

      safeLogWithRequest(req, {
        action: "register_user",
        description: `User registered with role ${role}`,
        resourceId: email,
        resourceType: "user",
      });

      res.status(201).json({ message: "User registered successfully" });
    }
  });
};

// Helper: Escape SQL strings
const esc = (str) => (str || "").replace(/'/g, "''");

// Helper: wrap connection.execute in a Promise
const execSql = (sql) =>
  new Promise((resolve, reject) => {
    connection.execute({
      sqlText: sql,
      complete: (err, stmt, rows) => {
        if (err) {
          try { err.message = `${err.message} | SQL: ${sql}`; } catch(e) {}
          return reject(err);
        }
        resolve({ stmt, rows });
      },
    });
  });

// Request password reset OTP (Forgot Password)
export const requestPasswordReset = async (req, res) => {
  const { email } = req.body;
  if (!email) return res.status(400).json({ message: "Email is required" });

  try {
    // Normalize email to lowercase for consistent storage
    const normalizedEmail = email.toLowerCase().trim();
    
    // Check if user exists
    const userSql = `SELECT * FROM USERS WHERE EMAIL='${esc(normalizedEmail)}' LIMIT 1`;
    const { rows: userRows } = await execSql(userSql);
    
    if (!userRows || userRows.length === 0) {
      // Don't reveal if user exists or not for security
      return res.json({ message: "If the email exists, a password reset OTP has been sent" });
    }

    // Generate OTP for password reset
    const otp = generateOTP();
    global.passwordResetStore[normalizedEmail] = { otp, expiry: Date.now() + 10 * 60 * 1000 }; // OTP valid for 10 min
    
    console.log(`[requestPasswordReset] OTP generated for ${normalizedEmail}: ${otp}`);
    
    // Send password reset OTP email asynchronously (don't block response)
    sendMail(normalizedEmail, "VDR Password Reset", `Your password reset OTP is: ${otp}. This OTP will expire in 10 minutes.`).catch(err => {
      console.error("[requestPasswordReset] Email send failed:", err);
    });
    
    safeLogWithRequest(req, {
      action: "request_password_reset",
      description: `Password reset OTP requested for ${normalizedEmail}`,
      resourceId: normalizedEmail,
      resourceType: "user",
    });
    
    res.json({ message: "If the email exists, a password reset OTP has been sent" });
  } catch (err) {
    console.error("[requestPasswordReset] error:", err.message);
    res.status(500).json({ error: err.message });
  }
};

// Reset password with OTP verification
export const resetPasswordWithOTP = async (req, res) => {
  const { email, otp, newPassword, confirmPassword } = req.body;
  
  if (!email || !otp || !newPassword || !confirmPassword) {
    return res.status(400).json({ message: "All fields are required" });
  }

  if (newPassword !== confirmPassword) {
    return res.status(400).json({ message: "New password and confirm password do not match" });
  }

  if (newPassword.length < 6) {
    return res.status(400).json({ message: "New password must be at least 6 characters" });
  }

  try {
    // Normalize email to lowercase for consistent lookup
    const normalizedEmail = email.toLowerCase().trim();
    
    // Verify OTP - convert both to strings for comparison
    const record = global.passwordResetStore[normalizedEmail];
    const otpString = String(otp).trim();
    const storedOtp = record ? String(record.otp).trim() : null;
    
    if (!record) {
      console.error(`[resetPasswordWithOTP] No OTP record found for email: ${normalizedEmail}`);
      return res.status(400).json({ message: "Invalid or expired OTP. Please request a new OTP." });
    }
    
    if (Date.now() > record.expiry) {
      console.error(`[resetPasswordWithOTP] OTP expired for email: ${normalizedEmail}`);
      delete global.passwordResetStore[normalizedEmail];
      return res.status(400).json({ message: "OTP has expired. Please request a new OTP." });
    }
    
    if (storedOtp !== otpString) {
      console.error(`[resetPasswordWithOTP] OTP mismatch for email: ${normalizedEmail}. Expected: ${storedOtp}, Got: ${otpString}`);
      return res.status(400).json({ message: "Invalid OTP. Please check and try again." });
    }

    // Check if user exists
    const userSql = `SELECT * FROM USERS WHERE EMAIL='${esc(normalizedEmail)}' LIMIT 1`;
    const { rows: userRows } = await execSql(userSql);
    
    if (!userRows || userRows.length === 0) {
      return res.status(404).json({ message: "User not found" });
    }

    // Hash new password and update
    const hashedPassword = bcrypt.hashSync(newPassword, 10);
    const updateSql = `UPDATE USERS SET PASSWORD='${hashedPassword}' WHERE EMAIL='${esc(normalizedEmail)}'`;
    await execSql(updateSql);

    // Delete the OTP from store after successful reset
    delete global.passwordResetStore[normalizedEmail];

    safeLogWithRequest(req, {
      action: "reset_password_with_otp",
      description: "Password reset successfully",
      resourceId: normalizedEmail,
      resourceType: "user",
    });

    res.json({ message: "Password reset successfully" });
  } catch (err) {
    console.error("[resetPasswordWithOTP] error:", err.message);
    res.status(500).json({ error: err.message });
  }
};
