import nodemailer from "nodemailer";
import dotenv from "dotenv";
dotenv.config();

// Create a transporter object only when credentials are present
// Support both MAIL_USER/MAIL_PASS and legacy EMAIL_USER/EMAIL_PASS env names
const MAIL_USER = process.env.MAIL_USER || process.env.EMAIL_USER || "";
const MAIL_PASS = process.env.MAIL_PASS || process.env.EMAIL_PASS || "";

let transporter = null;
let isMailerConfigured = false;
if (MAIL_USER && MAIL_PASS) {
  // Prefer explicit SMTP host/port if provided (works for Outlook/Gmail SMTP)
  const MAIL_HOST = process.env.MAIL_HOST || process.env.SMTP_HOST || "";
  const MAIL_PORT = parseInt(process.env.MAIL_PORT || process.env.SMTP_PORT || "0", 10) || 0;

  if (MAIL_HOST && MAIL_PORT) {
    transporter = nodemailer.createTransport({
      host: MAIL_HOST,
      port: MAIL_PORT,
      secure: MAIL_PORT === 465, // true for 465, false for other ports
      auth: {
        user: MAIL_USER,
        pass: MAIL_PASS,
      },
      tls: {
        rejectUnauthorized: false,
      },
      connectionTimeout: 10000, // 10 seconds timeout
      greetingTimeout: 10000,
      socketTimeout: 10000,
    });
  } else {
    // Fallback to a service-based transport (e.g., gmail) if no host/port provided
    transporter = nodemailer.createTransport({
      service: process.env.MAIL_SERVICE || "gmail",
      auth: {
        user: MAIL_USER,
        pass: MAIL_PASS,
      },
      connectionTimeout: 10000,
      greetingTimeout: 10000,
      socketTimeout: 10000,
    });
  }
  isMailerConfigured = true;
} else {
  console.warn("Mailer not configured: set MAIL_USER and MAIL_PASS to enable email sending");
}

// Send email function (safe: no-op if mailer not configured)
export const sendMail = async (to, subject, html) => {
  if (!isMailerConfigured) {
    console.warn("sendMail skipped (mailer not configured)", to, subject);
    return { success: false, error: "Mailer not configured" };
  }

  try {
    const info = await transporter.sendMail({
      from: MAIL_USER,
      to,
      subject,
      html,
    });
    console.log("✅ Email sent successfully:", info.messageId);
    return { success: true, messageId: info.messageId };
  } catch (error) {
    // Log error but don't throw - email failures shouldn't break the app
    const errorMsg = error && error.message ? error.message : String(error);
    console.error("❌ Error sending email to", to, ":", errorMsg);
    return { success: false, error: errorMsg };
  }
};

export { transporter, isMailerConfigured };
 