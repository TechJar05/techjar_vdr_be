// backend/utils/sendMail.js
import { sendMail } from "../config/mailer.js";

// Re-export the central sendMail (it is a safe no-op when mailer is not configured)
export { sendMail };
