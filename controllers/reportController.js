import connection from "../config/snowflake.js";

// Helper: wrap connection.execute in Promise
const execSql = (sql) =>
  new Promise((resolve, reject) => {
    connection.execute({
      sqlText: sql,
      complete: (err, stmt, rows) => {
        if (err) return reject(err);
        resolve({ stmt, rows });
      },
    });
  });

// Helper: escape single quotes for SQL
const esc = (s = "") => String(s).replace(/'/g, "''");

/**
 * GET FILES REPORT - List all files with metadata (created by, views, downloads, shares, location, created/modified date)
 * Ordered by creation date (most recent first)
 */
export const getFilesReport = async (req, res) => {
  try {
    const userEmail = req.user?.email;
    if (!userEmail) return res.status(400).json({ error: "User email missing" });

    // Create or ensure FILES table has necessary metadata columns
    try {
      const alterViewsSql = `ALTER TABLE FILES ADD COLUMN IF NOT EXISTS VIEWS_COUNT NUMBER DEFAULT 0`;
      await execSql(alterViewsSql);
      const alterDownloadsSql = `ALTER TABLE FILES ADD COLUMN IF NOT EXISTS DOWNLOADS_COUNT NUMBER DEFAULT 0`;
      await execSql(alterDownloadsSql);
      const alterSharesSql = `ALTER TABLE FILES ADD COLUMN IF NOT EXISTS SHARES_COUNT NUMBER DEFAULT 0`;
      await execSql(alterSharesSql);
    } catch (alterErr) {
      // Columns might already exist, ignore
    }

    // Detect which FILES/FOLDERS columns exist and construct a safe SELECT
    const colExists = async (table, column) => {
      const sql = `SELECT COUNT(*) AS CNT FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_NAME='${table.toUpperCase()}' AND COLUMN_NAME='${column.toUpperCase()}' AND TABLE_SCHEMA = CURRENT_SCHEMA()`;
      try {
        const { rows } = await execSql(sql);
        return rows && rows.length > 0 && Number(rows[0].CNT || rows[0].COUNT || 0) > 0;
      } catch (e) {
        return false;
      }
    };

    const f_has_file_size = await colExists('FILES', 'FILE_SIZE');
    const f_has_file_size_mb = await colExists('FILES', 'FILE_SIZE_MB');
    const f_has_file_type = await colExists('FILES', 'FILE_TYPE');
    const f_has_created_by = await colExists('FILES', 'CREATED_BY');
    const f_has_uploaded_by = await colExists('FILES', 'UPLOADED_BY');
    const f_has_created_at = await colExists('FILES', 'CREATED_AT');
    const f_has_uploaded_at = await colExists('FILES', 'UPLOADED_AT');
    const f_has_modified_at = await colExists('FILES', 'MODIFIED_AT');
    const f_has_updated_at = await colExists('FILES', 'UPDATED_AT');
    const f_has_views = await colExists('FILES', 'VIEWS_COUNT');
    const f_has_downloads = await colExists('FILES', 'DOWNLOADS_COUNT');
    const f_has_shares = await colExists('FILES', 'SHARES_COUNT');

    const fo_has_name = await colExists('FOLDERS', 'NAME');
    const fo_has_folder_name = await colExists('FOLDERS', 'FOLDER_NAME');

    const selectCols = [];
    selectCols.push('F.ID');
    selectCols.push('F.FILE_NAME');
    if (f_has_file_size) selectCols.push('F.FILE_SIZE AS FILE_SIZE');
    else if (f_has_file_size_mb) selectCols.push('F.FILE_SIZE_MB AS FILE_SIZE');
    else selectCols.push('0 AS FILE_SIZE');

    if (f_has_file_type) selectCols.push("F.FILE_TYPE AS FILE_TYPE");
    else selectCols.push("'' AS FILE_TYPE");

    selectCols.push('F.FOLDER_ID');

    // CREATED_BY
    if (f_has_created_by && f_has_uploaded_by) selectCols.push('COALESCE(F.CREATED_BY, F.UPLOADED_BY) AS CREATED_BY');
    else if (f_has_created_by) selectCols.push('F.CREATED_BY AS CREATED_BY');
    else if (f_has_uploaded_by) selectCols.push('F.UPLOADED_BY AS CREATED_BY');
    else selectCols.push("'' AS CREATED_BY");

    // CREATED_AT
    if (f_has_created_at && f_has_uploaded_at) selectCols.push('COALESCE(F.CREATED_AT, F.UPLOADED_AT) AS CREATED_AT');
    else if (f_has_created_at) selectCols.push('F.CREATED_AT AS CREATED_AT');
    else if (f_has_uploaded_at) selectCols.push('F.UPLOADED_AT AS CREATED_AT');
    else selectCols.push('NULL AS CREATED_AT');

    // MODIFIED_AT
    if (f_has_modified_at) selectCols.push('F.MODIFIED_AT AS MODIFIED_AT');
    else if (f_has_updated_at) selectCols.push('F.UPDATED_AT AS MODIFIED_AT');
    else selectCols.push('NULL AS MODIFIED_AT');

    if (f_has_views) selectCols.push('COALESCE(F.VIEWS_COUNT,0) AS VIEWS_COUNT');
    else selectCols.push('0 AS VIEWS_COUNT');

    if (f_has_downloads) selectCols.push('COALESCE(F.DOWNLOADS_COUNT,0) AS DOWNLOADS_COUNT');
    else selectCols.push('0 AS DOWNLOADS_COUNT');

    if (f_has_shares) selectCols.push('COALESCE(F.SHARES_COUNT,0) AS SHARES_COUNT');
    else selectCols.push('0 AS SHARES_COUNT');

    // Folder name
    if (fo_has_name && fo_has_folder_name) selectCols.push("COALESCE(FO.NAME, FO.FOLDER_NAME, '') AS FOLDER_NAME");
    else if (fo_has_name) selectCols.push("FO.NAME AS FOLDER_NAME");
    else if (fo_has_folder_name) selectCols.push("FO.FOLDER_NAME AS FOLDER_NAME");
    else selectCols.push("'' AS FOLDER_NAME");

    // Build ORDER BY using available date column (avoid COALESCE unless both columns exist)
    let orderBy = 'F.ID';
    if (f_has_created_at && f_has_uploaded_at) orderBy = 'COALESCE(F.CREATED_AT, F.UPLOADED_AT)';
    else if (f_has_created_at) orderBy = 'F.CREATED_AT';
    else if (f_has_uploaded_at) orderBy = 'F.UPLOADED_AT';

    const filesSql = `SELECT ${selectCols.join(',\n        ')} FROM FILES F LEFT JOIN FOLDERS FO ON F.FOLDER_ID = FO.ID ORDER BY ${orderBy} DESC`;

    const { rows: files } = await execSql(filesSql);

    // Fetch file share history for the "shared with" details
    const sharesSql = `
      SELECT * FROM FILE_SHARES
      ORDER BY SHARED_AT DESC
    `;
    let sharesData = [];
    try {
      const { rows: shares } = await execSql(sharesSql);
      sharesData = shares || [];
    } catch (shareErr) {
      // FILE_SHARES table might not exist yet
      sharesData = [];
    }

    // Enrich files with share details
    const enrichedFiles = (files || []).map((file) => {
      const fileShares = sharesData.filter((s) => s.FILE_ID === file.ID || s.FILE_ID === file.id);
      return {
        ...file,
        SHARES: fileShares,
      };
    });

    return res.json(enrichedFiles || []);
  } catch (err) {
    console.error("[getFilesReport] error:", err.message);
    return res.status(500).json({ error: err.message });
  }
};

/**
 * GET FILE SHARE REPORT - List all files that have been shared (via approved ACCESS_REQUESTS)
 * A file is "shared" when it has approved access requests from other users.
 * Share types include: VIEW, DOWNLOAD, UPLOAD, COMMENT, etc.
 * Returns: files grouped by ID with count of approved share requests.
 */
export const getFileShareReport = async (req, res) => {
  try {
    const userEmail = req.user?.email;
    if (!userEmail) return res.status(400).json({ error: "User email missing" });

    // Detect which FILES/FOLDERS columns exist
    const colExists = async (table, column) => {
      const sql = `SELECT COUNT(*) AS CNT FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_NAME='${table.toUpperCase()}' AND COLUMN_NAME='${column.toUpperCase()}' AND TABLE_SCHEMA = CURRENT_SCHEMA()`;
      try {
        const { rows } = await execSql(sql);
        return rows && rows.length > 0 && Number(rows[0].CNT || rows[0].COUNT || 0) > 0;
      } catch (e) {
        return false;
      }
    };

    const f_has_file_size = await colExists('FILES', 'FILE_SIZE');
    const f_has_file_size_mb = await colExists('FILES', 'FILE_SIZE_MB');
    const f_has_file_type = await colExists('FILES', 'FILE_TYPE');
    const f_has_created_by = await colExists('FILES', 'CREATED_BY');
    const f_has_uploaded_by = await colExists('FILES', 'UPLOADED_BY');
    const f_has_created_at = await colExists('FILES', 'CREATED_AT');
    const f_has_uploaded_at = await colExists('FILES', 'UPLOADED_AT');
    const fo_has_name = await colExists('FOLDERS', 'NAME');
    const fo_has_folder_name = await colExists('FOLDERS', 'FOLDER_NAME');
    const f_has_downloads = await colExists('FILES', 'DOWNLOADS_COUNT');
    const f_has_views = await colExists('FILES', 'VIEWS_COUNT');

    const fileSizeExpr = f_has_file_size ? 'F.FILE_SIZE' : f_has_file_size_mb ? 'F.FILE_SIZE_MB' : '0';
    const fileTypeExpr = f_has_file_type ? 'F.FILE_TYPE' : "''";
    const createdByExpr = f_has_created_by && f_has_uploaded_by ? 'COALESCE(F.CREATED_BY, F.UPLOADED_BY)' : f_has_created_by ? 'F.CREATED_BY' : f_has_uploaded_by ? 'F.UPLOADED_BY' : "''";
    const createdAtExpr = f_has_created_at && f_has_uploaded_at ? 'COALESCE(F.CREATED_AT, F.UPLOADED_AT)' : f_has_created_at ? 'F.CREATED_AT' : f_has_uploaded_at ? 'F.UPLOADED_AT' : 'NULL';
    const folderNameExpr = fo_has_name && fo_has_folder_name ? "COALESCE(FO.NAME, FO.FOLDER_NAME, '')" : fo_has_name ? 'FO.NAME' : fo_has_folder_name ? 'FO.FOLDER_NAME' : "''";
    const viewsExpr = f_has_views ? 'COALESCE(F.VIEWS_COUNT,0)' : '0';
    const downloadsExpr = f_has_downloads ? 'COALESCE(F.DOWNLOADS_COUNT,0)' : '0';

    // Query: Files with approved access requests (shares) grouped by file
    // SHARE_COUNT = number of DISTINCT USERS the file is shared with (not the number of access requests)
    const fileShareSql = `
      SELECT
        F.ID,
        MAX(F.FILE_NAME) AS FILE_NAME,
        MAX(${fileSizeExpr}) AS FILE_SIZE,
        MAX(${fileTypeExpr}) AS FILE_TYPE,
        MAX(F.FOLDER_ID) AS FOLDER_ID,
        MAX(${createdByExpr}) AS CREATED_BY,
        MAX(${createdAtExpr}) AS CREATED_AT,
        MAX(${folderNameExpr}) AS FOLDER_NAME,
          MAX(${viewsExpr}) AS VIEWS_COUNT,
          MAX(${downloadsExpr}) AS DOWNLOADS_COUNT,
          COUNT(DISTINCT AR.USER_EMAIL) AS SHARE_COUNT,
          MAX(AR.APPROVED_AT) AS LAST_SHARED_AT
      FROM FILES F
      LEFT JOIN FOLDERS FO ON F.FOLDER_ID = FO.ID
      LEFT JOIN ACCESS_REQUESTS AR ON F.ID = AR.ITEM_ID AND AR.ITEM_TYPE = 'file' AND AR.STATUS = 'approved'
      GROUP BY F.ID
      HAVING COUNT(DISTINCT AR.USER_EMAIL) > 0
      ORDER BY SHARE_COUNT DESC, MAX(AR.APPROVED_AT) DESC
    `;

    const { rows: sharedFiles } = await execSql(fileShareSql);

    // Fetch all approved access requests for files to enrich with share details
    const allSharesSql = `
      SELECT * FROM ACCESS_REQUESTS
      WHERE ITEM_TYPE = 'file' AND STATUS = 'approved'
      ORDER BY APPROVED_AT DESC
    `;
    let allShares = [];
    try {
      const { rows: shares } = await execSql(allSharesSql);
      allShares = shares || [];
    } catch (shareErr) {
      console.warn("[getFileShareReport] Could not fetch detailed shares:", shareErr.message);
      allShares = [];
    }

    // Enrich files with detailed share data (approved requests)
    const enrichedSharedFiles = (sharedFiles || []).map((file) => {
      const fileShares = allShares.filter((s) => s.ITEM_ID === file.ID);
      return {
        ...file,
        SHARES: fileShares, // List of approved access requests (who requested, when approved, what access type, approved by whom)
      };
    });

    return res.json(enrichedSharedFiles || []);
  } catch (err) {
    console.error("[getFileShareReport] error:", err.message);
    return res.status(500).json({ error: err.message });
  }
};

/**
 * GET FILE ACTIVITY - List all activities related to a specific file
 * Includes:
 * - File metadata (name, size, type, created by, created at)
 * - Share activity: approved access requests showing who got access, what type (VIEW/DOWNLOAD/etc), when it was approved, and who approved it
 */
export const getFileActivity = async (req, res) => {
  try {
    const { fileId } = req.params;
    const userEmail = req.user?.email;
    if (!userEmail) return res.status(400).json({ error: "User email missing" });

    // Fetch file details
    const fileSql = `SELECT * FROM FILES WHERE ID='${esc(fileId)}' LIMIT 1`;
    const { rows: fileRows } = await execSql(fileSql);
    if (!fileRows || fileRows.length === 0) {
      return res.status(404).json({ error: "File not found" });
    }

    const file = fileRows[0];

    // Fetch all approved access requests (shares) for this file
    // A "share" is when an admin approves a user's request for VIEW/DOWNLOAD/etc access
    const shareActivitySql = `
      SELECT 
        ID,
        USER_EMAIL AS REQUESTED_BY,
        ACCESS_TYPES,
        APPROVED_BY,
        APPROVED_AT,
        REQUESTED_AT
      FROM ACCESS_REQUESTS
      WHERE ITEM_ID='${esc(fileId)}' AND ITEM_TYPE='file' AND STATUS='approved'
      ORDER BY APPROVED_AT DESC
    `;
    let shareActivity = [];
    try {
      const { rows: shares } = await execSql(shareActivitySql);
      shareActivity = shares || [];
    } catch (shareErr) {
      console.warn("[getFileActivity] Could not fetch share activity:", shareErr.message);
      shareActivity = [];
    }

    // Calculate counts from share activity
    const accessTypeCounts = {};
    const uniqueUsers = new Set();
    const allAccessTypes = new Set();
    shareActivity.forEach((share) => {
      // Track unique users
      uniqueUsers.add(share.REQUESTED_BY);
      
      const types = (share.ACCESS_TYPES || "").split(",").map((t) => t.trim()).filter(Boolean);
      types.forEach((type) => {
        accessTypeCounts[type] = (accessTypeCounts[type] || 0) + 1;
        allAccessTypes.add(type);
      });
    });

    return res.json({
      file,
      shareActivity, // List of approved access requests: who requested, what access type, when approved, who approved
      sharesCount: uniqueUsers.size, // Number of DISTINCT USERS the file is shared with
      accessTypeCounts, // Breakdown by access type: { VIEW: 2, DOWNLOAD: 3, ... }
      viewsCount: file.VIEWS_COUNT || 0,
      downloadsCount: file.DOWNLOADS_COUNT || 0,
    });
  } catch (err) {
    console.error("[getFileActivity] error:", err.message);
    return res.status(500).json({ error: err.message });
  }
};
