import connection from "../config/snowflake.js";

let trashSchemaEnsured = false;

const ensureTrashTable = () =>
  new Promise((resolve, reject) => {
    if (trashSchemaEnsured) return resolve();

    const sql = `
      CREATE TABLE IF NOT EXISTS TRASH (
        ID VARCHAR(16777216) NOT NULL,
        ITEM_TYPE VARCHAR(16777216),
        ITEM_NAME VARCHAR(16777216),
        DELETED_BY VARCHAR(16777216),
        DELETED_AT TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
        RESTORED BOOLEAN DEFAULT FALSE,
        FOLDER_ID VARCHAR(16777216),
        FILE_URL VARCHAR(16777216),
        FILE_SIZE NUMBER(38,0),
        FILE_TYPE VARCHAR(16777216),
        UPLOADED_BY VARCHAR(16777216),
        UPLOADED_AT TIMESTAMP_LTZ,
        CREATED_BY VARCHAR(16777216),
        CREATED_AT TIMESTAMP_LTZ,
        PRIMARY KEY (ID)
      )
    `;

    connection.execute({
      sqlText: sql,
      complete: (err) => {
        if (err) {
          try { err.message = `${err.message} | SQL: ${sql}`; } catch (e) {}
          return reject(err);
        }
        trashSchemaEnsured = true;
        resolve();
      },
    });
  });

export default ensureTrashTable;


