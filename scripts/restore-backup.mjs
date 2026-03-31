import fs from 'fs/promises';
import path from 'path';
import { fileURLToPath } from 'url';
import mysql from 'mysql2/promise';
import dotenv from 'dotenv';

dotenv.config();

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const defaultBackup = path.join(__dirname, '..', 'backups', 'mentors_crm_portable.sql');
const backupFile = process.argv[2] ? path.resolve(process.argv[2]) : defaultBackup;

const connectionOptions = {
  host: process.env.DB_HOST,
  port: Number(process.env.DB_PORT || 3306),
  user: process.env.DB_USER,
  password: process.env.DB_PASSWORD,
  database: process.env.DB_NAME,
  multipleStatements: true,
};

const main = async () => {
  const sql = await fs.readFile(backupFile, 'utf8');
  const connection = await mysql.createConnection(connectionOptions);
  try {
    await connection.query(sql);
    console.log(`Restored database from ${backupFile}`);
  } finally {
    await connection.end();
  }
};

main().catch((error) => {
  console.error(`Restore failed: ${error.message}`);
  process.exitCode = 1;
});
