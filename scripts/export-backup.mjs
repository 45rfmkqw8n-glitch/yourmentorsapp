import fs from 'fs/promises';
import path from 'path';
import { fileURLToPath } from 'url';
import { pool } from '../src/db.js';

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const backupDir = path.join(__dirname, '..', 'backups');
const portable = process.argv.includes('--portable') || process.env.BACKUP_PORTABLE === '1';
const backupFile = path.join(
  backupDir,
  portable ? 'mentors_crm_portable.sql' : 'mentors_crm_backup.sql'
);

const formatDateTime = (value) => {
  if (!value) return 'NULL';
  if (value instanceof Date) {
    return value.toISOString().slice(0, 19).replace('T', ' ');
  }
  const parsed = new Date(value);
  if (!Number.isNaN(parsed.getTime())) {
    return parsed.toISOString().slice(0, 19).replace('T', ' ');
  }
  return String(value);
};

const escapeSql = (value) => {
  if (value === null || value === undefined) return 'NULL';
  if (value instanceof Date) {
    return `'${formatDateTime(value)}'`;
  }
  if (typeof value === 'boolean') {
    return value ? '1' : '0';
  }
  if (typeof value === 'number') {
    return Number.isFinite(value) ? String(value) : 'NULL';
  }
  if (Buffer.isBuffer(value)) {
    return `X'${value.toString('hex')}'`;
  }
  const text = typeof value === 'object' ? JSON.stringify(value) : String(value);
  const escaped = text
    .replace(/\\/g, '\\\\')
    .replace(/\u0000/g, '\\0')
    .replace(/\n/g, '\\n')
    .replace(/\r/g, '\\r')
    .replace(/\u001a/g, '\\Z')
    .replace(/'/g, "''");
  return `'${escaped}'`;
};

const chunk = (array, size) => {
  const output = [];
  for (let index = 0; index < array.length; index += size) {
    output.push(array.slice(index, index + size));
  }
  return output;
};

const getTables = async () => {
  const [rows] = await pool.query(
    'SELECT table_name FROM information_schema.tables WHERE table_schema = DATABASE() ORDER BY table_name'
  );
  return rows.map((row) => String(Object.values(row)[0]));
};

const getCreateStatement = async (table) => {
  const [rows] = await pool.query(`SHOW CREATE TABLE \`${table}\``);
  const row = rows[0] ?? {};
  return row['Create Table'] ?? Object.values(row)[1] ?? '';
};

const buildInsertStatements = (table, rows) => {
  if (!rows.length) return [];
  const columns = Object.keys(rows[0]);
  const statements = [];
  for (const group of chunk(rows, 100)) {
    const values = group
      .map((row) => `(${columns.map((column) => escapeSql(row[column])).join(', ')})`)
      .join(',\n  ');
    statements.push(
      `INSERT INTO \`${table}\` (${columns.map((column) => `\`${column}\``).join(', ')}) VALUES\n  ${values};`
    );
  }
  return statements;
};

const main = async () => {
  await fs.mkdir(backupDir, { recursive: true });

  const tables = await getTables();
  const lines = [];
  lines.push('-- Mentors CRM full SQL backup');
  lines.push(`-- Generated at ${new Date().toISOString()}`);
  lines.push('SET NAMES utf8mb4;');
  lines.push('SET FOREIGN_KEY_CHECKS = 0;');
  if (!portable) {
    lines.push('DROP DATABASE IF EXISTS `mentors_crm`;');
    lines.push('CREATE DATABASE `mentors_crm` CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci;');
    lines.push('USE `mentors_crm`;');
  }
  lines.push('');

  for (const table of tables) {
    lines.push(`DROP TABLE IF EXISTS \`${table}\`;`);
  }
  lines.push('');

  for (const table of tables) {
    const createStatement = await getCreateStatement(table);
    lines.push(`${createStatement};`);
    lines.push('');
  }

  for (const table of tables) {
    const [rows] = await pool.query(`SELECT * FROM \`${table}\``);
    const statements = buildInsertStatements(table, rows);
    if (statements.length > 0) {
      lines.push(...statements);
      lines.push('');
    }
  }

  lines.push('SET FOREIGN_KEY_CHECKS = 1;');

  await fs.writeFile(backupFile, lines.join('\n'), 'utf8');
  console.log(`Backup written to ${backupFile}`);

  await pool.end();
};

main().catch(async (error) => {
  console.error(`Backup export failed: ${error.message}`);
  await pool.end().catch(() => {});
  process.exitCode = 1;
});
