import express from 'express';
import cors from 'cors';
import dotenv from 'dotenv';
import bcrypt from 'bcryptjs';
import jwt from 'jsonwebtoken';
import fs from 'fs/promises';
import path from 'path';
import { fileURLToPath } from 'url';
import { pool } from './db.js';

dotenv.config();

const app = express();
const __dirname = path.dirname(fileURLToPath(import.meta.url));
app.use(cors());
app.use(express.json({ limit: '25mb' }));

// Root health check for uptime monitors & load balancers
app.get('/health', async (req, res) => {
  try {
    await pool.query('SELECT 1');
    res.json({ status: 'healthy', timestamp: new Date().toISOString() });
  } catch (error) {
    res.status(500).json({ status: 'unhealthy', error: error.message });
  }
});

const toBool = (value) => (value ? 1 : 0);
const nowTs = () => new Date().toISOString().slice(0, 19).replace('T', ' ');
const defaultUserImage = 'https://www.pngitem.com/pimgs/m/35-350426_profile-icon-png-default-profile-picture-png-transparent.png';
const JWT_SECRET = process.env.JWT_SECRET || 'mentors-dev-secret';
const JWT_EXPIRES_IN = process.env.JWT_EXPIRES_IN || '7d';
const SUPER_ROLES = new Set(['admin', 'franchise_owner']);
const AUTH_PUBLIC_ROUTES = new Set(['/health', '/auth/login']);

const normalizeRole = (value) => String(value ?? 'agent').toLowerCase();

const isSuperUser = (user) => SUPER_ROLES.has(normalizeRole(user?.role));

const hashPassword = async (password) => bcrypt.hash(String(password ?? ''), 10);

const verifyPassword = async (plainPassword, storedPassword) => {
  const plain = String(plainPassword ?? '');
  const stored = String(storedPassword ?? '');
  if (!stored) return false;
  if (stored.startsWith('$2a$') || stored.startsWith('$2b$') || stored.startsWith('$2y$')) {
    return bcrypt.compare(plain, stored);
  }
  return plain === stored;
};

const issueToken = (userId, role) =>
  jwt.sign({ sub: Number(userId), role: normalizeRole(role) }, JWT_SECRET, {
    expiresIn: JWT_EXPIRES_IN,
  });

const selectUserColumns = `
  id, name, email, phone, password, role, title, department, department_id, jobtitle_id,
  parent_id, status, image, created_at, updated_at
`;

const toSqlDateTime = (value, fallback = nowTs()) => {
  if (value === undefined || value === null || value === '') return fallback;
  if (value instanceof Date) {
    return value.toISOString().slice(0, 19).replace('T', ' ');
  }
  if (typeof value === 'string') {
    const trimmed = value.trim();
    if (!trimmed) return fallback;
    if (/^\d{4}-\d{2}-\d{2}$/.test(trimmed)) {
      return `${trimmed} 00:00:00`;
    }
    const normalized = trimmed.replace('T', ' ').replace('Z', '');
    if (/^\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}/.test(normalized)) {
      return normalized.slice(0, 19);
    }
    const parsed = new Date(trimmed);
    if (!Number.isNaN(parsed.getTime())) {
      return parsed.toISOString().slice(0, 19).replace('T', ' ');
    }
  }
  const parsed = new Date(value);
  if (!Number.isNaN(parsed.getTime())) {
    return parsed.toISOString().slice(0, 19).replace('T', ' ');
  }
  return fallback;
};

const toSqlDateOnly = (value, fallback = null) => {
  if (value === undefined || value === null || value === '') return fallback;
  if (value instanceof Date) {
    return value.toISOString().slice(0, 10);
  }
  if (typeof value === 'string') {
    const trimmed = value.trim();
    if (!trimmed) return fallback;
    if (/^\d{4}-\d{2}-\d{2}$/.test(trimmed)) {
      return trimmed.slice(0, 10);
    }
    const parsed = new Date(trimmed);
    if (!Number.isNaN(parsed.getTime())) {
      return parsed.toISOString().slice(0, 10);
    }
  }
  const parsed = new Date(value);
  if (!Number.isNaN(parsed.getTime())) {
    return parsed.toISOString().slice(0, 10);
  }
  return fallback;
};

const toClientDateTime = (value) => {
  if (!value) return '';
  const parsed = value instanceof Date ? value : new Date(value);
  return Number.isNaN(parsed.getTime()) ? String(value) : parsed.toISOString();
};

const toClientDateOnly = (value) => {
  const iso = toClientDateTime(value);
  return iso ? iso.slice(0, 10) : '';
};

const parseJsonArray = (value) => {
  if (Array.isArray(value)) {
    return value;
  }
  if (!value) {
    return [];
  }
  if (typeof value !== 'string') {
    return [];
  }
  try {
    const parsed = JSON.parse(value);
    return Array.isArray(parsed) ? parsed : [];
  } catch {
    return [];
  }
};

const serializeUser = (row) => ({
  id: row.id,
  name: row.name,
  email: row.email,
  phone: row.phone ?? '',
  role: row.role ?? 'agent',
  title: row.title ?? undefined,
  department: row.department ?? undefined,
  departmentId: row.department_id ?? undefined,
  jobtitleId: row.jobtitle_id ?? undefined,
  teamLeaderId: row.parent_id ?? undefined,
  avatar: row.image ?? undefined,
  isActive: Number(row.status ?? row.isActive ?? 0) === 1,
  createdAt: toClientDateTime(row.created_at),
});

const buildInClause = (values) => {
  if (!values || values.length === 0) {
    return { clause: 'NULL', values: [] };
  }
  return {
    clause: values.map(() => '?').join(', '),
    values,
  };
};

const assignMissingIds = async (table, rows) => {
  if (!rows.length) {
    return [];
  }
  const [maxRows] = await pool.query(`SELECT COALESCE(MAX(id), 0) AS maxId FROM ${table}`);
  let nextId = Number(maxRows[0]?.maxId ?? 0) + 1;
  const usedIds = new Set();

  return rows.map((row) => {
    const candidateId = Number(row.id);
    const hasValidId = Number.isInteger(candidateId) && candidateId > 0 && !usedIds.has(candidateId);
    const id = hasValidId ? candidateId : nextId++;
    usedIds.add(id);
    return {
      ...row,
      id,
    };
  });
};

const nextTableId = async (table) => {
  const [rows] = await pool.query(`SELECT COALESCE(MAX(id), 0) AS maxId FROM ${table}`);
  return Number(rows[0]?.maxId ?? 0) + 1;
};

const getStatusCatalog = async () => {
  const [rows] = await pool.query('SELECT id, name FROM statuses ORDER BY id ASC');
  return rows
    .map((row) => ({
      id: Number(row.id),
      name: String(row.name ?? '').trim() || `Status ${row.id}`,
    }))
    .filter((row) => Number.isInteger(row.id) && row.id > 0);
};

const resolveLeadStatus = async ({ statusId, status } = {}) => {
  const catalog = await getStatusCatalog();
  const defaultStatus = catalog[0] ?? { id: 1, name: 'New Lead' };
  const byId = statusId !== undefined && statusId !== null
    ? catalog.find((row) => row.id === Number(statusId))
    : null;
  if (byId) {
    return { statusId: byId.id, status: byId.name };
  }
  const byName = status
    ? catalog.find((row) => row.name.toLowerCase() === String(status).trim().toLowerCase())
    : null;
  if (byName) {
    return { statusId: byName.id, status: byName.name };
  }
  return {
    statusId: Number.isInteger(Number(statusId)) && Number(statusId) > 0 ? Number(statusId) : defaultStatus.id,
    status: String(status ?? defaultStatus.name ?? 'New Lead').trim() || defaultStatus.name || 'New Lead',
  };
};

const resolveColdCallStatusId = async (statusId) => {
  const catalog = await getStatusCatalog();
  const defaultStatus = catalog[0] ?? { id: 1, name: 'New Lead' };
  const byId = statusId !== undefined && statusId !== null
    ? catalog.find((row) => row.id === Number(statusId))
    : null;
  return byId?.id ?? defaultStatus.id;
};

const lookupNameById = async (table, id) => {
  if (id === undefined || id === null || id === '') return '';
  const [rows] = await pool.query(`SELECT name FROM ${table} WHERE id = ? LIMIT 1`, [id]);
  return String(rows[0]?.name ?? '').trim();
};

const reportColumnsAll = [
  'id',
  'comment_id',
  'lead_id',
  'cold_call_id',
  'entity_type',
  'entity_id',
  'client_name',
  'client_phone',
  'agent_id',
  'agent_name',
  'team_leader_id',
  'team_leader_name',
  'status_id',
  'status_name',
  'project_id',
  'project_name',
  'developer_id',
  'developer_name',
  'content',
  'created_by',
  'created_by_id',
  'created_at',
  'updated_at',
];

const getUserSummaryById = async (id) => {
  if (id === undefined || id === null || id === '') return null;
  const [rows] = await pool.query('SELECT id, name, parent_id, role FROM users WHERE id = ? LIMIT 1', [id]);
  return rows[0] ?? null;
};

const getLeadSummaryById = async (id) => {
  if (id === undefined || id === null || id === '') return null;
  const [rows] = await pool.query(
    `SELECT leads.*, statuses.name AS status_name,
            projects.name AS project_name, projects.id AS project_id,
            COALESCE(projects.developer_id, projects.developerId) AS developer_id,
            developers.name AS developer_name
     FROM leads
     LEFT JOIN statuses ON statuses.id = leads.status_id
     LEFT JOIN projects ON projects.id = leads.project_id
     LEFT JOIN developers ON developers.id = COALESCE(projects.developer_id, projects.developerId)
     WHERE leads.id = ? LIMIT 1`,
    [id]
  );
  return rows[0] ?? null;
};

const getColdCallSummaryById = async (id) => {
  if (id === undefined || id === null || id === '') return null;
  const [rows] = await pool.query(
    `SELECT cold_calls.*, statuses.name AS status_name
     FROM cold_calls
     LEFT JOIN statuses ON statuses.id = cold_calls.status_id
     WHERE cold_calls.id = ? LIMIT 1`,
    [id]
  );
  return rows[0] ?? null;
};

const buildReportRowFromComment = async (commentRow) => {
  const commentId = Number(commentRow.id ?? 0);
  const entityType = String(commentRow.entity_type ?? (commentRow.cold_call_id ? 'cold_call' : 'lead')).toLowerCase() === 'cold_call'
    ? 'cold_call'
    : 'lead';
  const entityId = Number(commentRow.entity_id ?? commentRow.lead_id ?? commentRow.cold_call_id ?? commentRow.leadId ?? 0) || null;
  const content = String(commentRow.body ?? commentRow.content ?? '').trim();
  const createdById = Number(commentRow.user_id ?? commentRow.createdById ?? 0) || null;
  const createdBy = String(commentRow.createdBy ?? commentRow.user_name ?? 'Unknown').trim() || 'Unknown';
  const createdAt = toSqlDateTime(commentRow.created_at ?? commentRow.createdAt ?? nowTs());
  const updatedAt = toSqlDateTime(commentRow.updated_at ?? commentRow.updatedAt ?? createdAt);
  const agent = await getUserSummaryById(createdById);
  const teamLeader = await getUserSummaryById(agent?.parent_id ?? null);
  const sourceEntity = entityType === 'cold_call'
    ? await getColdCallSummaryById(entityId)
    : await getLeadSummaryById(entityId);

  return {
    comment_id: commentId,
    lead_id: entityType === 'lead' ? entityId : null,
    cold_call_id: entityType === 'cold_call' ? entityId : null,
    entity_type: entityType,
    entity_id: entityId,
    client_name: String(sourceEntity?.name ?? 'Unknown').trim() || 'Unknown',
    client_phone: String(sourceEntity?.phone ?? '').trim(),
    agent_id: createdById,
    agent_name: String(agent?.name ?? createdBy).trim() || createdBy,
    team_leader_id: teamLeader?.id ?? null,
    team_leader_name: String(teamLeader?.name ?? '').trim(),
    status_id: sourceEntity?.status_id ?? sourceEntity?.statusId ?? null,
    status_name: String(sourceEntity?.status_name ?? sourceEntity?.status ?? '').trim(),
    project_id: sourceEntity?.project_id ?? sourceEntity?.projectId ?? null,
    project_name: String(sourceEntity?.project_name ?? sourceEntity?.project ?? '').trim(),
    developer_id: sourceEntity?.developer_id ?? sourceEntity?.developerId ?? null,
    developer_name: String(sourceEntity?.developer_name ?? '').trim(),
    content,
    created_by: createdBy,
    created_by_id: createdById ?? 0,
    created_at: createdAt,
    updated_at: updatedAt,
  };
};

const rebuildReportsFromComments = async () => {
  const [commentRows] = await pool.query('SELECT * FROM comments ORDER BY id ASC');
  const reportRows = [];
  for (const commentRow of commentRows) {
    try {
      reportRows.push(await buildReportRowFromComment(commentRow));
    } catch (error) {
      console.warn(`Skipping report rebuild for comment ${commentRow?.id ?? 'unknown'}: ${error.message}`);
    }
  }
  const available = await getExistingColumns('reports');
  const columns = reportColumnsAll.filter((column) => available.has(column));
  await replaceTable(
    'reports',
    columns,
    reportRows.map((row) => columns.map((column) => row[column] ?? null))
  );
};

const loadAuthUser = async (userId) => {
  const [rows] = await pool.query(`SELECT ${selectUserColumns} FROM users WHERE id = ? LIMIT 1`, [userId]);
  return rows[0] ?? null;
};

const getAccessibleUserIds = async (user) => {
  if (!user) return [];
  if (isSuperUser(user)) {
    const [rows] = await pool.query('SELECT id FROM users');
    return rows.map((row) => Number(row.id));
  }

  const [rows] = await pool.query('SELECT id, parent_id FROM users WHERE status = 1 OR status IS NULL');
  const childrenByParent = new Map();

  rows.forEach((row) => {
    const parentId = row.parent_id ?? null;
    if (!childrenByParent.has(parentId)) {
      childrenByParent.set(parentId, []);
    }
    childrenByParent.get(parentId).push(Number(row.id));
  });

  const ids = new Set([Number(user.id)]);
  const stack = [Number(user.id)];

  while (stack.length > 0) {
    const current = stack.pop();
    const children = childrenByParent.get(current) ?? [];
    for (const child of children) {
      if (!ids.has(child)) {
        ids.add(child);
        stack.push(child);
      }
    }
  }

  return [...ids];
};

const hashIncomingPassword = async (password, fallback) => {
  const value = String(password ?? '').trim();
  if (!value) {
    return fallback ?? null;
  }
  if (value.startsWith('$2a$') || value.startsWith('$2b$') || value.startsWith('$2y$')) {
    return value;
  }
  return hashPassword(value);
};

const ensureColumn = async (table, column, definition) => {
  try {
    const [rows] = await pool.query('SHOW COLUMNS FROM ?? LIKE ?', [table, column]);
    if (rows.length === 0) {
      await pool.query(`ALTER TABLE ?? ADD COLUMN ?? ${definition}`, [table, column]);
    }
  } catch (error) {
    if (String(error?.message ?? '').includes("doesn't exist")) {
      return;
    }
    console.warn(`Schema check failed for ${table}.${column}: ${error.message}`);
  }
};

const ensureColumnDefinition = async (table, column, definition) => {
  try {
    const [rows] = await pool.query('SHOW COLUMNS FROM ?? LIKE ?', [table, column]);
    if (rows.length > 0) {
      await pool.query(`ALTER TABLE ?? MODIFY COLUMN ?? ${definition}`, [table, column]);
    }
  } catch (error) {
    if (String(error?.message ?? '').includes("doesn't exist")) {
      return;
    }
    console.warn(`Schema adjust failed for ${table}.${column}: ${error.message}`);
  }
};

const getExistingColumns = async (table) => {
  const [rows] = await pool.query('SHOW COLUMNS FROM ??', [table]);
  return new Set(rows.map(row => row.Field));
};

const pickAvailableColumns = async (table, preferredColumns) => {
  const available = await getExistingColumns(table);
  return [...new Set(preferredColumns.filter((column) => available.has(column)))];
};

const ensureSchema = async () => {
  await ensureColumn('users', 'parent_id', 'INT NULL');
  await ensureColumn('users', 'role', "VARCHAR(50) DEFAULT 'agent'");
  await ensureColumn('users', 'title', 'VARCHAR(255) NULL');
  await ensureColumn('users', 'department', 'VARCHAR(255) NULL');
  await ensureColumn('users', 'department_id', 'INT NULL');
  await ensureColumn('users', 'jobtitle_id', 'INT NULL');
  await ensureColumn('users', 'status', 'TINYINT(1) DEFAULT 1');
  await ensureColumn('users', 'image', 'LONGTEXT NULL');
  await ensureColumnDefinition('users', 'image', 'LONGTEXT NULL');
  await ensureColumn('users', 'phone', 'VARCHAR(50) NULL');
  await ensureColumn('users', 'password', 'VARCHAR(255) NULL');
  await ensureColumn('users', 'created_at', 'TIMESTAMP NULL');
  await ensureColumn('users', 'updated_at', 'TIMESTAMP NULL');
  await ensureColumn('users', 'isActive', 'TINYINT(1) DEFAULT 1');
  await ensureColumn('users', 'is_active', 'TINYINT(1) DEFAULT 1');
  await ensureColumnDefinition('users', 'isActive', 'TINYINT(1) DEFAULT 1');
  await ensureColumnDefinition('users', 'is_active', 'TINYINT(1) DEFAULT 1');

  await ensureColumn('projects', 'developer_id', 'INT NULL');
  await ensureColumn('projects', 'created_at', 'TIMESTAMP NULL');
  await ensureColumn('projects', 'updated_at', 'TIMESTAMP NULL');
  await ensureColumn('projects', 'developerId', 'INT NULL');
  await ensureColumnDefinition('projects', 'developerId', 'INT NULL');

  await ensureColumn('leads', 'whatsapp', 'VARCHAR(50) NULL');
  await ensureColumn('leads', 'workphone', 'VARCHAR(50) NULL');
  await ensureColumn('leads', 'type', "VARCHAR(50) NOT NULL DEFAULT 'Lead'");
  await ensureColumn('leads', 'name', 'VARCHAR(255) NOT NULL');
  await ensureColumn('leads', 'phone', 'VARCHAR(50) NOT NULL');
  await ensureColumn('leads', 'email', 'VARCHAR(255) NULL');
  await ensureColumn('leads', 'status', "VARCHAR(255) NOT NULL DEFAULT 'New Lead'");
  await ensureColumn('leads', 'statusId', 'INT NOT NULL DEFAULT 1');
  await ensureColumn('leads', 'source', 'VARCHAR(255) NULL');
  await ensureColumn('leads', 'sourceId', 'INT NULL');
  await ensureColumn('leads', 'project', 'VARCHAR(255) NULL');
  await ensureColumn('leads', 'projectId', 'INT NULL');
  await ensureColumn('leads', 'assignedTo', 'VARCHAR(255) NULL');
  await ensureColumn('leads', 'assignedToId', 'INT NULL');
  await ensureColumn('leads', 'teamLeader', 'VARCHAR(255) NULL');
  await ensureColumn('leads', 'teamLeaderId', 'INT NULL');
  await ensureColumn('leads', 'date', 'DATE NULL');
  await ensureColumn('leads', 'isPotential', 'TINYINT(1) NOT NULL DEFAULT 0');
  await ensureColumn('leads', 'project_id', 'INT NULL');
  await ensureColumn('leads', 'status_id', 'INT NULL');
  await ensureColumn('leads', 'source_id', 'INT NULL');
  await ensureColumn('leads', 'comment', 'TEXT NULL');
  await ensureColumn('leads', 'user_id', 'INT NULL');
  await ensureColumn('leads', 'created_at', 'TIMESTAMP NULL');
  await ensureColumn('leads', 'updated_at', 'TIMESTAMP NULL');

  await ensureColumn('cold_calls', 'status_id', 'INT NULL');
  await ensureColumn('cold_calls', 'user_id', 'INT NULL');
  await ensureColumn('cold_calls', 'notes', 'LONGTEXT NULL');
  await ensureColumn('cold_calls', 'created_at', 'TIMESTAMP NULL');
  await ensureColumn('cold_calls', 'updated_at', 'TIMESTAMP NULL');

  await ensureColumn('comments', 'leadId', 'INT NOT NULL DEFAULT 0');
  await ensureColumn('comments', 'content', 'TEXT NOT NULL');
  await ensureColumn('comments', 'createdBy', 'VARCHAR(255) NOT NULL DEFAULT \'Unknown\'');
  await ensureColumn('comments', 'createdById', 'INT NOT NULL DEFAULT 0');
  await ensureColumn('comments', 'createdAt', 'VARCHAR(50) NOT NULL DEFAULT \'\'');
  await ensureColumn('comments', 'lead_id', 'INT NULL');
  await ensureColumn('comments', 'cold_call_id', 'INT NULL');
  await ensureColumn('comments', 'entity_type', "VARCHAR(50) NULL");
  await ensureColumn('comments', 'entity_id', 'INT NULL');
  await ensureColumn('comments', 'body', 'TEXT NULL');
  await ensureColumn('comments', 'user_id', 'INT NULL');
  await ensureColumn('comments', 'created_at', 'TIMESTAMP NULL');
  await ensureColumn('comments', 'updated_at', 'TIMESTAMP NULL');

  await ensureColumn('reports', 'comment_id', 'INT NOT NULL');
  await ensureColumn('reports', 'lead_id', 'INT NULL');
  await ensureColumn('reports', 'cold_call_id', 'INT NULL');
  await ensureColumn('reports', 'entity_type', "VARCHAR(50) NOT NULL DEFAULT 'lead'");
  await ensureColumn('reports', 'entity_id', 'INT NULL');
  await ensureColumn('reports', 'client_name', "VARCHAR(255) NOT NULL DEFAULT 'Unknown'");
  await ensureColumn('reports', 'client_phone', 'VARCHAR(50) NULL');
  await ensureColumn('reports', 'agent_id', 'INT NULL');
  await ensureColumn('reports', 'agent_name', 'VARCHAR(255) NULL');
  await ensureColumn('reports', 'team_leader_id', 'INT NULL');
  await ensureColumn('reports', 'team_leader_name', 'VARCHAR(255) NULL');
  await ensureColumn('reports', 'status_id', 'INT NULL');
  await ensureColumn('reports', 'status_name', 'VARCHAR(255) NULL');
  await ensureColumn('reports', 'project_id', 'INT NULL');
  await ensureColumn('reports', 'project_name', 'VARCHAR(255) NULL');
  await ensureColumn('reports', 'developer_id', 'INT NULL');
  await ensureColumn('reports', 'developer_name', 'VARCHAR(255) NULL');
  await ensureColumn('reports', 'content', 'TEXT NOT NULL');
  await ensureColumn('reports', 'created_by', "VARCHAR(255) NOT NULL DEFAULT 'Unknown'");
  await ensureColumn('reports', 'created_by_id', 'INT NOT NULL DEFAULT 0');
  await ensureColumn('reports', 'created_at', 'TIMESTAMP NULL');
  await ensureColumn('reports', 'updated_at', 'TIMESTAMP NULL');

  await ensureColumn('deals', 'leadId', 'INT NOT NULL DEFAULT 0');
  await ensureColumn('deals', 'leadName', 'VARCHAR(255) NOT NULL DEFAULT \'Unknown\'');
  await ensureColumn('deals', 'clientPhone', 'VARCHAR(50) NULL');
  await ensureColumn('deals', 'agentId', 'INT NOT NULL DEFAULT 0');
  await ensureColumn('deals', 'agentName', 'VARCHAR(255) NOT NULL DEFAULT \'Unknown\'');
  await ensureColumn('deals', 'projectId', 'INT NOT NULL DEFAULT 0');
  await ensureColumn('deals', 'projectName', 'VARCHAR(255) NOT NULL DEFAULT \'Unknown\'');
  await ensureColumn('deals', 'developerId', 'INT NOT NULL DEFAULT 0');
  await ensureColumn('deals', 'developerName', 'VARCHAR(255) NOT NULL DEFAULT \'Unknown\'');
  await ensureColumn('deals', 'amount', 'DECIMAL(18,2) NOT NULL DEFAULT 0');
  await ensureColumn('deals', 'commission', 'DECIMAL(18,2) NOT NULL DEFAULT 0');
  await ensureColumn('deals', 'status', "VARCHAR(50) NOT NULL DEFAULT 'pending'");
  await ensureColumn('deals', 'reservationPrice', 'DECIMAL(18,2) NULL');
  await ensureColumn('deals', 'reservationDate', 'VARCHAR(50) NULL');
  await ensureColumn('deals', 'contractPrice', 'DECIMAL(18,2) NULL');
  await ensureColumn('deals', 'contractDate', 'VARCHAR(50) NULL');
  await ensureColumn('deals', 'unitNumber', 'VARCHAR(100) NULL');
  await ensureColumn('deals', 'createdAt', 'VARCHAR(50) NULL');
  await ensureColumn('deals', 'date', 'VARCHAR(50) NOT NULL DEFAULT \'\'');
  await ensureColumn('deals', 'notes', 'TEXT NULL');
  await ensureColumn('deals', 'lead_id', 'INT NULL');
  await ensureColumn('deals', 'user_id', 'INT NULL');
  await ensureColumn('deals', 'lead_name', 'VARCHAR(255) NULL');
  await ensureColumn('deals', 'client_phone', 'VARCHAR(50) NULL');
  await ensureColumn('deals', 'agent_id', 'INT NULL');
  await ensureColumn('deals', 'project_id', 'INT NULL');
  await ensureColumn('deals', 'developer_id', 'INT NULL');
  await ensureColumn('deals', 'broker_id', 'INT NULL');
  await ensureColumn('deals', 'attachments', 'LONGTEXT NULL');
  await ensureColumn('deals', 'reservations_images', 'LONGTEXT NULL');
  await ensureColumn('deals', 'price', 'DECIMAL(12,2) NULL');
  await ensureColumn('deals', 'commission', 'DECIMAL(12,2) NULL');
  await ensureColumn('deals', 'status', 'VARCHAR(50) NULL');
  await ensureColumn('deals', 'stage', 'VARCHAR(255) NULL');
  await ensureColumn('deals', 'reservation_price', 'DECIMAL(12,2) NULL');
  await ensureColumn('deals', 'reservation_date', 'DATE NULL');
  await ensureColumn('deals', 'contract_date', 'DATE NULL');
  await ensureColumn('deals', 'unit_number', 'VARCHAR(255) NULL');
  await ensureColumn('deals', 'comment', 'TEXT NULL');
  await ensureColumn('deals', 'created_at', 'TIMESTAMP NULL');
  await ensureColumn('deals', 'updated_at', 'TIMESTAMP NULL');

  await ensureColumn('statuses', 'color', 'VARCHAR(50) NULL');
  await ensureColumn('statuses', 'created_at', 'TIMESTAMP NULL');
  await ensureColumn('statuses', 'updated_at', 'TIMESTAMP NULL');

  await ensureColumn('sources', 'color', 'VARCHAR(50) NULL');
  await ensureColumn('sources', 'created_at', 'TIMESTAMP NULL');
  await ensureColumn('sources', 'updated_at', 'TIMESTAMP NULL');

  await ensureColumn('developers', 'phone', 'VARCHAR(50) NULL');
  await ensureColumn('developers', 'email', 'VARCHAR(255) NULL');
  await ensureColumn('developers', 'whatsapp', 'VARCHAR(50) NULL');
  await ensureColumn('developers', 'website', 'VARCHAR(255) NULL');
  await ensureColumn('developers', 'description', 'TEXT NULL');
  await ensureColumn('developers', 'logo', 'VARCHAR(255) NULL');
  await ensureColumn('developers', 'created_at', 'TIMESTAMP NULL');
  await ensureColumn('developers', 'updated_at', 'TIMESTAMP NULL');

  await ensureColumn('agent_care', 'name', 'VARCHAR(255) NULL');
  await ensureColumn('agent_care', 'phone', 'VARCHAR(50) NULL');
  await ensureColumn('agent_care', 'whatsapp', 'VARCHAR(50) NULL');
  await ensureColumn('agent_care', 'email', 'VARCHAR(255) NULL');

  await ensureColumn('departments', 'created_at', 'TIMESTAMP NULL');
  await ensureColumn('departments', 'updated_at', 'TIMESTAMP NULL');
  await ensureColumn('jobtitles', 'department_id', 'INT NULL');
  await ensureColumn('jobtitles', 'created_at', 'TIMESTAMP NULL');
  await ensureColumn('jobtitles', 'updated_at', 'TIMESTAMP NULL');
  await ensureColumn('brokers', 'phone', 'VARCHAR(50) NULL');
  await ensureColumn('brokers', 'developer_id', 'INT NULL');
  await ensureColumn('brokers', 'created_at', 'TIMESTAMP NULL');
  await ensureColumn('brokers', 'updated_at', 'TIMESTAMP NULL');

  await ensureColumn('activity_logs', 'user_id', 'INT NULL');
  await ensureColumn('activity_logs', 'entity_type', 'VARCHAR(100) NULL');
  await ensureColumn('activity_logs', 'entity_id', 'INT NULL');
  await ensureColumn('activity_logs', 'action', 'VARCHAR(100) NULL');
  await ensureColumn('activity_logs', 'summary', 'TEXT NULL');
  await ensureColumn('activity_logs', 'metadata', 'LONGTEXT NULL');
  await ensureColumn('activity_logs', 'created_at', 'TIMESTAMP NULL');

  await ensureColumn('calendar_events', 'title', 'VARCHAR(255) NULL');
  await ensureColumn('calendar_events', 'type', 'VARCHAR(100) NULL');
  await ensureColumn('calendar_events', 'notes', 'TEXT NULL');
  await ensureColumn('calendar_events', 'start_at', 'TIMESTAMP NULL');
  await ensureColumn('calendar_events', 'end_at', 'TIMESTAMP NULL');
  await ensureColumn('calendar_events', 'due_date', 'DATE NULL');
  await ensureColumn('calendar_events', 'lead_id', 'INT NULL');
  await ensureColumn('calendar_events', 'cold_call_id', 'INT NULL');
  await ensureColumn('calendar_events', 'deal_id', 'INT NULL');
  await ensureColumn('calendar_events', 'user_id', 'INT NULL');
  await ensureColumn('calendar_events', 'created_at', 'TIMESTAMP NULL');
  await ensureColumn('calendar_events', 'updated_at', 'TIMESTAMP NULL');

  await ensureColumn('tasks', 'title', 'VARCHAR(255) NULL');
  await ensureColumn('tasks', 'status', 'VARCHAR(100) NULL');
  await ensureColumn('tasks', 'priority', 'VARCHAR(50) NULL');
  await ensureColumn('tasks', 'due_date', 'DATE NULL');
  await ensureColumn('tasks', 'assigned_to_id', 'INT NULL');
  await ensureColumn('tasks', 'lead_id', 'INT NULL');
  await ensureColumn('tasks', 'notes', 'TEXT NULL');
  await ensureColumn('tasks', 'created_at', 'TIMESTAMP NULL');
  await ensureColumn('tasks', 'updated_at', 'TIMESTAMP NULL');

  await ensureColumn('knowledge_base', 'title', 'VARCHAR(255) NULL');
  await ensureColumn('knowledge_base', 'category', 'VARCHAR(100) NULL');
  await ensureColumn('knowledge_base', 'content', 'LONGTEXT NULL');
  await ensureColumn('knowledge_base', 'project_id', 'INT NULL');
  await ensureColumn('knowledge_base', 'developer_id', 'INT NULL');
  await ensureColumn('knowledge_base', 'price_range', 'VARCHAR(100) NULL');
  await ensureColumn('knowledge_base', 'payment_plan', 'VARCHAR(255) NULL');
  await ensureColumn('knowledge_base', 'delivery_date', 'VARCHAR(50) NULL');
  await ensureColumn('knowledge_base', 'created_at', 'TIMESTAMP NULL');
  await ensureColumn('knowledge_base', 'updated_at', 'TIMESTAMP NULL');

  await ensureColumn('call_history', 'lead_id', 'INT NULL');
  await ensureColumn('call_history', 'cold_call_id', 'INT NULL');
  await ensureColumn('call_history', 'user_id', 'INT NULL');
  await ensureColumn('call_history', 'result', 'VARCHAR(100) NULL');
  await ensureColumn('call_history', 'notes', 'TEXT NULL');
  await ensureColumn('call_history', 'call_at', 'TIMESTAMP NULL');
  await ensureColumn('call_history', 'created_at', 'TIMESTAMP NULL');
  await ensureColumn('call_history', 'updated_at', 'TIMESTAMP NULL');

  await ensureColumn('pipeline', 'title', 'VARCHAR(255) NULL');
  await ensureColumn('pipeline', 'stage', 'VARCHAR(100) NULL');
  await ensureColumn('pipeline', 'value', 'DECIMAL(12,2) NULL');
  await ensureColumn('pipeline', 'owner_id', 'INT NULL');
  await ensureColumn('pipeline', 'lead_id', 'INT NULL');
  await ensureColumn('pipeline', 'deal_id', 'INT NULL');
  await ensureColumn('pipeline', 'notes', 'TEXT NULL');
  await ensureColumn('pipeline', 'created_at', 'TIMESTAMP NULL');
  await ensureColumn('pipeline', 'updated_at', 'TIMESTAMP NULL');

  await ensureColumn('notifications', 'user_id', 'INT NULL');
  await ensureColumn('notifications', 'title', 'VARCHAR(255) NULL');
  await ensureColumn('notifications', 'body', 'TEXT NULL');
  await ensureColumn('notifications', 'is_read', 'TINYINT(1) DEFAULT 0');
  await ensureColumn('notifications', 'created_at', 'TIMESTAMP NULL');
  await ensureColumn('notifications', 'updated_at', 'TIMESTAMP NULL');

  await pool.query(`
    CREATE TABLE IF NOT EXISTS activity_logs (
      id INT PRIMARY KEY AUTO_INCREMENT,
      user_id INT NULL,
      entity_type VARCHAR(100) NULL,
      entity_id INT NULL,
      action VARCHAR(100) NULL,
      summary TEXT NULL,
      metadata LONGTEXT NULL,
      created_at TIMESTAMP NULL
    )
  `);
  await pool.query(`
    CREATE TABLE IF NOT EXISTS calendar_events (
      id INT PRIMARY KEY AUTO_INCREMENT,
      title VARCHAR(255) NOT NULL,
      type VARCHAR(100) NULL,
      notes TEXT NULL,
      start_at TIMESTAMP NULL,
      end_at TIMESTAMP NULL,
      due_date DATE NULL,
      lead_id INT NULL,
      cold_call_id INT NULL,
      deal_id INT NULL,
      user_id INT NULL,
      created_at TIMESTAMP NULL,
      updated_at TIMESTAMP NULL
    )
  `);
  await pool.query(`
    CREATE TABLE IF NOT EXISTS tasks (
      id INT PRIMARY KEY AUTO_INCREMENT,
      title VARCHAR(255) NOT NULL,
      status VARCHAR(100) NULL,
      priority VARCHAR(50) NULL,
      due_date DATE NULL,
      assigned_to_id INT NULL,
      lead_id INT NULL,
      notes TEXT NULL,
      created_at TIMESTAMP NULL,
      updated_at TIMESTAMP NULL
    )
  `);
  await pool.query(`
    CREATE TABLE IF NOT EXISTS knowledge_base (
      id INT PRIMARY KEY AUTO_INCREMENT,
      title VARCHAR(255) NOT NULL,
      category VARCHAR(100) NULL,
      content LONGTEXT NULL,
      project_id INT NULL,
      developer_id INT NULL,
      price_range VARCHAR(100) NULL,
      payment_plan VARCHAR(255) NULL,
      delivery_date VARCHAR(50) NULL,
      created_at TIMESTAMP NULL,
      updated_at TIMESTAMP NULL
    )
  `);
  await pool.query(`
    CREATE TABLE IF NOT EXISTS call_history (
      id INT PRIMARY KEY AUTO_INCREMENT,
      lead_id INT NULL,
      cold_call_id INT NULL,
      user_id INT NULL,
      result VARCHAR(100) NULL,
      notes TEXT NULL,
      call_at TIMESTAMP NULL,
      created_at TIMESTAMP NULL,
      updated_at TIMESTAMP NULL
    )
  `);
  await pool.query(`
    CREATE TABLE IF NOT EXISTS pipeline (
      id INT PRIMARY KEY AUTO_INCREMENT,
      title VARCHAR(255) NOT NULL,
      stage VARCHAR(100) NULL,
      value DECIMAL(12,2) NULL,
      owner_id INT NULL,
      lead_id INT NULL,
      deal_id INT NULL,
      notes TEXT NULL,
      created_at TIMESTAMP NULL,
      updated_at TIMESTAMP NULL
    )
  `);
  await pool.query(`
    CREATE TABLE IF NOT EXISTS notifications (
      id INT PRIMARY KEY AUTO_INCREMENT,
      user_id INT NULL,
      title VARCHAR(255) NOT NULL,
      body TEXT NULL,
      is_read TINYINT(1) DEFAULT 0,
      created_at TIMESTAMP NULL,
      updated_at TIMESTAMP NULL
    )
  `);
  await pool.query(`
    CREATE TABLE IF NOT EXISTS reports (
      id INT PRIMARY KEY AUTO_INCREMENT,
      comment_id INT NOT NULL,
      lead_id INT NULL,
      cold_call_id INT NULL,
      entity_type VARCHAR(50) NOT NULL DEFAULT 'lead',
      entity_id INT NULL,
      client_name VARCHAR(255) NOT NULL DEFAULT 'Unknown',
      client_phone VARCHAR(50) NULL,
      agent_id INT NULL,
      agent_name VARCHAR(255) NULL,
      team_leader_id INT NULL,
      team_leader_name VARCHAR(255) NULL,
      status_id INT NULL,
      status_name VARCHAR(255) NULL,
      project_id INT NULL,
      project_name VARCHAR(255) NULL,
      developer_id INT NULL,
      developer_name VARCHAR(255) NULL,
      content TEXT NOT NULL,
      created_by VARCHAR(255) NOT NULL DEFAULT 'Unknown',
      created_by_id INT NOT NULL DEFAULT 0,
      created_at TIMESTAMP NULL,
      updated_at TIMESTAMP NULL,
      UNIQUE KEY comment_id_unique (comment_id)
    )
  `);
};

const loadSeed = async (filename) => {
  const seedPath = path.join(__dirname, '..', 'seed', filename);
  const raw = await fs.readFile(seedPath, 'utf-8');
  return JSON.parse(raw);
};

const seedIfEmpty = async (table, columns, rows) => {
  if (!rows.length) return;
  try {
    const [[result]] = await pool.query('SELECT COUNT(*) as count FROM ??', [table]);
    if (result.count > 0) return;
    const placeholders = rows.map(() => `(${columns.map(() => '?').join(',')})`).join(',');
    const values = rows.flat();
    await pool.query(`INSERT INTO ${table} (${columns.join(',')}) VALUES ${placeholders}`, values);
  } catch (error) {
    console.warn(`Seed skipped for ${table}: ${error.message}`);
  }
};

const seedBaseData = async () => {
  const now = nowTs();
  const [usersSeed, titlesSeed, statusesSeed, sourcesSeed, projectsSeed, agentCareSeed] =
    await Promise.all([
      loadSeed('users.json'),
      loadSeed('titles.json'),
      loadSeed('statuses.json'),
      loadSeed('sources.json'),
      loadSeed('projects.json'),
      loadSeed('agent-care.json'),
    ]);

  const userColumnsAvailable = await getExistingColumns('users');
  const userColumnsAll = [
    'id',
    'name',
    'email',
    'phone',
    'password',
    'role',
    'title',
    'department',
    'status',
    'isActive',
    'is_active',
    'created_at',
    'createdAt',
    'updated_at',
    'updatedAt',
  ];
  const userColumns = userColumnsAll.filter(col => userColumnsAvailable.has(col));
  const userRows = await Promise.all(usersSeed.map(async user => {
    const row = {
      id: user.id,
      name: user.name,
      email: user.email,
      phone: user.phone ?? null,
      password: await hashPassword(user.password ?? 'password'),
      role: user.role ?? 'agent',
      title: user.title ?? null,
      department: user.department ?? null,
      status: 1,
      isActive: 1,
      is_active: 1,
      created_at: now,
      createdAt: now,
      updated_at: now,
      updatedAt: now,
    };
    return userColumns.map(col => (row[col] ?? null));
  }));
  await seedIfEmpty('users', userColumns, userRows);

  const departmentId = await ensureDepartment('Sales');
  await seedIfEmpty(
    'jobtitles',
    ['id', 'name', 'department_id', 'created_at', 'updated_at'],
    titlesSeed.map(title => [title.id, title.name, departmentId, now, now])
  );

  await seedIfEmpty(
    'statuses',
    ['id', 'name', 'color', 'created_at', 'updated_at'],
    statusesSeed.map(status => [status.id, status.name, status.color, now, now])
  );

  await seedIfEmpty(
    'sources',
    ['id', 'name', 'color', 'created_at', 'updated_at'],
    sourcesSeed.map(source => [source.id, source.name, source.color, now, now])
  );

  const projectColumnsAvailable = await getExistingColumns('projects');
  const projectColumnsAll = [
    'id',
    'name',
    'developer_id',
    'developerId',
    'created_at',
    'createdAt',
    'updated_at',
    'updatedAt',
  ];
  const projectColumns = projectColumnsAll.filter(col => projectColumnsAvailable.has(col));
  const projectRows = projectsSeed.map(project => {
    const row = {
      id: project.id,
      name: project.name,
      developer_id: project.developerId ?? null,
      developerId: project.developerId ?? null,
      created_at: now,
      createdAt: now,
      updated_at: now,
      updatedAt: now,
    };
    return projectColumns.map(col => (row[col] ?? null));
  });
  await seedIfEmpty('projects', projectColumns, projectRows);

  if (agentCareSeed) {
    await seedIfEmpty(
      'agent_care',
      ['id', 'name', 'phone', 'whatsapp', 'email'],
      [[agentCareSeed.id, agentCareSeed.name, agentCareSeed.phone, agentCareSeed.whatsapp, agentCareSeed.email]]
    );
  }
};

const replaceTable = async (table, columns, rows) => {
  const connection = await pool.getConnection();
  try {
    await connection.beginTransaction();
    await connection.query(`DELETE FROM ${table}`);
    if (rows.length > 0) {
      const placeholders = rows.map(() => `(${columns.map(() => '?').join(',')})`).join(',');
      const values = rows.flat();
      await connection.query(
        `INSERT INTO ${table} (${columns.join(',')}) VALUES ${placeholders}`,
        values
      );
    }
    await connection.commit();
  } catch (error) {
    await connection.rollback();
    throw error;
  } finally {
    connection.release();
  }
};

const ensureDepartment = async (name) => {
  const [rows] = await pool.query('SELECT id FROM departments WHERE name = ? LIMIT 1', [name]);
  if (rows.length > 0) return rows[0].id;
  const [result] = await pool.query(
    'INSERT INTO departments (name, created_at, updated_at) VALUES (?, ?, ?)',
    [name, nowTs(), nowTs()]
  );
  return result.insertId;
};

const upsertDevelopers = async (rows) => {
  if (!rows.length) return;
  const [columnRows] = await pool.query('SHOW COLUMNS FROM ?? LIKE ?', ['developers', 'lastUpdated']);
  const includeLastUpdated = columnRows.length > 0;
  const columns = ['id', 'name', 'phone', 'email', 'whatsapp', 'website', 'description', 'logo'];
  if (includeLastUpdated) {
    columns.push('lastUpdated');
  }
  const placeholders = rows.map(() => `(${columns.map(() => '?').join(',')})`).join(',');
  const values = rows.flatMap(item => {
    const base = [
      item.id,
      item.name,
      item.phone ?? null,
      item.email ?? null,
      item.whatsapp ?? null,
      item.website ?? null,
      item.description ?? null,
      item.logo ?? null,
    ];
    if (includeLastUpdated) {
      base.push(item.lastUpdated ?? nowTs());
    }
    return base;
  });
  const updates = columns
    .filter(column => column !== 'id')
    .map(column => `${column}=VALUES(${column})`)
    .join(',');
  await pool.query(
    `INSERT INTO developers (${columns.join(',')}) VALUES ${placeholders} ON DUPLICATE KEY UPDATE ${updates}`,
    values
  );
};

const upsertRows = async (table, columns, rows) => {
  if (!rows.length) return;
  const placeholders = rows.map(() => `(${columns.map(() => '?').join(',')})`).join(',');
  const values = rows.flat();
  const updates = columns
    .filter((column) => column !== 'id')
    .map((column) => `${column}=VALUES(${column})`)
    .join(',');
  await pool.query(
    `INSERT INTO ${table} (${columns.join(',')}) VALUES ${placeholders} ON DUPLICATE KEY UPDATE ${updates}`,
    values
  );
};

const deleteRowsByIds = async (table, ids) => {
  if (!ids.length) return;
  const { clause, values } = buildInClause(ids);
  await pool.query(`DELETE FROM ${table} WHERE id IN (${clause})`, values);
};

const createActivityLog = async ({ userId, entityType, entityId, action, summary, metadata }) => {
  try {
    await pool.query(
      `INSERT INTO activity_logs (user_id, entity_type, entity_id, action, summary, metadata, created_at)
       VALUES (?, ?, ?, ?, ?, ?, ?)`,
      [
        userId ?? null,
        entityType ?? null,
        entityId ?? null,
        action ?? null,
        summary ?? null,
        metadata ? JSON.stringify(metadata) : null,
        nowTs(),
      ]
    );
  } catch (error) {
    console.warn(`Activity log skipped: ${error.message}`);
  }
};

const resolveVisibleRows = async (table, scopeField, currentUser, scopeIds) => {
  if (isSuperUser(currentUser)) {
    const [rows] = await pool.query(`SELECT id FROM ${table}`);
    return rows.map((row) => Number(row.id));
  }
  const [rows] = await pool.query(
    `SELECT id FROM ${table} WHERE ${scopeField} IN (${scopeIds.map(() => '?').join(', ')})`,
    scopeIds
  );
  return rows.map((row) => Number(row.id));
};

const syncScopedRows = async ({
  table,
  columns,
  rows,
  currentUser,
  scopeField,
  mapRow,
}) => {
  if (isSuperUser(currentUser)) {
    const mappedRows = await assignMissingIds(table, rows.map((item) => mapRow(item)));
    await replaceTable(
      table,
      columns,
      mappedRows.map((row) => columns.map((column) => row[column] ?? null))
    );
    return;
  }

  const visibleUserIds = await getAccessibleUserIds(currentUser);
  const mappedRows = await assignMissingIds(table, rows
    .map((item) => mapRow(item))
    .filter((row) => {
      const ownerId = row[scopeField];
      if (ownerId === null || ownerId === undefined || ownerId === '') {
        row[scopeField] = currentUser.id;
        return true;
      }
      return visibleUserIds.includes(Number(ownerId));
    }));

  const visibleRowIds = await resolveVisibleRows(table, scopeField, currentUser, visibleUserIds);
  const incomingIds = new Set(mappedRows.map((row) => String(row.id)));
  const idsToDelete = visibleRowIds.filter((id) => !incomingIds.has(String(id)));

  await upsertRows(
    table,
    columns,
    mappedRows.map((row) => columns.map((column) => row[column] ?? null))
  );
  await deleteRowsByIds(table, idsToDelete);
};

app.use('/api', async (req, res, next) => {
  if (AUTH_PUBLIC_ROUTES.has(req.path) && req.method === 'GET') {
    return next();
  }
  if (AUTH_PUBLIC_ROUTES.has(req.path) && req.method === 'POST') {
    return next();
  }

  const authHeader = String(req.headers.authorization ?? '');
  const token = authHeader.startsWith('Bearer ') ? authHeader.slice(7).trim() : '';

  if (!token) {
    return res.status(401).json({ error: 'Unauthorized' });
  }

  try {
    const payload = jwt.verify(token, JWT_SECRET);
    const currentUser = await loadAuthUser(payload.sub);
    if (!currentUser || Number(currentUser.status ?? 1) !== 1) {
      return res.status(401).json({ error: 'Unauthorized' });
    }
    req.auth = payload;
    req.currentUser = currentUser;
    next();
  } catch (error) {
    return res.status(401).json({ error: 'Unauthorized' });
  }
});

app.get('/api/health', async (_req, res) => {
  try {
    await pool.query('SELECT 1');
    res.json({ ok: true });
  } catch (error) {
    res.status(500).json({ ok: false, error: error.message });
  }
});

app.get('/api/users', async (req, res) => {
  try {
    const currentUser = req.currentUser;
    const columns = `id, name, email, phone, role, title, department, department_id, jobtitle_id, parent_id, status, image, created_at`;
    if (isSuperUser(currentUser)) {
      const [rows] = await pool.query(`SELECT ${columns} FROM users`);
      res.json(rows.map(serializeUser));
      return;
    }

    const visibleUserIds = await getAccessibleUserIds(currentUser);
    const [rows] = await pool.query(
      `SELECT ${columns}
       FROM users
       WHERE id IN (${visibleUserIds.map(() => '?').join(', ')})`,
      visibleUserIds
    );
    res.json(rows.map(serializeUser));
  } catch (error) {
    res.status(500).json({ error: error.message });
  }
});

app.get('/api/auth/me', async (req, res) => {
  try {
    if (!req.currentUser) {
      res.status(401).json({ error: 'Unauthorized' });
      return;
    }
    res.json({ user: serializeUser(req.currentUser) });
  } catch (error) {
    res.status(500).json({ error: error.message });
  }
});

app.post('/api/auth/login', async (req, res) => {
  const email = String(req.body?.email ?? '').trim();
  const password = String(req.body?.password ?? '');
  if (!email || !password) {
    res.status(400).json({ error: 'Email and password are required' });
    return;
  }

  try {
    const [rows] = await pool.query(
      `SELECT ${selectUserColumns}
       FROM users
       WHERE email = ?
       LIMIT 1`,
      [email]
    );

    if (!rows.length) {
      res.status(401).json({ error: 'Invalid email or password' });
      return;
    }

    const userRow = rows[0];
    const isValid = await verifyPassword(password, userRow.password);
    if (!isValid) {
      res.status(401).json({ error: 'Invalid email or password' });
      return;
    }

    if (!String(userRow.password ?? '').startsWith('$2')) {
      const upgraded = await hashPassword(password);
      await pool.query('UPDATE users SET password = ?, updated_at = ? WHERE id = ?', [
        upgraded,
        nowTs(),
        userRow.id,
      ]);
      userRow.password = upgraded;
    }

    const token = issueToken(userRow.id, userRow.role);
    res.json({ token, user: serializeUser(userRow) });
  } catch (error) {
    res.status(500).json({ error: error.message });
  }
});

app.get('/api/profile', async (req, res) => {
  try {
    if (!req.currentUser) {
      res.status(401).json({ error: 'Unauthorized' });
      return;
    }
    res.json({ user: serializeUser(req.currentUser) });
  } catch (error) {
    res.status(500).json({ error: error.message });
  }
});

app.put('/api/profile', async (req, res) => {
  try {
    if (!req.currentUser) {
      res.status(401).json({ error: 'Unauthorized' });
      return;
    }
    const nextName = String(req.body?.name ?? req.currentUser.name ?? '').trim();
    const nextPhone = String(req.body?.phone ?? req.currentUser.phone ?? '').trim();
    const nextImage = String(req.body?.avatar ?? req.body?.image ?? req.currentUser.image ?? defaultUserImage).trim() || defaultUserImage;
    await pool.query(
      'UPDATE users SET name = ?, phone = ?, image = ?, updated_at = ? WHERE id = ?',
      [nextName, nextPhone, nextImage, nowTs(), req.currentUser.id]
    );
    const [rows] = await pool.query(`SELECT ${selectUserColumns} FROM users WHERE id = ? LIMIT 1`, [req.currentUser.id]);
    res.json({ user: serializeUser(rows[0]) });
  } catch (error) {
    res.status(500).json({ error: error.message });
  }
});

app.post('/api/auth/change-password', async (req, res) => {
  try {
    if (!req.currentUser) {
      res.status(401).json({ error: 'Unauthorized' });
      return;
    }
    const currentPassword = String(req.body?.currentPassword ?? '');
    const newPassword = String(req.body?.newPassword ?? '');
    if (!currentPassword || !newPassword) {
      res.status(400).json({ error: 'Current password and new password are required' });
      return;
    }
    const isValid = await verifyPassword(currentPassword, req.currentUser.password);
    if (!isValid) {
      res.status(400).json({ error: 'Current password is incorrect' });
      return;
    }
    const hashedPassword = await hashPassword(newPassword);
    await pool.query('UPDATE users SET password = ?, updated_at = ? WHERE id = ?', [
      hashedPassword,
      nowTs(),
      req.currentUser.id,
    ]);
    await createActivityLog({
      userId: req.currentUser.id,
      entityType: 'users',
      entityId: req.currentUser.id,
      action: 'password_changed',
      summary: 'Password changed',
      metadata: null,
    });
    res.json({ ok: true });
  } catch (error) {
    res.status(500).json({ error: error.message });
  }
});

app.get('/api/titles', async (_req, res) => {
  try {
    const [rows] = await pool.query(
      `SELECT jobtitles.id, jobtitles.name, departments.name AS department
       FROM jobtitles
       LEFT JOIN departments ON departments.id = jobtitles.department_id`
    );
    res.json(
      rows.map(row => ({
        id: row.id,
        name: row.name,
        department: row.department ?? 'Sales',
      }))
    );
  } catch (error) {
    res.status(500).json({ error: error.message });
  }
});

app.get('/api/developers', async (_req, res) => {
  try {
    const [rows] = await pool.query('SELECT * FROM developers');
    res.json(
      rows.map(row => ({
        id: row.id,
        name: row.name,
        logo: row.logo ?? '',
        description: row.description ?? '',
        phone: row.phone ?? '',
        whatsapp: row.whatsapp ?? '',
        email: row.email ?? '',
        website: row.website ?? '',
        lastUpdated: toClientDateOnly(row.updated_at),
      }))
    );
  } catch (error) {
    res.status(500).json({ error: error.message });
  }
});

app.get('/api/projects', async (_req, res) => {
  try {
    const [rows] = await pool.query(
      `SELECT projects.id, projects.name, projects.developer_id, developers.name AS developer_name
       FROM projects
       LEFT JOIN developers ON developers.id = projects.developer_id`
    );
    res.json(
      rows.map(row => ({
        id: row.id,
        name: row.name,
        developerId: row.developer_id ?? row.developerId ?? undefined,
        developerName: row.developer_name ?? undefined,
      }))
    );
  } catch (error) {
    res.status(500).json({ error: error.message });
  }
});

app.get('/api/statuses', async (_req, res) => {
  try {
    const [rows] = await pool.query('SELECT id, name, color FROM statuses');
    res.json(rows);
  } catch (error) {
    res.status(500).json({ error: error.message });
  }
});

app.get('/api/lead-sources', async (_req, res) => {
  try {
    const [rows] = await pool.query('SELECT id, name, color FROM sources');
    res.json(rows);
  } catch (error) {
    res.status(500).json({ error: error.message });
  }
});

app.get('/api/leads', async (req, res) => {
  try {
    const currentUser = req.currentUser;
    const visibleUserIds = await getAccessibleUserIds(currentUser);
    const userFilter = isSuperUser(currentUser)
      ? ''
      : `WHERE leads.user_id IN (${visibleUserIds.map(() => '?').join(', ')})`;
    const [rows] = await pool.query(
      `SELECT leads.*, statuses.name AS status_name, sources.name AS source_name,
              projects.name AS project_name, users.name AS user_name, users.parent_id AS parent_id, users.role AS user_role,
              tl.name AS team_leader_name
       FROM leads
       LEFT JOIN statuses ON statuses.id = leads.status_id
       LEFT JOIN sources ON sources.id = leads.source_id
       LEFT JOIN projects ON projects.id = leads.project_id
       LEFT JOIN users ON users.id = leads.user_id
       LEFT JOIN users AS tl ON tl.id = users.parent_id
       ${userFilter}`,
      isSuperUser(currentUser) ? [] : visibleUserIds
    );
    res.json(
      rows.map(row => ({
        id: row.id,
        type: 'Lead',
        name: row.name,
        phone: row.phone,
        whatsapp: row.whatsapp ?? undefined,
        workPhone: row.workphone ?? undefined,
        status: row.status_name ?? '',
        statusId: row.status_id ?? row.statusId ?? 0,
        source: row.source_name ?? undefined,
        sourceId: row.source_id ?? row.sourceId ?? undefined,
        project: row.project_name ?? undefined,
        projectId: row.project_id ?? row.projectId ?? undefined,
        assignedTo: row.user_name ?? undefined,
        assignedToId: row.user_id ?? row.userId ?? undefined,
        teamLeader: row.user_role === 'team_leader' ? row.user_name ?? undefined : row.team_leader_name ?? undefined,
        teamLeaderId: row.user_role === 'team_leader' ? row.user_id ?? row.userId ?? undefined : row.parent_id ?? row.parentId ?? undefined,
        date: row.date ?? toClientDateOnly(row.created_at),
        isPotential: (row.status_name ?? '').toLowerCase().includes('potential')
          && !(row.status_name ?? '').toLowerCase().includes('non'),
      }))
    );
  } catch (error) {
    res.status(500).json({ error: error.message });
  }
});

app.get('/api/cold-calls', async (req, res) => {
  try {
    const currentUser = req.currentUser;
    const visibleUserIds = await getAccessibleUserIds(currentUser);
    const userFilter = isSuperUser(currentUser)
      ? ''
      : `WHERE cold_calls.user_id IN (${visibleUserIds.map(() => '?').join(', ')})`;
    const [rows] = await pool.query(
      `SELECT cold_calls.*, statuses.name AS status_name,
              users.name AS user_name, users.parent_id AS parent_id, users.role AS user_role, tl.name AS team_leader_name
       FROM cold_calls
       LEFT JOIN statuses ON statuses.id = cold_calls.status_id
       LEFT JOIN users ON users.id = cold_calls.user_id
       LEFT JOIN users AS tl ON tl.id = users.parent_id
       ${userFilter}`,
      isSuperUser(currentUser) ? [] : visibleUserIds
    );
    res.json(
      rows.map(row => ({
        id: row.id,
        type: 'Coldcall',
        name: row.name,
        phone: row.phone,
        status: row.status_name ?? '',
        statusId: row.status_id ?? row.statusId ?? 0,
        assignedTo: row.user_name ?? undefined,
        assignedToId: row.user_id ?? row.userId ?? undefined,
        teamLeader: row.user_role === 'team_leader' ? row.user_name ?? undefined : row.team_leader_name ?? undefined,
        teamLeaderId: row.user_role === 'team_leader' ? row.user_id ?? row.userId ?? undefined : row.parent_id ?? row.parentId ?? undefined,
        date: toClientDateOnly(row.created_at),
        isPotential: (row.status_name ?? '').toLowerCase().includes('potential')
          && !(row.status_name ?? '').toLowerCase().includes('non'),
      }))
    );
  } catch (error) {
    res.status(500).json({ error: error.message });
  }
});

app.get('/api/comments', async (req, res) => {
  try {
    const currentUser = req.currentUser;
    const visibleUserIds = await getAccessibleUserIds(currentUser);
    const userFilter = isSuperUser(currentUser)
      ? ''
      : `WHERE comments.user_id IN (${visibleUserIds.map(() => '?').join(', ')})`;
    const [rows] = await pool.query(
      `SELECT comments.*, users.name AS user_name
       FROM comments
       LEFT JOIN users ON users.id = comments.user_id
       ${userFilter}`,
      isSuperUser(currentUser) ? [] : visibleUserIds
    );
    res.json(
      rows.map(row => ({
        id: row.id,
        leadId: row.entity_id ?? row.lead_id ?? row.cold_call_id ?? row.leadId,
        entityType: row.entity_type ?? (row.cold_call_id ? 'cold_call' : 'lead'),
        entityId: row.entity_id ?? row.lead_id ?? row.cold_call_id ?? row.leadId,
        content: row.body ?? row.content ?? '',
        createdBy: row.user_name ?? row.createdBy ?? 'Unknown',
        createdById: row.user_id ?? row.createdById ?? row.userId,
        createdAt: toClientDateTime(row.created_at ?? row.createdAt),
      }))
    );
  } catch (error) {
    res.status(500).json({ error: error.message });
  }
});

app.get('/api/deals', async (req, res) => {
  try {
    const currentUser = req.currentUser;
    const visibleUserIds = await getAccessibleUserIds(currentUser);
    const userFilter = isSuperUser(currentUser)
      ? ''
      : `WHERE deals.user_id IN (${visibleUserIds.map(() => '?').join(', ')})`;
    const [rows] = await pool.query(
      `SELECT deals.*, projects.name AS project_name, developers.name AS developer_name, users.name AS user_name
       FROM deals
       LEFT JOIN projects ON projects.id = deals.project_id
       LEFT JOIN developers ON developers.id = deals.developer_id
       LEFT JOIN users ON users.id = deals.user_id
       ${userFilter}`,
      isSuperUser(currentUser) ? [] : visibleUserIds
    );
    res.json(
      rows.map(row => ({
        id: row.id,
        leadId: row.leadId ?? row.lead_id ?? row.id,
        leadName: row.leadName ?? row.lead_name ?? row.name ?? '',
        clientPhone: row.clientPhone ?? row.client_phone ?? row.phone ?? '',
        agentId: row.agentId ?? row.agent_id ?? row.user_id ?? row.userId ?? 0,
        agentName: row.agentName ?? row.user_name ?? '',
        projectId: row.projectId ?? row.project_id ?? 0,
        projectName: row.projectName ?? row.project_name ?? '',
        developerId: row.developerId ?? row.developer_id ?? 0,
        developerName: row.developerName ?? row.developer_name ?? '',
        amount: Number(row.amount ?? row.price ?? row.contractPrice ?? 0),
        commission: Number(row.commission ?? 0),
        status: row.status ?? 'pending',
        stage: row.stage ?? undefined,
        reservationPrice: row.reservationPrice ?? row.reservation_price ?? undefined,
        reservationDate: row.reservationDate ?? row.reservation_date ?? undefined,
        contractPrice: row.contractPrice ?? row.price ?? undefined,
        contractDate: row.contractDate ?? row.contract_date ?? undefined,
        unitNumber: row.unitNumber ?? row.unit_number ?? undefined,
        createdAt: toClientDateTime(row.createdAt ?? row.created_at),
        date: row.date ?? row.contract_date ?? toClientDateOnly(row.created_at ?? row.createdAt),
        notes: row.notes ?? row.comment ?? undefined,
        attachments: parseJsonArray(row.attachments),
        reservationImages: parseJsonArray(row.reservations_images),
      }))
    );
  } catch (error) {
    res.status(500).json({ error: error.message });
  }
});

registerCrudRoutes({
  routeBase: 'users',
  table: 'users',
  baseSql: 'SELECT * FROM users',
  orderBy: 'id ASC',
  writeRoles: ['admin', 'franchise_owner'],
  includeGet: false,
  serialize: serializeUser,
  createRow: async (body) => {
    const nextId = await nextTableId('users');
    const available = await getExistingColumns('users');
    const createdAt = toSqlDateTime(body.createdAt ?? nowTs());
    const isActive = body.isActive ?? body.status ?? true;
    const password = await hashIncomingPassword(body.password ?? body.newPassword ?? '', 'password');
    const row = {
      id: nextId,
      type: String(body.type ?? 'Lead'),
      name: String(body.name ?? '').trim(),
      email: String(body.email ?? '').trim(),
      phone: String(body.phone ?? '').trim() || null,
      password,
      department_id: body.departmentId ?? body.department_id ?? null,
      jobtitle_id: body.jobtitleId ?? body.jobtitle_id ?? null,
      parent_id: body.teamLeaderId ?? body.parent_id ?? null,
      role: normalizeRole(body.role ?? 'agent'),
      title: body.title ?? null,
      department: body.department ?? null,
      status: toBool(isActive),
      isActive: toBool(isActive),
      is_active: toBool(isActive),
      image: body.avatar ?? body.image ?? defaultUserImage,
      created_at: createdAt,
      createdAt,
      updated_at: nowTs(),
      updatedAt: nowTs(),
    };
    const columns = [
      'id',
      'type',
      'name',
      'email',
      'phone',
      'password',
      'department_id',
      'jobtitle_id',
      'parent_id',
      'role',
      'title',
      'department',
      'status',
      'isActive',
      'is_active',
      'image',
      'created_at',
      'createdAt',
      'updated_at',
      'updatedAt',
    ].filter((column) => available.has(column));
    return {
      columns,
      values: columns.map((column) => row[column] ?? null),
      summary: `User created: ${row.name}`,
    };
  },
  updateRow: async (body, existing) => {
    const available = await getExistingColumns('users');
    const password = await hashIncomingPassword(body.password ?? body.newPassword ?? '', existing.password ?? null);
    const isActive = body.isActive ?? body.status ?? Number(existing.status ?? 1) === 1;
    const row = {
      type: String(body.type ?? existing.type ?? 'Lead'),
      name: String(body.name ?? existing.name ?? '').trim(),
      email: String(body.email ?? existing.email ?? '').trim(),
      phone: String(body.phone ?? existing.phone ?? '').trim() || null,
      password,
      department_id: body.departmentId ?? body.department_id ?? existing.department_id ?? null,
      jobtitle_id: body.jobtitleId ?? body.jobtitle_id ?? existing.jobtitle_id ?? null,
      parent_id: body.teamLeaderId ?? body.parent_id ?? existing.parent_id ?? null,
      role: normalizeRole(body.role ?? existing.role ?? 'agent'),
      title: body.title ?? existing.title ?? null,
      department: body.department ?? existing.department ?? null,
      status: toBool(isActive),
      isActive: toBool(isActive),
      is_active: toBool(isActive),
      image: body.avatar ?? body.image ?? existing.image ?? defaultUserImage,
      updated_at: nowTs(),
      updatedAt: nowTs(),
    };
    const columns = [
      'type',
      'name',
      'email',
      'phone',
      'password',
      'department_id',
      'jobtitle_id',
      'parent_id',
      'role',
      'title',
      'department',
      'status',
      'isActive',
      'is_active',
      'image',
      'updated_at',
      'updatedAt',
    ].filter((column) => available.has(column));
    return {
      columns,
      values: columns.map((column) => row[column] ?? null),
      summary: `User updated: ${row.name}`,
    };
  },
  deleteSummary: (row) => `User deleted: ${row?.name ?? row?.id ?? ''}`,
});

registerCrudRoutes({
  routeBase: 'titles',
  table: 'jobtitles',
  baseSql: 'SELECT * FROM jobtitles',
  orderBy: 'id ASC',
  writeRoles: ['admin', 'franchise_owner'],
  includeGet: false,
  createRow: async (body) => {
    const available = await getExistingColumns('jobtitles');
    const departmentName = String(body.department ?? body.departmentName ?? 'Sales').trim() || 'Sales';
    const departmentId = body.departmentId ?? body.department_id ?? await ensureDepartment(departmentName);
    const row = {
      name: String(body.name ?? '').trim(),
      department_id: departmentId,
      created_at: nowTs(),
      updated_at: nowTs(),
    };
    const columns = ['name', 'department_id', 'created_at', 'updated_at'].filter((column) => available.has(column));
    return {
      columns,
      values: columns.map((column) => row[column] ?? null),
      summary: `Job title created: ${row.name}`,
    };
  },
  updateRow: async (body, existing) => {
    const available = await getExistingColumns('jobtitles');
    const departmentName = String(body.department ?? body.departmentName ?? 'Sales').trim() || 'Sales';
    const departmentId = body.departmentId ?? body.department_id ?? existing.department_id ?? await ensureDepartment(departmentName);
    const row = {
      name: String(body.name ?? existing.name ?? '').trim(),
      department_id: departmentId,
      updated_at: nowTs(),
    };
    const columns = ['name', 'department_id', 'updated_at'].filter((column) => available.has(column));
    return {
      columns,
      values: columns.map((column) => row[column] ?? null),
      summary: `Job title updated: ${row.name}`,
    };
  },
  deleteSummary: (row) => `Job title deleted: ${row?.name ?? row?.id ?? ''}`,
});

registerCrudRoutes({
  routeBase: 'developers',
  table: 'developers',
  baseSql: 'SELECT * FROM developers',
  orderBy: 'id ASC',
  writeRoles: ['admin', 'franchise_owner'],
  includeGet: false,
  createRow: async (body) => {
    const nextId = await nextTableId('developers');
    const available = await getExistingColumns('developers');
    const row = {
      id: nextId,
      type: String(body.type ?? 'Coldcall'),
      name: String(body.name ?? '').trim(),
      phone: String(body.phone ?? '').trim() || null,
      email: String(body.email ?? '').trim() || null,
      whatsapp: String(body.whatsapp ?? '').trim() || null,
      website: String(body.website ?? '').trim() || null,
      description: String(body.description ?? '').trim() || null,
      logo: String(body.logo ?? '').trim() || null,
      images: body.images ?? null,
      videos: body.videos ?? null,
      pdfs: body.pdfs ?? null,
      created_at: nowTs(),
      updated_at: nowTs(),
    };
    const columns = [
      'id',
      'type',
      'name',
      'phone',
      'email',
      'whatsapp',
      'website',
      'description',
      'logo',
      'images',
      'videos',
      'pdfs',
      'created_at',
      'updated_at',
    ].filter((column) => available.has(column));
    return {
      columns,
      values: columns.map((column) => row[column] ?? null),
      summary: `Developer created: ${row.name}`,
    };
  },
  updateRow: async (body, existing) => {
    const available = await getExistingColumns('developers');
    const row = {
      type: String(body.type ?? existing.type ?? 'Coldcall'),
      name: String(body.name ?? existing.name ?? '').trim(),
      phone: String(body.phone ?? existing.phone ?? '').trim() || null,
      email: String(body.email ?? existing.email ?? '').trim() || null,
      whatsapp: String(body.whatsapp ?? existing.whatsapp ?? '').trim() || null,
      website: String(body.website ?? existing.website ?? '').trim() || null,
      description: String(body.description ?? existing.description ?? '').trim() || null,
      logo: String(body.logo ?? existing.logo ?? '').trim() || null,
      images: body.images ?? existing.images ?? null,
      videos: body.videos ?? existing.videos ?? null,
      pdfs: body.pdfs ?? existing.pdfs ?? null,
      updated_at: nowTs(),
    };
    const columns = [
      'type',
      'name',
      'phone',
      'email',
      'whatsapp',
      'website',
      'description',
      'logo',
      'images',
      'videos',
      'pdfs',
      'updated_at',
    ].filter((column) => available.has(column));
    return {
      columns,
      values: columns.map((column) => row[column] ?? null),
      summary: `Developer updated: ${row.name}`,
    };
  },
  deleteSummary: (row) => `Developer deleted: ${row?.name ?? row?.id ?? ''}`,
});

registerCrudRoutes({
  routeBase: 'projects',
  table: 'projects',
  baseSql: 'SELECT * FROM projects',
  orderBy: 'id ASC',
  writeRoles: ['admin', 'franchise_owner'],
  includeGet: false,
  createRow: async (body) => {
    const nextId = await nextTableId('projects');
    const available = await getExistingColumns('projects');
    const row = {
      id: nextId,
      type: String(body.type ?? 'Lead'),
      name: String(body.name ?? '').trim(),
      developer_id: body.developerId ?? body.developer_id ?? null,
      created_at: nowTs(),
      updated_at: nowTs(),
    };
    const columns = ['id', 'name', 'developer_id', 'created_at', 'updated_at'].filter((column) => available.has(column));
    return {
      columns,
      values: columns.map((column) => row[column] ?? null),
      summary: `Project created: ${row.name}`,
    };
  },
  updateRow: async (body, existing) => {
    const available = await getExistingColumns('projects');
    const row = {
      name: String(body.name ?? existing.name ?? '').trim(),
      developer_id: body.developerId ?? body.developer_id ?? existing.developer_id ?? null,
      updated_at: nowTs(),
    };
    const columns = ['name', 'developer_id', 'updated_at'].filter((column) => available.has(column));
    return {
      columns,
      values: columns.map((column) => row[column] ?? null),
      summary: `Project updated: ${row.name}`,
    };
  },
  deleteSummary: (row) => `Project deleted: ${row?.name ?? row?.id ?? ''}`,
});

registerCrudRoutes({
  routeBase: 'statuses',
  table: 'statuses',
  baseSql: 'SELECT * FROM statuses',
  orderBy: 'id ASC',
  writeRoles: ['admin', 'franchise_owner'],
  includeGet: false,
  createRow: async (body) => {
    const nextId = await nextTableId('statuses');
    const available = await getExistingColumns('statuses');
    const row = {
      id: nextId,
      type: String(body.type ?? 'Coldcall'),
      name: String(body.name ?? '').trim(),
      color: String(body.color ?? '#3B82F6').trim() || '#3B82F6',
      created_at: nowTs(),
      updated_at: nowTs(),
    };
    const columns = ['id', 'name', 'color', 'created_at', 'updated_at'].filter((column) => available.has(column));
    return {
      columns,
      values: columns.map((column) => row[column] ?? null),
      summary: `Status created: ${row.name}`,
    };
  },
  updateRow: async (body, existing) => {
    const available = await getExistingColumns('statuses');
    const row = {
      name: String(body.name ?? existing.name ?? '').trim(),
      color: String(body.color ?? existing.color ?? '#3B82F6').trim() || '#3B82F6',
      updated_at: nowTs(),
    };
    const columns = ['name', 'color', 'updated_at'].filter((column) => available.has(column));
    return {
      columns,
      values: columns.map((column) => row[column] ?? null),
      summary: `Status updated: ${row.name}`,
    };
  },
  deleteSummary: (row) => `Status deleted: ${row?.name ?? row?.id ?? ''}`,
});

registerCrudRoutes({
  routeBase: 'lead-sources',
  table: 'sources',
  baseSql: 'SELECT * FROM sources',
  orderBy: 'id ASC',
  writeRoles: ['admin', 'franchise_owner'],
  includeGet: false,
  createRow: async (body) => {
    const nextId = await nextTableId('sources');
    const available = await getExistingColumns('sources');
    const row = {
      id: nextId,
      name: String(body.name ?? '').trim(),
      color: String(body.color ?? '#3B82F6').trim() || '#3B82F6',
      created_at: nowTs(),
      updated_at: nowTs(),
    };
    const columns = ['id', 'name', 'color', 'created_at', 'updated_at'].filter((column) => available.has(column));
    return {
      columns,
      values: columns.map((column) => row[column] ?? null),
      summary: `Source created: ${row.name}`,
    };
  },
  updateRow: async (body, existing) => {
    const available = await getExistingColumns('sources');
    const row = {
      name: String(body.name ?? existing.name ?? '').trim(),
      color: String(body.color ?? existing.color ?? '#3B82F6').trim() || '#3B82F6',
      updated_at: nowTs(),
    };
    const columns = ['name', 'color', 'updated_at'].filter((column) => available.has(column));
    return {
      columns,
      values: columns.map((column) => row[column] ?? null),
      summary: `Source updated: ${row.name}`,
    };
  },
  deleteSummary: (row) => `Source deleted: ${row?.name ?? row?.id ?? ''}`,
});

registerCrudRoutes({
  routeBase: 'leads',
  table: 'leads',
  baseSql: 'SELECT * FROM leads',
  orderBy: 'id DESC',
  includeGet: false,
  createRow: async (body, currentUser) => {
    const nextId = await nextTableId('leads');
    const available = await getExistingColumns('leads');
    const assignedToId = body.assignedToId ?? body.userId ?? currentUser.id;
    const statusDetails = await resolveLeadStatus({ statusId: body.statusId ?? body.status_id, status: body.status });
    const leadType = String(body.type ?? 'Lead').trim() || 'Lead';
    const leadDate = toSqlDateOnly(body.date ?? nowTs()) ?? toSqlDateOnly(nowTs());
    const projectName = String(body.project ?? body.projectName ?? '').trim() || null;
    const sourceName = String(body.source ?? body.sourceName ?? '').trim() || null;
    const assignedName = String(body.assignedTo ?? body.assignedToName ?? '').trim() || null;
    const teamLeaderName = String(body.teamLeader ?? body.teamLeaderName ?? '').trim() || null;
    const isPotential = body.isPotential !== undefined
      ? toBool(body.isPotential)
      : Number(statusDetails.statusId) === 3 || statusDetails.status.toLowerCase().includes('potential');
    const row = {
      id: nextId,
      type: leadType,
      name: String(body.name ?? '').trim(),
      phone: String(body.phone ?? '').trim(),
      whatsapp: String(body.whatsapp ?? '').trim() || null,
      workphone: String(body.workPhone ?? body.workphone ?? '').trim() || null,
      email: String(body.email ?? '').trim() || null,
      status: statusDetails.status,
      statusId: statusDetails.statusId,
      source: sourceName,
      sourceId: body.sourceId ?? body.source_id ?? null,
      project: projectName,
      assignedTo: assignedName,
      assignedToId,
      teamLeader: teamLeaderName,
      teamLeaderId: body.teamLeaderId ?? body.teamLeader_id ?? null,
      date: leadDate,
      isPotential,
      project_id: body.projectId ?? body.project_id ?? null,
      projectId: body.projectId ?? body.project_id ?? null,
      status_id: statusDetails.statusId,
      statusId: statusDetails.statusId,
      source_id: body.sourceId ?? body.source_id ?? null,
      sourceId: body.sourceId ?? body.source_id ?? null,
      comment: body.comment ?? null,
      user_id: assignedToId,
      assignedToId,
      created_at: toSqlDateTime(body.createdAt ?? nowTs()),
      createdAt: toSqlDateTime(body.createdAt ?? nowTs()),
      updated_at: toSqlDateTime(body.updatedAt ?? nowTs()),
      updatedAt: toSqlDateTime(body.updatedAt ?? nowTs()),
    };
    const columns = [
      'id',
      'type',
      'name',
      'phone',
      'whatsapp',
      'workphone',
      'email',
      'status',
      'statusId',
      'source',
      'sourceId',
      'project',
      'assignedTo',
      'assignedToId',
      'teamLeader',
      'teamLeaderId',
      'date',
      'isPotential',
      'project_id',
      'projectId',
      'status_id',
      'source_id',
      'comment',
      'user_id',
      'created_at',
      'createdAt',
      'updated_at',
      'updatedAt',
    ].filter((column) => available.has(column));
    return {
      columns,
      values: columns.map((column) => row[column] ?? null),
      summary: `Lead created: ${row.name}`,
    };
  },
  updateRow: async (body, existing, currentUser) => {
    const available = await getExistingColumns('leads');
    const assignedToId = body.assignedToId ?? body.userId ?? existing.user_id ?? currentUser.id;
    const statusDetails = await resolveLeadStatus({
      statusId: body.statusId ?? body.status_id ?? existing.status_id ?? existing.statusId,
      status: body.status ?? existing.status ?? null,
    });
    const leadType = String(body.type ?? existing.type ?? 'Lead').trim() || 'Lead';
    const leadDate = toSqlDateOnly(body.date ?? existing.date ?? nowTs()) ?? toSqlDateOnly(nowTs());
    const projectName = String(body.project ?? body.projectName ?? existing.project ?? '').trim() || (existing.project ?? null);
    const sourceName = String(body.source ?? body.sourceName ?? existing.source ?? '').trim() || (existing.source ?? null);
    const assignedName = String(body.assignedTo ?? body.assignedToName ?? existing.assignedTo ?? '').trim() || (existing.assignedTo ?? null);
    const teamLeaderName = String(body.teamLeader ?? body.teamLeaderName ?? existing.teamLeader ?? '').trim() || (existing.teamLeader ?? null);
    const isPotential = body.isPotential !== undefined
      ? toBool(body.isPotential)
      : Number(existing.isPotential ?? 0) === 1 || Number(statusDetails.statusId) === 3 || statusDetails.status.toLowerCase().includes('potential');
    const row = {
      type: leadType,
      name: String(body.name ?? existing.name ?? '').trim(),
      phone: String(body.phone ?? existing.phone ?? '').trim(),
      whatsapp: String(body.whatsapp ?? existing.whatsapp ?? '').trim() || null,
      workphone: String(body.workPhone ?? body.workphone ?? existing.workphone ?? '').trim() || null,
      email: String(body.email ?? existing.email ?? '').trim() || null,
      status: statusDetails.status,
      statusId: statusDetails.statusId,
      source: sourceName,
      sourceId: body.sourceId ?? body.source_id ?? existing.source_id ?? null,
      project: projectName,
      project_id: body.projectId ?? body.project_id ?? existing.project_id ?? null,
      projectId: body.projectId ?? body.project_id ?? existing.project_id ?? null,
      assignedTo: assignedName,
      assignedToId,
      teamLeader: teamLeaderName,
      teamLeaderId: body.teamLeaderId ?? body.teamLeader_id ?? existing.teamLeaderId ?? existing.teamLeader_id ?? null,
      date: leadDate,
      isPotential,
      status_id: statusDetails.statusId,
      statusId: statusDetails.statusId,
      source_id: body.sourceId ?? body.source_id ?? existing.source_id ?? null,
      sourceId: body.sourceId ?? body.source_id ?? existing.source_id ?? null,
      comment: body.comment ?? existing.comment ?? null,
      user_id: assignedToId,
      assignedToId,
      updated_at: toSqlDateTime(body.updatedAt ?? nowTs()),
      updatedAt: toSqlDateTime(body.updatedAt ?? nowTs()),
    };
    const columns = [
      'type',
      'name',
      'phone',
      'whatsapp',
      'workphone',
      'email',
      'status',
      'statusId',
      'source',
      'sourceId',
      'project',
      'assignedTo',
      'assignedToId',
      'teamLeader',
      'teamLeaderId',
      'date',
      'isPotential',
      'project_id',
      'projectId',
      'status_id',
      'source_id',
      'comment',
      'user_id',
      'updated_at',
      'updatedAt',
    ].filter((column) => available.has(column));
    return {
      columns,
      values: columns.map((column) => row[column] ?? null),
      summary: `Lead updated: ${row.name}`,
    };
  },
  deleteSummary: (row) => `Lead deleted: ${row?.name ?? row?.id ?? ''}`,
});

registerCrudRoutes({
  routeBase: 'cold-calls',
  table: 'cold_calls',
  baseSql: 'SELECT * FROM cold_calls',
  orderBy: 'id DESC',
  includeGet: false,
  createRow: async (body, currentUser) => {
    const nextId = await nextTableId('cold_calls');
    const available = await getExistingColumns('cold_calls');
    const assignedToId = body.assignedToId ?? body.userId ?? currentUser.id;
    const statusId = await resolveColdCallStatusId(body.statusId ?? body.status_id);
    const row = {
      id: nextId,
      name: String(body.name ?? '').trim(),
      phone: String(body.phone ?? '').trim(),
      status_id: statusId,
      statusId: statusId,
      user_id: assignedToId,
      assignedToId,
      notes: body.notes ?? null,
      created_at: toSqlDateTime(body.createdAt ?? nowTs()),
      createdAt: toSqlDateTime(body.createdAt ?? nowTs()),
      updated_at: toSqlDateTime(body.updatedAt ?? nowTs()),
      updatedAt: toSqlDateTime(body.updatedAt ?? nowTs()),
    };
    const columns = [
      'id',
      'name',
      'phone',
      'status_id',
      'statusId',
      'user_id',
      'assignedToId',
      'notes',
      'created_at',
      'createdAt',
      'updated_at',
      'updatedAt',
    ].filter((column) => available.has(column));
    return {
      columns,
      values: columns.map((column) => row[column] ?? null),
      summary: `Cold call created: ${row.name}`,
    };
  },
  updateRow: async (body, existing, currentUser) => {
    const available = await getExistingColumns('cold_calls');
    const assignedToId = body.assignedToId ?? body.userId ?? existing.user_id ?? currentUser.id;
    const statusId = await resolveColdCallStatusId(body.statusId ?? body.status_id ?? existing.status_id);
    const row = {
      name: String(body.name ?? existing.name ?? '').trim(),
      phone: String(body.phone ?? existing.phone ?? '').trim(),
      status_id: statusId,
      statusId: statusId,
      user_id: assignedToId,
      assignedToId,
      notes: body.notes ?? existing.notes ?? null,
      updated_at: toSqlDateTime(body.updatedAt ?? nowTs()),
      updatedAt: toSqlDateTime(body.updatedAt ?? nowTs()),
    };
    const columns = [
      'name',
      'phone',
      'status_id',
      'statusId',
      'user_id',
      'assignedToId',
      'notes',
      'updated_at',
      'updatedAt',
    ].filter((column) => available.has(column));
    return {
      columns,
      values: columns.map((column) => row[column] ?? null),
      summary: `Cold call updated: ${row.name}`,
    };
  },
  deleteSummary: (row) => `Cold call deleted: ${row?.name ?? row?.id ?? ''}`,
});

registerCrudRoutes({
  routeBase: 'comments',
  table: 'comments',
  baseSql: 'SELECT * FROM comments',
  orderBy: 'id DESC',
  includeGet: false,
  createRow: async (body, currentUser) => {
    const nextId = await nextTableId('comments');
    const available = await getExistingColumns('comments');
    const entityType = String(body.entityType ?? (body.coldCallId ? 'cold_call' : 'lead')).toLowerCase();
    const entityId = body.entityId ?? body.leadId ?? body.coldCallId ?? null;
    const leadId = body.leadId ?? (entityType === 'cold_call' ? entityId : entityId);
    const content = String(body.content ?? body.body ?? '').trim();
    const createdById = body.createdById ?? body.userId ?? currentUser.id;
    const createdBy = String(body.createdBy ?? currentUser.name ?? 'Unknown').trim() || 'Unknown';
    const createdAt = String(body.createdAt ?? nowTs());
    const row = {
      id: nextId,
      leadId,
      content,
      createdBy,
      createdById,
      createdAt,
      user_id: createdById,
      lead_id: entityType === 'cold_call' ? null : entityId,
      cold_call_id: entityType === 'cold_call' ? entityId : null,
      entity_type: entityType,
      entity_id: entityId,
      body: content,
      created_at: toSqlDateTime(body.createdAt ?? nowTs()),
      updated_at: toSqlDateTime(body.updatedAt ?? nowTs()),
    };
    const columns = [
      'id',
      'leadId',
      'content',
      'createdBy',
      'createdById',
      'createdAt',
      'user_id',
      'lead_id',
      'cold_call_id',
      'entity_type',
      'entity_id',
      'body',
      'created_at',
      'updated_at',
    ].filter((column) => available.has(column));
    return {
      columns,
      values: columns.map((column) => row[column] ?? null),
      summary: `Comment created`,
    };
  },
  updateRow: async (body, existing, currentUser) => {
    const available = await getExistingColumns('comments');
    const entityType = String(body.entityType ?? existing.entity_type ?? (existing.cold_call_id ? 'cold_call' : 'lead')).toLowerCase();
    const entityId = body.entityId ?? body.leadId ?? body.coldCallId ?? existing.entity_id ?? existing.lead_id ?? existing.cold_call_id ?? null;
    const leadId = body.leadId ?? existing.leadId ?? (entityType === 'cold_call' ? entityId : entityId);
    const content = String(body.content ?? body.body ?? existing.body ?? '').trim();
    const createdById = body.createdById ?? body.userId ?? existing.user_id ?? currentUser.id;
    const createdBy = String(body.createdBy ?? existing.createdBy ?? currentUser.name ?? 'Unknown').trim() || 'Unknown';
    const createdAt = String(body.createdAt ?? existing.createdAt ?? nowTs());
    const row = {
      leadId,
      content,
      createdBy,
      createdById,
      createdAt,
      user_id: createdById,
      lead_id: entityType === 'cold_call' ? null : entityId,
      cold_call_id: entityType === 'cold_call' ? entityId : null,
      entity_type: entityType,
      entity_id: entityId,
      body: content,
      updated_at: toSqlDateTime(body.updatedAt ?? nowTs()),
    };
    const columns = [
      'leadId',
      'content',
      'createdBy',
      'createdById',
      'createdAt',
      'user_id',
      'lead_id',
      'cold_call_id',
      'entity_type',
      'entity_id',
      'body',
      'updated_at',
    ].filter((column) => available.has(column));
    return {
      columns,
      values: columns.map((column) => row[column] ?? null),
      summary: `Comment updated`,
    };
  },
  deleteSummary: (row) => `Comment deleted`,
  afterCreate: async () => {
    await rebuildReportsFromComments();
  },
  afterUpdate: async () => {
    await rebuildReportsFromComments();
  },
  afterDelete: async () => {
    await rebuildReportsFromComments();
  },
});

app.get('/api/reports', async (req, res) => {
  try {
    const currentUser = req.currentUser;
    const visibleUserIds = await getAccessibleUserIds(currentUser);
    const userFilter = isSuperUser(currentUser)
      ? ''
      : `WHERE reports.agent_id IN (${visibleUserIds.map(() => '?').join(', ')})`;
    const [rows] = await pool.query(
      `SELECT reports.* FROM reports ${userFilter} ORDER BY reports.created_at DESC, reports.id DESC`,
      isSuperUser(currentUser) ? [] : visibleUserIds
    );
    res.json(
      rows.map((row) => ({
        id: row.id,
        commentId: row.comment_id ?? row.commentId ?? row.id,
        leadId: row.lead_id ?? row.leadId ?? null,
        coldCallId: row.cold_call_id ?? row.coldCallId ?? null,
        entityType: row.entity_type ?? 'lead',
        entityId: row.entity_id ?? row.entityId ?? null,
        clientName: row.client_name ?? 'Unknown',
        clientPhone: row.client_phone ?? '',
        agentId: row.agent_id ?? null,
        agentName: row.agent_name ?? '',
        teamLeaderId: row.team_leader_id ?? null,
        teamLeaderName: row.team_leader_name ?? '',
        statusId: row.status_id ?? null,
        statusName: row.status_name ?? '',
        projectId: row.project_id ?? null,
        projectName: row.project_name ?? '',
        developerId: row.developer_id ?? null,
        developerName: row.developer_name ?? '',
        content: row.content ?? '',
        createdBy: row.created_by ?? '',
        createdById: row.created_by_id ?? null,
        createdAt: toClientDateTime(row.created_at),
        updatedAt: toClientDateTime(row.updated_at),
      }))
    );
  } catch (error) {
    res.status(500).json({ error: error.message });
  }
});

registerCrudRoutes({
  routeBase: 'deals',
  table: 'deals',
  baseSql: 'SELECT * FROM deals',
  orderBy: 'id DESC',
  includeGet: false,
  writeRoles: ['admin', 'franchise_owner'],
  createRow: async (body, currentUser) => {
    const nextId = await nextTableId('deals');
    const available = await getExistingColumns('deals');
    const leadName = String(body.leadName ?? body.name ?? '').trim();
    const clientPhone = String(body.clientPhone ?? body.phone ?? '').trim();
    const agentId = body.agentId ?? body.userId ?? currentUser.id;
    const amount = Number(body.amount ?? body.contractPrice ?? body.price ?? 0) || 0;
    const contractPrice = Number(body.contractPrice ?? body.amount ?? body.price ?? 0) || 0;
    const leadId = body.leadId ?? nextId;
    const agentName = String(body.agentName ?? '').trim() || (await lookupNameById('users', agentId)) || currentUser.name || 'Unknown';
    const projectId = body.projectId ?? null;
    const projectName = String(body.projectName ?? '').trim() || (await lookupNameById('projects', projectId)) || 'Unknown';
    const developerId = body.developerId ?? null;
    const developerName = String(body.developerName ?? '').trim() || (await lookupNameById('developers', developerId)) || 'Unknown';
    const dealDate = toSqlDateOnly(body.date ?? body.contractDate ?? nowTs()) ?? toSqlDateOnly(nowTs());
    const statusValue = String(body.status ?? 'pending').trim() || 'pending';
    const row = {
      id: nextId,
      leadId,
      lead_id: leadId,
      leadName,
      lead_name: leadName,
      clientPhone,
      client_phone: clientPhone,
      agentId,
      agent_id: agentId,
      agentName,
      agent_name: agentName,
      projectId,
      project_id: projectId,
      projectName,
      project_name: projectName,
      developerId,
      developer_id: developerId,
      developerName,
      developer_name: developerName,
      brokerId: body.brokerId ?? null,
      broker_id: body.brokerId ?? null,
      attachments: JSON.stringify(body.attachments ?? []),
      reservations_images: JSON.stringify(body.reservationImages ?? body.reservationsImages ?? []),
      reservation_price: body.reservationPrice ?? null,
      reservation_date: toSqlDateOnly(body.reservationDate),
      amount: contractPrice || amount || null,
      price: contractPrice || amount || null,
      contract_date: toSqlDateOnly(body.contractDate),
      unit_number: body.unitNumber ?? null,
      commission: body.commission ?? 0,
      stage: body.stage ?? null,
      comment: body.notes ?? body.comment ?? null,
      status: statusValue,
      user_id: agentId,
      createdAt: toSqlDateTime(body.createdAt ?? nowTs()),
      date: dealDate,
      created_at: toSqlDateTime(body.createdAt ?? nowTs()),
      updated_at: toSqlDateTime(body.updatedAt ?? nowTs()),
    };
    const columns = [
      'id',
      'leadId',
      'lead_id',
      'leadName',
      'lead_name',
      'clientPhone',
      'client_phone',
      'agentId',
      'agent_id',
      'agentName',
      'agent_name',
      'projectId',
      'project_id',
      'projectName',
      'project_name',
      'developerId',
      'developer_id',
      'developerName',
      'developer_name',
      'brokerId',
      'broker_id',
      'attachments',
      'reservations_images',
      'reservation_price',
      'reservation_date',
      'amount',
      'price',
      'contract_date',
      'unit_number',
      'commission',
      'stage',
      'comment',
      'status',
      'user_id',
      'createdAt',
      'date',
      'created_at',
      'updated_at',
    ].filter((column) => available.has(column));
    return {
      columns,
      values: columns.map((column) => row[column] ?? null),
      summary: `Deal created: ${leadName}`,
    };
  },
  updateRow: async (body, existing, currentUser) => {
    const available = await getExistingColumns('deals');
    const leadName = String(body.leadName ?? body.name ?? existing.leadName ?? existing.lead_name ?? existing.name ?? '').trim();
    const clientPhone = String(body.clientPhone ?? body.phone ?? existing.clientPhone ?? existing.client_phone ?? existing.phone ?? '').trim();
    const agentId = body.agentId ?? body.userId ?? existing.agentId ?? existing.agent_id ?? existing.user_id ?? currentUser.id;
    const amount = Number(body.amount ?? body.contractPrice ?? body.price ?? existing.price ?? 0) || 0;
    const contractPrice = Number(body.contractPrice ?? body.amount ?? body.price ?? existing.price ?? 0) || 0;
    const leadId = body.leadId ?? existing.leadId ?? existing.lead_id ?? existing.id;
    const agentName = String(body.agentName ?? existing.agentName ?? existing.agent_name ?? '').trim() || (await lookupNameById('users', agentId)) || currentUser.name || 'Unknown';
    const projectId = body.projectId ?? existing.projectId ?? existing.project_id ?? null;
    const projectName = String(body.projectName ?? existing.projectName ?? existing.project_name ?? '').trim() || (await lookupNameById('projects', projectId)) || 'Unknown';
    const developerId = body.developerId ?? existing.developerId ?? existing.developer_id ?? null;
    const developerName = String(body.developerName ?? existing.developerName ?? existing.developer_name ?? '').trim() || (await lookupNameById('developers', developerId)) || 'Unknown';
    const dealDate = toSqlDateOnly(body.date ?? existing.date ?? body.contractDate ?? existing.contract_date ?? nowTs()) ?? toSqlDateOnly(nowTs());
    const statusValue = String(body.status ?? existing.status ?? 'pending').trim() || 'pending';
    const row = {
      leadId,
      lead_id: leadId,
      leadName,
      lead_name: leadName,
      clientPhone,
      client_phone: clientPhone,
      agentId,
      agent_id: agentId,
      agentName,
      agent_name: agentName,
      projectId,
      project_id: projectId,
      projectName,
      project_name: projectName,
      developerId,
      developer_id: developerId,
      developerName,
      developer_name: developerName,
      brokerId: body.brokerId ?? existing.brokerId ?? existing.broker_id ?? null,
      broker_id: body.brokerId ?? existing.brokerId ?? existing.broker_id ?? null,
      attachments: JSON.stringify(body.attachments ?? existing.attachments ?? []),
      reservations_images: JSON.stringify(body.reservationImages ?? body.reservationsImages ?? existing.reservationImages ?? existing.reservations_images ?? []),
      reservation_price: body.reservationPrice ?? existing.reservation_price ?? null,
      reservation_date: toSqlDateOnly(body.reservationDate ?? existing.reservation_date),
      amount: contractPrice || amount || existing.amount || existing.price || null,
      price: contractPrice || amount || (existing.price ?? null),
      contract_date: toSqlDateOnly(body.contractDate ?? existing.contract_date),
      unit_number: body.unitNumber ?? existing.unit_number ?? null,
      commission: body.commission ?? existing.commission ?? 0,
      stage: body.stage ?? existing.stage ?? null,
      comment: body.notes ?? body.comment ?? existing.comment ?? null,
      status: statusValue,
      user_id: agentId,
      createdAt: toSqlDateTime(body.createdAt ?? existing.createdAt ?? existing.created_at ?? nowTs()),
      date: dealDate,
      updated_at: toSqlDateTime(body.updatedAt ?? nowTs()),
    };
    const columns = [
      'leadId',
      'lead_id',
      'leadName',
      'lead_name',
      'clientPhone',
      'client_phone',
      'agentId',
      'agent_id',
      'agentName',
      'agent_name',
      'projectId',
      'project_id',
      'projectName',
      'project_name',
      'developerId',
      'developer_id',
      'developerName',
      'developer_name',
      'brokerId',
      'broker_id',
      'attachments',
      'reservations_images',
      'reservation_price',
      'reservation_date',
      'amount',
      'price',
      'contract_date',
      'unit_number',
      'commission',
      'stage',
      'comment',
      'status',
      'user_id',
      'createdAt',
      'date',
      'updated_at',
    ].filter((column) => available.has(column));
    return {
      columns,
      values: columns.map((column) => row[column] ?? null),
      summary: `Deal updated: ${leadName}`,
    };
  },
  deleteSummary: (row) => `Deal deleted: ${row?.leadName ?? row?.lead_name ?? row?.name ?? row?.id ?? ''}`,
});

app.post('/api/leads/import', async (req, res) => {
  try {
    const rows = Array.isArray(req.body?.rows) ? req.body.rows : Array.isArray(req.body) ? req.body : [];
    const currentUser = req.currentUser;
    const available = await getExistingColumns('leads');
    const existingColumns = await pickAvailableColumns('leads', [
      'id',
      'type',
      'name',
      'phone',
      'whatsapp',
      'workphone',
      'email',
      'status',
      'statusId',
      'source',
      'sourceId',
      'project',
      'assignedTo',
      'assignedToId',
      'teamLeader',
      'teamLeaderId',
      'date',
      'isPotential',
      'project_id',
      'projectId',
      'status_id',
      'source_id',
      'comment',
      'user_id',
      'created_at',
      'createdAt',
      'updated_at',
      'updatedAt',
    ]);
    const existingPhones = new Set();
    if (available.has('phone')) {
      const [existingRows] = await pool.query('SELECT phone FROM leads');
      existingRows.forEach((row) => {
        if (row.phone) existingPhones.add(String(row.phone).replace(/\D/g, ''));
      });
    }
    const normalizedRows = [];
    let nextId = await nextTableId('leads');
    for (const item of rows) {
      const phone = String(item.phone ?? '').trim();
      const normalizedPhone = phone.replace(/\D/g, '');
      if (!normalizedPhone || existingPhones.has(normalizedPhone)) {
        continue;
      }
      existingPhones.add(normalizedPhone);
      const assignedToId = item.assignedToId ?? item.userId ?? currentUser.id;
      const statusDetails = await resolveLeadStatus({ statusId: item.statusId ?? item.status_id, status: item.status });
      const leadType = String(item.type ?? 'Lead').trim() || 'Lead';
      const leadDate = toSqlDateOnly(item.date ?? nowTs()) ?? toSqlDateOnly(nowTs());
      const row = {
        id: nextId++,
        type: leadType,
        name: String(item.name ?? 'Unknown').trim() || 'Unknown',
        phone,
        whatsapp: String(item.whatsapp ?? '').trim() || null,
        workphone: String(item.workPhone ?? item.workphone ?? '').trim() || null,
        email: String(item.email ?? '').trim() || null,
        status: statusDetails.status,
        statusId: statusDetails.statusId,
        source: String(item.source ?? item.sourceName ?? '').trim() || null,
        sourceId: item.sourceId ?? item.source_id ?? null,
        project: String(item.project ?? item.projectName ?? '').trim() || null,
        project_id: item.projectId ?? null,
        projectId: item.projectId ?? null,
        assignedTo: String(item.assignedTo ?? '').trim() || null,
        assignedToId,
        teamLeader: String(item.teamLeader ?? '').trim() || null,
        teamLeaderId: item.teamLeaderId ?? item.teamLeader_id ?? null,
        date: leadDate,
        isPotential: item.isPotential !== undefined
          ? toBool(item.isPotential)
          : Number(statusDetails.statusId) === 3 || statusDetails.status.toLowerCase().includes('potential'),
        status_id: statusDetails.statusId,
        source_id: item.sourceId ?? item.source_id ?? null,
        comment: item.comment ?? null,
        user_id: assignedToId,
        created_at: toSqlDateTime(item.createdAt ?? nowTs()),
        createdAt: toSqlDateTime(item.createdAt ?? nowTs()),
        updated_at: toSqlDateTime(item.updatedAt ?? nowTs()),
        updatedAt: toSqlDateTime(item.updatedAt ?? nowTs()),
      };
      normalizedRows.push(row);
    }
    if (normalizedRows.length > 0) {
      await upsertRows(
        'leads',
        existingColumns,
        normalizedRows.map((row) => existingColumns.map((column) => row[column] ?? null))
      );
    }
    await createActivityLog({
      userId: currentUser.id,
      entityType: 'leads',
      entityId: null,
      action: 'imported',
      summary: `Leads imported (${normalizedRows.length})`,
      metadata: { count: normalizedRows.length },
    });
    res.json({ ok: true, count: normalizedRows.length });
  } catch (error) {
    res.status(500).json({ error: error.message });
  }
});

app.post('/api/cold-calls/import', async (req, res) => {
  try {
    const rows = Array.isArray(req.body?.rows) ? req.body.rows : Array.isArray(req.body) ? req.body : [];
    const currentUser = req.currentUser;
    const existingColumns = await pickAvailableColumns('cold_calls', [
      'id',
      'name',
      'phone',
      'status_id',
      'statusId',
      'user_id',
      'assignedToId',
      'notes',
      'created_at',
      'createdAt',
      'updated_at',
      'updatedAt',
    ]);
    const existingPhones = new Set();
    const [existingRows] = await pool.query('SELECT phone FROM cold_calls');
    existingRows.forEach((row) => {
      if (row.phone) existingPhones.add(String(row.phone).replace(/\D/g, ''));
    });
    const normalizedRows = [];
    let nextId = await nextTableId('cold_calls');
    for (const item of rows) {
      const phone = String(item.phone ?? '').trim();
      const normalizedPhone = phone.replace(/\D/g, '');
      if (!normalizedPhone || existingPhones.has(normalizedPhone)) {
        continue;
      }
      existingPhones.add(normalizedPhone);
      const assignedToId = item.assignedToId ?? item.userId ?? currentUser.id;
      const statusId = await resolveColdCallStatusId(item.statusId ?? item.status_id);
      const row = {
        id: nextId++,
        name: String(item.name ?? 'Unknown').trim() || 'Unknown',
        phone,
        status_id: statusId,
        statusId: statusId,
        user_id: assignedToId,
        assignedToId,
        notes: item.notes ?? null,
        created_at: toSqlDateTime(item.createdAt ?? nowTs()),
        createdAt: toSqlDateTime(item.createdAt ?? nowTs()),
        updated_at: toSqlDateTime(item.updatedAt ?? nowTs()),
        updatedAt: toSqlDateTime(item.updatedAt ?? nowTs()),
      };
      normalizedRows.push(row);
    }
    if (normalizedRows.length > 0) {
      await upsertRows(
        'cold_calls',
        existingColumns,
        normalizedRows.map((row) => existingColumns.map((column) => row[column] ?? null))
      );
    }
    await createActivityLog({
      userId: currentUser.id,
      entityType: 'cold_calls',
      entityId: null,
      action: 'imported',
      summary: `Cold calls imported (${normalizedRows.length})`,
      metadata: { count: normalizedRows.length },
    });
    res.json({ ok: true, count: normalizedRows.length });
  } catch (error) {
    res.status(500).json({ error: error.message });
  }
});

app.get('/api/agent-care', async (_req, res) => {
  try {
    const [rows] = await pool.query('SELECT * FROM agent_care');
    res.json(rows);
  } catch (error) {
    res.status(500).json({ error: error.message });
  }
});

app.post('/api/sync/users', async (req, res) => {
  const rows = Array.isArray(req.body) ? req.body : [];
  try {
    const available = await getExistingColumns('users');
    const columnsAll = [
      'id',
      'name',
      'email',
      'phone',
      'password',
      'department_id',
      'jobtitle_id',
      'parent_id',
      'role',
      'title',
      'department',
      'status',
      'isActive',
      'is_active',
      'image',
      'created_at',
      'createdAt',
      'updated_at',
      'updatedAt',
    ];
    const columns = columnsAll.filter((col) => available.has(col));
    const currentUser = req.currentUser;
    const [existingRows] = await pool.query('SELECT id, password, image, created_at FROM users');
    const existingById = new Map(existingRows.map((row) => [String(row.id), row]));
    const incomingRows = await assignMissingIds('users', rows.map((user) => {
      const createdAt = toSqlDateTime(user.createdAt ?? nowTs());
      return {
        id: user.id,
        name: user.name,
        email: user.email,
        phone: user.phone ?? null,
        password: user.password ?? null,
        department_id: user.departmentId ?? null,
        jobtitle_id: user.jobtitleId ?? null,
        parent_id: user.teamLeaderId ?? null,
        role: user.role ?? 'agent',
        title: user.title ?? null,
        department: user.department ?? null,
        status: toBool(user.isActive ?? true),
        isActive: toBool(user.isActive ?? true),
        is_active: toBool(user.isActive ?? true),
        image: user.avatar ?? defaultUserImage,
        created_at: createdAt,
        createdAt,
        updated_at: nowTs(),
        updatedAt: nowTs(),
      };
    }));

    const visibleUserIds = isSuperUser(currentUser)
      ? incomingRows.map((row) => row.id)
      : await getAccessibleUserIds(currentUser);
    const scopedRows = isSuperUser(currentUser)
      ? incomingRows
      : incomingRows.filter((row) => {
          const rowId = Number(row.id);
          const parentId = Number(row.parent_id ?? 0);
          return rowId === Number(currentUser.id)
            || visibleUserIds.includes(rowId)
            || visibleUserIds.includes(parentId);
        });

    const normalizedRows = [];
    for (const row of scopedRows) {
      const existing = existingById.get(String(row.id));
      const password = await hashIncomingPassword(row.password ?? existing?.password, existing?.password ?? null);
      normalizedRows.push({
        ...row,
        password,
        image: row.image ?? existing?.image ?? defaultUserImage,
      });
    }

    if (isSuperUser(currentUser)) {
      await replaceTable(
        'users',
        columns,
        normalizedRows.map((row) => columns.map((col) => row[col] ?? null))
      );
    } else {
      const [scopeRows] = await pool.query(
        `SELECT id FROM users WHERE id IN (${visibleUserIds.map(() => '?').join(', ')})`,
        visibleUserIds
      );
      const visibleIds = new Set(scopeRows.map((row) => String(row.id)));
      const incomingIds = new Set(normalizedRows.map((row) => String(row.id)));
      await upsertRows(
        'users',
        columns,
        normalizedRows.map((row) => columns.map((col) => row[col] ?? null))
      );
      const idsToDelete = [...visibleIds].filter((id) => !incomingIds.has(id));
      await deleteRowsByIds('users', idsToDelete);
      // Ensure any orphaned child records keep their parent if the parent row is missing.
    }
    await createActivityLog({
      userId: currentUser.id,
      entityType: 'users',
      entityId: null,
      action: 'synced',
      summary: `Users synced (${normalizedRows.length})`,
      metadata: { count: normalizedRows.length },
    });
    res.json({ ok: true, count: rows.length });
  } catch (error) {
    res.status(500).json({ error: error.message });
  }
});

app.post('/api/sync/titles', async (req, res) => {
  const rows = Array.isArray(req.body) ? req.body : [];
  try {
    const payloads = [];
    for (const item of rows) {
      const departmentName = String(item.department ?? 'Sales').trim() || 'Sales';
      const departmentId = await ensureDepartment(departmentName);
      payloads.push([
        item.id,
        item.name,
        departmentId,
        nowTs(),
        nowTs(),
      ]);
    }
    await replaceTable(
      'jobtitles',
      ['id', 'name', 'department_id', 'created_at', 'updated_at'],
      payloads
    );
    await createActivityLog({
      userId: req.currentUser.id,
      entityType: 'jobtitles',
      entityId: null,
      action: 'synced',
      summary: `Job titles synced (${rows.length})`,
      metadata: { count: rows.length },
    });
    res.json({ ok: true, count: rows.length });
  } catch (error) {
    res.status(500).json({ error: error.message });
  }
});

app.post('/api/sync/developers', async (req, res) => {
  const rows = Array.isArray(req.body) ? req.body : [];
  try {
    await replaceTable(
      'developers',
      ['id', 'name', 'phone', 'email', 'whatsapp', 'website', 'description', 'logo', 'created_at', 'updated_at'],
      rows.map(item => [
        item.id,
        item.name,
        item.phone ?? null,
        item.email ?? null,
        item.whatsapp ?? null,
        item.website ?? null,
        item.description ?? null,
        item.logo ?? null,
        nowTs(),
        nowTs(),
      ])
    );
    res.json({ ok: true, count: rows.length });
  } catch (error) {
    res.status(500).json({ error: error.message });
  }
});

app.post('/api/sync/projects', async (req, res) => {
  const rows = Array.isArray(req.body) ? req.body : [];
  try {
    const available = await getExistingColumns('projects');
    const columnsAll = ['id', 'name', 'developer_id', 'developerId', 'created_at', 'createdAt', 'updated_at', 'updatedAt'];
    const columns = columnsAll.filter(col => available.has(col));
    const values = rows.map(item => {
      const createdAt = toSqlDateTime(item.createdAt);
      const updatedAt = toSqlDateTime(item.updatedAt);
      const row = {
        id: item.id,
        name: item.name,
        developer_id: item.developerId ?? null,
        developerId: item.developerId ?? null,
        created_at: createdAt,
        createdAt,
        updated_at: updatedAt,
        updatedAt,
      };
      return columns.map(col => row[col] ?? null);
    });
    await replaceTable('projects', columns, values);
    res.json({ ok: true, count: rows.length });
  } catch (error) {
    res.status(500).json({ error: error.message });
  }
});

app.post('/api/sync/statuses', async (req, res) => {
  const rows = Array.isArray(req.body) ? req.body : [];
  try {
    await replaceTable(
      'statuses',
      ['id', 'name', 'color', 'created_at', 'updated_at'],
      rows.map(item => [item.id, item.name, item.color, nowTs(), nowTs()])
    );
    res.json({ ok: true, count: rows.length });
  } catch (error) {
    res.status(500).json({ error: error.message });
  }
});

app.post('/api/sync/lead-sources', async (req, res) => {
  const rows = Array.isArray(req.body) ? req.body : [];
  try {
    await replaceTable(
      'sources',
      ['id', 'name', 'color', 'created_at', 'updated_at'],
      rows.map(item => [item.id, item.name, item.color, nowTs(), nowTs()])
    );
    res.json({ ok: true, count: rows.length });
  } catch (error) {
    res.status(500).json({ error: error.message });
  }
});

app.post('/api/sync/leads', async (req, res) => {
  const rows = Array.isArray(req.body) ? req.body : [];
  try {
    await syncScopedRows({
      table: 'leads',
      rows,
      currentUser: req.currentUser,
      scopeField: 'user_id',
      columns: [
        'id',
        'name',
        'phone',
        'whatsapp',
        'workphone',
        'date',
        'project_id',
        'status_id',
        'source_id',
        'comment',
        'user_id',
        'created_at',
        'updated_at',
      ],
      mapRow: (item) => ({
        id: item.id,
        name: item.name,
        phone: item.phone,
        whatsapp: item.whatsapp ?? null,
        workphone: item.workPhone ?? null,
        date: toSqlDateOnly(item.date),
        project_id: item.projectId ?? null,
        status_id: item.statusId ?? null,
        source_id: item.sourceId ?? null,
        comment: item.comment ?? null,
        user_id: item.assignedToId ?? req.currentUser.id,
        created_at: toSqlDateTime(item.createdAt),
        updated_at: toSqlDateTime(item.updatedAt),
      }),
    });
    await createActivityLog({
      userId: req.currentUser.id,
      entityType: 'leads',
      entityId: null,
      action: 'synced',
      summary: `Leads synced (${rows.length})`,
      metadata: { count: rows.length },
    });
    res.json({ ok: true, count: rows.length });
  } catch (error) {
    res.status(500).json({ error: error.message });
  }
});

app.post('/api/sync/cold-calls', async (req, res) => {
  const rows = Array.isArray(req.body) ? req.body : [];
  try {
    await syncScopedRows({
      table: 'cold_calls',
      rows,
      currentUser: req.currentUser,
      scopeField: 'user_id',
      columns: ['id', 'name', 'phone', 'status_id', 'user_id', 'notes', 'created_at', 'updated_at'],
      mapRow: (item) => ({
        id: item.id,
        name: item.name,
        phone: item.phone,
        status_id: item.statusId ?? null,
        user_id: item.assignedToId ?? req.currentUser.id,
        notes: item.notes ?? null,
        created_at: toSqlDateTime(item.createdAt),
        updated_at: toSqlDateTime(item.updatedAt),
      }),
    });
    await createActivityLog({
      userId: req.currentUser.id,
      entityType: 'cold_calls',
      entityId: null,
      action: 'synced',
      summary: `Cold calls synced (${rows.length})`,
      metadata: { count: rows.length },
    });
    res.json({ ok: true, count: rows.length });
  } catch (error) {
    res.status(500).json({ error: error.message });
  }
});

app.post('/api/sync/comments', async (req, res) => {
  const rows = Array.isArray(req.body) ? req.body : [];
  try {
    const available = await getExistingColumns('comments');
    const columnsAll = [
      'id',
      'leadId',
      'content',
      'createdBy',
      'createdById',
      'createdAt',
      'lead_id',
      'cold_call_id',
      'entity_type',
      'entity_id',
      'body',
      'user_id',
      'created_at',
      'updated_at',
    ];
    const columns = columnsAll.filter((col) => available.has(col));
    const normalizedRows = rows.map((item) => {
      const entityId = Number(item.entityId ?? item.leadId ?? 0) || null;
      const entityType = item.entityType ?? 'lead';
      const createdAt = toSqlDateTime(item.createdAt);
      const createdById = item.createdById ?? req.currentUser.id;
      const createdBy = item.createdBy ?? req.currentUser.name ?? 'Unknown';
      const content = item.content ?? '';
      return {
        id: item.id,
        leadId: entityId,
        content,
        createdBy,
        createdById,
        createdAt,
        lead_id: entityType === 'cold_call' ? null : entityId,
        cold_call_id: entityType === 'cold_call' ? entityId : null,
        entity_type: entityType,
        entity_id: entityId,
        body: content,
        user_id: createdById,
        created_at: createdAt,
        updated_at: toSqlDateTime(item.updatedAt ?? createdAt),
      };
    });

    if (isSuperUser(req.currentUser)) {
      await replaceTable(
        'comments',
        columns,
        normalizedRows.map((row) => columns.map((column) => row[column] ?? null))
      );
    } else {
      const visibleUserIds = await getAccessibleUserIds(req.currentUser);
      const scopedRows = normalizedRows.filter((row) => row.user_id === req.currentUser.id || visibleUserIds.includes(Number(row.user_id)));
      const [existingRows] = await pool.query(
        `SELECT id FROM comments WHERE user_id IN (${visibleUserIds.map(() => '?').join(', ')})`,
        visibleUserIds
      );
      const visibleIds = new Set(existingRows.map((row) => String(row.id)));
      const incomingIds = new Set(scopedRows.map((row) => String(row.id)));
      await upsertRows(
        'comments',
        columns,
        scopedRows.map((row) => columns.map((column) => row[column] ?? null))
      );
      const idsToDelete = [...visibleIds].filter((id) => !incomingIds.has(id));
      await deleteRowsByIds('comments', idsToDelete);
    }
    await createActivityLog({
      userId: req.currentUser.id,
      entityType: 'comments',
      entityId: null,
      action: 'synced',
      summary: `Comments synced (${rows.length})`,
      metadata: { count: rows.length },
    });
    await rebuildReportsFromComments();
    res.json({ ok: true, count: rows.length });
  } catch (error) {
    res.status(500).json({ error: error.message });
  }
});

app.post('/api/sync/deals', async (req, res) => {
  const rows = Array.isArray(req.body) ? req.body : [];
  try {
    if (!isSuperUser(req.currentUser)) {
      res.status(403).json({ error: 'Forbidden' });
      return;
    }
    await syncScopedRows({
      table: 'deals',
      rows,
      currentUser: req.currentUser,
      scopeField: 'user_id',
      // Preserve compatibility with both the legacy camelCase schema and the newer snake_case schema.
      columns: Array.from(new Set([
        'id',
        'leadId',
        'leadName',
        'clientPhone',
        'agentId',
        'agentName',
        'projectId',
        'projectName',
        'developerId',
        'developerName',
        'amount',
        'commission',
        'status',
        'stage',
        'reservationPrice',
        'reservationDate',
        'contractPrice',
        'contractDate',
        'unitNumber',
        'createdAt',
        'date',
        'notes',
        'lead_id',
        'lead_name',
        'client_phone',
        'agent_id',
        'project_id',
        'developer_id',
        'broker_id',
        'attachments',
        'reservations_images',
        'reservation_price',
        'reservation_date',
        'price',
        'contract_date',
        'unit_number',
        'commission',
        'stage',
        'comment',
        'status',
        'user_id',
        'created_at',
        'updated_at',
      ])),
      mapRow: (item) => ({
        id: item.id,
        leadId: item.leadId ?? item.id,
        leadName: item.leadName ?? item.name ?? '',
        clientPhone: item.clientPhone ?? item.phone ?? null,
        agentId: item.agentId ?? item.userId ?? req.currentUser.id,
        agentName: item.agentName ?? '',
        projectId: item.projectId ?? null,
        projectName: item.projectName ?? '',
        developerId: item.developerId ?? null,
        developerName: item.developerName ?? '',
        amount: item.amount ?? item.contractPrice ?? item.price ?? null,
        contractPrice: item.contractPrice ?? item.amount ?? item.price ?? null,
        commission: item.commission ?? 0,
        status: item.status ?? null,
        stage: item.stage ?? null,
        reservationPrice: item.reservationPrice ?? null,
        reservationDate: toSqlDateOnly(item.reservationDate),
        contractPrice: item.contractPrice ?? item.amount ?? item.price ?? null,
        contractDate: toSqlDateOnly(item.contractDate),
        unitNumber: item.unitNumber ?? null,
        createdAt: toSqlDateTime(item.createdAt),
        date: item.date ?? toSqlDateOnly(item.contractDate ?? item.createdAt),
        notes: item.notes ?? item.comment ?? null,
        lead_id: item.leadId ?? item.id,
        lead_name: item.leadName ?? item.name ?? '',
        client_phone: item.clientPhone ?? item.phone ?? null,
        agent_id: item.agentId ?? item.userId ?? req.currentUser.id,
        project_id: item.projectId ?? null,
        developer_id: item.developerId ?? null,
        broker_id: item.brokerId ?? null,
        attachments: JSON.stringify(item.attachments ?? []),
        reservations_images: JSON.stringify(item.reservationImages ?? item.reservationsImages ?? []),
        reservation_price: item.reservationPrice ?? null,
        reservation_date: toSqlDateOnly(item.reservationDate),
        price: item.contractPrice ?? item.amount ?? item.price ?? null,
        contract_date: toSqlDateOnly(item.contractDate),
        unit_number: item.unitNumber ?? null,
        commission: item.commission ?? 0,
        stage: item.stage ?? null,
        comment: item.notes ?? null,
        user_id: item.agentId ?? item.userId ?? req.currentUser.id,
        created_at: toSqlDateTime(item.createdAt),
        updated_at: toSqlDateTime(item.updatedAt),
      }),
    });
    await createActivityLog({
      userId: req.currentUser.id,
      entityType: 'deals',
      entityId: null,
      action: 'synced',
      summary: `Deals synced (${rows.length})`,
      metadata: { count: rows.length },
    });
    res.json({ ok: true, count: rows.length });
  } catch (error) {
    res.status(500).json({ error: error.message });
  }
});

app.post('/api/sync/agent-care', async (req, res) => {
  const item = req.body || null;
  if (!item) {
    res.status(400).json({ error: 'Missing agent care data' });
    return;
  }
  try {
    await replaceTable(
      'agent_care',
      ['id', 'name', 'phone', 'whatsapp', 'email'],
      [[item.id, item.name, item.phone, item.whatsapp, item.email]]
    );
    res.json({ ok: true });
  } catch (error) {
    res.status(500).json({ error: error.message });
  }
});

function registerCrudRoutes({
  routeBase,
  table,
  baseSql,
  orderBy = 'id DESC',
  scopeField = null,
  writeRoles = null,
  includeGet = true,
  serialize = (row) => row,
  createRow,
  updateRow,
  deleteSummary = (row) => row?.title ?? row?.name ?? `ID ${row?.id ?? ''}`,
  afterCreate,
  afterUpdate,
  afterDelete,
}) {
  const canWrite = (user) => {
    if (!writeRoles || writeRoles.length === 0) return true;
    return writeRoles.includes(normalizeRole(user?.role));
  };

  if (includeGet) {
    app.get(`/api/${routeBase}`, async (req, res) => {
      try {
        let sql = baseSql;
        let params = [];

        if (scopeField && !isSuperUser(req.currentUser)) {
          const visibleUserIds = await getAccessibleUserIds(req.currentUser);
          sql += ` WHERE ${scopeField} IN (${visibleUserIds.map(() => '?').join(', ')})`;
          params = visibleUserIds;
        }

        if (orderBy) {
          sql += ` ORDER BY ${orderBy}`;
        }

        const [rows] = await pool.query(sql, params);
        res.json(rows.map(serialize));
      } catch (error) {
        res.status(500).json({ error: error.message });
      }
    });
  }

  app.post(`/api/${routeBase}`, async (req, res) => {
    try {
      if (!canWrite(req.currentUser)) {
        res.status(403).json({ error: 'Forbidden' });
        return;
      }
      const payload = await createRow(req.body, req.currentUser);
      const columns = payload.columns;
      const values = payload.values;
      const placeholders = columns.map(() => '?').join(', ');
      const [result] = await pool.query(
        `INSERT INTO ${table} (${columns.join(', ')}) VALUES (${placeholders})`,
        values
      );
      const explicitId =
        payload.id ??
        (Array.isArray(payload.columns) && Array.isArray(payload.values) && payload.columns.includes('id')
          ? payload.values[payload.columns.indexOf('id')]
          : null);
      const createdId = result.insertId && Number(result.insertId) > 0 ? Number(result.insertId) : explicitId ?? null;
      await createActivityLog({
        userId: req.currentUser.id,
        entityType: table,
        entityId: createdId,
        action: 'created',
        summary: payload.summary ?? `${table} created`,
        metadata: payload.metadata ?? null,
      });
      if (afterCreate) {
        await afterCreate({
          id: createdId,
          payload,
          body: req.body,
          currentUser: req.currentUser,
        });
      }
      res.json({ ok: true, id: createdId });
    } catch (error) {
      res.status(500).json({ error: error.message });
    }
  });

  app.put(`/api/${routeBase}/:id`, async (req, res) => {
    try {
      if (!canWrite(req.currentUser)) {
        res.status(403).json({ error: 'Forbidden' });
        return;
      }
      const [existingRows] = await pool.query(`SELECT * FROM ${table} WHERE id = ? LIMIT 1`, [
        req.params.id,
      ]);
      const existing = existingRows[0] ?? null;
      if (!existing) {
        res.status(404).json({ error: 'Not found' });
        return;
      }
      const payload = await updateRow(req.body, existing, req.currentUser);
      const setClause = payload.columns.map((column) => `${column} = ?`).join(', ');
      await pool.query(
        `UPDATE ${table} SET ${setClause} WHERE id = ?`,
        [...payload.values, req.params.id]
      );
      await createActivityLog({
        userId: req.currentUser.id,
        entityType: table,
        entityId: Number(req.params.id),
        action: 'updated',
        summary: payload.summary ?? `${table} updated`,
        metadata: payload.metadata ?? null,
      });
      if (afterUpdate) {
        await afterUpdate({
          id: Number(req.params.id),
          payload,
          existing,
          body: req.body,
          currentUser: req.currentUser,
        });
      }
      res.json({ ok: true });
    } catch (error) {
      res.status(500).json({ error: error.message });
    }
  });

  app.delete(`/api/${routeBase}/:id`, async (req, res) => {
    try {
      if (!canWrite(req.currentUser)) {
        res.status(403).json({ error: 'Forbidden' });
        return;
      }
      const [existingRows] = await pool.query(`SELECT * FROM ${table} WHERE id = ? LIMIT 1`, [
        req.params.id,
      ]);
      const existing = existingRows[0] ?? null;
      await pool.query(`DELETE FROM ${table} WHERE id = ?`, [req.params.id]);
      await createActivityLog({
        userId: req.currentUser.id,
        entityType: table,
        entityId: Number(req.params.id),
        action: 'deleted',
        summary: `${deleteSummary(existing)}`,
        metadata: existing ?? null,
      });
      if (afterDelete) {
        await afterDelete({
          id: Number(req.params.id),
          existing,
          body: req.body,
          currentUser: req.currentUser,
        });
      }
      res.json({ ok: true });
    } catch (error) {
      res.status(500).json({ error: error.message });
    }
  });
}

registerCrudRoutes({
  routeBase: 'departments',
  table: 'departments',
  baseSql: 'SELECT id, name, created_at, updated_at FROM departments',
  orderBy: 'id ASC',
  writeRoles: ['admin', 'franchise_owner'],
  serialize: (row) => ({
    id: row.id,
    name: row.name,
  }),
  createRow: async (body) => ({
    columns: ['name', 'created_at', 'updated_at'],
    values: [String(body.name ?? '').trim(), nowTs(), nowTs()],
    summary: `Department created: ${String(body.name ?? '').trim()}`,
  }),
  updateRow: async (body, existing) => ({
    columns: ['name', 'updated_at'],
    values: [String(body.name ?? existing.name ?? '').trim(), nowTs()],
    summary: `Department updated: ${String(body.name ?? existing.name ?? '').trim()}`,
  }),
  deleteSummary: (row) => `Department deleted: ${row?.name ?? row?.id ?? ''}`,
});

registerCrudRoutes({
  routeBase: 'jobtitles',
  table: 'jobtitles',
  baseSql: `SELECT jobtitles.id, jobtitles.name, jobtitles.department_id, departments.name AS department_name
            FROM jobtitles
            LEFT JOIN departments ON departments.id = jobtitles.department_id`,
  orderBy: 'jobtitles.id ASC',
  writeRoles: ['admin', 'franchise_owner'],
  serialize: (row) => ({
    id: row.id,
    name: row.name,
    departmentId: row.department_id ?? null,
    departmentName: row.department_name ?? 'Sales',
  }),
  createRow: async (body) => ({
    columns: ['name', 'department_id', 'created_at', 'updated_at'],
    values: [
      String(body.name ?? '').trim(),
      body.departmentId ?? body.department_id ?? null,
      nowTs(),
      nowTs(),
    ],
    summary: `Job title created: ${String(body.name ?? '').trim()}`,
  }),
  updateRow: async (body, existing) => ({
    columns: ['name', 'department_id', 'updated_at'],
    values: [
      String(body.name ?? existing.name ?? '').trim(),
      body.departmentId ?? body.department_id ?? existing.department_id ?? null,
      nowTs(),
    ],
    summary: `Job title updated: ${String(body.name ?? existing.name ?? '').trim()}`,
  }),
  deleteSummary: (row) => `Job title deleted: ${row?.name ?? row?.id ?? ''}`,
});

registerCrudRoutes({
  routeBase: 'brokers',
  table: 'brokers',
  baseSql: `SELECT brokers.id, brokers.name, brokers.phone, brokers.developer_id, developers.name AS developer_name
            FROM brokers
            LEFT JOIN developers ON developers.id = brokers.developer_id`,
  orderBy: 'brokers.id ASC',
  writeRoles: ['admin', 'franchise_owner'],
  serialize: (row) => ({
    id: row.id,
    name: row.name,
    phone: row.phone ?? '',
    developerId: row.developer_id ?? null,
    developerName: row.developer_name ?? 'Unknown',
  }),
  createRow: async (body) => ({
    columns: ['name', 'phone', 'developer_id', 'created_at', 'updated_at'],
    values: [
      String(body.name ?? '').trim(),
      String(body.phone ?? '').trim() || null,
      body.developerId ?? body.developer_id ?? null,
      nowTs(),
      nowTs(),
    ],
    summary: `Broker created: ${String(body.name ?? '').trim()}`,
  }),
  updateRow: async (body, existing) => ({
    columns: ['name', 'phone', 'developer_id', 'updated_at'],
    values: [
      String(body.name ?? existing.name ?? '').trim(),
      String(body.phone ?? existing.phone ?? '').trim() || null,
      body.developerId ?? body.developer_id ?? existing.developer_id ?? null,
      nowTs(),
    ],
    summary: `Broker updated: ${String(body.name ?? existing.name ?? '').trim()}`,
  }),
  deleteSummary: (row) => `Broker deleted: ${row?.name ?? row?.id ?? ''}`,
});

registerCrudRoutes({
  routeBase: 'calendar-events',
  table: 'calendar_events',
  baseSql: 'SELECT * FROM calendar_events',
  orderBy: 'COALESCE(start_at, created_at) DESC',
  scopeField: 'user_id',
  serialize: (row) => ({
    id: row.id,
    title: row.title,
    type: row.type ?? '',
    notes: row.notes ?? '',
    startAt: toClientDateTime(row.start_at),
    endAt: toClientDateTime(row.end_at),
    dueDate: toClientDateOnly(row.due_date),
    leadId: row.lead_id ?? null,
    coldCallId: row.cold_call_id ?? null,
    dealId: row.deal_id ?? null,
    userId: row.user_id ?? null,
  }),
  createRow: async (body, currentUser) => ({
    columns: ['title', 'type', 'notes', 'start_at', 'end_at', 'due_date', 'lead_id', 'cold_call_id', 'deal_id', 'user_id', 'created_at', 'updated_at'],
    values: [
      String(body.title ?? '').trim(),
      body.type ?? null,
      body.notes ?? null,
      body.startAt ? toSqlDateTime(body.startAt) : null,
      body.endAt ? toSqlDateTime(body.endAt) : null,
      body.dueDate ? toSqlDateOnly(body.dueDate) : null,
      body.leadId ?? null,
      body.coldCallId ?? null,
      body.dealId ?? null,
      body.userId ?? currentUser.id,
      nowTs(),
      nowTs(),
    ],
    summary: `Calendar event created: ${String(body.title ?? '').trim()}`,
  }),
  updateRow: async (body, existing, currentUser) => ({
    columns: ['title', 'type', 'notes', 'start_at', 'end_at', 'due_date', 'lead_id', 'cold_call_id', 'deal_id', 'user_id', 'updated_at'],
    values: [
      String(body.title ?? existing.title ?? '').trim(),
      body.type ?? existing.type ?? null,
      body.notes ?? existing.notes ?? null,
      body.startAt ? toSqlDateTime(body.startAt) : existing.start_at ?? null,
      body.endAt ? toSqlDateTime(body.endAt) : existing.end_at ?? null,
      body.dueDate ? toSqlDateOnly(body.dueDate) : existing.due_date ?? null,
      body.leadId ?? existing.lead_id ?? null,
      body.coldCallId ?? existing.cold_call_id ?? null,
      body.dealId ?? existing.deal_id ?? null,
      body.userId ?? existing.user_id ?? currentUser.id,
      nowTs(),
    ],
    summary: `Calendar event updated: ${String(body.title ?? existing.title ?? '').trim()}`,
  }),
});

registerCrudRoutes({
  routeBase: 'tasks',
  table: 'tasks',
  baseSql: 'SELECT * FROM tasks',
  orderBy: 'COALESCE(due_date, created_at) DESC',
  scopeField: 'assigned_to_id',
  serialize: (row) => ({
    id: row.id,
    title: row.title,
    status: row.status ?? '',
    priority: row.priority ?? '',
    dueDate: toClientDateOnly(row.due_date),
    assignedToId: row.assigned_to_id ?? null,
    leadId: row.lead_id ?? null,
    notes: row.notes ?? '',
  }),
  createRow: async (body, currentUser) => ({
    columns: ['title', 'status', 'priority', 'due_date', 'assigned_to_id', 'lead_id', 'notes', 'created_at', 'updated_at'],
    values: [
      String(body.title ?? '').trim(),
      body.status ?? 'Open',
      body.priority ?? 'Normal',
      body.dueDate ? toSqlDateOnly(body.dueDate) : null,
      body.assignedToId ?? currentUser.id,
      body.leadId ?? null,
      body.notes ?? null,
      nowTs(),
      nowTs(),
    ],
    summary: `Task created: ${String(body.title ?? '').trim()}`,
  }),
  updateRow: async (body, existing, currentUser) => ({
    columns: ['title', 'status', 'priority', 'due_date', 'assigned_to_id', 'lead_id', 'notes', 'updated_at'],
    values: [
      String(body.title ?? existing.title ?? '').trim(),
      body.status ?? existing.status ?? 'Open',
      body.priority ?? existing.priority ?? 'Normal',
      body.dueDate ? toSqlDateOnly(body.dueDate) : existing.due_date ?? null,
      body.assignedToId ?? existing.assigned_to_id ?? currentUser.id,
      body.leadId ?? existing.lead_id ?? null,
      body.notes ?? existing.notes ?? null,
      nowTs(),
    ],
    summary: `Task updated: ${String(body.title ?? existing.title ?? '').trim()}`,
  }),
});

registerCrudRoutes({
  routeBase: 'knowledge-base',
  table: 'knowledge_base',
  baseSql: 'SELECT * FROM knowledge_base',
  orderBy: 'id DESC',
  serialize: (row) => ({
    id: row.id,
    title: row.title,
    category: row.category ?? '',
    content: row.content ?? '',
    projectId: row.project_id ?? null,
    developerId: row.developer_id ?? null,
    priceRange: row.price_range ?? '',
    paymentPlan: row.payment_plan ?? '',
    deliveryDate: row.delivery_date ?? '',
  }),
  createRow: async (body) => ({
    columns: ['title', 'category', 'content', 'project_id', 'developer_id', 'price_range', 'payment_plan', 'delivery_date', 'created_at', 'updated_at'],
    values: [
      String(body.title ?? '').trim(),
      body.category ?? null,
      body.content ?? null,
      body.projectId ?? null,
      body.developerId ?? null,
      body.priceRange ?? null,
      body.paymentPlan ?? null,
      body.deliveryDate ?? null,
      nowTs(),
      nowTs(),
    ],
    summary: `Knowledge base item created: ${String(body.title ?? '').trim()}`,
  }),
  updateRow: async (body, existing) => ({
    columns: ['title', 'category', 'content', 'project_id', 'developer_id', 'price_range', 'payment_plan', 'delivery_date', 'updated_at'],
    values: [
      String(body.title ?? existing.title ?? '').trim(),
      body.category ?? existing.category ?? null,
      body.content ?? existing.content ?? null,
      body.projectId ?? existing.project_id ?? null,
      body.developerId ?? existing.developer_id ?? null,
      body.priceRange ?? existing.price_range ?? null,
      body.paymentPlan ?? existing.payment_plan ?? null,
      body.deliveryDate ?? existing.delivery_date ?? null,
      nowTs(),
    ],
    summary: `Knowledge base item updated: ${String(body.title ?? existing.title ?? '').trim()}`,
  }),
});

registerCrudRoutes({
  routeBase: 'call-history',
  table: 'call_history',
  baseSql: 'SELECT * FROM call_history',
  orderBy: 'COALESCE(call_at, created_at) DESC',
  scopeField: 'user_id',
  serialize: (row) => ({
    id: row.id,
    leadId: row.lead_id ?? null,
    coldCallId: row.cold_call_id ?? null,
    userId: row.user_id ?? null,
    result: row.result ?? '',
    notes: row.notes ?? '',
    callAt: toClientDateTime(row.call_at),
  }),
  createRow: async (body, currentUser) => ({
    columns: ['lead_id', 'cold_call_id', 'user_id', 'result', 'notes', 'call_at', 'created_at', 'updated_at'],
    values: [
      body.leadId ?? null,
      body.coldCallId ?? null,
      body.userId ?? currentUser.id,
      body.result ?? 'Not Answered',
      body.notes ?? null,
      body.callAt ? toSqlDateTime(body.callAt) : nowTs(),
      nowTs(),
      nowTs(),
    ],
    summary: `Call history created: ${body.result ?? 'Not Answered'}`,
  }),
  updateRow: async (body, existing, currentUser) => ({
    columns: ['lead_id', 'cold_call_id', 'user_id', 'result', 'notes', 'call_at', 'updated_at'],
    values: [
      body.leadId ?? existing.lead_id ?? null,
      body.coldCallId ?? existing.cold_call_id ?? null,
      body.userId ?? existing.user_id ?? currentUser.id,
      body.result ?? existing.result ?? 'Not Answered',
      body.notes ?? existing.notes ?? null,
      body.callAt ? toSqlDateTime(body.callAt) : existing.call_at ?? nowTs(),
      nowTs(),
    ],
    summary: `Call history updated: ${body.result ?? existing.result ?? 'Not Answered'}`,
  }),
});

registerCrudRoutes({
  routeBase: 'pipeline',
  table: 'pipeline',
  baseSql: 'SELECT * FROM pipeline',
  orderBy: 'id DESC',
  scopeField: 'owner_id',
  serialize: (row) => ({
    id: row.id,
    title: row.title,
    stage: row.stage ?? '',
    value: Number(row.value ?? 0),
    ownerId: row.owner_id ?? null,
    leadId: row.lead_id ?? null,
    dealId: row.deal_id ?? null,
    notes: row.notes ?? '',
  }),
  createRow: async (body, currentUser) => ({
    columns: ['title', 'stage', 'value', 'owner_id', 'lead_id', 'deal_id', 'notes', 'created_at', 'updated_at'],
    values: [
      String(body.title ?? '').trim(),
      body.stage ?? null,
      body.value ?? null,
      body.ownerId ?? currentUser.id,
      body.leadId ?? null,
      body.dealId ?? null,
      body.notes ?? null,
      nowTs(),
      nowTs(),
    ],
    summary: `Pipeline item created: ${String(body.title ?? '').trim()}`,
  }),
  updateRow: async (body, existing, currentUser) => ({
    columns: ['title', 'stage', 'value', 'owner_id', 'lead_id', 'deal_id', 'notes', 'updated_at'],
    values: [
      String(body.title ?? existing.title ?? '').trim(),
      body.stage ?? existing.stage ?? null,
      body.value ?? existing.value ?? null,
      body.ownerId ?? existing.owner_id ?? currentUser.id,
      body.leadId ?? existing.lead_id ?? null,
      body.dealId ?? existing.deal_id ?? null,
      body.notes ?? existing.notes ?? null,
      nowTs(),
    ],
    summary: `Pipeline item updated: ${String(body.title ?? existing.title ?? '').trim()}`,
  }),
});

registerCrudRoutes({
  routeBase: 'notifications',
  table: 'notifications',
  baseSql: 'SELECT * FROM notifications',
  orderBy: 'created_at DESC',
  scopeField: 'user_id',
  serialize: (row) => ({
    id: row.id,
    userId: row.user_id ?? null,
    title: row.title,
    body: row.body ?? '',
    isRead: Number(row.is_read ?? 0) === 1,
  }),
  createRow: async (body, currentUser) => ({
    columns: ['user_id', 'title', 'body', 'is_read', 'created_at', 'updated_at'],
    values: [
      body.userId ?? currentUser.id,
      String(body.title ?? '').trim(),
      body.body ?? null,
      Number(body.isRead) === 1 || body.isRead === true || body.isRead === '1' ? 1 : 0,
      nowTs(),
      nowTs(),
    ],
    summary: `Notification created: ${String(body.title ?? '').trim()}`,
  }),
  updateRow: async (body, existing, currentUser) => ({
    columns: ['user_id', 'title', 'body', 'is_read', 'updated_at'],
    values: [
      body.userId ?? existing.user_id ?? currentUser.id,
      String(body.title ?? existing.title ?? '').trim(),
      body.body ?? existing.body ?? null,
      Number(body.isRead ?? existing.is_read) === 1 || body.isRead === true || body.isRead === '1' ? 1 : 0,
      nowTs(),
    ],
    summary: `Notification updated: ${String(body.title ?? existing.title ?? '').trim()}`,
  }),
});

app.get('/api/activity-logs', async (req, res) => {
  try {
    const currentUser = req.currentUser;
    const visibleUserIds = await getAccessibleUserIds(currentUser);
    const [rows] = isSuperUser(currentUser)
      ? await pool.query(
          `SELECT activity_logs.*, users.name AS user_name
           FROM activity_logs
           LEFT JOIN users ON users.id = activity_logs.user_id
           ORDER BY activity_logs.created_at DESC`
        )
      : await pool.query(
          `SELECT activity_logs.*, users.name AS user_name
           FROM activity_logs
           LEFT JOIN users ON users.id = activity_logs.user_id
           WHERE activity_logs.user_id IN (${visibleUserIds.map(() => '?').join(', ')})
           ORDER BY activity_logs.created_at DESC`,
          visibleUserIds
        );

    res.json(
      rows.map((row) => ({
        id: row.id,
        userId: row.user_id ?? null,
        userName: row.user_name ?? '',
        entityType: row.entity_type ?? '',
        entityId: row.entity_id ?? null,
        action: row.action ?? '',
        summary: row.summary ?? '',
        metadata: row.metadata ? (() => {
          try {
            return JSON.parse(row.metadata);
          } catch {
            return row.metadata;
          }
        })() : null,
        createdAt: toClientDateTime(row.created_at),
      }))
    );
  } catch (error) {
    res.status(500).json({ error: error.message });
  }
});

const port = process.env.PORT ? Number(process.env.PORT) : 4000;

const seed = async () => {
  try {
    const seedPath = path.join(__dirname, '..', 'seed', 'developers.json');
    const seedData = JSON.parse(await fs.readFile(seedPath, 'utf-8'));
    if (Array.isArray(seedData) && seedData.length > 0) {
      await upsertDevelopers(seedData);
    }
  } catch (error) {
    console.error('Developer seed failed', error.message);
  }
};

const startServer = async () => {
  try {
    await ensureSchema();
    await seedBaseData();
    await rebuildReportsFromComments();
    await seed();
  } catch (error) {
    console.error('Startup check failed', error.message);
  }
  app.listen(port, () => {
    console.log(`Mentors CRM API running on port ${port}`);
  });
};

startServer();
