import bcrypt from 'bcryptjs';
import { pool } from '../src/db.js';

const nowTs = () => new Date().toISOString().slice(0, 19).replace('T', ' ');

const escapeLike = (value) => String(value).replace(/[%_]/g, '\\$&');

const normalize = (value) => String(value ?? '').trim().toLowerCase();

const toSqlDate = (value) => {
  if (!value) return nowTs();
  if (value instanceof Date) return value.toISOString().slice(0, 19).replace('T', ' ');
  const parsed = new Date(value);
  return Number.isNaN(parsed.getTime()) ? nowTs() : parsed.toISOString().slice(0, 19).replace('T', ' ');
};

const pickMap = (rows) => new Map(rows.map((row) => [Number(row.id), row]));

const insertRow = async (connection, table, columns, values) => {
  const columnSql = columns.map((column) => `\`${column}\``).join(', ');
  const placeholders = columns.map(() => '?').join(', ');
  await connection.query(
    `INSERT INTO \`${table}\` (${columnSql}) VALUES (${placeholders})`,
    values
  );
};

const seedLeads = [
  {
    name: 'Sample - Maryam Hassan',
    phone: '+201000001001',
    status: 'Potential',
    source: 'Meta',
    projectId: 1,
    note: 'Interested in a 3BR villa. Wants a site visit and a brochure.',
  },
  {
    name: 'Sample - Omar Adel',
    phone: '+201000001002',
    status: 'Call back',
    source: 'Referral',
    projectId: 8,
    note: 'Asked for a callback on Thursday evening.',
  },
  {
    name: 'Sample - Dina Mostafa',
    phone: '+201000001003',
    status: 'Whatsapp',
    source: 'Website',
    projectId: 13,
    note: 'Prefers WhatsApp only. Requested price range details.',
  },
  {
    name: 'Sample - Ahmed Samir',
    phone: '+201000001004',
    status: 'Hot case',
    source: 'Internal Database',
    projectId: 14,
    note: 'Ready to reserve if the unit matches budget.',
  },
  {
    name: 'Sample - Yara Khaled',
    phone: '+201000001005',
    status: 'No answer',
    source: 'Meta',
    projectId: 15,
    note: 'No answer after two calls. Keep on follow-up list.',
  },
  {
    name: 'Sample - Salma Youssef',
    phone: '+201000001006',
    status: 'Non potential',
    source: 'Walk-in',
    projectId: 1,
    note: 'Budget below the current starting price.',
  },
];

const dealTemplates = [];

const main = async () => {
  const connection = await pool.getConnection();
  try {
    await connection.beginTransaction();

    const [statusRows] = await connection.query('SELECT id, name FROM statuses ORDER BY id');
    const [sourceRows] = await connection.query('SELECT id, name FROM sources ORDER BY id');
    const [projectRows] = await connection.query('SELECT id, name, developerId, developer_id FROM projects ORDER BY id');
    const [developerRows] = await connection.query('SELECT id, name FROM developers ORDER BY id');
    const [userRows] = await connection.query('SELECT id, name, role FROM users ORDER BY id');

    const statusByName = new Map(statusRows.map((row) => [normalize(row.name), row]));
    const sourceByName = new Map(sourceRows.map((row) => [normalize(row.name), row]));
    const projectsById = pickMap(projectRows);
    const developersById = pickMap(developerRows);
    const usersById = pickMap(userRows);
    const [leadIdRow] = await connection.query('SELECT COALESCE(MAX(id), 0) + 1 AS nextId FROM leads');
    const [dealIdRow] = await connection.query('SELECT COALESCE(MAX(id), 0) + 1 AS nextId FROM deals');
    const [commentIdRow] = await connection.query('SELECT COALESCE(MAX(id), 0) + 1 AS nextId FROM comments');
    let nextLeadId = Number(leadIdRow[0]?.nextId ?? 1);
    let nextDealId = Number(dealIdRow[0]?.nextId ?? 1);
    let nextCommentId = Number(commentIdRow[0]?.nextId ?? 1);

    const agent = usersById.get(3);
    const teamLeader = usersById.get(4);
    const salesManager = usersById.get(7);
    const director = usersById.get(6);

    if (!agent || !teamLeader || !salesManager || !director) {
      throw new Error('Required hierarchy users are missing. Seed aborted.');
    }

    const sampleLeadIds = await connection.query(
      'SELECT id FROM leads WHERE name LIKE ?',
      ['Sample - %']
    ).then(([rows]) => rows.map((row) => Number(row.id)));

    if (sampleLeadIds.length > 0) {
      const placeholders = sampleLeadIds.map(() => '?').join(', ');
      await connection.query(
        `DELETE FROM comments WHERE leadId IN (${placeholders}) OR lead_id IN (${placeholders}) OR entity_id IN (${placeholders})`,
        [...sampleLeadIds, ...sampleLeadIds, ...sampleLeadIds]
      );
      await connection.query(
        `DELETE FROM reports WHERE lead_id IN (${placeholders}) OR cold_call_id IN (${placeholders}) OR entity_id IN (${placeholders})`,
        [...sampleLeadIds, ...sampleLeadIds, ...sampleLeadIds]
      );
      await connection.query(
        `DELETE FROM deals WHERE leadId IN (${placeholders}) OR lead_id IN (${placeholders})`,
        [...sampleLeadIds, ...sampleLeadIds]
      );
      await connection.query(`DELETE FROM leads WHERE id IN (${placeholders})`, sampleLeadIds);
    }

    const insertedLeads = [];
    for (const row of seedLeads) {
      const status = statusByName.get(normalize(row.status));
      const source = sourceByName.get(normalize(row.source));
      const project = projectsById.get(Number(row.projectId));

      if (!status) {
        throw new Error(`Missing status: ${row.status}`);
      }
      if (!project) {
        throw new Error(`Missing project id: ${row.projectId}`);
      }

      const createdAt = nowTs();
      await insertRow(connection, 'leads', [
        'id',
        'type',
        'name',
        'phone',
        'whatsapp',
        'workPhone',
        'email',
        'status',
        'statusId',
        'source',
        'sourceId',
        'project',
        'projectId',
        'assignedTo',
        'assignedToId',
        'teamLeader',
        'teamLeaderId',
        'date',
        'isPotential',
        'project_id',
        'status_id',
        'source_id',
        'comment',
        'user_id',
        'created_at',
        'updated_at',
      ], [
        nextLeadId++,
        'Lead',
        row.name,
        row.phone,
        row.phone,
        null,
        null,
        status.name,
        Number(status.id),
        source ? source.name : 'Meta',
        source ? Number(source.id) : 1,
        project.name,
        Number(project.id),
        agent.name,
        Number(agent.id),
        teamLeader.name,
        Number(teamLeader.id),
        createdAt,
        /potential|hot case|whatsapp/i.test(row.status) ? 1 : 0,
        Number(project.id),
        Number(status.id),
        source ? Number(source.id) : 1,
        row.note,
        Number(agent.id),
        createdAt,
        createdAt,
      ]);
      insertedLeads.push({
        id: nextLeadId - 1,
        ...row,
        statusId: Number(status.id),
        sourceId: source ? Number(source.id) : 1,
        projectName: project.name,
      });
    }

    const leadByName = new Map(insertedLeads.map((row) => [row.name, row]));

    const comments = [];

    for (const item of comments) {
      const lead = leadByName.get(item.leadName);
      if (!lead) continue;
      const createdAt = nowTs();
      await insertRow(connection, 'comments', [
        'id',
        'leadId',
        'content',
        'createdBy',
        'createdById',
        'createdAt',
        'lead_id',
        'user_id',
        'body',
        'entity_type',
        'entity_id',
        'created_at',
        'updated_at',
      ], [
        nextCommentId++,
        lead.id,
        item.content,
        item.createdBy,
        item.createdById,
        createdAt,
        lead.id,
        item.createdById,
        item.content,
        'lead',
        lead.id,
        createdAt,
        createdAt,
      ]);
    }

    for (const item of dealTemplates) {
      const lead = leadByName.get(item.leadName);
      if (!lead) continue;
      const project = projectsById.get(Number(lead.projectId));
      const developerId = Number(project?.developerId ?? project?.developer_id ?? 1);
      const developer = developersById.get(developerId) ?? developersById.get(1);
      const createdAt = nowTs();
      await insertRow(connection, 'deals', [
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
        'price',
        'reservation_price',
        'reservation_date',
        'contract_date',
        'unit_number',
        'comment',
        'created_at',
        'updated_at',
        'user_id',
        'broker_id',
        'attachments',
        'reservations_images',
      ], [
        nextDealId++,
        lead.id,
        lead.name,
        lead.phone,
        Number(agent.id),
        agent.name,
        Number(project.id),
        project.name,
        developerId,
        developer?.name ?? 'Unknown',
        item.amount,
        item.commission,
        item.status,
        item.stage,
        item.reservationPrice,
        createdAt,
        item.contractPrice,
        createdAt,
        item.unitNumber,
        createdAt,
        createdAt,
        item.note,
        lead.id,
        lead.name,
        lead.phone,
        Number(agent.id),
        Number(project.id),
        developerId,
        item.amount,
        item.reservationPrice,
        createdAt.slice(0, 10),
        createdAt.slice(0, 10),
        item.unitNumber,
        item.note,
        createdAt,
        createdAt,
        Number(agent.id),
        null,
        JSON.stringify(item.attachments ?? []),
        JSON.stringify(item.reservationImages ?? []),
      ]);
    }

    await connection.commit();

    const [leadCount] = await connection.query('SELECT COUNT(*) AS count FROM leads WHERE name LIKE ?', ['Sample - %']);
    const [dealCount] = await connection.query('SELECT COUNT(*) AS count FROM deals WHERE leadName LIKE ?', ['Sample - %']);
    const [commentCount] = await connection.query('SELECT COUNT(*) AS count FROM comments WHERE content LIKE ?', ['%sample%']);

    console.log(`Seeded sample leads: ${leadCount[0].count}`);
    console.log(`Seeded sample deals: ${dealCount[0].count}`);
    console.log(`Seeded sample comments: ${commentCount[0].count}`);
    console.log('Sample data seed completed.');
  } catch (error) {
    await connection.rollback();
    console.error(`Sample seed failed: ${error.message}`);
    throw error;
  } finally {
    connection.release();
    await pool.end();
  }
};

main();
