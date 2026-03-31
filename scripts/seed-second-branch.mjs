import bcrypt from 'bcryptjs';
import { pool } from '../src/db.js';

const DEFAULT_PASSWORD = 'Mentors123!';
const DEFAULT_AVATAR = 'https://www.pngitem.com/pimgs/m/35-350426_profile-icon-png-default-profile-picture-png-transparent.png';

const nowTs = () => new Date().toISOString().slice(0, 19).replace('T', ' ');

const normalize = (value) =>
  String(value ?? '')
    .normalize('NFD')
    .replace(/[\u0300-\u036f]/g, '')
    .toLowerCase()
    .replace(/[^a-z0-9]+/g, '.')
    .replace(/^\.+|\.+$/g, '');

const safeName = (value) => String(value ?? '').trim();

const hashPassword = async (password) => bcrypt.hash(String(password ?? DEFAULT_PASSWORD), 10);

const nextTableId = async (connection, table) => {
  const [rows] = await connection.query(`SELECT COALESCE(MAX(id), 0) + 1 AS nextId FROM \`${table}\``);
  return Number(rows[0]?.nextId ?? 1);
};

const findExistingRow = async (connection, table, column, value) => {
  const [rows] = await connection.query(
    `SELECT * FROM \`${table}\` WHERE LOWER(\`${column}\`) = LOWER(?) ORDER BY id ASC LIMIT 1`,
    [value]
  );
  return rows[0] ?? null;
};

const ensureJobtitle = async (connection, name, departmentId = 1) => {
  const existing = await findExistingRow(connection, 'jobtitles', 'name', name);
  if (existing) {
    if (Number(existing.department_id ?? 0) !== Number(departmentId)) {
      await connection.query(
        'UPDATE jobtitles SET department_id = ?, updated_at = ? WHERE id = ?',
        [departmentId, nowTs(), existing.id]
      );
    }
    return Number(existing.id);
  }

  const id = await nextTableId(connection, 'jobtitles');
  await connection.query(
    'INSERT INTO jobtitles (id, name, department_id, created_at, updated_at) VALUES (?, ?, ?, ?, ?)',
    [id, name, departmentId, nowTs(), nowTs()]
  );
  return id;
};

const ensureUser = async (connection, user) => {
  const email = safeName(user.email);
  const existing = await findExistingRow(connection, 'users', 'email', email);
  const passwordHash = existing?.password ? existing.password : await hashPassword(user.password ?? DEFAULT_PASSWORD);
  const payload = {
    name: safeName(user.name),
    email,
    phone: safeName(user.phone),
    role: String(user.role ?? 'agent').trim().toLowerCase(),
    title: user.title ?? null,
    department: user.department ?? 'Sales',
    teamLeaderId: user.teamLeaderId ?? null,
    parent_id: user.parent_id ?? user.teamLeaderId ?? null,
    avatar: existing?.avatar ?? user.avatar ?? DEFAULT_AVATAR,
    isActive: 1,
    createdAt: existing?.createdAt ?? user.createdAt ?? nowTs(),
    status: 1,
    image: existing?.image ?? user.image ?? user.avatar ?? DEFAULT_AVATAR,
    password: passwordHash,
    created_at: existing?.created_at ?? user.created_at ?? nowTs(),
    updated_at: nowTs(),
    is_active: 1,
    department_id: user.department_id ?? 1,
    jobtitle_id: user.jobtitle_id ?? null,
  };

  const columns = [
    'name',
    'email',
    'phone',
    'role',
    'title',
    'department',
    'teamLeaderId',
    'avatar',
    'isActive',
    'createdAt',
    'parent_id',
    'status',
    'image',
    'password',
    'created_at',
    'updated_at',
    'is_active',
    'department_id',
    'jobtitle_id',
  ];

  if (existing) {
    await connection.query(
      `UPDATE users SET ${columns.map((column) => `\`${column}\` = ?`).join(', ')} WHERE id = ?`,
      [...columns.map((column) => payload[column] ?? null), existing.id]
    );
    return Number(existing.id);
  }

  const id = await nextTableId(connection, 'users');
  await connection.query(
    `INSERT INTO users (id, ${columns.map((column) => `\`${column}\``).join(', ')})
     VALUES (?, ${columns.map(() => '?').join(', ')})`,
    [id, ...columns.map((column) => payload[column] ?? null)]
  );
  return id;
};

const branch = {
  director: {
    name: 'Ahmed Adel Mansour Mohamed Elgendy',
    email: `${normalize('Ahmed Adel Mansour Mohamed Elgendy')}@mentors.com`,
    phone: '01092020001',
    role: 'director',
    title: 'Sales Director',
    department: 'Sales',
  },
  supervisors: [
    {
      key: 'moaatz-adel',
      name: 'Moaatz Adel Mansour Mohamed Elgendy',
      email: `${normalize('Moaatz Adel Mansour Mohamed Elgendy')}@mentors.com`,
      phone: '01092020002',
      role: 'sales_supervisor',
      title: 'Sales Supervisor',
      department: 'Sales',
    },
    {
      key: 'ali-hesham',
      name: 'Ali Hesham Ali Ali Zain Eldin',
      email: `${normalize('Ali Hesham Ali Ali Zain Eldin')}@mentors.com`,
      phone: '01092020003',
      role: 'sales_supervisor',
      title: 'Sales Supervisor',
      department: 'Sales',
    },
  ],
  salesManager: {
    name: 'Mohamed Abdelhamid Mohamed Batat',
    email: `${normalize('Mohamed Abdelhamid Mohamed Batat')}@mentors.com`,
    phone: '01092020004',
    role: 'sales_manager',
    title: 'Senior Sales Manager',
    department: 'Sales',
  },
  teamLeaders: [
    {
      key: 'youssef',
      name: 'Youssef Magdy Mohamed Elghareeb Mohamed Elabeedy',
      email: `${normalize('Youssef Magdy Mohamed Elghareeb Mohamed Elabeedy')}@mentors.com`,
      phone: '01092020005',
      role: 'team_leader',
      title: 'Sales Team Leader',
      department: 'Sales',
    },
    {
      key: 'bishoy',
      name: 'Bishoy Ezzat Saad Farag',
      email: `${normalize('Bishoy Ezzat Saad Farag')}@mentors.com`,
      phone: '01092020006',
      role: 'team_leader',
      title: 'Sales Team Leader',
      department: 'Sales',
    },
    {
      key: 'gasser',
      name: 'Gasser Ahmed Mohamed Abdallah Abdelfattah',
      email: `${normalize('Gasser Ahmed Mohamed Abdallah Abdelfattah')}@mentors.com`,
      phone: '01092020007',
      role: 'team_leader',
      title: 'Sales Team Leader',
      department: 'Sales',
    },
    {
      key: 'mohamed-ahmed',
      name: 'Mohamed Ahmed Abdelraouf Abdelmasoud Omar',
      email: `${normalize('Mohamed Ahmed Abdelraouf Abdelmasoud Omar')}@mentors.com`,
      phone: '01092020008',
      role: 'team_leader',
      title: 'Sales Team Leader',
      department: 'Sales',
    },
    {
      key: 'ahmed-reda',
      name: 'Ahmed Reda Abdelsadek Shabaan',
      email: `${normalize('Ahmed Reda Abdelsadek Shabaan')}@mentors.com`,
      phone: '01092020009',
      role: 'team_leader',
      title: 'Sales Team Leader',
      department: 'Sales',
    },
  ],
  agents: [
    {
      parent: 'youssef',
      name: 'Saleh Seddky Saleh Aqila',
      email: `${normalize('Saleh Seddky Saleh Aqila')}@mentors.com`,
      phone: '01092021001',
      role: 'agent',
      title: 'Property Consultant',
      department: 'Sales',
    },
    {
      parent: 'youssef',
      name: 'Ziad Abdelnaby Zain Elabdeen Sayed',
      email: `${normalize('Ziad Abdelnaby Zain Elabdeen Sayed')}@mentors.com`,
      phone: '01092021002',
      role: 'agent',
      title: 'Property Consultant',
      department: 'Sales',
    },
    {
      parent: 'youssef',
      name: 'Adham Saeed Rezk Abdelreheem',
      email: `${normalize('Adham Saeed Rezk Abdelreheem')}@mentors.com`,
      phone: '01092021003',
      role: 'agent',
      title: 'Senior Property Consultant',
      department: 'Sales',
    },
    {
      parent: 'bishoy',
      name: 'Ahmed Hossam Eldin Sedeek Brakat',
      email: `${normalize('Ahmed Hossam Eldin Sedeek Brakat')}@mentors.com`,
      phone: '01092021004',
      role: 'agent',
      title: 'Senior Property Consultant',
      department: 'Sales',
    },
    {
      parent: 'bishoy',
      name: 'Mahmoud Gamal Abdelkhaleq Imam Amin',
      email: `${normalize('Mahmoud Gamal Abdelkhaleq Imam Amin')}@mentors.com`,
      phone: '01092021005',
      role: 'agent',
      title: 'Senior Property Consultant',
      department: 'Sales',
    },
    {
      parent: 'bishoy',
      name: 'Juvani Bassem Alfy Henary',
      email: `${normalize('Juvani Bassem Alfy Henary')}@mentors.com`,
      phone: '01092021006',
      role: 'agent',
      title: 'Property Consultant',
      department: 'Sales',
    },
    {
      parent: 'bishoy',
      name: 'Sandy Nasser Hussin Shalaby',
      email: `${normalize('Sandy Nasser Hussin Shalaby')}@mentors.com`,
      phone: '01092021007',
      role: 'agent',
      title: 'Property Advisor',
      department: 'Sales',
    },
    {
      parent: 'gasser',
      name: 'Moaatz Mahmoud Hassan Wasfi',
      email: `${normalize('Moaatz Mahmoud Hassan Wasfi')}@mentors.com`,
      phone: '01092021008',
      role: 'agent',
      title: 'Property Consultant',
      department: 'Sales',
    },
    {
      parent: 'gasser',
      name: 'Mahmoud Tarfaya Gomaa Habib',
      email: `${normalize('Mahmoud Tarfaya Gomaa Habib')}@mentors.com`,
      phone: '01092021009',
      role: 'agent',
      title: 'Property Advisor',
      department: 'Sales',
    },
    {
      parent: 'gasser',
      name: 'Mohamed Sayed Ibrahim Ahmed',
      email: `${normalize('Mohamed Sayed Ibrahim Ahmed')}@mentors.com`,
      phone: '01092021010',
      role: 'agent',
      title: 'Property Advisor',
      department: 'Sales',
    },
    {
      parent: 'gasser',
      name: 'Ahmed Mohamed Abdelhay Ghazi Nassef',
      email: `${normalize('Ahmed Mohamed Abdelhay Ghazi Nassef')}@mentors.com`,
      phone: '01092021011',
      role: 'agent',
      title: 'Property Consultant',
      department: 'Sales',
    },
    {
      parent: 'mohamed-ahmed',
      name: 'Mostafa Mohamed Aliwa Mohamed Awad',
      email: `${normalize('Mostafa Mohamed Aliwa Mohamed Awad')}@mentors.com`,
      phone: '01092021012',
      role: 'agent',
      title: 'Property Advisor',
      department: 'Sales',
    },
    {
      parent: 'mohamed-ahmed',
      name: 'Belal Kareem Hamed Salama',
      email: `${normalize('Belal Kareem Hamed Salama')}@mentors.com`,
      phone: '01092021013',
      role: 'agent',
      title: 'Senior Property Consultant',
      department: 'Sales',
    },
    {
      parent: 'mohamed-ahmed',
      name: 'Fouad Salah Hassan Fouad Ismail Hassan',
      email: `${normalize('Fouad Salah Hassan Fouad Ismail Hassan')}@mentors.com`,
      phone: '01092021014',
      role: 'agent',
      title: 'Property Consultant',
      department: 'Sales',
    },
    {
      parent: 'mohamed-ahmed',
      name: 'Fatma Emad Abdelhafiz Mohamed Abdeldayem',
      email: `${normalize('Fatma Emad Abdelhafiz Mohamed Abdeldayem')}@mentors.com`,
      phone: '01092021015',
      role: 'agent',
      title: 'Senior Property Consultant',
      department: 'Sales',
    },
    {
      parent: 'ahmed-reda',
      name: 'Ali Zain Elabdeen Mohamed Abdelwahab',
      email: `${normalize('Ali Zain Elabdeen Mohamed Abdelwahab')}@mentors.com`,
      phone: '01092021016',
      role: 'agent',
      title: 'Property Consultant',
      department: 'Sales',
    },
  ],
};

const main = async () => {
  const connection = await pool.getConnection();
  try {
    await connection.beginTransaction();

    const salesDepartmentId = 1;
    const jobtitleIds = {
      salesSupervisor: await ensureJobtitle(connection, 'Sales Supervisor', salesDepartmentId),
      salesDirector: await ensureJobtitle(connection, 'Sales Director', salesDepartmentId),
      seniorSalesManager: await ensureJobtitle(connection, 'Senior Sales Manager', salesDepartmentId),
      salesTeamLeader: await ensureJobtitle(connection, 'Sales Team Leader', salesDepartmentId),
      seniorPropertyConsultant: await ensureJobtitle(connection, 'Senior Property Consultant', salesDepartmentId),
      propertyConsultant: await ensureJobtitle(connection, 'Property Consultant', salesDepartmentId),
      propertyAdvisor: await ensureJobtitle(connection, 'Property Advisor', salesDepartmentId),
    };

    const directorId = await ensureUser(connection, {
      ...branch.director,
      department_id: salesDepartmentId,
      jobtitle_id: jobtitleIds.salesDirector,
      parent_id: null,
      teamLeaderId: null,
    });

    const supervisorIds = [];
    for (const item of branch.supervisors) {
      const id = await ensureUser(connection, {
        ...item,
        department_id: salesDepartmentId,
        jobtitle_id: jobtitleIds.salesSupervisor,
        parent_id: directorId,
        teamLeaderId: directorId,
      });
      supervisorIds.push(id);
    }

    const salesManagerId = await ensureUser(connection, {
      ...branch.salesManager,
      department_id: salesDepartmentId,
      jobtitle_id: jobtitleIds.seniorSalesManager,
      parent_id: directorId,
      teamLeaderId: directorId,
    });

    const teamLeaderIds = new Map();
    for (const item of branch.teamLeaders) {
      const id = await ensureUser(connection, {
        name: item.name,
        email: item.email,
        phone: item.phone,
        role: item.role,
        title: item.title,
        department: item.department,
        department_id: salesDepartmentId,
        jobtitle_id: jobtitleIds.salesTeamLeader,
        parent_id: salesManagerId,
        teamLeaderId: salesManagerId,
      });
      teamLeaderIds.set(item.key, id);
    }

    for (const item of branch.agents) {
      const parentId = teamLeaderIds.get(item.parent);
      if (!parentId) {
        throw new Error(`Missing team leader for agent: ${item.name}`);
      }

      const jobtitleId =
        item.title === 'Property Consultant'
          ? jobtitleIds.propertyConsultant
          : item.title === 'Senior Property Consultant'
            ? jobtitleIds.seniorPropertyConsultant
            : jobtitleIds.propertyAdvisor;

      await ensureUser(connection, {
        name: item.name,
        email: item.email,
        phone: item.phone,
        role: item.role,
        title: item.title,
        department: item.department,
        department_id: salesDepartmentId,
        jobtitle_id: jobtitleId,
        parent_id: parentId,
        teamLeaderId: parentId,
      });
    }

    await connection.commit();

    console.log('Second branch seeded successfully.');
    console.log(`Director id: ${directorId}`);
    console.log(`Supervisors created/updated: ${supervisorIds.length}`);
    console.log(`Sales manager id: ${salesManagerId}`);
    console.log(`Team leaders created/updated: ${teamLeaderIds.size}`);
    console.log(`Agents created/updated: ${branch.agents.length}`);
    console.log(`Default password for new users: ${DEFAULT_PASSWORD}`);
  } catch (error) {
    await connection.rollback();
    console.error(`Seed branch failed: ${error.message}`);
    process.exitCode = 1;
  } finally {
    connection.release();
    await pool.end();
  }
};

main();
