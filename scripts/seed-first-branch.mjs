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
    name: 'Mohamed Abdelsalam Abdelhameed Abdelaziz Keshk',
    email: `${normalize('Mohamed Abdelsalam Abdelhameed Abdelaziz Keshk')}@mentors.com`,
    phone: '01090010001',
    role: 'director',
    title: 'Sales Director',
    department: 'Sales',
  },
  salesManager: {
    name: 'Omar Abdelsalam Abdelhameed Abdelaziz Keshk',
    email: `${normalize('Omar Abdelsalam Abdelhameed Abdelaziz Keshk')}@mentors.com`,
    phone: '01090010002',
    role: 'sales_manager',
    title: 'Senior Sales Manager',
    department: 'Sales',
  },
  teamLeaders: [
    {
      key: 'amr',
      name: 'Amr Mohsen Mohamed Abass Selim',
      email: `${normalize('Amr Mohsen Mohamed Abass Selim')}@mentors.com`,
      phone: '01090010003',
      role: 'team_leader',
      title: 'Sales Team Leader',
      department: 'Sales',
    },
    {
      key: 'ahmed-hesham',
      name: 'Ahmed Hesham Kamal Ahmed Kamel',
      email: `${normalize('Ahmed Hesham Kamal Ahmed Kamel')}@mentors.com`,
      phone: '01090010004',
      role: 'team_leader',
      title: 'Sales Team Leader',
      department: 'Sales',
    },
    {
      key: 'ahmed-soliman',
      name: 'Ahmed Mohamed Soliman Afifi',
      email: `${normalize('Ahmed Mohamed Soliman Afifi')}@mentors.com`,
      phone: '01090010005',
      role: 'team_leader',
      title: 'Sales Team Leader',
      department: 'Sales',
    },
    {
      key: 'shimaa',
      name: 'Shimaa Shady Ahmed Abdelmalek',
      email: `${normalize('Shimaa Shady Ahmed Abdelmalek')}@mentors.com`,
      phone: '01090010006',
      role: 'team_leader',
      title: 'Sales Team Leader',
      department: 'Sales',
    },
  ],
  agents: [
    {
      parent: 'amr',
      name: 'Nourhan Ali Ahmed Ali',
      email: `${normalize('Nourhan Ali Ahmed Ali')}@mentors.com`,
      phone: '01090011001',
      role: 'agent',
      title: 'Property Consultant',
      department: 'Sales',
    },
    {
      parent: 'amr',
      name: 'Rawan Ayman Yehia Abdelghany',
      email: `${normalize('Rawan Ayman Yehia Abdelghany')}@mentors.com`,
      phone: '01090011002',
      role: 'agent',
      title: 'Senior Property Consultant',
      department: 'Sales',
    },
    {
      parent: 'amr',
      name: 'Rana Emad Maher Abdelrahman Abdelazim',
      email: `${normalize('Rana Emad Maher Abdelrahman Abdelazim')}@mentors.com`,
      phone: '01090011003',
      role: 'agent',
      title: 'Property Advisor',
      department: 'Sales',
    },
    {
      parent: 'amr',
      name: 'Maha Hassan Zaki Atta',
      email: `${normalize('Maha Hassan Zaki Atta')}@mentors.com`,
      phone: '01090011004',
      role: 'agent',
      title: 'Property Advisor',
      department: 'Sales',
    },
    {
      parent: 'ahmed-hesham',
      name: 'Nadeen Yasser Abdelhamid Taha',
      email: `${normalize('Nadeen Yasser Abdelhamid Taha')}@mentors.com`,
      phone: '01090011005',
      role: 'agent',
      title: 'Property Advisor',
      department: 'Sales',
    },
    {
      parent: 'ahmed-hesham',
      name: 'Ahmed Mohamed Abdelfatah Ahmed Elsenary',
      email: `${normalize('Ahmed Mohamed Abdelfatah Ahmed Elsenary')}@mentors.com`,
      phone: '01090011006',
      role: 'agent',
      title: 'Property Advisor',
      department: 'Sales',
    },
    {
      parent: 'ahmed-hesham',
      name: 'Ahmed Ali Mohamed Elbalshy',
      email: `${normalize('Ahmed Ali Mohamed Elbalshy')}@mentors.com`,
      phone: '01090011007',
      role: 'agent',
      title: 'Property Advisor',
      department: 'Sales',
    },
    {
      parent: 'ahmed-hesham',
      name: 'Seif Ahmed Ibrahim Abass Abdelgawad',
      email: `${normalize('Seif Ahmed Ibrahim Abass Abdelgawad')}@mentors.com`,
      phone: '01090011008',
      role: 'agent',
      title: 'Property Advisor',
      department: 'Sales',
    },
    {
      parent: 'ahmed-hesham',
      name: 'Ziad Hesham Hussen Mohamed Elkhouly',
      email: `${normalize('Ziad Hesham Hussen Mohamed Elkhouly')}@mentors.com`,
      phone: '01090011009',
      role: 'agent',
      title: 'Property Advisor',
      department: 'Sales',
    },
    {
      parent: 'ahmed-soliman',
      name: 'Amira Mohamed Hussen Abass Abdelrazik',
      email: `${normalize('Amira Mohamed Hussen Abass Abdelrazik')}@mentors.com`,
      phone: '01090011010',
      role: 'agent',
      title: 'Property Consultant',
      department: 'Sales',
    },
    {
      parent: 'ahmed-soliman',
      name: 'Mariam Mohamed Amin Mousa Hussen',
      email: `${normalize('Mariam Mohamed Amin Mousa Hussen')}@mentors.com`,
      phone: '01090011011',
      role: 'agent',
      title: 'Property Consultant',
      department: 'Sales',
    },
    {
      parent: 'ahmed-soliman',
      name: 'Mariam Mohamed Roshdy Ibrahim Eldesouky Mahmoud',
      email: `${normalize('Mariam Mohamed Roshdy Ibrahim Eldesouky Mahmoud')}@mentors.com`,
      phone: '01090011012',
      role: 'agent',
      title: 'Property Consultant',
      department: 'Sales',
    },
    {
      parent: 'shimaa',
      name: 'Fatma Mostafa Moheb Eldin Kamal',
      email: `${normalize('Fatma Mostafa Moheb Eldin Kamal')}@mentors.com`,
      phone: '01090011013',
      role: 'agent',
      title: 'Property Consultant',
      department: 'Sales',
    },
  ],
  salesAdmins: [
    {
      name: 'yassmine essam abdelaziz hefnawy',
      email: `${normalize('yassmine essam abdelaziz hefnawy')}@mentors.com`,
      phone: '01090012001',
      role: 'sales_admin',
      title: 'Sales Admin',
      department: 'Sales',
    },
    {
      name: 'Alaa fathy elsayed elmandouh mohamed ali hegazy',
      email: `${normalize('Alaa fathy elsayed elmandouh mohamed ali hegazy')}@mentors.com`,
      phone: '01090012002',
      role: 'sales_admin',
      title: 'Sales Admin',
      department: 'Sales',
    },
    {
      name: 'fatma shady ahmed abdelmalek',
      email: `${normalize('fatma shady ahmed abdelmalek')}@mentors.com`,
      phone: '01090012003',
      role: 'sales_admin',
      title: 'Sales Admin',
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
      salesAdmin: await ensureJobtitle(connection, 'Sales Admin', salesDepartmentId),
      salesDirector: await ensureJobtitle(connection, 'Sales Director', salesDepartmentId),
      seniorSalesManager: await ensureJobtitle(connection, 'Senior Sales Manager', salesDepartmentId),
      salesTeamLeader: await ensureJobtitle(connection, 'Sales Team Leader', salesDepartmentId),
      seniorPropertyConsultant: await ensureJobtitle(connection, 'Senior Property Consultant', salesDepartmentId),
      propertyConsultant: await ensureJobtitle(connection, 'Property Consultant', salesDepartmentId),
      propertyAdvisor: await ensureJobtitle(connection, 'Property Advisor', salesDepartmentId),
    };

    const salesAdminIds = [];
    for (const item of branch.salesAdmins) {
      const id = await ensureUser(connection, {
        ...item,
        department_id: salesDepartmentId,
        jobtitle_id: jobtitleIds.salesAdmin,
        parent_id: null,
        teamLeaderId: null,
      });
      salesAdminIds.push(id);
    }

    const directorId = await ensureUser(connection, {
      ...branch.director,
      department_id: salesDepartmentId,
      jobtitle_id: jobtitleIds.salesDirector,
      parent_id: null,
      teamLeaderId: null,
    });

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

    console.log('First branch seeded successfully.');
    console.log(`Sales admins created/updated: ${salesAdminIds.length}`);
    console.log(`Director id: ${directorId}`);
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
