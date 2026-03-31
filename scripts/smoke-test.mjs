const base = process.env.SMOKE_BASE_URL || 'http://localhost:4001';

const request = async (path, init = {}) => {
  const res = await fetch(base + path, {
    ...init,
    headers: {
      'content-type': 'application/json',
      ...(init.headers || {}),
    },
    body: init.body === undefined ? undefined : JSON.stringify(init.body),
  });
  const text = await res.text();
  let data = null;
  if (text) {
    try {
      data = JSON.parse(text);
    } catch {
      data = text;
    }
  }
  return { ok: res.ok, status: res.status, text, data };
};

const ok = (result, label) => {
  if (!result.ok) {
    throw new Error(`${label} -> ${result.status}: ${result.text}`);
  }
  return result.data;
};

const fail = (result, status, label) => {
  if (result.ok || result.status !== status) {
    throw new Error(`${label} -> expected ${status}, got ${result.status}: ${result.text}`);
  }
};

const login = async (email, password) => ok(await request('/api/auth/login', {
  method: 'POST',
  body: { email, password },
}), `login ${email}`);

const authHeaders = (token) => ({ Authorization: `Bearer ${token}` });

const cleanup = [];
const remember = (route, id) => cleanup.push({ route, id });

const destroyCleanup = async (auth) => {
  for (const item of cleanup.reverse()) {
    try {
      await request(`/api/${item.route}/${item.id}`, { method: 'DELETE', headers: auth });
    } catch {
      // best-effort cleanup
    }
  }
};

const main = async () => {
  const now = Date.now();
  const admin = await login('admin@mentors.com', 'password');
  const adminAuth = authHeaders(admin.token);
  try {
    ok(await request('/api/health'), 'health');

  const developers = ok(await request('/api/developers', { headers: adminAuth }), 'list developers');
  const devId = developers[0]?.id;
  if (!devId) throw new Error('No developers found');

  const projects = ok(await request('/api/projects', { headers: adminAuth }), 'list projects');
  const existingProject = projects.find((project) => Number(project.developerId ?? project.developer_id) === Number(devId)) ?? projects[0];
  if (!existingProject) throw new Error('No project found');

  const agentEmail = `smoke.agent.${now}@mentors.com`;
  const agentPassword = 'Temp1234!';
  const createdAgent = ok(await request('/api/users', {
    method: 'POST',
    headers: adminAuth,
    body: {
      name: `Smoke Agent ${now}`,
      email: agentEmail,
      phone: `0110${String(now).slice(-8)}`,
      password: agentPassword,
      role: 'agent',
      isActive: true,
    },
  }), 'create agent');
  remember('users', createdAgent.id);

  const agent = ok(await request('/api/users', { headers: adminAuth }), 'refetch users')
    .find((user) => user.email === agentEmail);
  if (!agent) throw new Error('Created agent not found in list');

  const agentLogin = await login(agentEmail, agentPassword);
  fail(
    await request('/api/departments', {
      method: 'POST',
      headers: authHeaders(agentLogin.token),
      body: { name: `Forbidden Dept ${now}` },
    }),
    403,
    'agent forbidden departments create'
  );

  const userEmail = `smoke.user.${now}@mentors.com`;
  const createdUser = ok(await request('/api/users', {
    method: 'POST',
    headers: adminAuth,
    body: {
      name: `Smoke User ${now}`,
      email: userEmail,
      phone: `0111${String(now).slice(-8)}`,
      password: 'Temp1234!',
      role: 'agent',
      isActive: true,
    },
  }), 'create user');
  remember('users', createdUser.id);
  const user = ok(await request('/api/users', { headers: adminAuth }), 'refetch users')
    .find((item) => item.email === userEmail);
  if (!user) throw new Error('Created user not found');
  await request(`/api/users/${user.id}`, {
    method: 'PUT',
    headers: adminAuth,
    body: {
      name: `Smoke User ${now} Updated`,
      role: 'agent',
      isActive: true,
    },
  });

  const createdProject = ok(await request('/api/projects', {
    method: 'POST',
    headers: adminAuth,
    body: {
      name: `Smoke Project ${now}`,
      developerId: devId,
    },
  }), 'create project');
  remember('projects', createdProject.id);
  const project = ok(await request('/api/projects', { headers: adminAuth }), 'refetch projects')
    .find((item) => item.name === `Smoke Project ${now}`);
  if (!project) throw new Error('Created project not found');
  await request(`/api/projects/${project.id}`, {
    method: 'PUT',
    headers: adminAuth,
    body: {
      name: `Smoke Project ${now} Updated`,
      developerId: devId,
    },
  });

  const leadPhone = `+2010000${String(now).slice(-8)}`;
  ok(await request('/api/leads', {
    method: 'POST',
    headers: adminAuth,
    body: {
      name: `Smoke Lead ${now}`,
      phone: leadPhone,
      type: 'Lead',
      projectId: project.id,
      assignedToId: admin.id,
      comment: 'seed',
    },
  }), 'create lead');
  const lead = ok(await request('/api/leads', { headers: adminAuth }), 'refetch leads')
    .find((item) => item.phone === leadPhone);
  if (!lead) throw new Error('Created lead not found');
  remember('leads', lead.id);
  await request(`/api/leads/${lead.id}`, {
    method: 'PUT',
    headers: adminAuth,
    body: {
      name: `Smoke Lead ${now} Updated`,
      phone: leadPhone,
      type: 'Lead',
      projectId: project.id,
      assignedToId: admin.id,
      comment: 'updated',
    },
  });

  const coldPhone = `+2010001${String(now).slice(-8)}`;
  ok(await request('/api/cold-calls', {
    method: 'POST',
    headers: adminAuth,
    body: {
      name: `Smoke Cold Call ${now}`,
      phone: coldPhone,
      assignedToId: admin.id,
    },
  }), 'create cold call');
  const coldCall = ok(await request('/api/cold-calls', { headers: adminAuth }), 'refetch cold calls')
    .find((item) => item.phone === coldPhone);
  if (!coldCall) throw new Error('Created cold call not found');
  remember('cold-calls', coldCall.id);
  await request(`/api/cold-calls/${coldCall.id}`, {
    method: 'PUT',
    headers: adminAuth,
    body: {
      name: `Smoke Cold Call ${now} Updated`,
      phone: coldPhone,
      assignedToId: admin.id,
    },
  });

  ok(await request('/api/comments', {
    method: 'POST',
    headers: adminAuth,
    body: {
      leadId: lead.id,
      content: 'Smoke comment',
    },
  }), 'create comment');
  const comment = ok(await request('/api/comments', { headers: adminAuth }), 'refetch comments')
    .find((item) => item.content === 'Smoke comment');
  if (!comment) throw new Error('Created comment not found');
  remember('comments', comment.id);
  await request(`/api/comments/${comment.id}`, {
    method: 'PUT',
    headers: adminAuth,
    body: { content: 'Smoke comment updated' },
  });

  const reports = ok(await request('/api/reports', { headers: adminAuth }), 'refetch reports');
  const report = reports.find((item) => Number(item.commentId ?? item.comment_id ?? 0) === Number(comment.id));
  if (!report) throw new Error('Created report not found');

  const dealPhone = `+2010002${String(now).slice(-8)}`;
  ok(await request('/api/deals', {
    method: 'POST',
    headers: adminAuth,
    body: {
      leadName: `Smoke Deal ${now}`,
      clientPhone: dealPhone,
      agentId: admin.id,
      agentName: admin.name,
      projectId: project.id,
      projectName: project.name,
      developerId: devId,
      developerName: developers.find((developer) => developer.id === devId)?.name ?? 'Unknown',
      amount: 2500000,
      commission: 5000,
      status: 'pending',
      stage: 'Initial',
      reservationPrice: 250000,
      reservationDate: '2026-03-23',
      contractPrice: 2500000,
      contractDate: '2026-03-24',
      unitNumber: 'A-101',
      createdAt: new Date().toISOString(),
      date: '2026-03-24',
      attachments: [
        { name: 'brochure.pdf', url: 'data:application/pdf;base64,MA==', type: 'application/pdf', size: 1, uploadedAt: new Date().toISOString() },
      ],
      reservationImages: [
        { name: 'reservation.jpg', url: 'data:image/jpeg;base64,MA==', type: 'image/jpeg', size: 1, uploadedAt: new Date().toISOString() },
      ],
    },
  }), 'create deal');
  const deal = ok(await request('/api/deals', { headers: adminAuth }), 'refetch deals')
    .find((item) => item.clientPhone === dealPhone);
  if (!deal) throw new Error('Created deal not found');
  remember('deals', deal.id);
  await request(`/api/deals/${deal.id}`, {
    method: 'PUT',
    headers: adminAuth,
    body: {
      ...deal,
      leadName: `Smoke Deal ${now} Updated`,
      attachments: [
        ...(deal.attachments ?? []),
        { name: 'extra.pdf', url: 'data:application/pdf;base64,MA==', type: 'application/pdf', size: 1, uploadedAt: new Date().toISOString() },
      ],
      reservationImages: [
        ...(deal.reservationImages ?? []),
        { name: 'extra.jpg', url: 'data:image/jpeg;base64,MA==', type: 'image/jpeg', size: 1, uploadedAt: new Date().toISOString() },
      ],
    },
  });
  const dealAfter = ok(await request('/api/deals', { headers: adminAuth }), 'refetch deals after update')
    .find((item) => item.id === deal.id);
  if (!dealAfter || (dealAfter.attachments ?? []).length < 2 || (dealAfter.reservationImages ?? []).length < 2) {
    throw new Error('Deal attachments/reservationImages did not persist');
  }

  const calendar = ok(await request('/api/calendar-events', {
    method: 'POST',
    headers: adminAuth,
    body: {
      title: `Smoke Cal ${now}`,
      type: 'Meeting',
      notes: 'note',
      userId: admin.id,
      startAt: '2026-03-23T10:00:00.000Z',
      endAt: '2026-03-23T11:00:00.000Z',
      dueDate: '2026-03-23',
    },
  }), 'create calendar event');
  remember('calendar-events', calendar.id);
  await request(`/api/calendar-events/${calendar.id}`, {
    method: 'PUT',
    headers: adminAuth,
    body: {
      title: `Smoke Cal ${now} updated`,
      type: 'Meeting',
      notes: 'note2',
      userId: admin.id,
    },
  });

  const task = ok(await request('/api/tasks', {
    method: 'POST',
    headers: adminAuth,
    body: {
      title: `Smoke Task ${now}`,
      status: 'Open',
      priority: 'High',
      dueDate: '2026-03-24',
      assignedToId: admin.id,
      notes: 'task note',
    },
  }), 'create task');
  remember('tasks', task.id);
  await request(`/api/tasks/${task.id}`, {
    method: 'PUT',
    headers: adminAuth,
    body: {
      title: `Smoke Task ${now} updated`,
      status: 'Done',
      priority: 'High',
      assignedToId: admin.id,
      notes: 'task note2',
    },
  });

  const pipe = ok(await request('/api/pipeline', {
    method: 'POST',
    headers: adminAuth,
    body: {
      title: `Smoke Pipe ${now}`,
      stage: 'Prospecting',
      value: 1000,
      ownerId: admin.id,
      notes: 'pipe note',
    },
  }), 'create pipeline');
  remember('pipeline', pipe.id);
  await request(`/api/pipeline/${pipe.id}`, {
    method: 'PUT',
    headers: adminAuth,
    body: {
      title: `Smoke Pipe ${now} updated`,
      stage: 'Negotiation',
      value: 1200,
      ownerId: admin.id,
      notes: 'pipe note2',
    },
  });

  const kb = ok(await request('/api/knowledge-base', {
    method: 'POST',
    headers: adminAuth,
    body: {
      title: `Smoke KB ${now}`,
      category: 'Project',
      content: 'details',
      projectId: project.id,
      developerId: devId,
      priceRange: '1M-2M',
      paymentPlan: '10/90',
      deliveryDate: '2028-12-31',
    },
  }), 'create knowledge base item');
  remember('knowledge-base', kb.id);
  await request(`/api/knowledge-base/${kb.id}`, {
    method: 'PUT',
    headers: adminAuth,
    body: {
      title: `Smoke KB ${now} updated`,
      category: 'Project',
      content: 'details2',
      projectId: project.id,
      developerId: devId,
      priceRange: '2M-3M',
      paymentPlan: '20/80',
      deliveryDate: '2029-12-31',
    },
  });

  const notif = ok(await request('/api/notifications', {
    method: 'POST',
    headers: adminAuth,
    body: {
      userId: admin.id,
      title: `Smoke Notif ${now}`,
      body: 'body',
      isRead: false,
    },
  }), 'create notification');
  remember('notifications', notif.id);
  await request(`/api/notifications/${notif.id}`, {
    method: 'PUT',
    headers: adminAuth,
    body: {
      userId: admin.id,
      title: `Smoke Notif ${now} updated`,
      body: 'body2',
      isRead: true,
    },
  });

  console.log(JSON.stringify({
    auth: true,
    roles: true,
    users: true,
    leads: true,
    coldCalls: true,
    comments: true,
    deals: true,
    attachments: true,
    calendar: true,
    tasks: true,
    pipeline: true,
    knowledgeBase: true,
    reports: true,
    notifications: true,
  }, null, 2));

  } finally {
    await destroyCleanup(adminAuth);
  }
};

main().catch(async (error) => {
  console.error(error.message);
  process.exitCode = 1;
});
