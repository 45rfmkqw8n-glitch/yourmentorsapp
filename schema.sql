CREATE DATABASE IF NOT EXISTS mentors_crm;
USE mentors_crm;

CREATE TABLE IF NOT EXISTS departments (
  id INT PRIMARY KEY AUTO_INCREMENT,
  name VARCHAR(255) NOT NULL,
  created_at TIMESTAMP NULL,
  updated_at TIMESTAMP NULL
);

CREATE TABLE IF NOT EXISTS jobtitles (
  id INT PRIMARY KEY AUTO_INCREMENT,
  name VARCHAR(255) NOT NULL,
  department_id INT NULL,
  created_at TIMESTAMP NULL,
  updated_at TIMESTAMP NULL,
  FOREIGN KEY (department_id) REFERENCES departments(id) ON DELETE SET NULL
);

CREATE TABLE IF NOT EXISTS users (
  id INT PRIMARY KEY,
  name VARCHAR(255) NOT NULL,
  email VARCHAR(255) NOT NULL,
  phone VARCHAR(50),
  password VARCHAR(255) DEFAULT 'password',
  department_id INT NULL,
  jobtitle_id INT NULL,
  parent_id INT NULL,
  role VARCHAR(50) DEFAULT 'agent',
  title VARCHAR(255),
  department VARCHAR(255),
  status TINYINT(1) DEFAULT 1,
  image LONGTEXT,
  created_at TIMESTAMP NULL,
  updated_at TIMESTAMP NULL,
  FOREIGN KEY (department_id) REFERENCES departments(id) ON DELETE SET NULL,
  FOREIGN KEY (jobtitle_id) REFERENCES jobtitles(id) ON DELETE SET NULL,
  FOREIGN KEY (parent_id) REFERENCES users(id) ON DELETE SET NULL
);

CREATE TABLE IF NOT EXISTS statuses (
  id INT PRIMARY KEY,
  name VARCHAR(255) NOT NULL,
  color VARCHAR(50) NOT NULL,
  created_at TIMESTAMP NULL,
  updated_at TIMESTAMP NULL
);

CREATE TABLE IF NOT EXISTS sources (
  id INT PRIMARY KEY,
  name VARCHAR(255) NOT NULL,
  color VARCHAR(50) NOT NULL,
  created_at TIMESTAMP NULL,
  updated_at TIMESTAMP NULL
);

CREATE TABLE IF NOT EXISTS developers (
  id INT PRIMARY KEY,
  name VARCHAR(255) NOT NULL,
  phone VARCHAR(50),
  email VARCHAR(255),
  whatsapp VARCHAR(50),
  website VARCHAR(255),
  description TEXT,
  logo VARCHAR(255),
  images TEXT,
  videos LONGTEXT,
  pdfs TEXT,
  created_at TIMESTAMP NULL,
  updated_at TIMESTAMP NULL
);

CREATE TABLE IF NOT EXISTS projects (
  id INT PRIMARY KEY,
  name VARCHAR(255) NOT NULL,
  developer_id INT NULL,
  created_at TIMESTAMP NULL,
  updated_at TIMESTAMP NULL,
  FOREIGN KEY (developer_id) REFERENCES developers(id) ON DELETE SET NULL
);

CREATE TABLE IF NOT EXISTS brokers (
  id INT PRIMARY KEY,
  name VARCHAR(255) NOT NULL,
  phone VARCHAR(50),
  developer_id INT NULL,
  created_at TIMESTAMP NULL,
  updated_at TIMESTAMP NULL,
  FOREIGN KEY (developer_id) REFERENCES developers(id) ON DELETE SET NULL
);

CREATE TABLE IF NOT EXISTS leads (
  id INT PRIMARY KEY,
  name VARCHAR(255) NOT NULL,
  phone VARCHAR(50) NOT NULL,
  whatsapp VARCHAR(50),
  workphone VARCHAR(50),
  date DATE NULL,
  project_id INT NULL,
  status_id INT NULL,
  source_id INT NULL,
  comment TEXT,
  user_id INT NULL,
  created_at TIMESTAMP NULL,
  updated_at TIMESTAMP NULL,
  FOREIGN KEY (project_id) REFERENCES projects(id) ON DELETE SET NULL,
  FOREIGN KEY (status_id) REFERENCES statuses(id) ON DELETE SET NULL,
  FOREIGN KEY (source_id) REFERENCES sources(id) ON DELETE SET NULL,
  FOREIGN KEY (user_id) REFERENCES users(id) ON DELETE SET NULL
);

CREATE TABLE IF NOT EXISTS cold_calls (
  id INT PRIMARY KEY,
  name VARCHAR(255) NOT NULL,
  phone VARCHAR(50) NOT NULL,
  status_id INT NOT NULL,
  user_id INT NULL,
  notes LONGTEXT,
  created_at TIMESTAMP NULL,
  updated_at TIMESTAMP NULL,
  FOREIGN KEY (status_id) REFERENCES statuses(id) ON DELETE CASCADE,
  FOREIGN KEY (user_id) REFERENCES users(id) ON DELETE SET NULL
);

CREATE TABLE IF NOT EXISTS comments (
  id INT PRIMARY KEY,
  user_id INT NOT NULL,
  lead_id INT NULL,
  cold_call_id INT NULL,
  entity_type VARCHAR(50) NULL,
  entity_id INT NULL,
  body TEXT NOT NULL,
  created_at TIMESTAMP NULL,
  updated_at TIMESTAMP NULL,
  FOREIGN KEY (user_id) REFERENCES users(id) ON DELETE CASCADE,
  FOREIGN KEY (lead_id) REFERENCES leads(id) ON DELETE CASCADE,
  FOREIGN KEY (cold_call_id) REFERENCES cold_calls(id) ON DELETE CASCADE
);

CREATE TABLE IF NOT EXISTS deals (
  id INT PRIMARY KEY,
  name VARCHAR(255) NOT NULL,
  phone VARCHAR(50) NOT NULL,
  project_id INT NULL,
  developer_id INT NULL,
  broker_id INT NULL,
  attachments LONGTEXT,
  reservations_images LONGTEXT,
  reservation_price DECIMAL(18,2),
  reservation_date DATE NULL,
  price DECIMAL(18,2),
  contract_date DATE NULL,
  unit_number VARCHAR(100),
  commission DECIMAL(18,2) DEFAULT 0,
  stage VARCHAR(50),
  comment TEXT,
  status VARCHAR(50),
  user_id INT NULL,
  created_at TIMESTAMP NULL,
  updated_at TIMESTAMP NULL,
  FOREIGN KEY (project_id) REFERENCES projects(id) ON DELETE SET NULL,
  FOREIGN KEY (developer_id) REFERENCES developers(id) ON DELETE SET NULL,
  FOREIGN KEY (broker_id) REFERENCES brokers(id) ON DELETE SET NULL,
  FOREIGN KEY (user_id) REFERENCES users(id) ON DELETE SET NULL
);

CREATE TABLE IF NOT EXISTS agent_care (
  id INT PRIMARY KEY,
  name VARCHAR(255) NOT NULL,
  phone VARCHAR(50) NOT NULL,
  whatsapp VARCHAR(50) NOT NULL,
  email VARCHAR(255) NOT NULL
);

CREATE TABLE IF NOT EXISTS activity_logs (
  id INT PRIMARY KEY AUTO_INCREMENT,
  user_id INT NULL,
  entity_type VARCHAR(100) NULL,
  entity_id INT NULL,
  action VARCHAR(100) NULL,
  summary TEXT NULL,
  metadata LONGTEXT NULL,
  created_at TIMESTAMP NULL
);

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
);

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
);

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
);

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
);

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
);

CREATE TABLE IF NOT EXISTS notifications (
  id INT PRIMARY KEY AUTO_INCREMENT,
  user_id INT NULL,
  title VARCHAR(255) NOT NULL,
  body TEXT NULL,
  is_read TINYINT(1) DEFAULT 0,
  created_at TIMESTAMP NULL,
  updated_at TIMESTAMP NULL
);
