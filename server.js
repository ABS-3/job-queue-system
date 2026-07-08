require('dotenv').config();

const express = require('express');
const cors = require('cors');
const { Queue } = require('bullmq');
const mysql = require('mysql2/promise');
const path = require('path');

const app = express();

const dbPool = mysql.createPool({
  host: process.env.DB_HOST,
  user: process.env.DB_USER,
  password: process.env.DB_PASSWORD,
  database: process.env.DB_NAME
});

app.use(cors());
app.use(express.json());
app.use(express.static(path.join(__dirname, 'public')));

// MySQL connection pool
const dbPool = mysql.createPool({
  host: 'localhost',
  user: 'queue_user',
  password: 'harold_john_369',
  database: 'jobqueue',
  waitForConnections: true,
  connectionLimit: 10,
  queueLimit: 0
});

// BullMQ queue
const queue = new Queue('jobQueue', {
  connection: { host: '127.0.0.1', port: 6379 }
});

// Simple test route
app.get('/ping', (req, res) => {
  console.log("🔍 /ping route hit");
  res.json({ message: "pong" });
});

// POST /jobs → enqueue job
app.post('/jobs', async (req, res) => {
  console.log("📥 Incoming job request:", req.body);
  const { name, payload } = req.body;
  try {
    const [result] = await dbPool.execute(
      'INSERT INTO jobs (name, payload, status) VALUES (?, ?, ?)',
      [name, JSON.stringify(payload), 'pending']
    );
    const mysqlId = result.insertId;
    console.log("✅ Job inserted into MySQL with ID:", mysqlId);

    await queue.add(name, { ...payload, mysqlId });
    console.log("✅ Job enqueued in BullMQ:", name);

    res.json({ message: 'Job enqueued', id: mysqlId });
  } catch (err) {
    console.error("❌ Error in POST /jobs:", err);
    res.status(500).json({ error: err.message });
  }
});

// GET /jobs/:id → job status
app.get('/jobs/:id', async (req, res) => {
  console.log("🔍 /jobs/:id route hit with ID:", req.params.id);
  try {
    const [rows] = await dbPool.execute('SELECT * FROM jobs WHERE id=?', [req.params.id]);
    console.log("Rows from jobs by ID:", rows);
    if (rows.length === 0) return res.status(404).json({ error: 'Job not found' });
    res.json(rows[0]);
  } catch (err) {
    console.error("❌ Error in GET /jobs/:id:", err);
    res.status(500).json({ error: err.message });
  }
});

// GET /jobs → list all jobs
app.get('/jobs', async (req, res) => {
  console.log("🔍 /jobs route hit");
  try {
    const [rows] = await dbPool.execute('SELECT * FROM jobs');
    console.log("Rows from jobs:", rows);
    res.json(rows);
  } catch (err) {
    console.error("❌ Error in GET /jobs:", err);
    res.status(500).json({ error: err.message });
  }
});


// GET /jobs/:id/logs → job logs
app.get('/jobs/:id/logs', async (req, res) => {
  console.log("🔍 /jobs/:id/logs route hit with ID:", req.params.id);
  try {
    const [rows] = await dbPool.execute(
      'SELECT * FROM job_logs WHERE job_id=? ORDER BY created_at ASC',
      [req.params.id]
    );
    res.json(rows);
  } catch (err) {
    console.error("❌ Error in GET /jobs/:id/logs:", err);
    res.status(500).json({ error: err.message });
  }
});

// GET /jobs/:id/attempts → job attempts
app.get('/jobs/:id/attempts', async (req, res) => {
  console.log("🔍 /jobs/:id/attempts route hit with ID:", req.params.id);
  try {
    const [rows] = await dbPool.execute(
      'SELECT * FROM job_attempts WHERE job_id=? ORDER BY attempt_time ASC',
      [req.params.id]
    );
    res.json(rows);
  } catch (err) {
    console.error("❌ Error in GET /jobs/:id/attempts:", err);
    res.status(500).json({ error: err.message });
  }
});



// GET /workers → list all workers
app.get('/workers', async (req, res) => {
  console.log("🔍 /workers route hit");
  try {
    const [rows] = await dbPool.execute('SELECT * FROM workers');
    console.log("Rows from workers:", rows);
    res.json(rows);
  } catch (err) {
    console.error("❌ Error in GET /workers:", err);
    res.status(500).json({ error: err.message });
  }
});

// GET /workers/:id → single worker details
app.get('/workers/:id', async (req, res) => {
  console.log("🔍 /workers/:id route hit with ID:", req.params.id);
  try {
    const [rows] = await dbPool.execute('SELECT * FROM workers WHERE id=?', [req.params.id]);
    console.log("Rows from workers by ID:", rows);
    if (rows.length === 0) return res.status(404).json({ error: 'Worker not found' });
    res.json(rows[0]);
  } catch (err) {
    console.error("❌ Error in GET /workers/:id:", err);
    res.status(500).json({ error: err.message });
  }
});

app.listen(3000, '0.0.0.0', () => {
  console.log('🚀 API running on http://localhost:3000');
});
