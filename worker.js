const { Worker } = require('bullmq');
const mysql = require('mysql2/promise');
const os = require('os');

const workerName = `${os.hostname()}-pid${process.pid}`;

async function connectDB() {
  return mysql.createConnection({
    host: 'localhost',
    user: 'queue_user',
    password: 'harold_john_369',
    database: 'jobqueue'
  });
}

// Register worker as active on startup
(async () => {
  const db = await connectDB();
  await db.execute(
    'INSERT INTO workers (name, status, last_seen) VALUES (?, ?, NOW()) ON DUPLICATE KEY UPDATE status=?, last_seen=NOW()',
    [workerName, 'active', 'active']
  );
  console.log(`✅ Worker ${workerName} registered as active`);
})().catch(err => console.error("❌ Worker registration failed:", err.message));

const worker = new Worker(
  'jobQueue',
  async job => {
    console.log("⚡ Worker picked up job:", job.id, job.name, job.data);

    const db = await connectDB();
    const mysqlId = job.data.mysqlId;

    // Mark worker busy
    await db.execute(
      'UPDATE workers SET status=?, last_seen=NOW() WHERE name=?',
      ['busy', workerName]
    );

    // Mark job as processing
    await db.execute(
      'UPDATE jobs SET status=?, worker_id=? WHERE id=?',
      ['processing', workerName, mysqlId]
    );

    // Insert attempt start
    await db.execute(
      'INSERT INTO job_attempts (job_id, attempt_number, status, attempt_time) VALUES (?, ?, ?, NOW())',
      [mysqlId, job.attemptsMade + 1, 'started']
    );

    try {
      console.log(`🔧 Worker ${workerName} processing job: ${job.name}`, job.data);

      if (job.name === 'emailJob') {
        console.log(`📧 Sending email to ${job.data.to} with subject "${job.data.subject}"`);
      }

      // Mark job completed
      await db.execute('UPDATE jobs SET status=? WHERE id=?', ['completed', mysqlId]);
      await db.execute('INSERT INTO job_logs (job_id, message, created_at) VALUES (?, ?, NOW())', [mysqlId, 'Job completed']);

      // Insert success attempt
      await db.execute(
        'INSERT INTO job_attempts (job_id, attempt_number, status, attempt_time) VALUES (?, ?, ?, NOW())',
        [mysqlId, job.attemptsMade + 1, 'success']
      );

      console.log(`✅ Job ${mysqlId} marked completed in DB`);

      // Mark worker idle after success
      await db.execute(
        'UPDATE workers SET status=?, last_seen=NOW() WHERE name=?',
        ['idle', workerName]
      );
    } catch (err) {
      console.error(`❌ Error processing job ${mysqlId}:`, err.message);

      await db.execute('UPDATE jobs SET status=? WHERE id=?', ['failed', mysqlId]);
      await db.execute('INSERT INTO job_logs (job_id, message, created_at) VALUES (?, ?, NOW())', [mysqlId, 'Job failed']);

      // Insert failure attempt
      await db.execute(
        'INSERT INTO job_attempts (job_id, attempt_number, status, error_message, attempt_time) VALUES (?, ?, ?, ?, NOW())',
        [mysqlId, job.attemptsMade + 1, 'failure', err.message]
      );

      console.log(`⚠️ Job ${mysqlId} marked failed in DB`);

      // Mark worker idle after failure
      await db.execute(
        'UPDATE workers SET status=?, last_seen=NOW() WHERE name=?',
        ['idle', workerName]
      );
    }
  },
  {
    connection: { host: '127.0.0.1', port: 6379 }
  }
);

console.log("🚀 Worker connected to Redis, waiting for jobs...");

worker.on('completed', job => {
  console.log(`🎉 Job ${job.data.mysqlId} completed successfully`);
});

worker.on('failed', (job, err) => {
  console.error(`💥 Job ${job?.data.mysqlId} failed: ${err.message}`);
});

// On shutdown → mark worker offline
process.on('SIGINT', async () => {
  const db = await connectDB();
  await db.execute(
    'UPDATE workers SET status=?, last_seen=NOW() WHERE name=?',
    ['offline', workerName]
  );
  console.log(`⚠️ Worker ${workerName} marked offline`);
  process.exit();
});
