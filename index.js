const { Queue } = require('bullmq');
const mysql = require('mysql2/promise');

async function main() {
  // Connect to MySQL
  const db = await mysql.createConnection({
    host: 'localhost',
    user: 'queue_user',
    password: 'harold_john_369',
    database: 'jobqueue'
  });

  // Connect to Redis queue
  const queue = new Queue('jobQueue', {
    connection: { host: '127.0.0.1', port: 6379 }
  });

  // Insert job into MySQL first
  const [result] = await db.execute(
    'INSERT INTO jobs (name, payload, status) VALUES (?, ?, ?)',
    ['emailJob', JSON.stringify({ to: 'user@example.com', subject: 'Hello from BullMQ!' }), 'pending']
  );

  const mysqlId = result.insertId; // keep integer ID

  // Enqueue job in BullMQ, carrying MySQL id in payload
  await queue.add(
    'emailJob',
    { to: 'user@example.com', subject: 'Hello from BullMQ!', mysqlId }
  );

  console.log(`Job enqueued successfully with DB id ${mysqlId}`);
}

main().catch(err => console.error(err));
