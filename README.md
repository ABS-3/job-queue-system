# Job Queue System

A basic job queue system built with **Node.js**, **Express**, **BullMQ**, **Redis**, and **MySQL**.  
It demonstrates backend orchestration, job processing, logging, and monitoring with a simple dashboard.

---

## Tech Stack
- Node.js (Express API)
- MySQL (jobs, workers, logs, attempts tables)
- Redis (queue broker)
- BullMQ (job scheduling and retries)
- HTML Dashboard (frontend monitoring)

---

## Features
- Enqueue jobs via dashboard form
- Worker processes jobs and updates MySQL
- Logs and job attempts stored in database
- Dashboard shows jobs, workers, logs, and attempts
- Secure DB connection using `.env` variables