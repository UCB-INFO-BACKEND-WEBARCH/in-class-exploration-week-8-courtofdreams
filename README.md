# Week 8 In-Class Exploration: Notification Service

## Overview

You're building a notification service that sends emails to users. The current implementation is **slow** - it blocks for 3 seconds per notification while "sending" the email. Your task is to convert it to use **background processing** with Redis and rq.

## The Problem

Try sending a notification:

```bash
time curl -X POST http://localhost:5000/notifications \
  -H "Content-Type: application/json" \
  -d '{"email": "test@example.com", "message": "Hello!"}'
```

It takes **3+ seconds** to respond! The server is blocked while "sending" the email. This doesn't scale - what if 100 users try to send notifications at once?

## Your Goal

Make `POST /notifications` return **immediately** (< 200ms) with a job ID. The actual sending happens in a background worker.

## Setup

### 1. Start Redis

```bash
docker-compose up -d redis
```

### 2. Install Dependencies

```bash
pip install -r requirements.txt
```

### 3. Run the Starter (observe the slowness!)

```bash
python app.py
```

Test it:
```bash
./test-commands.sh
```

## Tasks

### Task 1: Create `tasks.py` (10 min)

Create a new file `tasks.py` with a background task function using the `@job` decorator:

```python
from rq.decorators import job
from redis import Redis
import os

redis_conn = Redis.from_url(os.getenv('REDIS_URL', 'redis://localhost:6379/0'))

@job('notifications', connection=redis_conn)
def send_notification(notification_id, email, message):
    # Your code here
    pass
```

**Key insight**: The `@job` decorator works just like Flask's `@app.route`:
- `@app.route('/path')` → function handles HTTP requests **NOW**
- `@job('queue_name')` → function handles queue jobs **LATER**

| Function | Parameters | Behavior |
|----------|------------|----------|
| `send_notification` | `notification_id`, `email`, `message` | Sleep 3 seconds, return result dict |

The function should:
- Print a message when starting
- Sleep for 3 seconds (simulating slow email API)
- Print a message when done
- Return a dict with `notification_id`, `email`, `status`, `sent_at`

### Task 2: Update `app.py` (10 min)

Modify `app.py` to use rq with the `.delay()` pattern:

| Step | Code Pattern |
|------|--------------|
| Import Redis | `from redis import Redis` |
| Import task | `from tasks import send_notification` |
| Connect to Redis | `redis_conn = Redis.from_url(os.getenv('REDIS_URL', 'redis://localhost:6379/0'))` |
| Queue task with .delay() | `job = send_notification.delay(id, email, message)` |
| Return job ID | Return `{"job_id": job.id}` with status `202` |

**Key insight**: The `@job` decorator adds a `.delay()` method to your function:
- `send_notification(id, email, msg)` → runs **NOW** (blocks for 3 seconds!)
- `send_notification.delay(id, email, msg)` → queues to run **LATER** (returns instantly!)

### Task 3: Add Job Status Endpoint (10 min)

Add `GET /jobs/<job_id>` to check job status:

| Step | Code Pattern |
|------|--------------|
| Import Job | `from rq.job import Job` |
| Fetch job | `job = Job.fetch(job_id, connection=redis_conn)` |
| Get status | `job.get_status()` returns "queued", "started", "finished", "failed" |
| Get result | `job.result` (available when finished) |

### Task 4: Run the Worker (5 min)

Start the rq worker to process jobs:

```bash
rq worker --url redis://localhost:6379/0 notifications
```

Or add it to docker-compose.yml:

```yaml
worker:
  build: .
  command: rq worker --url redis://redis:6379/0 notifications
  depends_on:
    - redis
```

### Task 5: Test It! (10 min)

1. POST should return instantly now
2. Worker logs show processing
3. Job status changes from "queued" → "started" → "finished"

```bash
# Should be instant!
time curl -X POST http://localhost:8001/notifications \
  -H "Content-Type: application/json" \
  -d '{"email": "test@example.com", "message": "Hello!"}'

# Check job status
curl http://localhost:8001/jobs/YOUR_JOB_ID
```

## Success Criteria

- [ ] POST `/notifications` returns in < 200ms
- [ ] Response includes `job_id` and status `202`
- [ ] Worker processes notifications (see worker logs)
- [ ] GET `/jobs/<id>` shows status progression
- [ ] Multiple notifications can be queued simultaneously

## Common Issues

| Problem | Solution |
|---------|----------|
| "Connection refused" to Redis | Start Redis: `docker-compose up -d redis` |
| Jobs stay "queued" forever | Start the worker: `rq worker ...` |
| Worker can't import tasks | Make sure `tasks.py` is in the same directory |
| Environment variables not found | Add `import os` and use `os.getenv()` |

## Extensions (if you finish early)

1. **Retry on failure**: Use `retry=Retry(max=3)` in enqueue
2. **Multiple queues**: Create "urgent" and "normal" queues
3. **Persist notifications**: Use SQLite instead of in-memory dict

## Submission

Commit your changes and push to your repository. Make sure:
- `tasks.py` exists with `@job` decorated `send_notification` function
- `app.py` uses `.delay()` for background processing
- Worker can process jobs successfully
