# Python Distributed Async

**A tiny, hackable distributed task runner for Python async functions.**

Python Distributed Async lets you send a Python function to a central FastAPI server, store it as a task, let one or more workers pull pending work, execute the function, and return the result. It is intentionally small: no Redis, no RabbitMQ, no heavy broker, no big framework. The default storage is SQLite through Peewee, and function payloads are serialized with `dill`, which makes it useful for experiments, internal tools, prototypes, and learning how distributed queues work under the hood.

> This project is not trying to replace Celery. It is a compact educational and practical alternative for cases where you want to run Python callables remotely with minimal infrastructure.

## Why this project exists

Most Python task queues are built around a broker: Redis, RabbitMQ, or another message system. That is the correct production architecture for many systems, but it also adds moving parts.

Python Distributed Async is useful when you want:

- a minimal queue server that starts with one Python command;
- workers that can run on another machine and poll tasks over HTTP;
- async Python functions as first-class tasks;
- scheduled execution by `execution_time`;
- a readable codebase small enough to modify in one sitting;
- a simple foundation for your own distributed execution experiments.

## What it can do

- Create tasks over HTTP.
- Serialize Python functions, args, and kwargs with `dill`.
- Store tasks in SQLite.
- Pull pending tasks from workers.
- Mark tasks as `WAITING`, `IN_PROGRESS`, `DONE`, or `FAILED`.
- Execute both async and sync callables.
- Wait for results through the API.
- Schedule a task for a future datetime.
- Run multiple workers against the same server.

## Architecture

```text
┌──────────────┐       POST /create_task        ┌──────────────┐
│ Producer     │ ─────────────────────────────▶ │ FastAPI API   │
│ script/app   │                                │ + SQLite DB   │
└──────────────┘                                └──────┬───────┘
                                                        │
                                                        │ GET /get_all_tasks
                                                        ▼
                                                ┌──────────────┐
                                                │ Worker       │
                                                │ client       │
                                                └──────┬───────┘
                                                        │
                                                        │ execute function
                                                        ▼
                                                POST /set_task_result
```

The server owns the task table. Workers poll the server, lock a task by moving it to `IN_PROGRESS`, execute it locally, and report either a serialized result or an error message.

## Installation

```bash
git clone https://github.com/megamen32/PythonDistributedAsync
cd PythonDistributedAsync
python -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```

## Start the server

```bash
python -m server.server
```

The API will be available at:

```text
http://localhost:8001
```

Health check:

```bash
curl http://localhost:8001/health
```

## Start a worker

In another terminal:

```bash
source .venv/bin/activate
python -m client.client
```

The worker polls the server every few seconds, starts waiting tasks, executes them, and sends results back.

## Run the example

```bash
source .venv/bin/activate
python examples/search.py
```

The example sends an async function to the server. A running worker receives the task, fetches OpenNET news titles, and returns the result.

## Minimal usage

```python
import base64
import datetime as dt
import dill
import requests


async def add(a: int, b: int) -> int:
    return a + b

payload = {
    "func": base64.b64encode(dill.dumps(add)).decode("utf-8"),
    "args": base64.b64encode(dill.dumps((2, 3))).decode("utf-8"),
    "kwargs": base64.b64encode(dill.dumps({})).decode("utf-8"),
    "execution_time": dt.datetime.now().isoformat(),
}

response = requests.post("http://localhost:8001/create_task", json=payload)
task_uuid = response.json()["task_uuid"]

result = requests.get(f"http://localhost:8001/get_task_result/{task_uuid}").json()
print(result)
```

## HTTP API

### `GET /health`

Returns a simple health status.

### `POST /create_task`

Creates a new task.

Request body:

```json
{
  "func": "base64(dill(function))",
  "args": "base64(dill(tuple))",
  "kwargs": "base64(dill(dict))",
  "execution_time": "2026-05-12T12:00:00"
}
```

Response:

```json
{
  "message": "Task created",
  "task_uuid": "...",
  "uuid": "..."
}
```

### `GET /get_all_tasks`

Returns all tasks that are still waiting for execution.

### `GET /get_task/{uuid}`

Returns a single task payload by UUID.

### `POST /start_task/{uuid}`

Moves a task from `WAITING` to `IN_PROGRESS`. If another worker already started it, the API returns `409`.

### `POST /set_task_result/{uuid}`

Stores a completed result or failure.

Success request:

```json
{
  "result": "base64(dill(result))"
}
```

Failure request:

```json
{
  "result": "base64(dill(error_text))",
  "error_message": "Something went wrong"
}
```

### `GET /get_task_result/{uuid}`

Waits for a task for up to 30 seconds. If the task is not finished, returns its current status. If it is finished, returns either `result` or `error`.

## Competitors and alternatives

### Celery

Celery is the heavyweight, mature standard. It supports real-time distributed processing, scheduling, brokers, result backends, retries, routing, monitoring, and complex workflows. Celery is better when you need production-grade reliability, operations tooling, and a large ecosystem.

Use Celery when you are building a serious production queue and already accept the infrastructure cost of Redis or RabbitMQ.

### Dramatiq

Dramatiq is a modern background task library focused on simplicity, reliability, and performance. It is usually easier to reason about than Celery while still being production-oriented.

Use Dramatiq when you want a cleaner production task queue with Redis or RabbitMQ and fewer historical layers than Celery.

### RQ

RQ, or Redis Queue, is a simple Python job queue backed by Redis or Valkey. Its main strength is low mental overhead: enqueue a Python function, run workers, get results.

Use RQ when you want something simple and proven and you are comfortable requiring Redis.

### Huey

Huey is lightweight and practical. It supports Redis, SQLite, file-system, and in-memory storage, plus scheduled and periodic tasks.

Use Huey when you want a small queue with more built-in features than this project but less operational weight than Celery.

### arq

arq is an asyncio-first Redis-backed queue and RPC tool. It fits projects that are already async and need a more formal worker model.

Use arq when your stack is asyncio-native and Redis is acceptable.

### Taskiq

Taskiq is a newer async distributed task manager. It supports sync and async functions and has a more extensible architecture.

Use Taskiq when you want an asyncio-friendly, production-minded framework with pluggable brokers and middleware.

## Who did it better?

For production, the honest answer is: **Celery, Dramatiq, RQ, Huey, arq, and Taskiq all do the general task-queue problem better than this project today.** They have stronger reliability semantics, better documentation, worker management, retries, monitoring, broker integrations, and larger user bases.

This project can still be valuable because it is much smaller and easier to understand. Its strongest niche is not “enterprise task queue”; it is:

- learning how distributed task queues work;
- building a local/internal tool quickly;
- experimenting with remote execution of Python callables;
- creating a custom queue where the code must stay tiny and hackable.

## Security warning

This project serializes and deserializes Python functions with `dill`. That is powerful, but it is also dangerous.

Do **not** expose this server to untrusted users or the public internet. A malicious serialized function can execute arbitrary Python code on a worker. Treat this as trusted-internal infrastructure only unless you add authentication, authorization, sandboxing, and a safer task registration model.

## Current limitations

- No authentication.
- No retry policy.
- No task timeout/kill mechanism.
- No worker heartbeat.
- No task priority.
- No pagination for large task lists.
- SQLite is fine for local/small usage but not ideal for many concurrent workers.
- `dill` payloads make arbitrary code execution possible.
- No CLI yet for creating tasks interactively.

## Roadmap ideas

- Add API token authentication.
- Add retries and retry delays.
- Add task priority.
- Add worker heartbeat and worker names.
- Add `/metrics` for Prometheus.
- Add a small CLI: `pda server`, `pda worker`, `pda submit`.
- Add Postgres support.
- Add safe named-task registration instead of arbitrary `dill` functions.
- Add tests with `pytest` and `httpx`.
- Add Docker Compose example.

## Development notes

The project is intentionally small:

```text
client/client.py      worker implementation
server/server.py      FastAPI API
task/task.py          task model and execution logic
examples/search.py    runnable example
```

Run a syntax check:

```bash
python -m py_compile client/client.py server/server.py task/task.py examples/search.py
```

## License

Add a license before publishing the project. MIT is a good default for a small educational infrastructure project.
