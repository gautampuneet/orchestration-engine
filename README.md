# Workflow Orchestration Engine

Event-driven DAG execution engine built with FastAPI, Celery, and Redis.


## Architecture

The system has three services:

**API Service** - Validates DAG structure, stores workflows, triggers execution, returns status and results. Does not execute tasks.

**Orchestrator Service** - Manages workflow execution. Initializes state, dispatches nodes as Celery tasks, handles completion callbacks, resolves dependencies atomically.

**Worker Service** - Executes individual nodes. Stateless and idempotent. Returns results via Celery callbacks to orchestrator.

All state lives in Redis. Orchestrator uses atomic operations (HINCRBY, SADD, SETNX) to handle concurrent node completions without locks or Lua scripts.

## Setup

Start all services with Docker Compose:

```bash
docker-compose up -d
```

This starts:
- Redis (port 6379)
- API server (port 8000)
- Orchestrator workers
- Task workers

API docs: **http://localhost:8000/docs**

## Basic usage

Create and run a workflow:

```bash
# Create workflow (returns execution_id)
curl -X POST http://localhost:8000/workflow \
  -H "Content-Type: application/json" \
  -d @examples/linear_dag.json

# Start execution
curl -X POST http://localhost:8000/workflow/trigger/{execution_id}

# Check status
curl http://localhost:8000/workflows/{execution_id}

# Get results
curl http://localhost:8000/workflows/{execution_id}/results
```

Example workflow format:

```json
{
  "name": "My Workflow",
  "nodes": [
    {
      "id": "fetch_data",
      "handler": "call_external_service",
      "config": {
        "url": "https://api.example.com/users/123",
        "method": "GET"
      },
      "dependencies": []
    },
    {
      "id": "analyze_with_llm",
      "handler": "llm_service",
      "config": {
        "model": "gpt-4",
        "prompt": "Analyze this user data: {{ fetch_data.result }}"
      },
      "dependencies": ["fetch_data"]
    }
  ]
}
```

## API endpoints

| Endpoint | Method | Description |
|----------|--------|-------------|
| `/workflow` | POST | Create a workflow |
| `/workflow/trigger/{id}` | POST | Start execution |
| `/workflows/{id}` | GET | Get status |
| `/workflows/{id}/results` | GET | Get outputs |
| `/docs` | GET | Swagger UI |
| `/health` | GET | Health check |

## Available handlers

| Handler | What it does | Config Parameters |
|---------|-------------|-------------------|
| `call_external_service` | Call external APIs | `url` (required), `method` (GET/POST/etc), `body`, `headers` |
| `llm_service` | LLM service calls | `model` (e.g. "gpt-4"), `prompt` (required) |
| `http_request` | Generic HTTP requests | `url`, `method`, `headers`, `body` |
| `compute` | Math operations | `operation` (add/subtract/multiply/divide), `a`, `b` |
| `transform` | String transformations | `input`, `transform_type` (uppercase/lowercase/etc) |
| `aggregate` | Combine multiple inputs | `inputs` (array), `aggregate_type` (sum/concat/list) |

## Templates

Reference outputs from previous nodes:

```json
{
  "id": "generate_summary",
  "handler": "llm_service",
  "config": {
    "model": "gpt-4",
    "prompt": "Summarize the following data: {{ fetch_user.data }}"
  },
  "dependencies": ["fetch_user"]
}
```


## Configuration

### Node timeouts

```json
{
  "id": "llm_analysis",
  "handler": "llm_service",
  "config": {
    "model": "gpt-4",
    "prompt": "Analyze this large dataset..."
  },
  "timeout_seconds": 600
}
```

Default: 300s (5 min), Max: 3600s (1 hour)

### Correlation IDs

Pass `X-Correlation-ID` header to trace requests across services:

```bash
curl -H "X-Correlation-ID: my-trace-id" http://localhost:8000/workflows/{id}
```

All logs will include this ID. If not provided, one is auto-generated.

### Environment variables

```bash
# .env file
REDIS_URL=redis://localhost:6379/0
```
## Local development

```bash
# Install dependencies
pip install -r requirements.txt

# Start Redis
docker run -d -p 6379:6379 redis:7-alpine

# Terminal 1 - API
uvicorn services.api.main:app --reload --port 8000

# Terminal 2 - Orchestrator
python -m services.orchestrator.main

# Terminal 3 - Workers
python -m services.worker.main
```

## Testing

```bash
# Run unit tests
pytest tests/ -v
```

## Features

### Automatic retry

External handlers (`http_request`, `llm_service`, `call_external_service`) retry automatically on:
- 5xx server errors
- 429 rate limits (respects Retry-After header)
- Network timeouts and connection failures

Uses exponential backoff (1s → 2s → 4s, max 60s) for up to 3 attempts.

### Smart workflow retry

Re-trigger failed workflows to retry only the failed nodes:

```bash
curl -X POST http://localhost:8000/workflow/trigger/{execution_id}
```

Completed nodes keep their results and won't re-run.

### Cascading failures

When a node fails, all downstream dependent nodes are marked as FAILED automatically.


## Examples

Check the `examples/` directory:
- `linear_dag.json` - Simple A → B → C workflow
- `fan_out_fan_in.json` - One node splits into many, then merges
- `concurrent_completion.json` - Tests concurrent parent completions

## Limits

- Max 1000 nodes per workflow
- Max 10KB config per node
- Max 500 characters per template
- Workflow state retained for 7 days

See [DESIGN.md](DESIGN.md) for implementation details.
