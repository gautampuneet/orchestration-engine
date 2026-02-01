# Design Doc

## Introduction

This is a DAG-based workflow engine. You define workflows as directed acyclic graphs (nodes + dependencies), trigger them via API, and the orchestrator executes nodes in the right order. Results flow between nodes using templates like `{{ node_id.output }}`.

The stack is FastAPI (API layer), Celery (task queue), Redis (state store), and Python workers. Three services run independently:

- **API**: Validates workflows, stores DAGs, triggers execution
- **Orchestrator**: Manages execution state, dispatches nodes when dependencies complete, handles failures
- **Worker**: Runs the actual tasks (LLM calls, external APIs, etc.)

We picked Redis + Celery because they're simple, fast, and we need atomic operations for correctness. Redis gives us `HINCRBY`, `SADD`, `SETNX` which are critical for handling concurrency without locks.

## Design Goals

Build a system that executes workflows correctly and predictably. That's it.

Correctness means exactly-once execution even when Celery retries, nodes fail, or multiple parents complete at the same millisecond. Predictable means the same DAG always executes the same way.

We're not building a feature-complete workflow engine. We're building something simple that works right.

## Workflow Model

A workflow is a DAG. Each node has:

- `id`: unique identifier
- `handler`: task type (like `llm_service` or `call_external_service`)
- `config`: parameters for the task, can use templates
- `dependencies`: list of parent node IDs
- `timeout_seconds`: max execution time

The API validates the DAG before storing it. No cycles, no missing dependencies, no duplicate IDs.

## Execution Flow

When you trigger a workflow:

1. Orchestrator reads the DAG from Redis
2. Initializes execution state (all nodes PENDING, counters set)
3. Dispatches root nodes (nodes with zero dependencies)
4. Each node goes: PENDING → RUNNING → COMPLETED (or FAILED)
5. When a node completes, orchestrator decrements `deps_remaining` for its children
6. When a child's counter hits zero, it gets dispatched
7. Workflow completes when all nodes are COMPLETED or any node is FAILED

Everything happens via Celery callbacks. Workers call back to orchestrator on success/failure, orchestrator dispatches next nodes.

## Dependency Resolution

Each node gets a counter initialized to its number of dependencies:

```python
deps_remaining[node_id] = len(dependencies)
```

When a parent completes, we atomically decrement:

```python
remaining = redis.hincrby(f"exec:{id}:deps_remaining", child_id, -1)
if remaining == 0:
    dispatch_node(child_id)
```

`HINCRBY` is atomic. If two parents finish at the exact same time, only one sees the counter hit zero. That's the one that dispatches.

Before dispatch, we also check:

```python
if redis.sadd(f"exec:{id}:dispatched", node_id) == 0:
    return  # already dispatched
```

`SADD` returns 0 if the element already exists. This is defense in depth. Even if something goes wrong with the counter, we won't dispatch twice.

This handles fan-in (multiple parents → one child) safely without locks or coordination.

## Template Resolution

Nodes can reference outputs from parent nodes using Jinja2 templates:

```json
{
  "prompt": "Summarize this: {{ nodeA.result }}"
}
```

The orchestrator resolves templates before dispatching a node. It loads all completed outputs from Redis, builds a context dict, and renders the templates.

We resolve in the orchestrator (not the worker) because the orchestrator already knows which nodes have completed. Workers shouldn't need to understand dependencies.

## Idempotency

Two places where duplicate execution could happen:

### Worker

Before running the task:

```python
key = f"exec:{id}:taskdone:{id}:{node_id}"
if not redis.setnx(key, "1"):
    return  # already ran
```

`SETNX` only sets if the key doesn't exist. If Celery redelivers or retries, we skip the work.

### Orchestrator

Before processing a completion callback:

```python
completion_key = f"exec:{id}:completion_processed:{node_id}"
if not redis.setnx(completion_key, "1"):
    return  # already processed
```

Prevents duplicate callbacks from decrementing counters twice or dispatching children multiple times.

These two guards guarantee exactly-once execution and exactly-once completion processing.

## Failure Handling

When a node fails, the workflow immediately becomes FAILED. We mark the workflow status and propagate FAILED state to all downstream nodes (BFS traversal).

Three guards ensure nothing moves forward after failure:

1. `on_node_success()` checks workflow status, ignores late completions
2. `_dispatch_node()` won't dispatch if workflow is FAILED
3. `_check_workflow_completion()` never overwrites FAILED status

FAILED is terminal. Once set, it stays set. No new nodes get dispatched, no status transitions allowed.

This is fail-fast. One node fails → entire workflow stops.

## Partial Retry Behavior

If you re-trigger a FAILED workflow, the orchestrator only re-runs the failed nodes. Completed nodes are skipped.

How it works:

1. Find all nodes with FAILED state
2. Reset them to PENDING (clear `dispatched` and `completion_processed` flags)
3. Recalculate their `deps_remaining` counters (only count dependencies that still need to run)
4. Dispatch any failed nodes whose dependencies are already complete

Completed nodes keep their outputs in Redis. Failed nodes can reference them via templates just like normal.

This happens naturally from the existing idempotency + state logic. We don't have a special retry engine. We just reset the FAILED nodes and let the normal execution flow take over.

## Correctness Invariants

The system relies on these atomic operations:

- `SADD` → a node can only be dispatched once
- `SETNX` (worker) → a node's task can only execute once
- `SETNX` (orchestrator) → a completion can only be processed once
- `HINCRBY` → dependency counters decrement atomically
- Fail-fast guards → FAILED state is guaranteed terminal

No distributed locks needed. Redis atomic operations give us what we need.

## Non-Goals

Things we explicitly don't do:

- **No nested workflows**: Nodes can't spawn child workflows. DAG is flat.
- **No scheduling**: No cron, no time-based triggers. You call the API to trigger.
- **No worker routing**: All workers pull from the same queue. No node-to-worker affinity.
- **No persistence outside Redis**: Everything lives in Redis with 7-day TTL. If Redis dies, in-flight workflows are gone (use Redis AOF/RDB if you need durability).
- **No workflow versioning**: Once a workflow is triggered, its DAG is immutable. No hot-swapping definitions.

## Future Improvements

We have automatic retries (exponential backoff, respects `Retry-After` headers) and timeouts (Celery soft/hard limits). What's missing:

- **Better persistence**: Write workflow state to Postgres or S3 for long-term storage
- **Observability**: Emit metrics, traces, and structured events for monitoring
- **Workflow versioning**: Support upgrading DAGs while keeping old executions running
