# Design Doc

## Goals

- Correctness under concurrent node completions (fan-in scenarios)
- Exactly-once node execution despite retries and failures
- Deterministic workflow execution

## How nodes know they're ready

Each node gets a counter set to the number of parents it has:

```python
deps_remaining["C"] = 2  # C waits for A and B
```

When a parent finishes, we decrement atomically:

```python
remaining = redis.hincrby(f"exec:{id}:deps_remaining", child_id, -1)
if remaining == 0:
    dispatch_node(child_id)
```

HINCRBY is atomic. Multiple parents can complete at the same time - only one will see the counter hit zero. That's when we dispatch.

No polling needed. Nodes start as soon as their dependencies finish.

## Fan-in (the tricky part)

Say nodes A and B both finish at exactly the same millisecond. They both try to dispatch C. C should only run once.

We use two guards:

**Counter (HINCRBY)** - Atomic decrement. Only one parent sees the counter hit zero.

**Dispatch set (SADD)** - Before dispatching, add to a set:

```python
if redis.sadd(f"exec:{id}:dispatched", node_id) == 0:
    return  # already dispatched, bail out
```

SADD returns 0 if the element was already there. Defense in depth - even if the counter somehow fails, this catches it.

Together these handle race conditions without locks. HINCRBY detects readiness correctly. SADD ensures we only dispatch once.

## Making sure things run once

### Workers

Before running anything:

```python
key = f"exec:{id}:taskdone:{id}:{node_id}"
if not redis.setnx(key, "1"):
    return  # already ran, skip it
```

SETNX only sets if the key doesn't exist. If Celery redelivers, we bail out early.

### Orchestrator

Same deal for completion callbacks:

```python
completion_key = f"exec:{id}:completion_processed:{node_id}"
if not redis.setnx(completion_key, "1"):
    return  # already processed this completion
```

Prevents duplicate callbacks from decrementing counters twice or dispatching children multiple times.

## When things fail

Node fails → workflow immediately becomes FAILED:

```python
meta["status"] = "FAILED"
```

Three guards stop everything from moving forward:

**Success callback** - `on_node_success()` checks if workflow is FAILED. Late completions get ignored.

**Dispatch check** - `_dispatch_node()` won't dispatch anything if workflow is FAILED.

**Status guard** - `_check_workflow_completion()` never overwrites FAILED status.

FAILED is terminal. Nothing can unset it, nothing new can dispatch. Workflow's done.

## What we don't do

Some things are intentionally left out:

**Workflow versioning** - Workflows are immutable once created. No upgrade path for running ones.

**Sub-workflows** - Nodes can't spawn child workflows. DAG stays flat.

**Durable persistence** - Everything's in Redis (7 day TTL). Use Redis persistence (AOF/RDB) or accept that restarting Redis kills in-flight workflows.

We did add:
- **Automatic retries** for external calls (5xx errors, network issues)
- **Timeouts** via Celery task limits
- **Smart workflow retry** to re-run just the failed parts

The goal is deterministic execution with strong correctness, not a feature-complete workflow engine. Each feature added increases state complexity. We're careful about that.
