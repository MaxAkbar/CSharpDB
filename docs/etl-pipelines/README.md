# ETL Pipelines Plan (SSIS-Lite for CSharpDB)

This document proposes a slim ETL pipeline feature inspired by SSIS, designed for embedded/local workloads.

The objective is to provide reliable, scriptable data movement with schema mapping and basic transforms, without building a full enterprise orchestration platform.

---

## Goals

- Simple package-driven ETL runs
- Strong local reliability (transactions + checkpoints + resume)
- Low memory footprint (streaming/batched execution)
- Clear observability (run status, counts, rejects)

---

## MVP Scope

### Sources

- CSV file
- JSON file
- CSharpDB table
- SQL query result

### Transforms

- select/projection
- rename columns
- cast/coerce types
- filter rows
- derive computed columns
- deduplicate rows

### Destinations

- CSharpDB table
- CSV file
- JSON file

### Execution Modes

- `validate` (schema/config checks only)
- `dry-run` (read + transform, no destination writes)
- `run` (full execution)

### Error Modes

- fail-fast
- skip-bad-rows with reject output

---

## Package Definition

Use JSON or YAML package files with a minimal structure:

- pipeline metadata (`name`, `version`)
- source configuration
- transform chain
- destination configuration
- options (`batchSize`, `errorMode`, `checkpointInterval`)
- optional incremental watermark definition

---

## Runtime Design

Add a new runtime project: `src/CSharpDB.Pipeline`.

Core components:

- connector interfaces for source/destination
- transform interface for row/batch processors
- orchestrator for pipeline lifecycle and metrics
- checkpoint manager for resumable execution
- run logger for auditability

Processing model:

- read in batches
- apply transforms in order
- write destination in transactional batches
- persist checkpoints and counters periodically

---

## Metadata and Observability

Track execution state in system tables:

- `_etl_runs`
- `_etl_steps`
- `_etl_checkpoints`
- `_etl_rejects`

Capture:

- run id, package name, start/end time, status
- rows read/written/rejected
- current step and last checkpoint
- error summary and reject file/table location

---

## CLI Plan

Add `etl` command group:

- `csharpdb etl validate <package>`
- `csharpdb etl run <package>`
- `csharpdb etl status <runId>`
- `csharpdb etl resume <runId>`

Future API equivalents can be added after CLI stabilization.

---

## Safety and Reliability

Write operations should enforce:

1. single-writer safety for destination writes
2. per-batch transactional boundaries
3. deterministic checkpoint updates
4. resumable runs from last successful checkpoint
5. no partial package state mutation on failure

---

## Delivery Phases

### Phase 1

- package schema
- package validator
- `validate` and `dry-run`

### Phase 2

- CSV and CSharpDB connectors
- baseline orchestrator and `run`

### Phase 3

- transform library (rename/cast/filter/derive/dedupe)
- reject handling and error policies

### Phase 4

- checkpoints and resume
- run metadata tables

### Phase 5

- JSON and SQL-query connectors
- CLI polish and docs/examples

### Phase 6 (Optional)

- scheduler integration
- Admin UI run viewer

---

## Out of Scope (Initial)

- visual drag-and-drop pipeline designer
- distributed execution cluster
- advanced DAG branching/conditional workflows
- enterprise secret vault integration
