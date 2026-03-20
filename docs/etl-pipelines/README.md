# ETL Pipelines Release Plan

This document turns the ETL roadmap item into a concrete delivery plan for CSharpDB.

The target is an SSIS-lite pipeline capability for local and embedded workloads that fits the current product shape:

- reusable engine/runtime components
- first-class client/API/CLI surfaces
- an admin UI for authoring and operating pipelines

This is a planning document only. It does not define code that has already been implemented.

---

## Release Outcome

For this release, ETL should ship as a package-driven pipeline system with:

- file, SQL, and CSharpDB-based ingestion
- ordered row transforms
- batch-oriented writes
- validation, dry-run, execute, and resume modes
- persisted run history and rejects
- automation through CLI and API
- admin UI support for authoring, validating, running, and monitoring pipelines

The feature should be designed so the runtime can be reused from:

- local embedded execution
- the existing .NET client
- the REST API
- the CLI
- the admin application

---

## Reuse First: What We Already Have

The current repo already has several building blocks that should be reused rather than re-created:

### Existing platform surfaces

- `src/CSharpDB.Client`
  Shared client abstraction already used by API and Admin. ETL should expose pipeline operations here so higher-level interfaces do not reimplement transport logic.
- `src/CSharpDB.Api`
  Minimal API endpoint pattern already exists for tables, views, procedures, saved queries, maintenance, and diagnostics.
- `src/CSharpDB.Cli`
  Command-mode pattern already exists for inspector and maintenance workflows.
- `src/CSharpDB.Admin`
  Blazor admin shell already supports object explorer, tabs, stateful editors, execution workflows, and saved UI state.

### Existing technical capabilities to leverage

- WAL, checkpoint, and persistence infrastructure in storage
- transaction support and single-writer safety expectations already present in the engine
- SQL execution and schema metadata access
- saved-query persistence patterns that can inform pipeline-package persistence/versioning
- existing admin patterns for:
  - object explorer entries
  - design/editor tabs
  - run/execute toolbars
  - stateful visual designers

### Existing UI precedent relevant to ETL

- `ProcedureTab.razor` shows how to model create/edit/run flows in Admin.
- `QueryDesignerPanel.razor` shows how to support visual design state, saved layouts, and canvas-based editing.
- The provided mockup file `pipeline-builder-v2_2.html` is a strong starting point for the ETL designer experience and should be adapted into the admin app rather than treated as a throwaway artifact.

---

## Capability Gaps To Close

The current repo does not yet appear to have ETL-specific runtime and management features. Before implementation, these gaps should be treated as explicit work items:

### Runtime gaps

- no dedicated pipeline runtime/orchestrator
- no connector abstraction for ETL sources and destinations
- no transform pipeline abstraction for row/batch processing
- no ETL package schema and validator
- no ETL run metadata model
- no reject-row handling model
- no ETL-specific checkpoint/resume contract

### Interface gaps

- no client API for creating, validating, running, resuming, or inspecting pipelines
- no REST endpoints for ETL lifecycle operations
- no CLI command group for ETL
- no admin UI object explorer category, designer tab, run history view, or monitoring view for ETL

### Product gaps

- no finalized pipeline package format
- no decision yet on where packages are stored:
  - filesystem package file
  - CSharpDB-managed catalog tables
  - both, with import/export

---

## Recommended Project Shape

ETL should be implemented in a separate project and then surfaced through existing interfaces.

### New projects

- `src/CSharpDB.Pipelines`
  Core package model, validation, connectors, transforms, orchestration, run contracts, and checkpoint logic.
- `src/CSharpDB.Pipelines.Abstractions` (optional if separation becomes useful)
  Shared interfaces for connectors/transforms if we want strict layering or future external extensions.

### Why a separate project is the right default

- keeps ETL concerns isolated from the core database engine
- allows the runtime to be consumed by CLI, API, Admin, and direct client use
- avoids mixing package orchestration concerns into existing query/procedure code paths
- makes versioning and testing cleaner

### Projects that should be extended, not forked

- `src/CSharpDB.Client`
- `src/CSharpDB.Api`
- `src/CSharpDB.Cli`
- `src/CSharpDB.Admin`

The release should not create a second client stack or a second admin host just for ETL.

---

## Proposed Functional Scope

### Sources

- CSV file
- JSON file
- CSharpDB table
- SQL query result

### Transforms

- projection/select
- rename columns
- type coercion/casting
- filter rows
- derived columns
- deduplicate rows

### Destinations

- CSharpDB table
- CSV file
- JSON file

### Execution modes

- `validate`
- `dry-run`
- `run`
- `resume`

### Error modes

- fail-fast
- skip-bad-rows with reject output

### Non-goals for this release

- distributed execution
- arbitrary DAG orchestration across many branches
- full enterprise scheduler/orchestrator
- custom code execution inside transforms

The provided mockup contains broader workflow concepts like condition nodes and try/catch shapes. Those are useful UI references, but they should not expand the first release beyond the package model above unless the runtime scope is intentionally widened.

---

## Package Model Plan

The package format should be defined first because every interface depends on it.

### Recommended shape

Use JSON as the primary persisted format for this release.

Reasons:

- aligns with current API/client payload conventions
- easier to validate consistently across CLI, API, and Admin
- easier to serialize in the admin UI
- avoids introducing YAML parsing/config complexity in the first version

### Package sections

- metadata
  - `name`
  - `version`
  - `description`
- source
- transforms
- destination
- execution options
  - `batchSize`
  - `errorMode`
  - `checkpointInterval`
  - `maxRejects`
- optional incremental settings
  - watermark column/expression
  - last processed value

### Validation expectations

Validation must confirm:

- source configuration completeness
- destination compatibility
- transform ordering and input/output column shape
- type compatibility
- invalid option combinations
- resumability prerequisites for incremental/checkpointed runs

---

## Runtime Architecture Plan

### Core runtime responsibilities

- load and validate package definitions
- build an executable pipeline plan
- stream or batch rows from source
- apply ordered transforms
- write destination batches transactionally
- persist run metrics and checkpoint state
- emit rejects and error summaries

### Processing model

1. parse package
2. validate source, transforms, and destination
3. open source reader
4. process rows in batches
5. write each batch in a transaction boundary
6. persist counters/checkpoints after successful batch completion
7. finalize run state and outputs

### Reuse strategy

- reuse existing database transaction and persistence semantics for destination writes
- reuse storage checkpoint concepts where applicable, but keep ETL checkpoints logically separate from WAL/database checkpoints
- reuse existing client-side models and transport patterns for higher-level interfaces

### Important design boundary

ETL checkpoints should represent pipeline progress, not storage-engine internals. The runtime may rely on existing durability guarantees, but ETL resume state must remain a distinct abstraction.

---

## Metadata and Persistence Plan

ETL needs first-class persisted metadata so runs can be inspected from API, CLI, and Admin.

### Recommended system tables

- `_etl_pipelines`
- `_etl_pipeline_versions`
- `_etl_runs`
- `_etl_run_steps`
- `_etl_checkpoints`
- `_etl_rejects`

### Why catalog tables matter

- Admin and CLI need a stable source of truth
- API endpoints need consistent status/history data
- imported/exported packages should be versionable
- run diagnostics should survive process restarts

### Filesystem vs database storage

Recommended release approach:

- support package execution from JSON file
- support importing packages into CSharpDB-managed ETL catalog tables
- support exporting stored packages back to JSON

This gives:

- file-based developer workflow
- database-backed operational workflow
- a clean path for the admin UI

---

## Client API Plan

The shared .NET client should become the primary abstraction for ETL management.

### Add ETL operations to `CSharpDB.Client`

- list pipelines
- get pipeline
- import pipeline
- export pipeline
- validate pipeline
- run pipeline
- resume pipeline
- cancel pipeline if supported later
- list runs
- get run status/details
- list rejects/checkpoints for a run

### Model additions

- pipeline definition model
- validation result model
- run request/response models
- run status and metrics models
- reject summary/detail models
- checkpoint summary model

### Why client first matters

The API and Admin already depend on the client layer. ETL should follow the same pattern so transport logic is defined once.

---

## REST API Plan

The REST API should expose ETL as a first-class `/api` area, following the existing endpoint organization.

### Proposed endpoint group

- `GET /api/etl/pipelines`
- `GET /api/etl/pipelines/{name}`
- `POST /api/etl/pipelines/import`
- `PUT /api/etl/pipelines/{name}`
- `DELETE /api/etl/pipelines/{name}`
- `POST /api/etl/pipelines/{name}/validate`
- `POST /api/etl/pipelines/{name}/run`
- `POST /api/etl/runs/{runId}/resume`
- `GET /api/etl/runs`
- `GET /api/etl/runs/{runId}`
- `GET /api/etl/runs/{runId}/rejects`

### API design notes

- follow current minimal API endpoint style
- return concise DTOs for list views and richer DTOs for detail views
- expose validation and dry-run results separately from execution results
- ensure errors use the existing structured error response style

---

## CLI Plan

The CLI should gain a dedicated ETL command group instead of overloading REPL meta-commands.

### Proposed command set

- `csharpdb etl validate <package-or-name>`
- `csharpdb etl run <package-or-name>`
- `csharpdb etl dry-run <package-or-name>`
- `csharpdb etl resume <runId>`
- `csharpdb etl list`
- `csharpdb etl status <runId>`
- `csharpdb etl rejects <runId>`
- `csharpdb etl import <file>`
- `csharpdb etl export <name> <file>`

### CLI behavior expectations

- deterministic exit codes for automation
- JSON output option for CI/devops use
- concise human-readable summaries by default
- clear run identifiers for follow-up commands

### Implementation fit

This should mirror the current command-mode pattern used for inspector and maintenance commands in `src/CSharpDB.Cli`.

---

## Admin UI Plan

The admin interface needs to support both authoring and operations.

### UI goals

- create and edit pipeline definitions
- validate packages before execution
- execute and resume runs
- view run history, status, and counters
- inspect rejects and checkpoints
- import/export package JSON

### Recommended release UI slices

#### 1. Pipeline Explorer integration

Add a new object category in the admin explorer for:

- pipelines
- pipeline runs

This should follow the existing `NavMenu.razor` object-group pattern.

#### 2. Pipeline Editor tab

Add a dedicated tab type in Admin similar to the existing procedure and query designer experiences.

This tab should support:

- metadata editing
- source/transform/destination editing
- package JSON preview
- validate/run actions
- import/export actions

#### 3. Pipeline Run Monitor tab

Add a tab for:

- active run status
- historical runs
- batch counters
- checkpoint information
- reject summaries

### Mockup adoption plan

The file `pipeline-builder-v2_2.html` should be used as the starting visual reference for the pipeline editor.

Recommended adaptation approach:

- preserve its overall layout:
  - toolbox/sidebar
  - canvas
  - configuration modal/panel
  - execution/log area
- translate its concepts into Blazor components and shared admin styling
- narrow available nodes to the release-supported runtime model

### Important UI scope decision

The mockup currently represents a richer workflow canvas than the runtime MVP.

For this release, the admin UI should expose only supported concepts:

- sources
- transform groups / ordered transform chains
- destinations
- success/failure status

Do not expose unsupported branching/condition/script semantics until the runtime supports them.

### Suggested Admin implementation shape

- new `TabKind` entries for pipeline editor and pipeline runs
- new explorer group in `NavMenu.razor`
- new Razor components for:
  - `PipelineTab.razor`
  - `PipelineRunTab.razor`
  - supporting editor/view models
- optional JS interop only where canvas drag/drop is genuinely needed

---

## Delivery Phases

### Phase 0: Architecture and contract freeze

- finalize package JSON schema
- define runtime interfaces
- define system-table model
- define client/API DTOs
- lock supported source/transform/destination matrix

Exit criteria:

- package contract reviewed
- interfaces reviewed
- scope trimmed to release-safe features

### Phase 1: Core runtime foundation

- create `CSharpDB.Pipelines`
- implement package loading and validation
- implement execution modes:
  - `validate`
  - `dry-run`
- define run result and diagnostics contracts

Exit criteria:

- packages validate consistently
- dry-run returns transformed row metrics without writes

### Phase 2: Initial execution path

- implement CSV and CSharpDB source/destination connectors
- implement baseline orchestrator
- implement transactional batch write path
- implement persisted run metadata

Exit criteria:

- end-to-end run into CSharpDB works
- run history is queryable

### Phase 3: Transform and error handling

- implement projection/rename/cast/filter/derive/dedupe
- implement reject capture
- implement fail-fast and skip-bad-rows policies

Exit criteria:

- transform chain works end-to-end
- rejects and counters are persisted

### Phase 4: Resume and operationalization

- implement ETL checkpoint model
- implement resume flow
- implement import/export and version handling

Exit criteria:

- interrupted runs can resume from durable ETL checkpoints

### Phase 5: Interface rollout

- add shared client ETL methods/models
- add REST API ETL endpoints
- add CLI ETL command group

Exit criteria:

- the same pipeline can be managed through client, API, and CLI

### Phase 6: Admin UI rollout

- add pipeline explorer entries
- add pipeline editor tab
- adapt the provided HTML mockup into a Blazor-based pipeline builder
- add run history/monitoring screens

Exit criteria:

- users can author, validate, run, and inspect pipelines from Admin

---

## Testing Plan

ETL should not ship without explicit test coverage across the runtime and interfaces.

### Required test layers

- unit tests for package validation
- unit tests for transforms
- integration tests for connectors
- resume/checkpoint reliability tests
- API tests for ETL endpoints
- CLI tests for ETL commands
- admin component tests where practical, plus manual smoke validation for the visual builder

### Critical scenarios

- schema mismatch detection
- type coercion failures
- reject-row persistence
- partial run interruption and resume
- duplicate import/update behavior
- large file streaming without unbounded memory growth

---

## Risks and Decisions To Review Before Coding

These should be reviewed before implementation begins:

1. package storage model
   Decide whether database-backed catalog storage is mandatory in v1 or whether file-first is acceptable with import/export added in the same release.
2. transform execution granularity
   Decide whether transforms operate row-by-row only or can operate on batches where needed.
3. incremental loads
   Decide whether watermark-driven incremental sync is true MVP or post-MVP.
4. visual canvas scope
   Decide whether the first admin UI must ship full drag-and-drop editing or a structured form-based editor plus JSON preview, with the canvas landing immediately after.
5. runtime placement
   Confirm ETL lives in a dedicated pipelines project and not inside engine/API/admin assemblies.

---

## Recommended Release Cut

If we want a strong but realistic first release, the recommended cut is:

- separate `CSharpDB.Pipelines` runtime project
- JSON package format
- CSV, SQL query, and CSharpDB sources
- CSharpDB, CSV, and JSON destinations
- core transform chain
- validate, dry-run, run, resume
- persisted ETL catalog/run tables
- client/API/CLI support
- admin UI with:
  - pipeline explorer
  - package editor
  - run monitor
  - mockup-inspired visual builder limited to supported runtime concepts

This keeps the release coherent while still delivering the admin experience requested for roadmap completion.

---

## Immediate Next Step

Before writing code, review and approve:

- the separate-project approach
- the package storage strategy
- the exact runtime MVP matrix
- the admin UI scope split between structured editor and visual builder

Once those are confirmed, implementation can start from Phase 0 and Phase 1 without reworking the public plan.
