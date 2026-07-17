# Security And Access Control Plan

Internal implementation plan for making CSharpDB suitable for trusted company
environments while preserving its low-friction embedded usage. The plan covers
the preferred combined daemon, the standalone REST host, remote clients, Admin,
and the release pipeline.

This file is intentionally kept under the top-level `docs/` folder and is not
part of the public `www` documentation site. Public documentation should be
updated as each milestone ships.

## Target Outcome

CSharpDB should have two clearly separated trust models:

- Embedded/direct callers continue to rely on the host process, operating
  system identity, and database-file permissions by default.
- Remote REST and gRPC callers use encrypted transport, an attributable
  identity, server-enforced permissions, and security audit records.

The enterprise-ready remote-host claim is not complete until CSharpDB can:

- Reject insecure remote configuration by default.
- Authenticate services and people through supported credential types.
- Authorize every REST endpoint and gRPC method with the same permission model.
- Prevent raw SQL from bypassing object and operation permissions.
- Restrict Admin features with server-authoritative scopes.
- Rotate credentials without avoidable downtime.
- Produce useful, redacted, exportable audit events.
- Publish verifiable release artifacts with an SBOM and provenance.
- Give operators tested hardening, secret-management, and incident-response
  guidance.

## Current Baseline

Implemented behavior to preserve and extend:

- `Direct` clients run in the caller's process and do not cross a CSharpDB
  authentication boundary.
- `CSharpDB.Api` and `CSharpDB.Daemon` support opt-in shared API-key
  authentication. The current validator compares key hashes in constant time.
- The API key is supported by both HTTP and gRPC clients through
  `CSharpDbClientOptions`.
- Remote security currently defaults to `Mode = None` for compatibility.
- HTTPS endpoints are understood by the client and ASP.NET Core/Kestrel can be
  configured by a host, but CSharpDB does not yet provide a complete, tested
  certificate lifecycle or secure remote-deployment profile.
- REST and gRPC share the same daemon-hosted `ICSharpDbClient`, but the shared
  key does not establish a named principal or carry roles and scopes.
- One API key currently grants access to the complete remote surface, including
  SQL, schema, inspection, maintenance, restore, and shard-admin operations.
- `SECURITY.md` already directs reporters to GitHub Private Vulnerability
  Reporting and states initial response targets.
- The release workflow uses NuGet Trusted Publishing and publishes SHA-256
  checksums for daemon archives. The Admin Store packaging helper can sign
  MSIX packages.

Known gaps:

- No first-class mTLS, JWT bearer, or OIDC configuration.
- No API-key identifier, expiry, overlap, revocation, or rotation workflow.
- No unified principal, permission catalog, RBAC, or resource-scoped grants.
- No SQL-aware authorization or transaction ownership enforcement.
- No Admin permission scopes or security-management workspace.
- Admin and Forms hosts currently use singleton backend clients, so a shared
  service credential cannot provide honest per-user authorization or audit
  attribution.
- Shard definitions can contain raw remote API keys and connection strings that
  are serialized into the master catalog. Public snapshots hide the values, but
  storage still needs a secret-reference design.
- No security audit event contract or supported audit sink.
- No SBOM or consistent signature/provenance bundle for every release artifact.
- No complete production hardening or secret-management guide.

`SECURITY.md` correctly states that database and WAL files are plaintext today.
That limitation must remain explicit throughout this work.

## Primary Implementation Seams

The plan should extend existing boundaries instead of creating
transport-specific security stacks:

- Keep ASP.NET Core authentication handlers, host option binding, and transport
  adapters in `src/CSharpDB.Api/Security` so the standalone REST host and daemon
  REST surface use the same implementation.
- Put transport-neutral principal, permission, policy, security-context, and
  audit contracts in a lower-level or new focused assembly that the API, daemon,
  Client, Admin, SQL, Execution, and Engine layers can consume without a reverse
  dependency on the web host.
- Attach operation and resource metadata through
  `CSharpDbRestApiHostExtensions` and the endpoint modules under
  `src/CSharpDB.Api/Endpoints`.
- Add matching authentication, authorization, and audit interception around
  `CSharpDbRpcService`; keep the protobuf method inventory synchronized with the
  REST operation catalog.
- Extend `CSharpDbClientOptions`, `HttpTransportClient`, and
  `GrpcTransportClient` with credential providers and client-certificate
  support rather than adding transport-only public APIs.
- Carry the same credential-provider support through `CSharpDB.Data` and
  `CSharpDbConnection` so authenticated remote ADO.NET remains a supported
  client surface without placing reusable secrets in connection strings.
- Follow the existing route-context propagation pattern for a request security
  context, while keeping ASP.NET Core's authenticated `ClaimsPrincipal` as the
  transport authority.
- Enforce SQL permissions across `CSharpDB.Sql`, engine execution, prepared and
  optimized paths, and stored executable objects. An authorization hook only in
  the normal query planner is not sufficient.
- Rework the Admin and Forms host client lifetime before claiming per-user
  permissions. Use per-user/per-circuit delegated clients for remote multi-user
  mode, or clearly identify and audit a single service principal for trusted
  local mode.
- Apply installer hardening under `deploy/daemon`, and release trust changes in
  `.github/workflows/release.yml` plus the existing packaging scripts.

## Trust Boundaries And Deployment Profiles

Security behavior must be based on an explicit deployment profile rather than
inferred only from which transport enum a client selected.

| Profile | Intended boundary | Required baseline |
| --- | --- | --- |
| Embedded | Same trusted process opens the database. | Host and file permissions; CSharpDB authentication remains optional. |
| Local daemon | A daemon is bound only to loopback for local tools. | Explicit loopback binding; authentication may be optional but an insecure-mode warning is visible. |
| Remote daemon | Clients connect across a machine or network boundary. | TLS plus at least one authentication scheme; authorization and audit enabled. |
| Proxy terminated | A trusted ingress terminates TLS before the daemon. | Explicit trusted-proxy configuration, protected backend network, authenticated callers, authorization, and audit. |

The standalone `CSharpDB.Api` host must use the same remote security components
and rules as the daemon's REST surface. It must not become a less secure route
to the same database operations.

### Non-Negotiable Rules

- Preserve default embedded/direct behavior. An application can opt into engine
  authorization, but a remote feature must not silently add authentication
  overhead to all embedded users.
- Treat authentication, authorization, and audit as separate stages. A valid
  credential does not imply full database access.
- Resolve identity and permissions on the server. Never trust roles, scopes, or
  resource names declared only by a client.
- Enforce one operation and permission catalog across REST and gRPC. Transport
  parity is a release gate.
- Use deny-by-default authorization for authenticated remote identities.
- Do not allow an authentication failure to fall back to another weaker or
  anonymous mode unless that composition is explicitly configured and tested.
- Never ship a certificate-validation bypass, accept unsigned JWTs, or log raw
  API keys, bearer tokens, private keys, certificate passwords, or secret
  configuration values.
- Authorize an entire SQL batch before its first statement executes so a later
  denied statement cannot leave partial effects.
- Bind transaction sessions to the authenticated principal and route context
  that created them.
- Keep the server authoritative when Admin hides or disables a feature.
- Preserve the original actor and correlation id through stored procedures,
  triggers, pipelines, sharding operations, and internal daemon calls.
- Validate security configuration at startup and fail closed for an invalid
  remote profile.
- Keep security metadata and audit data out of normal user-query surfaces unless
  an explicit permission grants access.

## Delivery Sequence

Milestones are ordered by dependency, not by calendar estimate.

| Milestone | Primary outcome | Depends on |
| --- | --- | --- |
| 1 | Security model, shared abstractions, governance, and rollout contract. | Current baseline. |
| 2 | First-class TLS and mTLS transport support. | Milestone 1. |
| 3 | JWT/OIDC and safe API-key lifecycle. | Milestones 1 and 2 for mTLS identity. |
| 4 | Unified RBAC and resource scopes. | Milestones 1 and 3. |
| 5 | SQL-level permission enforcement. | Milestone 4. |
| 6 | Permission-aware Admin experiences. | Milestones 3 through 5. |
| 7 | Complete security audit pipeline. | Milestones 1, 3, 4, and 5. |
| 8 | Release and software supply-chain security. | Can run after Milestone 1 in parallel. |
| 9 | Operational hardening and secret-management documentation. | Milestones 2 through 8. |
| 10 | Enterprise readiness verification and secure-default cutover. | All prior milestones. |

## Milestones

### 1. Security Model, Shared Foundations, And Governance

Goal: define the security contract before adding independent authentication and
authorization features that could diverge between transports.

Architecture work:

- Write a threat model covering database, WAL, backup, configuration, logs,
  daemon memory, Admin credentials, release artifacts, and security metadata.
- Model actors including an embedded host, local operator, remote user, service
  account, security administrator, unauthenticated network caller, malicious
  database user, and compromised client.
- Record trust boundaries for direct calls, REST, native gRPC, gRPC-Web,
  reverse proxies, shard routing, Admin, and release automation.
- Introduce a transport-neutral authenticated-principal model containing a
  stable subject id, display name, authentication scheme, trusted claims, and
  credential id. Do not put raw credentials on the principal.
- Introduce one server-side operation descriptor with operation id, required
  permission, resource extractor, audit category, and sensitivity level.
- Inventory every REST endpoint and gRPC method and assign it an operation
  descriptor. Add a test that fails when a new remote operation has no mapping.
- Define a request security context that flows from authentication through
  authorization, database execution, and audit without using client-controlled
  ambient state.
- Define the minimal audit event contract and a test sink in this milestone so
  authentication and authorization work is observable as it lands. Milestone 7
  completes durable sinks, retention, and operational coverage.
- Reserve a bootstrap `security.manage` gate and administrator identity for
  credential lifecycle operations. Milestone 4 replaces this bootstrap check
  with the complete role and resource-grant evaluator.
- Decide through short architecture records:
  - where principals, role bindings, and grants are stored;
  - how the first security administrator is bootstrapped;
  - how a locked-out deployment is recovered locally;
  - where the remote service boundary and SQL engine boundary each enforce
    authorization;
  - whether Admin runs as a delegated per-user client or an explicitly named
    service principal in each deployment profile;
  - how sharded daemons preserve identity and route scope;
  - which release-signing and provenance formats are supported.
- Add startup validation that reports the selected profile, bindings,
  authentication schemes, authorization mode, and audit status with all secret
  values redacted.

Governance work:

- Expand `SECURITY.md` rather than creating a second vulnerability intake
  policy.
- Define supported released versions and the backport policy, not only support
  for `main`.
- Document private triage ownership, severity classification, acknowledgement,
  status-update, remediation, coordinated-disclosure, and advisory publication
  steps.
- Define target remediation windows by severity and the conditions that may
  require an out-of-band security release.
- Add a private maintainer runbook for reproducing reports, preserving
  evidence, requesting a CVE when appropriate, crediting reporters, and
  notifying downstream users.
- Link issue templates and public documentation only to the canonical
  `SECURITY.md` process.

Acceptance criteria:

- The threat model covers both embedded and remote profiles and names the
  assumptions that CSharpDB cannot enforce.
- REST and gRPC operation inventories have no unmapped callable methods.
- A request has one stable principal and correlation id across its complete
  execution path.
- Invalid or contradictory security configuration stops a remote host before it
  opens a network listener.
- Startup diagnostics contain no secret values.
- The vulnerability process is actionable for a released product and is still
  reachable through GitHub Private Vulnerability Reporting.

### 2. TLS And Mutual TLS

Goal: provide a supported encrypted transport for daemon REST, native gRPC,
gRPC-Web, and standalone REST deployments.

Server transport work:

- Add a documented CSharpDB transport-security profile layered on standard
  ASP.NET Core/Kestrel configuration instead of inventing a separate web server.
- Support server certificates loaded from a protected file, operating-system
  certificate store, or configuration-provider supplied secret reference.
- Support certificate-chain delivery, private-key password resolution, expiry
  diagnostics, and reload/rotation without changing application credentials.
- Require TLS 1.2 or newer by default for remote profiles and use operating
  system cipher policy unless an explicitly supported override is needed.
- Ensure REST, native HTTP/2 gRPC, and gRPC-Web can share a TLS listener.
- Refuse a non-loopback plaintext listener in the remote profile.
- Provide an explicit proxy-terminated profile. Trust forwarded headers only
  from configured proxy addresses and document how the proxy-to-daemon segment
  is protected.
- Make CORS an explicit allowlist for remote browser deployments. Do not retain
  `AllowAnyOrigin`, `AllowAnyMethod`, and `AllowAnyHeader` as a production
  default.

mTLS work:

- Support optional and required client-certificate modes, with required mode as
  the normal mTLS service-to-service configuration.
- Validate certificate chain, validity period, intended usage, configured trust
  anchors, and revocation according to an explicit revocation policy.
- Map a validated certificate to a principal using configured SAN URI, SAN DNS,
  subject, issuer, or thumbprint rules. Prefer stable SAN-based identifiers.
- Reject ambiguous mappings and certificates that validate cryptographically but
  have no authorized identity mapping.
- Audit certificate subject identifier, issuer identifier, serial number or
  thumbprint, and validation outcome without logging private material.

Client work:

- Add supported client-certificate selection for HTTP and gRPC clients.
- Allow a custom trust bundle for private company CAs without requiring a
  global certificate-validation callback.
- Resolve certificate passwords and keys through secret providers; do not embed
  them in endpoint URLs or diagnostic strings.
- Surface certificate expiry and trust failures as specific client errors.
- Preserve normal hostname validation and add negative hostname-mismatch tests.

Acceptance criteria:

- The same valid certificate serves REST and gRPC on the daemon listener.
- Plain HTTP to a remote-profile listener is unavailable or redirects only when
  that redirect is explicitly safe for the deployment.
- Expired, not-yet-valid, untrusted, wrong-EKU, revoked, and unmapped client
  certificates are rejected.
- Certificate rotation succeeds without exposing the key or accepting an
  unintended plaintext window.
- Native gRPC, gRPC-Web, and REST integration tests cover public and private CA
  chains.
- Loopback development remains simple and clearly labeled as a local-only
  exception.

### 3. Authentication And Credential Lifecycle

Goal: support enterprise identities and safely managed service credentials
without making one shared secret the permanent security model.

API-key work:

- Replace the single anonymous secret with records that have a non-secret key
  id, principal id, display name, verifier, created time, optional expiry,
  status, and last-used metadata.
- Generate high-entropy keys and show the secret only at creation time. Persist
  only an appropriate one-way verifier and non-secret metadata.
- Support multiple active keys so operators can create a replacement, deploy it
  to clients, observe use, and revoke the old key without downtime.
- Add explicit create, list-metadata, rotate, disable, and revoke operations
  guarded by `security.manage`.
- Make revocation effective across REST and gRPC without requiring clients to
  reconnect indefinitely.
- Preserve the existing single-key configuration during a documented migration
  period, but mark it as a legacy unscoped credential.
- Replace raw API keys and connection strings embedded in shard catalog
  definitions with secret references or provider-resolved credentials. Apply
  the same rule to future certificate and bearer credentials, with migration and
  redaction tests for existing catalogs.
- Rate-limit repeated authentication failures per source and credential id while
  avoiding a response that reveals whether a key id exists.

JWT and OIDC work:

- Add JWT bearer authentication with strict signature, issuer, audience,
  lifetime, not-before, clock-skew, and algorithm validation.
- Add OIDC discovery and JWKS retrieval with bounded caching, automatic signing
  key refresh, timeout behavior, and last-known-good rules.
- Support an explicit issuer and audience allowlist. Never accept a token merely
  because its signature is valid.
- Map subject, tenant if configured, name, groups, and role claims through an
  allowlisted claim-mapping policy.
- Define how multiple issuers are isolated so identical `sub` claims cannot
  collide.
- Return correct HTTP `401` challenges and gRPC `Unauthenticated` status without
  leaking validation detail to the caller.

Unified identity work:

- Normalize API-key, JWT/OIDC, and mTLS identities into the principal model from
  Milestone 1.
- Allow more than one authentication scheme on a listener only through an
  explicit composition policy. Do not silently downgrade from mTLS or JWT to
  anonymous access.
- Add token-provider and refresh callbacks to remote clients and Admin so
  short-lived tokens do not require rebuilding the entire client graph.
- Add equivalent authenticated connection and credential-provider support to
  `CSharpDB.Data` for remote HTTP/gRPC ADO.NET use, without embedding reusable
  secrets in persisted connection strings.
- Redact authorization headers, API-key headers, JWT claims not explicitly
  approved for diagnostics, and certificate secrets from logs and exceptions.

Acceptance criteria:

- API-key rotation has a tested overlap window and old-key revocation test for
  both transports.
- Expired, disabled, revoked, malformed, or incorrectly scoped credentials fail
  closed.
- OIDC signing-key rotation succeeds without accepting an untrusted key.
- Discovery or JWKS outage behavior matches the documented cache policy.
- API key ids, `(issuer, subject)` pairs, and certificate identities produce
  stable, non-colliding principal ids.
- Authentication tests assert that responses and logs do not disclose secrets
  or unnecessary token-validation detail.
- `ICSharpDbClient` and `CSharpDbConnection` pass the same remote credential,
  refresh, revocation, TLS, and error-semantics tests.

### 4. Role-Based Access Control And Resource Scopes

Goal: enforce least privilege consistently before a request reaches a database
operation.

Permission model:

- Define stable permission ids and resource types. Permission checks must use
  ids rather than UI labels or transport route strings.
- Start with additive grants and role composition. Defer explicit deny rules
  until their inheritance and precedence semantics are fully specified.
- Support database-wide grants plus narrower table, view, collection,
  procedure, pipeline, keyspace, and shard-admin scopes where the operation can
  identify a resource safely.
- Prevent wildcard and quoted-identifier ambiguities by storing normalized
  resource identities separately from their display names.
- Version role and permission metadata so migrations can be validated and
  rolled forward safely.

Initial permission families should cover at least:

| Area | Representative permission ids | Protected examples |
| --- | --- | --- |
| Metadata and data | `database.read`, `table.read`, `table.write`, `collection.read`, `collection.write` | Info, browse, row, and document operations. |
| SQL and transactions | `sql.execute.read`, `sql.execute.write`, `transaction.manage` | SQL batches and transaction sessions. |
| Schema | `schema.read`, `schema.manage` | Tables, columns, indexes, views, triggers, and saved definitions. |
| Executable objects | `procedure.execute`, `pipeline.execute`, `pipeline.manage` | Procedures, pipeline runs, and pipeline definitions. |
| Operations | `maintenance.run`, `backup.create`, `backup.restore`, `diagnostics.read` | Checkpoint, backup, restore, vacuum, reindex, and storage inspection. |
| Sharding | `sharding.read`, `sharding.manage`, `migration.manage`, `failover.manage` | Maps, directories, migrations, and future failover. |
| Security | `security.read`, `security.manage`, `audit.read` | Principals, roles, grants, credentials, and audit records. |

Built-in roles should be small, documented compositions of permissions:

| Role | Intended use |
| --- | --- |
| Reader | Read database metadata and permitted data resources. |
| Writer | Reader permissions plus writes to granted data resources. |
| SchemaManager | Create and change schema objects without receiving restore or security administration. |
| Operator | Maintenance, backup, diagnostics, and explicitly granted shard operations. |
| AuditReader | Read audit events but not change security configuration. |
| SecurityAdmin | Manage identities, credentials, roles, and grants. |
| DatabaseAdmin | Full database administration for a named database, excluding host-level release and operating-system controls. |

Enforcement work:

- Add one authorization service shared by REST and gRPC adapters.
- Resolve the operation descriptor and resource before the handler invokes
  `ICSharpDbClient`.
- Map authentication claims to role bindings through server-side configuration
  or the protected security catalog.
- Protect backup/restore, inspection paths, shard catalog changes, migrations,
  and security administration with distinct high-risk permissions.
- Add host-configured filesystem capability roots for every remote path-taking
  feature. Canonicalize paths, reject traversal and symlink/reparse-point
  escapes, enforce separate read/create/overwrite permissions, and never treat
  a database role as permission to access arbitrary files owned by the service
  account.
- Apply filesystem containment consistently to backup/restore, storage
  inspection, pipeline CSV/JSON sources and sinks, SQL external tables,
  import/export, and sharding data sources.
- Bind transaction ids, migration jobs, and other resumable handles to their
  creating principal unless an explicit takeover permission applies.
- Decide and consistently apply metadata existence-hiding rules so a denied
  caller cannot enumerate protected tables or collections through differing
  errors.
- Return HTTP `403` and gRPC `PermissionDenied` for authenticated callers that
  lack permission, with a stable machine-readable denial code.
- Add a protected, local-only break-glass recovery procedure that cannot be
  invoked remotely by default and always creates an audit record.

Acceptance criteria:

- Every REST endpoint and gRPC method has the same operation id, permission,
  resource semantics, and allow/deny result.
- A new endpoint or RPC without an authorization mapping fails CI.
- Built-in roles cannot perform operations outside their documented grants.
- Resource-scoped grants do not leak access to a similarly named resource,
  another route, or another shard.
- A permitted database operation cannot escape its configured filesystem roots
  through absolute paths, traversal, symlinks, reparse points, or overwrite
  races.
- Authentication success plus authorization denial returns `403` or
  `PermissionDenied`, not an authentication error.
- Security catalog changes require `security.manage` and cannot remove the last
  usable administrator without an explicit protected recovery path.

### 5. SQL-Level Permissions

Goal: prevent `ExecuteSql`, stored executable objects, and transactions from
bypassing the resource checks applied to typed API methods.

SQL authorization work:

- Extend the SQL parser and bound representation to produce an authorization
  requirement set from the parsed syntax tree. Do not authorize by regular
  expression or raw string matching.
- Resolve every referenced table, view, collection bridge, procedure, trigger,
  and schema object to its canonical catalog identity before execution.
- Require read permissions for `SELECT` and metadata access, write permissions
  for `INSERT`, `UPDATE`, and `DELETE`, schema permissions for DDL, and explicit
  execute permissions for stored procedures and pipelines.
- Parse and authorize every statement in a batch before executing the first
  statement. A denied later statement must produce no partial write.
- Re-check permissions at execution time. Do not make authorization depend on a
  plan cache that can outlive a grant change or principal session.
- Cover engine shortcuts and non-planner paths, including simple insert and
  primary-key lookup fast paths, prepared execution, typed CRUD, collections,
  procedures, triggers, views, and protected system catalogs.
- Define first-release stored procedure, view, and trigger behavior as invoker
  rights. Any future definer-rights feature requires a separate threat model,
  explicit ownership rules, and audit coverage.
- Preserve the initiating actor through nested procedure, trigger, and pipeline
  execution and prevent nested execution from gaining the object's creator
  permissions.
- Bind transaction sessions to principal, route context, and database. Reject
  use, commit, or rollback by a different principal.
- Filter metadata enumeration by permission and use consistent existence-hiding
  semantics.
- Add security administration APIs for grants and role membership. SQL
  `GRANT`/`REVOKE` syntax may be added only when it maps exactly to the same
  protected service, validation, and audit path.
- Make engine-level authorization opt-in for embedded use and mandatory for
  remote profiles. Document that direct file access by a different process
  bypasses daemon identities and must be prevented with OS permissions.

High-risk test cases:

- Multi-statement batches containing an allowed read followed by denied DDL or
  DML.
- Comments, unusual whitespace, quoted identifiers, aliases, subqueries, CTEs,
  views, triggers, and nested procedure calls.
- Permission changes while a prepared or cached plan exists.
- A transaction id replayed by another principal or through a different route.
- Sharded SQL that supplies route context but lacks permission for the resolved
  keyspace or resource.
- External tables and other SQL file access outside the host capability roots,
  including traversal and link-based escapes.
- Error paths that could reveal the existence or schema of a denied object.

Acceptance criteria:

- Raw SQL cannot perform an operation that the same principal is denied through
  a typed API.
- Authorization of a denied batch is atomic and produces no database change.
- Grant revocation affects subsequent execution without a daemon restart.
- Stored procedures, triggers, and pipelines cannot be used for privilege
  escalation under invoker-rights rules.
- Remote transaction sessions cannot be stolen or moved between identities or
  route contexts.
- Embedded clients behave as before unless their host explicitly enables engine
  authorization.

### 6. Admin UI Permission Scopes

Goal: make Admin safe and understandable for non-administrator users while
keeping all security decisions authoritative on the server.

Admin connection work:

- Add host-level authentication and authorization to the Admin and Forms web
  surfaces through ASP.NET Core authentication and required authorization on
  Razor routes, SignalR/Blazor circuits, and auxiliary download/control
  endpoints. Revalidate long-lived circuit identities after expiry or revocation.
  Keep the Desktop shell's loopback boundary distinct from multi-user web login.
- Define secure browser session, antiforgery, cookie, sign-out, and OIDC callback
  behavior when Admin itself is hosted for multiple users.
- Add API-key, bearer-token/OIDC, client-certificate, and private-CA support to
  remote connection profiles.
- Store reusable secrets in an operating-system credential store or configured
  secret provider. Persist only non-secret connection metadata in normal Admin
  settings.
- Show the authenticated identity, authentication method, token or certificate
  expiry, active database, and effective roles without displaying credentials.
- Refresh short-lived tokens safely and distinguish expired authentication from
  denied authorization.
- Clear secrets and cached authorization state on sign-out, connection change,
  or principal change.
- Replace the singleton remote backend identity with per-user/per-circuit clients
  or an on-behalf-of token flow when Admin is used by multiple people. If a
  trusted local profile intentionally uses one service identity, label it and
  audit it as such rather than presenting service actions as individual users.
- Keep inbound browser identity separate from outbound daemon delegation or
  service identity, and document which identity the daemon will authorize and
  audit.
- Require authorization on the import/export download route and tie its one-time
  token, plus similar bearer handles, to the authenticated principal,
  connection, expiry, and intended operation.

Permission-aware UI work:

- Request a protected effective-permissions summary after authentication.
- Hide unavailable navigation where that improves clarity and disable visible
  actions when showing them helps explain a missing permission.
- Re-check on every server operation; UI state must never be treated as an
  authorization decision.
- Apply scopes across query, table data, collections, schema design, forms,
  reports, pipelines, import/export, maintenance, diagnostics, and sharding
  workspaces.
- Treat in-process code modules and host callbacks as host-level,
  remote-code-execution-equivalent capabilities. Disable remote authoring and
  execution in enterprise profiles by default; any enablement must be an
  explicit local or host deployment control, preferably for signed modules, and
  remain outside ordinary `DatabaseAdmin` grants. Explain that database roles
  cannot sandbox code already executing as the daemon identity.
- Preserve per-tab route isolation and include the route/keyspace in effective
  permission checks for sharded connections.
- Handle HTTP `401`/`403` and gRPC `Unauthenticated`/`PermissionDenied`
  distinctly with useful, non-sensitive remediation text.
- Add a Security workspace for principals, role bindings, grants, API-key
  metadata and rotation, and authentication diagnostics. Require
  `security.manage` for mutations.
- Reveal a newly generated API key only once and require explicit confirmation
  before revocation or a grant that materially expands privilege.
- Replace caller-supplied operator attribution in sharding, migration, and
  pipeline histories with the authenticated actor; retain free-form operator
  comments as separate untrusted text.

Acceptance criteria:

- Anonymous browser users cannot establish a protected Admin/Forms circuit,
  reach protected Razor routes, or redeem an import/export download token.
- Reader, Writer, SchemaManager, Operator, AuditReader, SecurityAdmin, and
  DatabaseAdmin sessions expose only their intended workflows.
- Manually invoking a hidden or disabled action is still denied by the daemon.
- Permission or role revocation becomes visible without requiring Admin to
  retain a stale privileged session indefinitely.
- Admin does not write raw tokens, API keys, certificate passwords, or private
  keys to settings, logs, crash reports, URLs, or tab state.
- Direct local Admin behavior remains unchanged unless the opened database has
  engine authorization enabled.

### 7. Audit Logging And Security Operations

Goal: produce attributable and exportable security records without exposing
credentials or unnecessarily copying sensitive data into logs.

Audit event contract:

- Define a versioned structured event with UTC timestamp, event id, category,
  actor principal id, authentication scheme, credential id, roles or policy
  version, operation id, resource identity, route context, transport, remote
  endpoint when trustworthy, correlation id, outcome, denial reason code,
  duration, and server instance id.
- Separate the stable machine-readable event from localized Admin display text.
- Carry the original actor through nested operations and record an explicit
  system actor only for genuine background work.
- Record a policy/version identifier so an authorization decision can be
  reconstructed after grants change.

Required events:

- Authentication success at a configurable sampling level, all authentication
  failures, credential expiry, revocation, and rate limiting.
- Authorization denials and high-risk authorization successes.
- Principal, role, grant, authentication-mode, API-key, certificate-mapping, and
  audit-configuration changes.
- Schema changes, restore, backup, reindex, vacuum, storage inspection,
  migrations, shard catalog changes, failover actions, and break-glass use.
- Daemon startup and shutdown, insecure-profile warnings, configuration
  validation failures, and audit sink health changes.
- SQL execution metadata appropriate to policy: operation class, affected
  resources, result, and a fingerprint or hash. Full SQL text and parameters
  must be off by default.

Sink and retention work:

- Provide an `ICSharpDbAuditSink`-style extension point with a supported
  structured rolling-file sink and integration with standard .NET logging or
  OpenTelemetry export.
- Document shipping audit records to an external append-only or immutable store
  when tamper resistance is required.
- Support ordered event sequence ids and an optional hash-chained or signed-batch
  format for detecting local-file modification; document that this does not
  protect records after the daemon host is fully compromised.
- Configure rotation, retention, backpressure, maximum event size, health
  reporting, and behavior when a sink is unavailable.
- Support fail-closed audit behavior for security-administration and break-glass
  changes. Make the availability tradeoff explicit for normal data operations.
- Protect local audit files with a dedicated service identity and permissions
  that do not grant normal database users write access.
- Redact secrets centrally and add automated canary-secret tests across event,
  application log, and exception output.
- Add the Admin Audit workspace only after the protected query/export surface is
  available, and guard it independently with `audit.read`.

Acceptance criteria:

- A security reviewer can correlate an authentication attempt, authorization
  decision, SQL or typed operation, and resulting high-risk change.
- Audit output contains no raw credential, private key, certificate password,
  bearer token, connection-string secret, or SQL parameter value by default.
- Rotation and sink outage tests do not silently discard high-risk events.
- Audit records from REST and gRPC use the same operation and resource ids.
- Only a principal with `audit.read` can query records through CSharpDB or Admin.
- Normal embedded use has no mandatory audit sink unless its host enables one.

### 8. Release And Software Supply-Chain Security

Goal: let downstream users verify what was built, where it came from, and which
components it contains.

CI security work:

- Add supported static analysis and dependency review for pull requests,
  including CodeQL or an equivalent C# analysis lane.
- Enable automated dependency-update review and a restore audit policy with a
  documented severity gate and exception process.
- Remove floating package versions, decide where lock files are appropriate,
  and use locked restore for release inputs. Resolve existing high-severity
  advisory findings before making the gate mandatory.
- Add secret scanning and prevent newly committed production credentials,
  signing material, or private keys from entering release artifacts.
- Pin or otherwise govern third-party workflow actions and minimize job
  permissions.
- Govern runner images, SDK patch roll-forward, compilers, and externally
  installed packaging tools where practical, and record their exact resolved
  versions in provenance.
- Keep release construction in CI with ephemeral credentials. Preserve NuGet
  Trusted Publishing rather than restoring a long-lived NuGet API key.
- Stage and test the exact NuGet, native, daemon, and Admin bytes before any
  irreversible external publication. Do not publish NuGet while other artifact
  jobs can still fail.
- Integrate the Admin Store package workflow as a reusable, production-signed
  release job, or define Admin as a separately versioned release channel with
  the same staging, verification, approval, and publication gates.
- Protect the publication step with a release environment and an explicit
  approval and recovery procedure for partial external publication.
- Restructure the workflow dependency graph as construction jobs that upload
  staged artifacts, followed by one trust/verification barrier, approval, and
  only then the external publishing jobs. Every publisher must depend on that
  barrier.
- Generate one signed release-subject manifest that binds each primary
  distributable's filename, size, SHA-256 digest, version, source commit,
  runtime, and SBOM digest. Publish signature and provenance records alongside
  it without creating recursive self-hash requirements.

SBOM work:

- Generate a standards-based SPDX or CycloneDX SBOM for every project-published
  primary distributable: NuGet package, native artifact, daemon archive, and
  Admin distribution.
- Include managed packages, native components, bundled runtimes, licenses,
  version, source commit, target runtime, and package hashes.
- Attach SBOMs to the GitHub release and include or reference the matching SBOM
  from each archive. An SBOM embedded before archive creation describes archive
  contents; the external signed manifest and provenance bind the final archive
  digest.
- Validate in CI that each primary distributable maps to exactly one matching
  SBOM and that subject/component identities and hashes agree. Do not recursively
  require SBOMs for signatures, attestations, checksum files, SBOMs themselves,
  or GitHub-generated source archives.
- Scan the produced SBOMs as well as the restore graph and apply the same
  vulnerability exception policy to components actually shipped in archives.

Signing and provenance work:

- Define a signing matrix for NuGet packages, daemon archives, native binaries,
  Windows executables/MSIX, and macOS distributions.
- Add a verifiable signature or Sigstore bundle for every primary distributable
  and the release-subject manifest, plus platform signing/notarization where the
  platform requires it.
- Require a production Authenticode identity for released Windows binaries and
  MSIX packages; the packaging helper's locally generated test certificate must
  remain a development-only path.
- Generate build provenance that binds the source repository, commit, workflow,
  runtime, artifact digest, and SBOM digest.
- Keep `SHA256SUMS.txt`, sign or attest the checksum file, and publish a
  cross-platform verification script and manual verification instructions.
- Protect signing identities with short-lived CI federation or a managed
  signing service, document rotation/revocation, and avoid repository-held
  private keys.
- Decide explicitly whether NuGet author signing adds value beyond Trusted
  Publishing and NuGet.org repository signing; record the decision and the
  supported verification path.
- Enforce final-byte ordering: generate component SBOMs before packaging when
  they will be embedded; build/package; platform sign and notarize/staple; test
  the exact final distributable; compute final digests and bind the SBOMs in the
  manifest/provenance; detached-sign or attest the metadata; and publish.

Acceptance criteria:

- Every project-published primary distributable has a matching checksum, SBOM,
  signature or verification bundle, and provenance record.
- Verification fails after any artifact or SBOM byte is modified.
- Release jobs use least-privilege permissions and no reusable publishing secret
  when a federated alternative exists.
- The release workflow fails if signing, SBOM generation, provenance, or
  artifact-to-metadata matching is incomplete.
- No package or asset is externally published until every staged release
  artifact has passed its platform tests and trust-metadata gates.
- A clean machine can verify a release by following the published instructions.

### 9. Security Hardening And Secret Management Guides

Goal: turn security features into a safe, repeatable production deployment
rather than leaving critical choices implicit.

Security hardening guide:

- Publish separate embedded, local-daemon, remote-daemon, and proxy-terminated
  reference configurations.
- Cover network binding, firewalls, trusted proxies, TLS/mTLS, allowed origins,
  request and header size limits, timeouts, authentication rate limits, and
  denial-of-service boundaries.
- Document least-privilege Windows service, systemd, and launchd identities and
  filesystem permissions for database, WAL, backup, configuration, certificate,
  and audit paths.
- Harden packaged service definitions where portable, including private
  temporary directories, restricted writable paths, safe umask, restart
  behavior, and operating-system sandboxing controls.
- Preserve the existing dedicated Linux service user and add systemd sandboxing
  plus `0600` secret-file permissions; use a constrained Windows service account
  with explicit install/data ACLs; and assign the macOS daemon a dedicated
  user/group instead of relying on root.
- Document backup/restore path restrictions, symlink and traversal defenses,
  ownership, encryption expectations, off-host copies, and restore testing.
- Cover CORS, development OpenAPI/Scalar exposure, error detail, log level,
  crash dumps, diagnostics endpoints, and safe support bundles.
- Include certificate, API-key, OIDC, role/grant, patch, backup, audit-retention,
  incident-response, and decommissioning runbooks.
- State clearly that TLS does not encrypt database, WAL, backup, audit, or crash
  dump files at rest.

Secret management guide:

- Classify API keys, certificate private keys and passwords, OIDC configuration
  secrets if any, database connection secrets, signing identities, and recovery
  credentials.
- Prefer configuration providers, protected files, operating-system credential
  stores, workload identities, or external secret managers over checked-in
  `appsettings.json` values.
- Document the exposure tradeoffs of environment variables and process command
  lines and provide a protected-file or provider-based alternative.
- Support secret references or provider callbacks so configuration can identify
  a secret without copying its value into logs or persisted Admin settings.
- Document initial provisioning, access review, rotation, revocation, backup,
  recovery, and deletion for each secret type.
- Provide examples for generic file/Kubernetes secret mounts and common .NET
  configuration providers without making one cloud vendor mandatory.

Documentation rollout:

- Update `src/CSharpDB.Daemon/README.md`, `src/CSharpDB.Api/README.md`,
  `src/CSharpDB.Client/README.md`, Admin documentation, service installer notes,
  and the public roadmap as features ship.
- Keep configuration examples free of working credentials and mark placeholder
  values unambiguously.
- Add an upgrade guide from `Mode = None` and the legacy single API key to the
  selected deployment profile and credential store.
- Add an enterprise readiness checklist that links to tested configuration,
  verification, backup, audit, and incident-response procedures.

Acceptance criteria:

- A new operator can deploy each supported profile without an undocumented
  security decision.
- Automated packaging tests verify secure permissions and settings in Windows,
  Linux, and macOS service assets where the platform permits.
- Documentation examples pass configuration validation and integration smoke
  tests.
- No production example stores a raw secret in source control or a command-line
  argument.
- The guide accurately describes plaintext-at-rest limitations and the external
  controls required to mitigate them.

### 10. Enterprise Readiness Verification And Secure Defaults

Goal: make the company-environment security claim repeatable, measurable, and
safe to carry across releases.

Verification work:

- Build a cross-transport conformance suite that runs each operation's
  authentication, authorization, resource-scope, error, and audit assertions
  against REST and gRPC.
- Add TLS/mTLS tests using generated root, intermediate, server, valid client,
  expired client, wrong-EKU, revoked, and untrusted certificates.
- Add JWT/OIDC tests for issuer, audience, algorithm, expiry, not-before, signing
  key rotation, claim mapping, discovery timeout, and replay-sensitive flows.
- Add API-key tests for entropy, one-time display, overlap rotation, expiry,
  disable, revocation, cache invalidation, and constant-time validation.
- Add exhaustive permission-matrix tests for built-in roles and representative
  resource-scoped grants.
- Add parser and property-based tests for SQL authorization, including nested
  and multi-statement inputs.
- Add audit redaction, retention, sink failure, correlation, and tamper-detection
  tests.
- Add upgrade tests from unsecured and legacy single-key configurations.
- Run a threat-model-based security review and remediate all release-blocking
  findings. Obtain an independent review before making a hardened public-server
  claim.
- Run a vulnerability-response tabletop from private report through triage,
  patch, advisory/CVE decision, signed release, notification, and postmortem.

Existing verification commands to retain and extend:

```powershell
dotnet test tests\CSharpDB.Api.Tests\CSharpDB.Api.Tests.csproj -c Release
dotnet test tests\CSharpDB.Daemon.Tests\CSharpDB.Daemon.Tests.csproj -c Release
dotnet test tests\CSharpDB.Data.Tests\CSharpDB.Data.Tests.csproj -c Release
dotnet test tests\CSharpDB.Admin.Forms.Tests\CSharpDB.Admin.Forms.Tests.csproj -c Release
dotnet test CSharpDB.slnx -c Release
dotnet build CSharpDB.slnx -c Release
```

Create a focused `CSharpDB.Security.Tests` project only if shared security tests
cannot remain discoverable and reusable in the existing API and daemon test
projects.

Enterprise readiness gates:

- A remote profile cannot start without protected transport and authentication.
- Every callable REST endpoint and gRPC method has an authorization and audit
  mapping.
- Raw SQL, stored objects, and transaction handles pass privilege-escalation
  tests.
- Admin passes role and secret-storage tests.
- Credential and certificate rotation runbooks pass without unintended downtime
  or an insecure fallback window.
- Security CI, dependency policy, SBOM generation, signing, and provenance all
  block an incomplete release.
- The hardening guide, secret-management guide, vulnerability process, and
  upgrade guide reflect the released behavior.
- No unresolved critical or high-severity finding remains without a documented,
  time-bounded exception accepted by the maintainers responsible for release.

## Planned Public Surface

Exact names should be frozen in the Milestone 1 architecture records. The
expected additive surface includes:

- Security profile and transport-security options shared by daemon and
  standalone REST hosting.
- Authentication scheme options for API key, JWT/OIDC, and mTLS.
- Server-side principal, permission, resource-scope, role-binding, and grant
  models.
- A security administration client kept separate from normal data operations,
  with matching REST and gRPC contracts.
- Token-provider and client-certificate options for `CSharpDbClientOptions` or
  an associated credential provider.
- Authenticated remote options for `CSharpDbConnection`, using the shared
  credential-provider model instead of connection-string secrets where
  possible.
- Effective-permission and credential-expiry models for Admin.
- A versioned audit event model and audit sink extension point.

Candidate configuration areas:

- `CSharpDB:Daemon:Security:Profile`.
- `CSharpDB:Daemon:Security:Transport`.
- `CSharpDB:Daemon:Security:Authentication`.
- `CSharpDB:Daemon:Security:Authorization`.
- `CSharpDB:Daemon:Security:Audit`.
- Equivalent `CSharpDB:Api:Security` areas for the standalone REST host.
- Standard Kestrel endpoint configuration for listener and server certificate
  details where it already provides the required capability.

Compatibility requirements:

- Continue accepting the current `Security:Mode`, `ApiKey`, and
  `ApiKeyHeaderName` settings during a documented transition.
- Reject conflicting legacy and new settings rather than guessing which one has
  precedence.
- Do not put permissions or roles in `ICSharpDbClient` calls where a remote
  client could forge them.
- Keep current data-operation contracts source-compatible where practical; add
  authentication refresh and security-administration APIs additively.
- Version REST and protobuf additions compatibly and keep old clients usable
  with API-key deployments during the supported migration window.

## Test Matrix

Transport and authentication:

- Loopback HTTP, remote TLS, proxy-terminated TLS, and required mTLS.
- REST, native gRPC, and gRPC-Web.
- `ICSharpDbClient` and `CSharpDB.Data`/ADO.NET remote client parity, including
  `RemoteGrpcConnectionTests` coverage.
- API key, JWT/OIDC, mTLS identity, and each explicitly supported composition.
- Valid, expired, revoked, malformed, untrusted, and rotated credentials.

Authorization:

- Each built-in role against every operation family.
- Database-wide and object-scoped grants.
- Similar and quoted resource names, missing resources, and existence hiding.
- Shard route, keyspace, catalog, directory, migration, and failover scopes.
- Transaction and resumable-operation ownership.
- Filesystem capability roots across maintenance, inspection, pipelines,
  external tables, import/export, and sharding, including traversal and link
  escapes.

SQL:

- Read, write, DDL, executable object, and metadata statements.
- Multi-statement, nested, cached, quoted, commented, and invalid statements.
- Views, triggers, procedures, pipelines, and transaction sessions.
- Revocation during a session and authorization policy changes during runtime.

Admin:

- Navigation and action visibility for every built-in role.
- Server denial after a hidden action is invoked directly.
- Token refresh, certificate expiry, permission refresh, sign-out, and
  connection switching.
- Secret persistence, logs, crash output, and tab-state redaction.

Audit and operations:

- Required event coverage, correlation, original actor, and policy version.
- Redaction canaries, event size limits, rotation, retention, and sink outage.
- Security configuration changes and break-glass recovery.
- Windows, Linux, and macOS service permissions and startup validation.

Supply chain:

- SBOM completeness and artifact digest matching.
- Signature, checksum, and provenance verification and negative tamper tests.
- Dependency and static-analysis gates.
- Clean-machine verification of published artifacts.

## Compatibility And Rollout

Use a staged migration rather than changing the remote default without warning:

1. Introduce deployment profiles, configuration validation, and warnings for
   `Mode = None` on non-loopback bindings. Preserve existing behavior for the
   compatibility release.
2. Make new daemon installations choose a profile explicitly. Generate secure
   remote examples and keep local-only examples bound to loopback.
3. Migrate the legacy shared key into the multi-key model with a clear key id
   and rotation procedure.
4. In a declared major-version boundary, refuse non-loopback plaintext or
   unauthenticated remote startup unless a narrowly named emergency override is
   explicitly set. Emit a high-severity startup and audit warning for that
   override.
5. Remove the override only after the deprecation window and upgrade tooling are
   published and tested.

Changing a listener from local to remote must trigger validation even when the
rest of the configuration did not change.

## Out Of Scope For The First Enterprise Security Release

- Transparent encryption of database, WAL, backup, audit, or crash-dump files
  at rest. This requires a separate storage-key-management design.
- Protection after the daemon host operating-system identity or process is fully
  compromised.
- A complete network firewall, WAF, DDoS mitigation, certificate authority, or
  identity provider supplied by CSharpDB.
- Multi-tenant isolation within one daemon process.
- Row-level and column-level security, data masking, and inference controls. The
  first SQL permission model is operation- and object-scoped.
- Definer-rights procedures or triggers.
- Automatic authorization federation between independently administered
  CSharpDB clusters.
- Compliance certification. The plan supplies controls and evidence; a company
  must still evaluate them against its own regulatory obligations.

## Assumptions

- The daemon remains the preferred combined REST/gRPC remote host.
- The standalone REST host remains supported and reuses the same security
  implementation.
- ASP.NET Core authentication, authorization, Kestrel, configuration, and
  logging primitives are reused where they satisfy the design.
- Company deployments can provide an external PKI, OIDC provider, protected
  secret source, network boundary, and immutable audit destination when those
  controls are required.
- Existing sharding, migration, maintenance, and Admin capabilities remain
  available but become subject to explicit permissions.
- Security work ships incrementally, but CSharpDB does not claim the complete
  enterprise-ready remote security posture until the Milestone 10 gates pass.

## Roadmap Item Coverage

| Requested item | Primary milestone |
| --- | --- |
| TLS support for REST/gRPC daemon mode | 2 |
| mTLS support for service-to-service deployments | 2 and 3 |
| JWT/OIDC authentication | 3 |
| Role-based access control | 4 |
| SQL-level permissions | 5 |
| Admin UI permission scopes | 6 |
| API key rotation | 3 |
| Audit logging | 7 |
| Security hardening guide | 9 |
| Secret management documentation | 9 |
| Signed releases | 8 |
| SBOM generation | 8 |
| Security policy and vulnerability reporting process | 1 |
