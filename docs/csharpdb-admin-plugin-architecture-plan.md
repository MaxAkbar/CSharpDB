# CSharpDB Admin Plugin Architecture Plan

**Status:** Proposed

**Last reviewed:** 2026-09-02

**Implementation status:** Not started

## Summary

Evolve CSharpDB.Admin from a modular monolith into a plugin-capable feature
host. The product will retain a small, stable core while bundled and future
features register themselves through versioned extension contracts.

The first implementation stages should establish the extension seams and
migrate existing first-party features. Loading arbitrary third-party binaries
should come later, after the contracts, failure isolation, security model, and
packaging behavior have been proven by bundled extensions.

## Current Architecture

CSharpDB.Admin already contains useful modular pieces:

- Forms, Reports, and Import/Export are separate Razor class libraries.
- Forms has a descriptor-based control registry that aggregates registrations,
  validates component types, and rejects duplicate identifiers.
- CSharpDB.Primitives contains extension-manifest and capability-policy
  concepts.
- The repository contains production worker-containment patterns in its
  migration and Entity Framework tooling.

However, Admin composition remains closed:

- The host directly references feature projects in
  [CSharpDB.Admin.csproj](../src/CSharpDB.Admin/CSharpDB.Admin.csproj).
- Feature services and endpoints are explicitly registered in
  [Program.cs](../src/CSharpDB.Admin/Program.cs).
- Tabs use a fixed enum and feature-specific state in
  [TabDescriptor.cs](../src/CSharpDB.Admin/Models/TabDescriptor.cs).
- Components are rendered through a fixed switch in
  [MainLayout.razor](../src/CSharpDB.Admin/Components/Layout/MainLayout.razor).
- Navigation and command-palette items are hardcoded in
  [NavMenu.razor](../src/CSharpDB.Admin/Components/Layout/NavMenu.razor) and
  [CommandPalette.razor](../src/CSharpDB.Admin/Components/Layout/CommandPalette.razor).
- Feature styles and scripts are explicitly included by
  [App.razor](../src/CSharpDB.Admin/Components/App.razor).

Consequently, adding a feature currently requires coordinated changes across
the host, even when the feature implementation is already in a separate
library.

## Target Architecture

~~~mermaid
flowchart LR
    B[Bundled first-party extensions] --> L[Startup extension loader]
    T[Trusted installed extensions] --> L
    L --> C[Validated feature catalog]
    C --> R[Contribution registries]
    R --> S[Core Admin shell]
    R --> W[Workspaces]
    R --> N[Navigation and commands]
    R --> A[Explorer sections and object actions]

    X[Sandboxed worker extensions] --> K[Capability broker]
    K --> S
    S --> H[Database, routes, dialogs and notifications]
~~~

There will be two extension trust classes:

1. **Trusted UI extensions** run in-process and may contribute Razor
   components and scoped services.
2. **Sandboxed operation extensions** run out-of-process and may contribute
   commands, providers, imports, exports, analysis, or pipeline operations
   through host-controlled capabilities. They may not supply arbitrary
   executable Razor UI.

In-process extensions must be described as fully trusted code. An
AssemblyLoadContext can isolate dependencies but is not a security boundary.

## Core and Extension Ownership

### Core host

Keep the following responsibilities in CSharpDB.Admin:

- application shell, tab lifecycle, theme, dialogs, notifications, help, and
  diagnostics;
- database connection, database switching, and readiness;
- route selection and route-aware client leasing;
- extension catalog, contribution registries, configuration, safe mode, and
  capability brokering;
- basic query, table, view, and collection browsing;
- basic schema inspection and editing.

### Bundled extensions

Ship the following as first-party extensions enabled by default:

- Forms;
- Reports;
- Import/Export;
- Data Model;
- Data Hygiene;
- Compare/Deploy;
- Pipelines;
- Code Modules;
- Observability;
- Storage Inspector;
- callback diagnostics;
- the Sharding management UI, while route management remains a core service.

Users should initially see no product or workflow change. The architectural
difference is that these features use the same extension contracts available
to future features.

## Extension Contracts

Create a small, packable **CSharpDB.Admin.Extensibility** project. It must not
depend on concrete CSharpDB.Admin implementation services.

The initial public surface should include:

- **IAdminExtensionModule** for extension identity and registrations;
- **AdminExtensionManifest** for package identity, compatibility,
  dependencies, capabilities, assets, and integrity metadata;
- **AdminWorkspaceDefinition** for workspace component, title, icon, opening
  policy, state version, and shell behavior;
- **IAdminNavigation** with generic open, activate, and close operations;
- **IAdminCommandProvider** for command palette, welcome, title-bar, and other
  command placements;
- **IAdminExplorerSectionProvider** for optional object-explorer sections;
- **IAdminContextActionProvider** for table, view, form, report, and other
  object actions;
- **IAdminWorkspaceContext** for route-aware database access, current tab
  state, notifications, and lifecycle information;
- **AdminFeatureCatalog** for validated, immutable, deterministic
  contributions.

Use namespaced string identifiers, such as
**csharpdb.forms.designer**, instead of expanding a central enum.

Registrations must:

- reject duplicate IDs;
- validate Razor component types;
- validate dependencies and host API compatibility;
- retain extension ownership and provenance;
- produce deterministic ordering independent of registration order;
- resolve handlers from scoped dependency injection only when invoked.

Workspace state should be namespaced, JSON-serializable, and versioned.
Feature-specific properties should gradually move out of the core tab model.

## Implementation Phases

### Phase 1: Contracts and catalog

- Add CSharpDB.Admin.Extensibility.
- Add the extension builder, manifest model, contribution descriptors, and
  immutable feature catalog.
- Register every current feature in the catalog while preserving existing
  rendering.
- Add configuration for enabled and disabled features.
- Fail startup with actionable diagnostics for duplicate contributions,
  missing dependencies, or incompatible built-in registrations.

This phase establishes the API without changing visible behavior.

### Phase 2: Registry-driven shell

- Replace the TabKind component switch with workspace definitions and
  DynamicComponent rendering.
- Replace feature-specific tab creation with
  **OpenTab(AdminTabRequest)**.
- Retain existing OpenXTab methods temporarily as compatibility extension
  methods owned by their features.
- Move route inheritance, route requirements, singleton/per-object behavior,
  active-state notifications, and privacy behavior into workspace metadata.
- Populate the command palette, welcome actions, title bar, navigation, and
  context menus from contribution providers.
- Split the object explorer into core sections and contributed sections.
- Wrap extension workspaces in error boundaries.

Preserve the existing behavior where inactive tabs remain mounted and hidden,
so transient editor state is not lost.

### Phase 3: First-party extension migration

Start with Forms, Reports, and Import/Export because they are already Razor
class libraries.

- Add thin Admin extension adapters for each library.
- Move their workspace registrations, commands, explorer sections, and object
  actions into those adapters.
- Replace host wrapper dependencies with IAdminWorkspaceContext and the
  route-aware client abstraction.
- Remove their concrete types and repositories from core shell components.
- Prefer scoped CSS and JavaScript modules over global asset links.
- Move download behavior behind a core-owned broker rather than granting
  arbitrary endpoint registration.

Next migrate Data Model, Data Hygiene, and Compare/Deploy, followed by the
remaining feature areas. Each migration must be independently releasable.

### Phase 4: Bundled extension discovery

- Place bundled extensions in an explicit, versioned publish directory.
- Discover manifests before the application service provider is built.
- Validate IDs, dependencies, host API range, target framework,
  architecture, file inventory, and SHA-256 hashes.
- Generate an **extensions.lock.json** file during portable and Store
  packaging.
- Add an Extensions diagnostics view with loaded, disabled, incompatible,
  quarantined, and failed states.
- Add a safe-mode startup option that disables all non-core extensions.
- Require restart after installing, updating, enabling, or disabling an
  extension.

Do not attempt hot unload in the initial design. Razor components, dependency
injection registrations, static events, and background work make reliable
live unloading substantially more complex.

### Phase 5: Trusted user-installed UI extensions

- Use an explicit per-user extension directory, outside database directories.
- Package an extension as an atomic, signed archive.
- Validate path traversal, links, file counts, size limits, hashes, publisher
  signatures, compatibility, and requested capabilities before extraction.
- Stage updates in a new version directory and activate them atomically.
- Retain the previous version for rollback.
- Require renewed approval whenever an update adds capabilities.
- Quarantine a failing extension without preventing Admin startup.

Do not load executable extensions from an opened database, its directory, or
database-owned metadata.

### Phase 6: Sandboxed worker extensions

- Productize a versioned out-of-process worker protocol.
- Broker SQL, database, file, and network access through explicit host
  capabilities.
- Enforce deadlines, cancellation, memory ceilings, bounded messages and
  logs, environment scrubbing, and process-tree termination.
- Sanitize diagnostics before returning them to the Admin UI.
- Render worker configuration and results through core-owned declarative
  components.

Reuse the repository's production worker-containment practices. The
test-only ExtensionSandbox prototype is useful evidence but is not itself a
production runtime.

## Security and Packaging Rules

- Treat every in-process UI extension as code with the user's effective
  database, filesystem, network, and process privileges.
- Do not treat dependency isolation or capability declarations as a sandbox.
- Cryptographically verify the canonical manifest and every declared
  artifact. The existing DbExtensionManifest policy vocabulary can be reused,
  but its current nonblank-signature check is not sufficient package
  verification.
- Keep enablement, approvals, and trust state per user and outside database
  files.
- Do not allow arbitrary same-origin JavaScript or endpoints by default.
- Serve approved assets from a normalized, extension-specific route with an
  exact inventory, content-type validation, immutable hashes, and no
  directory listing.
- Preserve user-installed extensions across host upgrades while allowing host
  updates to replace bundled extensions.
- Verify the exact bundled extension inventory in portable ZIP, MSIX, and
  release workflows.

## Verification Plan

### Contract and composition tests

- A fixture extension can add a workspace, navigation item, command, explorer
  section, and table context action without editing shell code.
- Registration order does not affect visible order.
- Duplicate IDs, missing dependencies, and incompatible API versions produce
  actionable failures.
- Disabling Forms or Reports removes its contributions without breaking shell
  dependency injection.
- Namespaced workspace state round-trips and supports version migration.

### Host regression tests

- Existing tab state and mounted-component behavior remain unchanged.
- Database switching and route-aware sharding behavior remain unchanged.
- Route inheritance and privacy behavior are metadata-driven.
- Keyboard navigation, command palette behavior, theme support, and
  accessibility remain intact.
- Forms, Reports, Import/Export, Data Model, Data Hygiene, Compare/Deploy, and
  schema editing retain their current behavior.

### Failure and security tests

- Missing or corrupt assemblies do not prevent safe-mode startup.
- Extension construction, rendering, and background-task failures are
  isolated and reported.
- Manifest signature, hash, capability, traversal, link, and size-limit
  validation is fail-closed.
- Interrupted installation and update operations leave either the old or new
  complete version active.
- Worker timeout, cancellation, crash, memory, malformed-message, and
  denied-capability cases cannot crash the host.

### Packaging tests

- Portable and Store packages contain the exact locked bundled-extension
  inventory.
- Every bundled extension can be discovered and smoke-loaded from a packaged
  layout.
- Compatible, incompatible, disabled, and quarantined fixture extensions
  produce the expected startup state.

## Initial Delivery Boundary

The recommended first delivery covers Phases 1 through 3:

1. establish the stable extension contracts;
2. make the shell registry-driven;
3. migrate existing first-party feature libraries through those contracts.

That creates a genuinely extensible Admin application while deferring
downloadable third-party binaries until the security, diagnostics, and
packaging foundations are ready.
