using CSharpDB.Migration;

namespace CSharpDB.Migration.SqlServer;

internal static partial class SqlServerCatalogBuilder
{
    private static int ProgrammableObjectCapacity(SqlServerCatalogSnapshot snapshot) =>
        checked(
            snapshot.Views.Count +
            snapshot.ViewColumns.Count +
            snapshot.Triggers.Count +
            snapshot.TriggerEvents.Count +
            snapshot.Routines.Count +
            snapshot.Modules.Count +
            snapshot.Parameters.Count +
            snapshot.ExpressionDependencyAudit.Dependencies.Count);

    private static void AddProgrammableObjects(
        SqlServerCatalogSnapshot snapshot,
        string databaseId,
        IReadOnlyDictionary<int, (SqlServerSchemaMetadata Metadata, string ObjectId)> schemasById,
        IReadOnlyDictionary<int, (SqlServerTableMetadata Metadata, string Id)> tablesByObjectId,
        IReadOnlyDictionary<
            (int ObjectId, int ColumnId),
            (SqlServerColumnMetadata Metadata, string Id)> tableColumnsByCatalogId,
        ICollection<MigrationCatalogObject> objects,
        ICollection<MigrationDiagnostic> diagnostics,
        CancellationToken cancellationToken)
    {
        Dictionary<int, SqlServerModuleMetadata> modulesByObjectId =
            snapshot.Modules.ToDictionary(static item => item.ObjectId);
        var representedModuleIds = new HashSet<int>();
        var catalogObjectIdsBySqlObjectId = new Dictionary<int, string>();
        var catalogColumnIdsBySqlColumnId =
            new Dictionary<(int ObjectId, int ColumnId), string>();

        foreach ((int objectId, (_, string id)) in tablesByObjectId)
            catalogObjectIdsBySqlObjectId.Add(objectId, id);
        foreach (((int objectId, int columnId), (_, string id)) in
                 tableColumnsByCatalogId)
        {
            catalogColumnIdsBySqlColumnId.Add((objectId, columnId), id);
        }

        var viewsByObjectId =
            new Dictionary<int, (SqlServerViewMetadata Metadata, string Id)>();
        foreach (SqlServerViewMetadata view in snapshot.Views
                     .OrderBy(static item => item.ObjectId))
        {
            cancellationToken.ThrowIfCancellationRequested();
            (SqlServerSchemaMetadata schema, string namespaceId) =
                schemasById[view.SchemaId];
            string viewId = ObjectId("view", schema.Name, view.Name);
            viewsByObjectId.Add(view.ObjectId, (view, viewId));
            catalogObjectIdsBySqlObjectId.Add(view.ObjectId, viewId);
            modulesByObjectId.TryGetValue(
                view.ObjectId,
                out SqlServerModuleMetadata? module);
            if (module is not null)
                representedModuleIds.Add(module.ObjectId);

            var facets = new List<MigrationCatalogFacet>
            {
                Facet("sqlServerObjectId", Invariant(view.ObjectId)),
                Facet("sqlServerViewColumnInventory", "true"),
                Facet("sqlServerReplicated", Boolean(view.IsReplicated)),
                Facet(
                    "sqlServerReplicationFilter",
                    Boolean(view.HasReplicationFilter)),
                Facet(
                    "sqlServerOpaqueMetadata",
                    Boolean(view.HasOpaqueMetadata)),
                Facet(
                    "sqlServerUncheckedAssemblyData",
                    Boolean(view.HasUncheckedAssemblyData)),
                Facet("sqlServerWithCheckOption", Boolean(view.WithCheckOption)),
                Facet(
                    "sqlServerDateCorrelationView",
                    Boolean(view.IsDateCorrelationView)),
                Facet("sqlServerIndexedView", Boolean(view.IsIndexed)),
                Facet(
                    "sqlServerPermissionViewDefinition",
                    NullableBoolean(view.HasViewDefinition)),
                Facet(
                    "sqlServerLedgerViewType",
                    view.LedgerViewType is null
                        ? null
                        : Invariant(view.LedgerViewType.Value)),
                Facet(
                    "sqlServerLedgerViewTypeDescription",
                    view.LedgerViewTypeDescription),
                Facet(
                    "sqlServerDroppedLedgerView",
                    NullableBoolean(view.IsDroppedLedgerView)),
            };
            AddModuleFacets(facets, module);
            objects.Add(new MigrationCatalogObject
            {
                ObjectId = viewId,
                Kind = MigrationObjectKind.View,
                ParentObjectId = namespaceId,
                SourceNamespace = schema.Name,
                SourceName = view.Name,
                Facets = facets.AsReadOnly(),
            });

            AddModuleDiagnostics(
                viewId,
                "view",
                expectsSqlBody: true,
                module,
                diagnostics);
            if (view.IsIndexed ||
                view.HasReplicationFilter ||
                view.HasOpaqueMetadata ||
                view.HasUncheckedAssemblyData ||
                view.WithCheckOption ||
                view.IsDateCorrelationView ||
                view.LedgerViewType is not (null or 0) ||
                view.IsDroppedLedgerView == true)
            {
                diagnostics.Add(Diagnostic(
                    viewId,
                    "MIG-SQLSERVER-VIEW-SHAPE-UNSUPPORTED-001",
                    MigrationDiagnosticSeverity.Error,
                    MigrationCompatibilityStatus.Unsupported,
                    "The view has SQL Server-specific behavior that is not target-compatible.",
                    "Indexed, replicated, opaque-metadata, assembly-dependent, check-option, date-correlation, and ledger views require source-specific semantics that this checkpoint does not lower.",
                    "Replace the view with a reviewed ordinary projection or provide an explicit target design.",
                    canOverride: false));
            }
        }

        foreach (SqlServerViewColumnMetadata column in snapshot.ViewColumns
                     .OrderBy(static item => item.ObjectId)
                     .ThenBy(static item => item.ColumnId))
        {
            cancellationToken.ThrowIfCancellationRequested();
            (SqlServerViewMetadata view, string viewId) =
                viewsByObjectId[column.ObjectId];
            SqlServerSchemaMetadata schema =
                schemasById[view.SchemaId].Metadata;
            string columnId = ObjectId(
                "view-column",
                schema.Name,
                view.Name,
                column.Name);
            catalogColumnIdsBySqlColumnId.Add(
                (column.ObjectId, column.ColumnId),
                columnId);

            bool userDefinedType = !string.Equals(
                column.TypeSchema,
                "sys",
                StringComparison.Ordinal);
            bool rowVersion = IsRowVersion(column.SystemTypeName);
            string logicalType = userDefinedType || rowVersion
                ? "native"
                : LogicalType(column.SystemTypeName);
            var facets = new List<MigrationCatalogFacet>
            {
                Facet("logicalType", logicalType),
                Facet("nullable", Boolean(column.IsNullable)),
                Facet("identity", "false"),
                Facet("rowVersion", Boolean(rowVersion)),
                Facet("sqlServerViewColumn", "true"),
                Facet("sqlServerObjectId", Invariant(column.ObjectId)),
                Facet("sqlServerColumnId", Invariant(column.ColumnId)),
                Facet("sqlServerTypeSchema", column.TypeSchema),
                Facet("sqlServerTypeName", column.TypeName),
                Facet("sqlServerSystemTypeName", column.SystemTypeName),
                Facet("sqlServerUserDefinedType", Boolean(userDefinedType)),
                Facet("sqlServerMaxLengthBytes", Invariant(column.MaxLength)),
                Facet("sqlServerPrecision", Invariant(column.Precision)),
                Facet("sqlServerScale", Invariant(column.Scale)),
                Facet("sqlServerCollation", column.Collation),
                Facet("sqlServerAnsiPadded", Boolean(column.IsAnsiPadded)),
                Facet("sqlServerHidden", Boolean(column.IsHidden)),
                Facet("sqlServerMasked", Boolean(column.IsMasked)),
                Facet("sqlServerEncryptionType", column.EncryptionType),
                Facet("sqlServerXmlDocument", Boolean(column.IsXmlDocument)),
                Facet("sqlServerXmlCollectionId", Invariant(column.XmlCollectionId)),
            };
            AddViewColumnLogicalFacets(facets, column);
            objects.Add(new MigrationCatalogObject
            {
                ObjectId = columnId,
                Kind = MigrationObjectKind.Column,
                ParentObjectId = viewId,
                SourceNamespace = schema.Name,
                SourceName = column.Name,
                NativeType = FormatViewColumnNativeType(column),
                Facets = facets.AsReadOnly(),
            });

            if (logicalType == "native" ||
                column.IsHidden ||
                column.IsMasked ||
                column.EncryptionType is not null ||
                column.XmlCollectionId != 0)
            {
                diagnostics.Add(Diagnostic(
                    columnId,
                    "MIG-SQLSERVER-VIEW-COLUMN-UNSUPPORTED-001",
                    MigrationDiagnosticSeverity.Error,
                    MigrationCompatibilityStatus.Unsupported,
                    "The view output column requires SQL Server-specific handling.",
                    "User-defined, rowversion, hidden, masked, encrypted, and typed-XML view columns are inventoried but not assigned target semantics.",
                    "Materialize and validate an ordinary scalar projection before migration.",
                    canOverride: false));
            }
        }

        var triggersByObjectId =
            new Dictionary<int, (SqlServerTriggerMetadata Metadata, string Id)>();
        foreach (SqlServerTriggerMetadata trigger in snapshot.Triggers
                     .OrderBy(static item => item.ObjectId))
        {
            cancellationToken.ThrowIfCancellationRequested();
            string? sourceNamespace = trigger.SchemaId is int schemaId
                ? schemasById[schemaId].Metadata.Name
                : null;
            string parentId = trigger.ParentClass switch
            {
                0 => databaseId,
                1 when tablesByObjectId.ContainsKey(
                    trigger.ParentObjectId) =>
                    tablesByObjectId[trigger.ParentObjectId].Id,
                1 when viewsByObjectId.ContainsKey(
                    trigger.ParentObjectId) =>
                    viewsByObjectId[trigger.ParentObjectId].Id,
                _ => throw new SqlServerMigrationException(
                    "SQL Server returned an unowned trigger."),
            };
            string triggerId = ObjectId(
                "trigger",
                sourceNamespace ?? "$database",
                Invariant(trigger.ParentClass),
                trigger.Name);
            triggersByObjectId.Add(trigger.ObjectId, (trigger, triggerId));
            catalogObjectIdsBySqlObjectId.Add(trigger.ObjectId, triggerId);
            modulesByObjectId.TryGetValue(
                trigger.ObjectId,
                out SqlServerModuleMetadata? module);
            if (module is not null)
                representedModuleIds.Add(module.ObjectId);

            var facets = new List<MigrationCatalogFacet>
            {
                Facet("sqlServerObjectId", Invariant(trigger.ObjectId)),
                Facet("sqlServerParentClass", Invariant(trigger.ParentClass)),
                Facet(
                    "sqlServerParentClassDescription",
                    trigger.ParentClassDescription),
                Facet(
                    "sqlServerParentObjectId",
                    Invariant(trigger.ParentObjectId)),
                Facet("sqlServerTriggerType", trigger.Type),
                Facet(
                    "sqlServerTriggerTypeDescription",
                    trigger.TypeDescription),
                Facet("sqlServerDisabled", Boolean(trigger.IsDisabled)),
                Facet(
                    "sqlServerNotForReplication",
                    Boolean(trigger.IsNotForReplication)),
                Facet(
                    "sqlServerInsteadOfTrigger",
                    Boolean(trigger.IsInsteadOfTrigger)),
                Facet("sqlServerInsertEvent", NullableBoolean(trigger.IsInsert)),
                Facet("sqlServerUpdateEvent", NullableBoolean(trigger.IsUpdate)),
                Facet("sqlServerDeleteEvent", NullableBoolean(trigger.IsDelete)),
                Facet(
                    "sqlServerFirstInsert",
                    NullableBoolean(trigger.IsFirstInsert)),
                Facet(
                    "sqlServerLastInsert",
                    NullableBoolean(trigger.IsLastInsert)),
                Facet(
                    "sqlServerFirstUpdate",
                    NullableBoolean(trigger.IsFirstUpdate)),
                Facet(
                    "sqlServerLastUpdate",
                    NullableBoolean(trigger.IsLastUpdate)),
                Facet(
                    "sqlServerFirstDelete",
                    NullableBoolean(trigger.IsFirstDelete)),
                Facet(
                    "sqlServerLastDelete",
                    NullableBoolean(trigger.IsLastDelete)),
                Facet(
                    "sqlServerPermissionViewDefinition",
                    NullableBoolean(trigger.HasViewDefinition)),
            };
            AddModuleFacets(facets, module);
            objects.Add(new MigrationCatalogObject
            {
                ObjectId = triggerId,
                Kind = MigrationObjectKind.Trigger,
                ParentObjectId = parentId,
                SourceNamespace = sourceNamespace,
                SourceName = trigger.Name,
                Facets = facets.AsReadOnly(),
            });

            bool sqlTrigger = string.Equals(
                trigger.Type,
                "TR",
                StringComparison.Ordinal);
            AddModuleDiagnostics(
                triggerId,
                "trigger",
                sqlTrigger,
                module,
                diagnostics);
            if (!sqlTrigger ||
                trigger.ParentClass != 1 ||
                trigger.IsDisabled ||
                trigger.IsNotForReplication ||
                trigger.IsInsteadOfTrigger)
            {
                diagnostics.Add(Diagnostic(
                    triggerId,
                    "MIG-SQLSERVER-TRIGGER-SHAPE-UNSUPPORTED-001",
                    MigrationDiagnosticSeverity.Error,
                    MigrationCompatibilityStatus.Unsupported,
                    "The trigger shape is not in the bounded target subset.",
                    "CLR, database-scoped, disabled, NOT FOR REPLICATION, and INSTEAD OF triggers are inventoried but cannot be silently lowered to a CSharpDB trigger.",
                    "Replace the trigger with reviewed target behavior and validate it independently.",
                    canOverride: false));
            }
        }

        foreach (SqlServerTriggerEventMetadata triggerEvent in
                 snapshot.TriggerEvents
                     .OrderBy(static item => item.ObjectId)
                     .ThenBy(static item => item.Type))
        {
            cancellationToken.ThrowIfCancellationRequested();
            (SqlServerTriggerMetadata trigger, string triggerId) =
                triggersByObjectId[triggerEvent.ObjectId];
            string eventId = ObjectId(
                "trigger-event",
                triggerId,
                Invariant(triggerEvent.Type));
            objects.Add(new MigrationCatalogObject
            {
                ObjectId = eventId,
                Kind = MigrationObjectKind.Other,
                ParentObjectId = triggerId,
                SourceNamespace = trigger.SchemaId is int schemaId
                    ? schemasById[schemaId].Metadata.Name
                    : null,
                SourceName = triggerEvent.TypeDescription,
                Facets =
                [
                    Facet("sqlServerObjectClass", "trigger-event"),
                    Facet("sqlServerTriggerObjectId", Invariant(triggerEvent.ObjectId)),
                    Facet("sqlServerTriggerEventType", Invariant(triggerEvent.Type)),
                    Facet(
                        "sqlServerTriggerEventTypeDescription",
                        triggerEvent.TypeDescription),
                    Facet("sqlServerTriggerEventFirst", Boolean(triggerEvent.IsFirst)),
                    Facet("sqlServerTriggerEventLast", Boolean(triggerEvent.IsLast)),
                    Facet(
                        "sqlServerTriggerEventGroupType",
                        triggerEvent.EventGroupType is null
                            ? null
                            : Invariant(triggerEvent.EventGroupType.Value)),
                    Facet(
                        "sqlServerTriggerEventGroupTypeDescription",
                        triggerEvent.EventGroupTypeDescription),
                ],
            });
        }

        var routinesByObjectId =
            new Dictionary<int, (SqlServerRoutineMetadata Metadata, string Id)>();
        foreach (SqlServerRoutineMetadata routine in snapshot.Routines
                     .OrderBy(static item => item.ObjectId))
        {
            cancellationToken.ThrowIfCancellationRequested();
            (SqlServerSchemaMetadata schema, string namespaceId) =
                schemasById[routine.SchemaId];
            string routineId = ObjectId(
                "routine",
                schema.Name,
                routine.Name);
            routinesByObjectId.Add(routine.ObjectId, (routine, routineId));
            catalogObjectIdsBySqlObjectId.Add(routine.ObjectId, routineId);
            modulesByObjectId.TryGetValue(
                routine.ObjectId,
                out SqlServerModuleMetadata? module);
            if (module is not null)
                representedModuleIds.Add(module.ObjectId);

            var facets = new List<MigrationCatalogFacet>
            {
                Facet("sqlServerObjectId", Invariant(routine.ObjectId)),
                Facet("sqlServerRoutineType", routine.Type),
                Facet(
                    "sqlServerRoutineTypeDescription",
                    routine.TypeDescription),
                Facet(
                    "sqlServerProcedureAutoExecuted",
                    NullableBoolean(routine.IsAutoExecuted)),
                Facet(
                    "sqlServerProcedureExecutionReplicated",
                    NullableBoolean(routine.IsExecutionReplicated)),
                Facet(
                    "sqlServerProcedureReplicationSerializableOnly",
                    NullableBoolean(routine.IsReplicationSerializableOnly)),
                Facet(
                    "sqlServerProcedureSkipsReplicationConstraints",
                    NullableBoolean(routine.SkipsReplicationConstraints)),
                Facet(
                    "sqlServerPermissionViewDefinition",
                    NullableBoolean(routine.HasViewDefinition)),
            };
            AddModuleFacets(facets, module);
            objects.Add(new MigrationCatalogObject
            {
                ObjectId = routineId,
                Kind = MigrationObjectKind.Routine,
                ParentObjectId = namespaceId,
                SourceNamespace = schema.Name,
                SourceName = routine.Name,
                Facets = facets.AsReadOnly(),
            });

            bool sqlRoutine = routine.Type is "P" or "FN" or "IF" or "TF" or "RF";
            AddModuleDiagnostics(
                routineId,
                "routine",
                sqlRoutine,
                module,
                diagnostics);
            diagnostics.Add(Diagnostic(
                routineId,
                "MIG-SQLSERVER-ROUTINE-UNSUPPORTED-001",
                MigrationDiagnosticSeverity.Error,
                MigrationCompatibilityStatus.Unsupported,
                "SQL Server routines do not have an automatic target lowering.",
                "Stored procedures, functions, replication-filter procedures, CLR routines, and extended procedures require explicit behavioral redesign.",
                "Reimplement and test the required behavior against the target application contract.",
                canOverride: false));
        }

        foreach (SqlServerParameterMetadata parameter in snapshot.Parameters
                     .OrderBy(static item => item.ObjectId)
                     .ThenBy(static item => item.ParameterId))
        {
            cancellationToken.ThrowIfCancellationRequested();
            (SqlServerRoutineMetadata routine, string routineId) =
                routinesByObjectId[parameter.ObjectId];
            SqlServerSchemaMetadata schema =
                schemasById[routine.SchemaId].Metadata;
            string parameterId = ObjectId(
                "parameter",
                schema.Name,
                routine.Name,
                Invariant(parameter.ParameterId));
            objects.Add(new MigrationCatalogObject
            {
                ObjectId = parameterId,
                Kind = MigrationObjectKind.Other,
                ParentObjectId = routineId,
                SourceNamespace = schema.Name,
                SourceName = parameter.ParameterId == 0 &&
                    string.IsNullOrEmpty(parameter.Name)
                    ? "$return"
                    : parameter.Name,
                Facets =
                [
                    Facet("sqlServerObjectClass", "routine-parameter"),
                    Facet("sqlServerObjectId", Invariant(parameter.ObjectId)),
                    Facet("sqlServerParameterId", Invariant(parameter.ParameterId)),
                    Facet(
                        "sqlServerParameterReturnValue",
                        Boolean(parameter.ParameterId == 0)),
                    Facet("sqlServerParameterName", parameter.Name),
                    Facet("sqlServerTypeSchema", parameter.TypeSchema),
                    Facet("sqlServerTypeName", parameter.TypeName),
                    Facet(
                        "sqlServerSystemTypeName",
                        parameter.SystemTypeName),
                    Facet("sqlServerMaxLengthBytes", Invariant(parameter.MaxLength)),
                    Facet("sqlServerPrecision", Invariant(parameter.Precision)),
                    Facet("sqlServerScale", Invariant(parameter.Scale)),
                    Facet("sqlServerParameterOutput", Boolean(parameter.IsOutput)),
                    Facet(
                        "sqlServerParameterCursorReference",
                        Boolean(parameter.IsCursorReference)),
                    Facet(
                        "sqlServerCatalogHasDefaultValue",
                        Boolean(parameter.HasDefaultValue)),
                    Facet(
                        "sqlServerParameterDefaultEvidence",
                        parameter.HasDefaultValue
                            ? "catalog-reported"
                            : "not-catalog-reported"),
                    Facet(
                        "sqlServerParameterXmlDocument",
                        Boolean(parameter.IsXmlDocument)),
                    Facet(
                        "sqlServerParameterXmlCollectionId",
                        Invariant(parameter.XmlCollectionId)),
                    Facet(
                        "sqlServerParameterReadOnly",
                        Boolean(parameter.IsReadOnly)),
                    Facet(
                        "sqlServerParameterNullable",
                        Boolean(parameter.IsNullable)),
                    Facet(
                        "sqlServerParameterEncryptionType",
                        parameter.EncryptionType),
                    Facet(
                        "sqlServerParameterUserDefinedType",
                        Boolean(parameter.IsUserDefined)),
                    Facet(
                        "sqlServerParameterAssemblyType",
                        Boolean(parameter.IsAssemblyType)),
                    Facet(
                        "sqlServerParameterTableType",
                        Boolean(parameter.IsTableType)),
                ],
            });
        }

        foreach (SqlServerModuleMetadata module in snapshot.Modules
                     .Where(item => !representedModuleIds.Contains(item.ObjectId))
                     .OrderBy(static item => item.ObjectId))
        {
            cancellationToken.ThrowIfCancellationRequested();
            (SqlServerSchemaMetadata schema, string namespaceId) =
                schemasById[module.SchemaId];
            string moduleId = ObjectId(
                "module",
                schema.Name,
                module.Name,
                module.ObjectType);
            catalogObjectIdsBySqlObjectId.Add(module.ObjectId, moduleId);
            var facets = new List<MigrationCatalogFacet>
            {
                Facet("sqlServerObjectClass", "legacy-or-unclassified-module"),
                Facet("sqlServerObjectId", Invariant(module.ObjectId)),
            };
            AddModuleFacets(facets, module);
            objects.Add(new MigrationCatalogObject
            {
                ObjectId = moduleId,
                Kind = MigrationObjectKind.Other,
                ParentObjectId = namespaceId,
                SourceNamespace = schema.Name,
                SourceName = module.Name,
                Facets = facets.AsReadOnly(),
            });
            AddModuleDiagnostics(
                moduleId,
                "legacy or unclassified module",
                expectsSqlBody: true,
                module,
                diagnostics);
            diagnostics.Add(Diagnostic(
                moduleId,
                "MIG-SQLSERVER-LEGACY-MODULE-UNSUPPORTED-001",
                MigrationDiagnosticSeverity.Error,
                MigrationCompatibilityStatus.Unsupported,
                "A legacy or unclassified SQL Server module was inventoried.",
                "Standalone rules, defaults, and module types outside the explicit view, trigger, and routine inventory do not have a target object contract.",
                "Replace the object with reviewed table constraints or application behavior.",
                canOverride: false));
        }

        foreach (MigrationCatalogObject structuralObject in objects.Where(
                     static item => item.Kind is
                         MigrationObjectKind.Key or
                         MigrationObjectKind.ForeignKey or
                         MigrationObjectKind.CheckConstraint or
                         MigrationObjectKind.Sequence))
        {
            string? sourceId = structuralObject.Facets.FirstOrDefault(
                static facet => string.Equals(
                    facet.Name,
                    "sqlServerObjectId",
                    StringComparison.Ordinal))?.Value;
            if (int.TryParse(
                    sourceId,
                    System.Globalization.NumberStyles.None,
                    System.Globalization.CultureInfo.InvariantCulture,
                    out int sqlObjectId))
            {
                if (!catalogObjectIdsBySqlObjectId.TryAdd(
                        sqlObjectId,
                        structuralObject.ObjectId))
                {
                    throw new SqlServerMigrationException(
                        "SQL Server returned colliding global object identifiers.");
                }
            }
        }

        AddExpressionDependencies(
            snapshot,
            databaseId,
            catalogObjectIdsBySqlObjectId,
            catalogColumnIdsBySqlColumnId,
            objects,
            diagnostics,
            cancellationToken);
        AddProgrammableQualificationDiagnostics(
            snapshot,
            databaseId,
            diagnostics);
    }

    private static void AddExpressionDependencies(
        SqlServerCatalogSnapshot snapshot,
        string databaseId,
        IReadOnlyDictionary<int, string> objectIdsBySqlObjectId,
        IReadOnlyDictionary<(int ObjectId, int ColumnId), string>
            columnIdsBySqlColumnId,
        ICollection<MigrationCatalogObject> objects,
        ICollection<MigrationDiagnostic> diagnostics,
        CancellationToken cancellationToken)
    {
        IReadOnlyDictionary<string, MigrationCatalogObject> currentObjects =
            objects.ToDictionary(static item => item.ObjectId, StringComparer.Ordinal);
        IReadOnlySet<string> cyclicDependencyRows =
            FindCyclicDependencyRows(
                snapshot.ExpressionDependencyAudit.Dependencies,
                objectIdsBySqlObjectId,
                columnIdsBySqlColumnId,
                cancellationToken);
        foreach (SqlServerExpressionDependencyMetadata dependency in
                 snapshot.ExpressionDependencyAudit.Dependencies
                     .OrderBy(static item => item.ReferencingClass)
                     .ThenBy(static item => item.ReferencingId)
                     .ThenBy(static item => item.ReferencingMinorId)
                     .ThenBy(static item => item.ReferencedClass)
                     .ThenBy(static item => item.ReferencedServerName, StringComparer.Ordinal)
                     .ThenBy(static item => item.ReferencedDatabaseName, StringComparer.Ordinal)
                     .ThenBy(static item => item.ReferencedSchemaName, StringComparer.Ordinal)
                     .ThenBy(static item => item.ReferencedEntityName, StringComparer.Ordinal)
                     .ThenBy(static item => item.ReferencedId)
                     .ThenBy(static item => item.ReferencedMinorId))
        {
            cancellationToken.ThrowIfCancellationRequested();
            string? referencingObjectId = ResolveReferencingEndpoint(
                dependency,
                objectIdsBySqlObjectId,
                columnIdsBySqlColumnId);
            string? referencedObjectId = ResolveReferencedEndpoint(
                dependency,
                objectIdsBySqlObjectId,
                columnIdsBySqlColumnId);
            bool externalServer = dependency.ReferencedServerName is not null;
            bool externalDatabase = dependency.ReferencedDatabaseName is not null;
            bool externalReference = externalServer || externalDatabase;
            bool resolvedLocal = IsResolvedLocalDependency(
                dependency,
                referencingObjectId,
                referencedObjectId);
            string classification =
                referencingObjectId is null
                    ? "untracked-referencer"
                    : externalServer
                        ? "external-server"
                        : externalDatabase
                            ? "external-database"
                            : dependency.IsCallerDependent
                                ? "caller-dependent"
                                : dependency.IsAmbiguous
                                    ? "ambiguous"
                                    : dependency.ReferencedClass != 1
                                        ? "non-object-reference"
                                        : dependency.ReferencedId is null
                                            ? "unresolved"
                                            : referencedObjectId is null
                                                ? "uninventoried-reference"
                                                : "resolved-local";
            string rowDigest = ExpressionDependencyRowDigest(dependency);
            bool inCycle = cyclicDependencyRows.Contains(rowDigest);
            string edgeId = ObjectId(
                "dependency",
                rowDigest);
            string parentId = referencingObjectId ?? databaseId;
            string? sourceNamespace = currentObjects.TryGetValue(
                parentId,
                out MigrationCatalogObject? parent)
                ? parent.SourceNamespace
                : null;
            var facets = new List<MigrationCatalogFacet>
            {
                Facet("sqlServerObjectClass", "expression-dependency"),
                Facet("sqlServerDependencyClassification", classification),
                Facet("sqlServerDependencyDigest", "sha256:" + rowDigest),
                Facet(
                    "sqlServerReferencingId",
                    Invariant(dependency.ReferencingId)),
                Facet(
                    "sqlServerReferencingMinorId",
                    Invariant(dependency.ReferencingMinorId)),
                Facet(
                    "sqlServerReferencingClass",
                    Invariant(dependency.ReferencingClass)),
                Facet(
                    "sqlServerReferencingClassDescription",
                    dependency.ReferencingClassDescription),
                Facet(
                    "sqlServerSchemaBoundReference",
                    Boolean(dependency.IsSchemaBoundReference)),
                Facet(
                    "sqlServerReferencedClass",
                    Invariant(dependency.ReferencedClass)),
                Facet(
                    "sqlServerReferencedClassDescription",
                    dependency.ReferencedClassDescription),
                Facet(
                    "sqlServerReferencedServerPresent",
                    Boolean(externalServer)),
                Facet(
                    "sqlServerReferencedServerDigest",
                    dependency.ReferencedServerName is null
                        ? null
                        : "sha256:" + SqlServerStableDigest.Text(
                            "csharpdb-sqlserver-external-server/v1",
                            dependency.ReferencedServerName)),
                Facet(
                    "sqlServerReferencedDatabasePresent",
                    Boolean(externalDatabase)),
                Facet(
                    "sqlServerReferencedDatabaseDigest",
                    dependency.ReferencedDatabaseName is null
                        ? null
                        : "sha256:" + SqlServerStableDigest.Text(
                            "csharpdb-sqlserver-external-database/v1",
                            dependency.ReferencedDatabaseName)),
                Facet(
                    "sqlServerReferencedSchemaPresent",
                    Boolean(dependency.ReferencedSchemaName is not null)),
                Facet(
                    "sqlServerReferencedSchemaDigest",
                    !externalReference ||
                    dependency.ReferencedSchemaName is null
                        ? null
                        : "sha256:" + SqlServerStableDigest.Text(
                            "csharpdb-sqlserver-external-schema/v1",
                            dependency.ReferencedSchemaName)),
                Facet(
                    "sqlServerReferencedSchemaName",
                    externalReference
                        ? null
                        : dependency.ReferencedSchemaName),
                Facet(
                    "sqlServerReferencedEntityDigest",
                    externalReference
                        ? "sha256:" + SqlServerStableDigest.Text(
                            "csharpdb-sqlserver-external-entity/v1",
                            dependency.ReferencedEntityName)
                        : null),
                Facet(
                    "sqlServerReferencedEntityName",
                    externalReference
                        ? null
                        : dependency.ReferencedEntityName),
                Facet(
                    "sqlServerReferencedId",
                    dependency.ReferencedId is null
                        ? null
                        : Invariant(dependency.ReferencedId.Value)),
                Facet(
                    "sqlServerReferencedMinorId",
                    Invariant(dependency.ReferencedMinorId)),
                Facet(
                    "sqlServerCallerDependent",
                    Boolean(dependency.IsCallerDependent)),
                Facet(
                    "sqlServerAmbiguous",
                    Boolean(dependency.IsAmbiguous)),
                Facet(
                    "sqlServerResolvedLocalEndpoint",
                    Boolean(resolvedLocal)),
                Facet(
                    "sqlServerDependencyCycle",
                    Boolean(inCycle)),
            };
            objects.Add(new MigrationCatalogObject
            {
                ObjectId = edgeId,
                Kind = MigrationObjectKind.Other,
                ParentObjectId = parentId,
                SourceNamespace = sourceNamespace,
                SourceName = "dependency-" + rowDigest[..16],
                Facets = facets.AsReadOnly(),
                DependsOn = resolvedLocal ? [referencedObjectId!] : [],
            });

            if (!resolvedLocal)
            {
                diagnostics.Add(Diagnostic(
                    edgeId,
                    "MIG-SQLSERVER-DEPENDENCY-UNRESOLVED-001",
                    MigrationDiagnosticSeverity.Error,
                    MigrationCompatibilityStatus.Unknown,
                    "A SQL Server expression dependency is not a resolved local object reference.",
                    $"The catalog classified this dependency as '{classification}'. External, caller-dependent, ambiguous, non-object, uninventoried, and otherwise unresolved references cannot establish target execution order.",
                    "Remove dynamic or external binding, or resolve and validate the reference with bounded T-SQL analysis.",
                    canOverride: false,
                    occurrenceKey: classification));
            }
            if (inCycle)
            {
                diagnostics.Add(Diagnostic(
                    edgeId,
                    "MIG-SQLSERVER-DEPENDENCY-CYCLE-001",
                    MigrationDiagnosticSeverity.Error,
                    MigrationCompatibilityStatus.Unknown,
                    "The resolved SQL Server dependency participates in a cycle.",
                    "The source reference is retained as an explicit dependency-edge object so the generic execution graph remains acyclic. No target creation order is inferred for the strongly connected component.",
                    "Break the module cycle or provide a reviewed multi-step target deployment and validation strategy.",
                    canOverride: false));
            }
        }
    }

    private static IReadOnlySet<string> FindCyclicDependencyRows(
        IReadOnlyList<SqlServerExpressionDependencyMetadata> dependencies,
        IReadOnlyDictionary<int, string> objectIdsBySqlObjectId,
        IReadOnlyDictionary<(int ObjectId, int ColumnId), string>
            columnIdsBySqlColumnId,
        CancellationToken cancellationToken)
    {
        var edges = new List<ResolvedDependencyEndpointEdge>();
        var nodes = new SortedSet<string>(StringComparer.Ordinal);
        var adjacencySets =
            new Dictionary<string, SortedSet<string>>(StringComparer.Ordinal);
        var reverseAdjacencySets =
            new Dictionary<string, SortedSet<string>>(StringComparer.Ordinal);

        static void EnsureNode(
            string node,
            IDictionary<string, SortedSet<string>> graph)
        {
            if (!graph.ContainsKey(node))
            {
                graph.Add(
                    node,
                    new SortedSet<string>(StringComparer.Ordinal));
            }
        }

        static Dictionary<string, string[]> MaterializeGraph(
            IEnumerable<string> orderedNodes,
            IReadOnlyDictionary<string, SortedSet<string>> graph,
            CancellationToken cancellationToken)
        {
            var result =
                new Dictionary<string, string[]>(StringComparer.Ordinal);
            foreach (string node in orderedNodes)
            {
                cancellationToken.ThrowIfCancellationRequested();
                SortedSet<string> source = graph[node];
                var neighbors = new string[source.Count];
                int ordinal = 0;
                foreach (string neighbor in source)
                {
                    cancellationToken.ThrowIfCancellationRequested();
                    neighbors[ordinal++] = neighbor;
                }
                result.Add(node, neighbors);
            }
            return result;
        }

        foreach (SqlServerExpressionDependencyMetadata dependency in dependencies)
        {
            cancellationToken.ThrowIfCancellationRequested();
            string? referencingObjectId = ResolveReferencingEndpoint(
                dependency,
                objectIdsBySqlObjectId,
                columnIdsBySqlColumnId);
            string? referencedObjectId = ResolveReferencedEndpoint(
                dependency,
                objectIdsBySqlObjectId,
                columnIdsBySqlColumnId);
            if (!IsResolvedLocalDependency(
                    dependency,
                    referencingObjectId,
                    referencedObjectId))
            {
                continue;
            }

            string from = referencingObjectId!;
            string to = referencedObjectId!;
            string rowDigest = ExpressionDependencyRowDigest(dependency);
            edges.Add(new ResolvedDependencyEndpointEdge(from, to, rowDigest));
            nodes.Add(from);
            nodes.Add(to);
            EnsureNode(from, adjacencySets);
            EnsureNode(to, adjacencySets);
            EnsureNode(from, reverseAdjacencySets);
            EnsureNode(to, reverseAdjacencySets);
            adjacencySets[from].Add(to);
            reverseAdjacencySets[to].Add(from);
        }

        if (edges.Count == 0)
            return new HashSet<string>(StringComparer.Ordinal);

        Dictionary<string, string[]> adjacency = MaterializeGraph(
            nodes,
            adjacencySets,
            cancellationToken);
        Dictionary<string, string[]> reverseAdjacency = MaterializeGraph(
            nodes,
            reverseAdjacencySets,
            cancellationToken);
        var visited = new HashSet<string>(StringComparer.Ordinal);
        var finishOrder = new List<string>(adjacency.Count);
        foreach (string start in nodes)
        {
            cancellationToken.ThrowIfCancellationRequested();
            if (!visited.Add(start))
                continue;

            var pending =
                new Stack<(string Node, int NextNeighborIndex)>();
            pending.Push((start, NextNeighborIndex: 0));
            while (pending.Count > 0)
            {
                cancellationToken.ThrowIfCancellationRequested();
                (string node, int nextNeighborIndex) = pending.Pop();
                string[] neighbors = adjacency[node];
                if (nextNeighborIndex >= neighbors.Length)
                {
                    finishOrder.Add(node);
                    continue;
                }

                pending.Push((node, nextNeighborIndex + 1));
                string next = neighbors[nextNeighborIndex];
                if (visited.Add(next))
                    pending.Push((next, NextNeighborIndex: 0));
            }
        }

        var componentByNode =
            new Dictionary<string, int>(StringComparer.Ordinal);
        var componentSizes = new List<int>();
        for (int ordinal = finishOrder.Count - 1; ordinal >= 0; ordinal--)
        {
            cancellationToken.ThrowIfCancellationRequested();
            string start = finishOrder[ordinal];
            if (componentByNode.ContainsKey(start))
                continue;

            int componentId = componentSizes.Count;
            int componentSize = 0;
            var pending = new Stack<string>();
            componentByNode.Add(start, componentId);
            pending.Push(start);
            while (pending.Count > 0)
            {
                cancellationToken.ThrowIfCancellationRequested();
                string node = pending.Pop();
                componentSize++;
                foreach (string next in reverseAdjacency[node])
                {
                    if (componentByNode.TryAdd(next, componentId))
                        pending.Push(next);
                }
            }
            componentSizes.Add(componentSize);
        }

        var cyclicRows = new HashSet<string>(StringComparer.Ordinal);
        foreach (ResolvedDependencyEndpointEdge edge in edges)
        {
            cancellationToken.ThrowIfCancellationRequested();
            int componentId = componentByNode[edge.From];
            if (componentId == componentByNode[edge.To] &&
                (componentSizes[componentId] > 1 ||
                 string.Equals(
                     edge.From,
                     edge.To,
                     StringComparison.Ordinal)))
            {
                cyclicRows.Add(edge.RowDigest);
            }
        }
        return cyclicRows;
    }

    private static bool IsResolvedLocalDependency(
        SqlServerExpressionDependencyMetadata dependency,
        string? referencingObjectId,
        string? referencedObjectId) =>
        referencingObjectId is not null &&
        referencedObjectId is not null &&
        dependency.ReferencedServerName is null &&
        dependency.ReferencedDatabaseName is null &&
        !dependency.IsCallerDependent &&
        !dependency.IsAmbiguous &&
        dependency.ReferencedClass == 1;

    private static string? ResolveReferencingEndpoint(
        SqlServerExpressionDependencyMetadata dependency,
        IReadOnlyDictionary<int, string> objectIdsBySqlObjectId,
        IReadOnlyDictionary<(int ObjectId, int ColumnId), string>
            columnIdsBySqlColumnId)
    {
        if (dependency.ReferencingClass == 1 &&
            dependency.ReferencingMinorId > 0)
        {
            return columnIdsBySqlColumnId.TryGetValue(
                (dependency.ReferencingId, dependency.ReferencingMinorId),
                out string? columnId)
                ? columnId
                : null;
        }

        return dependency.ReferencingClass is 1 or 12 &&
               objectIdsBySqlObjectId.TryGetValue(
                   dependency.ReferencingId,
                   out string? objectId)
            ? objectId
            : null;
    }

    private static string? ResolveReferencedEndpoint(
        SqlServerExpressionDependencyMetadata dependency,
        IReadOnlyDictionary<int, string> objectIdsBySqlObjectId,
        IReadOnlyDictionary<(int ObjectId, int ColumnId), string>
            columnIdsBySqlColumnId)
    {
        if (dependency.ReferencedClass != 1 ||
            dependency.ReferencedId is not int referencedId)
        {
            return null;
        }

        if (dependency.ReferencedMinorId > 0 &&
            columnIdsBySqlColumnId.TryGetValue(
                (referencedId, dependency.ReferencedMinorId),
                out string? columnId))
        {
            return columnId;
        }

        return dependency.ReferencedMinorId == 0 &&
               objectIdsBySqlObjectId.TryGetValue(
                   referencedId,
                   out string? objectId)
            ? objectId
            : null;
    }

    private static void AddModuleFacets(
        ICollection<MigrationCatalogFacet> facets,
        SqlServerModuleMetadata? module)
    {
        facets.Add(Facet("sqlServerModulePresent", Boolean(module is not null)));
        facets.Add(Facet("sqlServerModuleAnalysis", "unparsed"));
        if (module is null)
        {
            facets.Add(Facet("sqlServerModuleDefinitionStatus", "unavailable"));
            return;
        }

        facets.Add(Facet("sqlServerModuleObjectType", module.ObjectType));
        facets.Add(Facet(
            "sqlServerModuleObjectTypeDescription",
            module.ObjectTypeDescription));
        facets.Add(Facet(
            "sqlServerModuleParentObjectId",
            Invariant(module.ParentObjectId)));
        facets.Add(Facet(
            "sqlServerModuleUsesAnsiNulls",
            Boolean(module.UsesAnsiNulls)));
        facets.Add(Facet(
            "sqlServerModuleUsesQuotedIdentifier",
            Boolean(module.UsesQuotedIdentifier)));
        facets.Add(Facet(
            "sqlServerModuleSchemaBound",
            Boolean(module.IsSchemaBound)));
        facets.Add(Facet(
            "sqlServerModuleUsesDatabaseCollation",
            Boolean(module.UsesDatabaseCollation)));
        facets.Add(Facet(
            "sqlServerModuleRecompiled",
            Boolean(module.IsRecompiled)));
        facets.Add(Facet(
            "sqlServerModuleNullOnNullInput",
            Boolean(module.NullOnNullInput)));
        facets.Add(Facet(
            "sqlServerModuleExecuteAs",
            module.ExecuteAsPrincipalId switch
            {
                null => "caller",
                -2 => "owner",
                _ => "explicit-principal",
            }));
        facets.Add(Facet(
            "sqlServerModuleNativeCompilation",
            Boolean(module.UsesNativeCompilation)));
        facets.Add(Facet(
            "sqlServerModuleInlineable",
            Boolean(module.IsInlineable)));
        facets.Add(Facet(
            "sqlServerModuleInlineType",
            Boolean(module.InlineType)));
        facets.Add(Facet(
            "sqlServerModuleEncrypted",
            NullableBoolean(module.IsEncrypted)));
        string definitionStatus = module.Definition is not null
            ? "available-unparsed"
            : module.IsEncrypted == true
                ? "encrypted"
                : "unavailable";
        facets.Add(Facet(
            "sqlServerModuleDefinitionStatus",
            definitionStatus));
        AddDefinitionDigestFacets(
            facets,
            "sqlServerModuleDefinition",
            "csharpdb-sqlserver-module-definition/v1",
            module.DefinitionBytes,
            module.Definition);
    }

    private static void AddModuleDiagnostics(
        string objectId,
        string description,
        bool expectsSqlBody,
        SqlServerModuleMetadata? module,
        ICollection<MigrationDiagnostic> diagnostics)
    {
        if (!expectsSqlBody)
        {
            diagnostics.Add(Diagnostic(
                objectId,
                "MIG-SQLSERVER-NONSQL-MODULE-UNSUPPORTED-001",
                MigrationDiagnosticSeverity.Error,
                MigrationCompatibilityStatus.Unsupported,
                $"The {description} is not a Transact-SQL module.",
                "CLR and other non-T-SQL modules require implementation metadata and execution semantics outside the bounded SQL module analyzer.",
                "Reimplement the behavior in reviewed target application code.",
                canOverride: false));
            return;
        }

        if (module?.Definition is not null)
        {
            diagnostics.Add(Diagnostic(
                objectId,
                "MIG-SQLSERVER-MODULE-ANALYSIS-PENDING-001",
                MigrationDiagnosticSeverity.Error,
                MigrationCompatibilityStatus.Unknown,
                $"The {description} body has not been analyzed.",
                "The bounded definition is represented only by byte count, character length, and digest. It has not been parsed, bound, lowered, scratch-executed, or differentially validated.",
                "Run the later ScriptDom and live target-validation checkpoints before enabling this object.",
                canOverride: false));
            return;
        }

        bool encrypted = module?.IsEncrypted == true;
        diagnostics.Add(Diagnostic(
            objectId,
            encrypted
                ? "MIG-SQLSERVER-MODULE-ENCRYPTED-001"
                : "MIG-SQLSERVER-MODULE-DEFINITION-UNAVAILABLE-001",
            MigrationDiagnosticSeverity.Error,
            MigrationCompatibilityStatus.Unknown,
            encrypted
                ? $"The {description} definition is encrypted."
                : $"The {description} definition is unavailable.",
            encrypted
                ? "SQL Server does not expose an encrypted module body, so its behavior and dependencies cannot be analyzed."
                : "A missing module row or definition without proven encryption can indicate incomplete metadata visibility and cannot be treated as an empty body.",
            encrypted
                ? "Supply a reviewed source definition or replace the module explicitly."
                : "Restore complete definition visibility and inspect again.",
            canOverride: false));
    }

    private static void AddProgrammableQualificationDiagnostics(
        SqlServerCatalogSnapshot snapshot,
        string databaseId,
        ICollection<MigrationDiagnostic> diagnostics)
    {
        if (!snapshot.ExpressionDependencyAudit.Attempted)
        {
            diagnostics.Add(Diagnostic(
                databaseId,
                "MIG-SQLSERVER-DEPENDENCY-AUDIT-MISSING-001",
                MigrationDiagnosticSeverity.Error,
                MigrationCompatibilityStatus.Unknown,
                "SQL Server expression-dependency evidence was not captured.",
                "A bounded SELECT from sys.sql_expression_dependencies is required to inventory catalog-maintained by-name references.",
                "Grant the least privilege required to read the dependency catalog and inspect again.",
                canOverride: false));
        }
        if (snapshot.Database.HasSelectSqlExpressionDependencies != true)
        {
            diagnostics.Add(Diagnostic(
                databaseId,
                "MIG-SQLSERVER-DEPENDENCY-PERMISSION-MISSING-001",
                MigrationDiagnosticSeverity.Error,
                MigrationCompatibilityStatus.Unknown,
                "SELECT permission for SQL Server expression dependencies is not proven.",
                "VIEW DEFINITION alone does not prove that sys.sql_expression_dependencies can be read completely by a least-privilege principal.",
                "Grant and verify SELECT on sys.sql_expression_dependencies, then inspect again.",
                canOverride: false));
        }
        if (snapshot.Views.Any(static item => item.HasViewDefinition == false) ||
            snapshot.Triggers.Any(static item => item.HasViewDefinition == false) ||
            snapshot.Routines.Any(static item => item.HasViewDefinition == false))
        {
            diagnostics.Add(Diagnostic(
                databaseId,
                "MIG-SQLSERVER-PROGRAMMABLE-VIEW-DEFINITION-MISSING-001",
                MigrationDiagnosticSeverity.Error,
                MigrationCompatibilityStatus.Unknown,
                "VIEW DEFINITION evidence is incomplete for programmable objects.",
                "At least one visible view, trigger, or routine explicitly reports that its definition cannot be viewed.",
                "Grant VIEW DEFINITION at a reviewed scope and inspect again.",
                canOverride: false));
        }

        diagnostics.Add(Diagnostic(
            databaseId,
            "MIG-SQLSERVER-DEPENDENCY-COVERAGE-PARTIAL-001",
            MigrationDiagnosticSeverity.Error,
            MigrationCompatibilityStatus.Unknown,
            "SQL Server's catalog dependency coverage is not a complete execution graph.",
            "Catalog-maintained expression dependencies do not completely cover dynamic SQL, temporary objects or procedures, rules and defaults, or system objects; do not fully describe non-schema-bound column references; and do not track numbered stored procedures greater than one. Server-level modules are outside the selected-database inventory.",
            "Use bounded T-SQL parsing plus reviewed live dependency resolution before treating module order as complete.",
            canOverride: false));
    }

    private static void ValidateProgrammableCounts(
        SqlServerCatalogSnapshot snapshot,
        SqlServerInspectionLimits limits)
    {
        if (snapshot.Views.Count > limits.MaxViews)
            throw LimitExceeded("view count");
        if (snapshot.ViewColumns.Count > limits.MaxViewColumns)
            throw LimitExceeded("view-column count");
        if (snapshot.Triggers.Count > limits.MaxTriggers)
            throw LimitExceeded("trigger count");
        if (snapshot.TriggerEvents.Count > limits.MaxTriggerEvents)
            throw LimitExceeded("trigger-event count");
        if (snapshot.Routines.Count > limits.MaxRoutines)
            throw LimitExceeded("routine count");
        if (snapshot.Modules.Count > limits.MaxModules)
            throw LimitExceeded("module count");
        if (snapshot.Parameters.Count > limits.MaxParameters)
            throw LimitExceeded("parameter count");
        if (snapshot.ExpressionDependencyAudit.Dependencies.Count >
            limits.MaxExpressionDependencies)
        {
            throw LimitExceeded("expression-dependency count");
        }

        long structuralRows = checked(
            (long)snapshot.Keys.Count +
            snapshot.Indexes.Count +
            snapshot.IndexColumns.Count +
            snapshot.ForeignKeys.Count +
            snapshot.ForeignKeyColumns.Count +
            snapshot.Checks.Count +
            snapshot.Sequences.Count +
            snapshot.Views.Count +
            snapshot.ViewColumns.Count +
            snapshot.Triggers.Count +
            snapshot.TriggerEvents.Count +
            snapshot.Routines.Count +
            snapshot.Modules.Count +
            snapshot.Parameters.Count +
            snapshot.ExpressionDependencyAudit.Dependencies.Count);
        if (structuralRows > limits.MaxStructuralRowsTotal)
            throw LimitExceeded("aggregate structural-row count");
    }

    private static void ValidateProgrammableSnapshot(
        SqlServerCatalogSnapshot snapshot,
        IReadOnlySet<int> schemaIds,
        IReadOnlySet<int> tableIds,
        IReadOnlySet<(int ObjectId, int ColumnId)> tableColumnIds,
        MetadataBudget budget,
        CancellationToken cancellationToken)
    {
        var viewsById = new Dictionary<int, SqlServerViewMetadata>();
        var viewNames = new HashSet<(int SchemaId, string Name)>();
        foreach (SqlServerViewMetadata view in snapshot.Views)
        {
            cancellationToken.ThrowIfCancellationRequested();
            if (view.ObjectId <= 0 ||
                !schemaIds.Contains(view.SchemaId) ||
                !viewsById.TryAdd(view.ObjectId, view) ||
                tableIds.Contains(view.ObjectId) ||
                !viewNames.Add((view.SchemaId, view.Name)))
            {
                throw new SqlServerMigrationException(
                    "SQL Server returned duplicate, invalid, or unowned view metadata.");
            }
            bool ledgerTuplePresent =
                view.LedgerViewType is not null &&
                view.LedgerViewTypeDescription is not null &&
                view.IsDroppedLedgerView is not null;
            bool ledgerTupleAbsent =
                view.LedgerViewType is null &&
                view.LedgerViewTypeDescription is null &&
                view.IsDroppedLedgerView is null;
            bool validLedgerTuple =
                view.LedgerViewType is 0 or 1 &&
                string.Equals(
                    view.LedgerViewTypeDescription,
                    view.LedgerViewType == 0
                        ? "NON_LEDGER_VIEW"
                        : "LEDGER_VIEW",
                    StringComparison.Ordinal) &&
                !(view.LedgerViewType == 0 &&
                  view.IsDroppedLedgerView == true);
            if (snapshot.Instance.ProductMajorVersion >= 16
                    ? !ledgerTuplePresent || !validLedgerTuple
                    : !ledgerTupleAbsent)
            {
                throw new SqlServerMigrationException(
                    "SQL Server returned invalid ledger-view metadata.");
            }
            budget.Add(view.Name, isName: true);
            budget.Add(view.LedgerViewTypeDescription);
        }

        var viewColumnIds = new HashSet<(int ObjectId, int ColumnId)>();
        var viewColumnNames = new HashSet<(int ObjectId, string Name)>();
        foreach (SqlServerViewColumnMetadata column in snapshot.ViewColumns)
        {
            cancellationToken.ThrowIfCancellationRequested();
            if (!viewsById.ContainsKey(column.ObjectId) ||
                column.ColumnId <= 0 ||
                !viewColumnIds.Add((column.ObjectId, column.ColumnId)) ||
                tableColumnIds.Contains((column.ObjectId, column.ColumnId)) ||
                !viewColumnNames.Add((column.ObjectId, column.Name)))
            {
                throw new SqlServerMigrationException(
                    "SQL Server returned duplicate, invalid, or unowned view-column metadata.");
            }
            budget.Add(column.Name, isName: true);
            budget.Add(column.TypeSchema, isName: true);
            budget.Add(column.TypeName, isName: true);
            budget.Add(column.SystemTypeName, isName: true);
            budget.Add(column.Collation);
            budget.Add(column.EncryptionType);
            ValidateViewColumnShape(column);
        }

        var triggersById = new Dictionary<int, SqlServerTriggerMetadata>();
        var triggerNames = new HashSet<(byte ParentClass, int SchemaId, string Name)>();
        foreach (SqlServerTriggerMetadata trigger in snapshot.Triggers)
        {
            cancellationToken.ThrowIfCancellationRequested();
            bool validParent = trigger.ParentClass switch
            {
                0 => trigger.ParentObjectId == 0 && trigger.SchemaId is null,
                1 => trigger.ParentObjectId > 0 &&
                    (tableIds.Contains(trigger.ParentObjectId) ||
                     viewsById.ContainsKey(trigger.ParentObjectId)) &&
                    trigger.SchemaId is int schemaId &&
                    schemaIds.Contains(schemaId),
                _ => false,
            };
            int nameSchemaId = trigger.SchemaId ?? 0;
            if (trigger.ObjectId <= 0 ||
                !validParent ||
                !triggersById.TryAdd(trigger.ObjectId, trigger) ||
                !triggerNames.Add((
                    trigger.ParentClass,
                    nameSchemaId,
                    trigger.Name)))
            {
                throw new SqlServerMigrationException(
                    "SQL Server returned duplicate, invalid, or unowned trigger metadata.");
            }
            budget.Add(trigger.ParentClassDescription);
            budget.Add(trigger.Name, isName: true);
            budget.Add(trigger.Type);
            budget.Add(trigger.TypeDescription);
        }

        var triggerEventIds = new HashSet<(int ObjectId, int Type)>();
        foreach (SqlServerTriggerEventMetadata triggerEvent in snapshot.TriggerEvents)
        {
            cancellationToken.ThrowIfCancellationRequested();
            if (!triggersById.ContainsKey(triggerEvent.ObjectId) ||
                triggerEvent.Type <= 0 ||
                triggerEvent.EventGroupType < 0 ||
                !triggerEventIds.Add((triggerEvent.ObjectId, triggerEvent.Type)))
            {
                throw new SqlServerMigrationException(
                    "SQL Server returned duplicate, invalid, or unowned trigger-event metadata.");
            }
            budget.Add(triggerEvent.TypeDescription);
            budget.Add(triggerEvent.EventGroupTypeDescription);
        }

        var routinesById = new Dictionary<int, SqlServerRoutineMetadata>();
        var routineNames = new HashSet<(int SchemaId, string Name)>();
        foreach (SqlServerRoutineMetadata routine in snapshot.Routines)
        {
            cancellationToken.ThrowIfCancellationRequested();
            if (routine.ObjectId <= 0 ||
                !schemaIds.Contains(routine.SchemaId) ||
                !routinesById.TryAdd(routine.ObjectId, routine) ||
                !routineNames.Add((routine.SchemaId, routine.Name)) ||
                tableIds.Contains(routine.ObjectId) ||
                viewsById.ContainsKey(routine.ObjectId) ||
                triggersById.ContainsKey(routine.ObjectId))
            {
                throw new SqlServerMigrationException(
                    "SQL Server returned duplicate, invalid, or unowned routine metadata.");
            }
            budget.Add(routine.Name, isName: true);
            budget.Add(routine.Type);
            budget.Add(routine.TypeDescription);
        }

        var modulesById = new Dictionary<int, SqlServerModuleMetadata>();
        foreach (SqlServerModuleMetadata module in snapshot.Modules)
        {
            cancellationToken.ThrowIfCancellationRequested();
            bool databaseTriggerModule =
                module.SchemaId == 0 &&
                triggersById.TryGetValue(
                    module.ObjectId,
                    out SqlServerTriggerMetadata? trigger) &&
                trigger.ParentClass == 0;
            bool consistentIdentity;
            if (viewsById.TryGetValue(
                    module.ObjectId,
                    out SqlServerViewMetadata? view))
            {
                consistentIdentity =
                    module.SchemaId == view.SchemaId &&
                    module.ParentObjectId == 0 &&
                    string.Equals(
                        module.Name,
                        view.Name,
                        StringComparison.Ordinal) &&
                    string.Equals(
                        module.ObjectType,
                        "V",
                        StringComparison.Ordinal);
            }
            else if (triggersById.TryGetValue(
                         module.ObjectId,
                         out SqlServerTriggerMetadata? moduleTrigger))
            {
                consistentIdentity =
                    module.SchemaId == (moduleTrigger.SchemaId ?? 0) &&
                    module.ParentObjectId == moduleTrigger.ParentObjectId &&
                    string.Equals(
                        module.Name,
                        moduleTrigger.Name,
                        StringComparison.Ordinal) &&
                    string.Equals(
                        module.ObjectType,
                        moduleTrigger.Type,
                        StringComparison.Ordinal);
            }
            else if (routinesById.TryGetValue(
                         module.ObjectId,
                         out SqlServerRoutineMetadata? routine))
            {
                consistentIdentity =
                    module.SchemaId == routine.SchemaId &&
                    module.ParentObjectId == 0 &&
                    string.Equals(
                        module.Name,
                        routine.Name,
                        StringComparison.Ordinal) &&
                    string.Equals(
                        module.ObjectType,
                        routine.Type,
                        StringComparison.Ordinal);
            }
            else
            {
                consistentIdentity =
                    module.ObjectType is "R" or "D" &&
                    schemaIds.Contains(module.SchemaId) &&
                    module.ParentObjectId == 0;
            }
            if (module.ObjectId <= 0 ||
                (!schemaIds.Contains(module.SchemaId) && !databaseTriggerModule) ||
                !consistentIdentity ||
                !modulesById.TryAdd(module.ObjectId, module))
            {
                throw new SqlServerMigrationException(
                    "SQL Server returned duplicate, invalid, or unowned module metadata.");
            }
            if (module.IsEncrypted == true && module.Definition is not null)
            {
                throw new SqlServerMigrationException(
                    "SQL Server returned clear text for a module marked encrypted.");
            }
            if (module.ExecuteAsPrincipalId is int executeAs &&
                (executeAs < -2 || executeAs == -1))
            {
                throw new SqlServerMigrationException(
                    "SQL Server returned invalid module execution-context metadata.");
            }
            budget.Add(module.Name, isName: true);
            budget.Add(module.ObjectType);
            budget.Add(module.ObjectTypeDescription);
            budget.ReserveExpression(module.DefinitionBytes);
            budget.AddExpression(module.Definition);
            ValidateDefinitionLength(
                module.Definition,
                module.DefinitionBytes,
                "module");
        }

        ValidateGlobalObjectIdOwnership(
            snapshot,
            cancellationToken);

        var parameterIds = new HashSet<(int ObjectId, int ParameterId)>();
        var parameterNames = new HashSet<(int ObjectId, string Name)>();
        foreach (SqlServerParameterMetadata parameter in snapshot.Parameters)
        {
            cancellationToken.ThrowIfCancellationRequested();
            bool validName =
                parameter.ParameterId == 0
                    ? string.IsNullOrEmpty(parameter.Name)
                    : !string.IsNullOrWhiteSpace(parameter.Name);
            if (!routinesById.ContainsKey(parameter.ObjectId) ||
                parameter.ParameterId < 0 ||
                !validName ||
                parameter.MaxLength < -1 ||
                parameter.Scale > parameter.Precision && parameter.Precision != 0 ||
                parameter.XmlCollectionId < 0 ||
                !parameterIds.Add((
                    parameter.ObjectId,
                    parameter.ParameterId)) ||
                !parameterNames.Add((parameter.ObjectId, parameter.Name)))
            {
                throw new SqlServerMigrationException(
                    "SQL Server returned duplicate, invalid, or unowned parameter metadata.");
            }
            ValidateParameterShape(parameter);
            budget.Add(parameter.Name, isName: true);
            budget.Add(parameter.TypeSchema, isName: true);
            budget.Add(parameter.TypeName, isName: true);
            budget.Add(parameter.SystemTypeName, isName: true);
            budget.Add(parameter.EncryptionType);
        }
        foreach (SqlServerRoutineMetadata routine in snapshot.Routines)
        {
            cancellationToken.ThrowIfCancellationRequested();
            bool hasScalarReturnParameter =
                parameterIds.Contains((routine.ObjectId, 0));
            if ((routine.Type is "FN" or "FS") !=
                hasScalarReturnParameter)
            {
                throw new SqlServerMigrationException(
                    "SQL Server returned invalid scalar-return parameter metadata.");
            }
        }

        SqlServerExpressionDependencyAuditMetadata audit =
            snapshot.ExpressionDependencyAudit;
        if ((snapshot.Database.HasSelectSqlExpressionDependencies == true) !=
                audit.Attempted ||
            !audit.Attempted && audit.Dependencies.Count != 0)
        {
            throw new SqlServerMigrationException(
                "SQL Server returned inconsistent expression-dependency audit metadata.");
        }
        var dependencies = new HashSet<SqlServerExpressionDependencyMetadata>();
        foreach (SqlServerExpressionDependencyMetadata dependency in
                 audit.Dependencies)
        {
            cancellationToken.ThrowIfCancellationRequested();
            bool inconsistentSchemaBinding =
                dependency.IsSchemaBoundReference &&
                (dependency.ReferencedId is null ||
                 dependency.ReferencedServerName is not null ||
                 dependency.ReferencedDatabaseName is not null);
            if (dependency.ReferencingId <= 0 ||
                dependency.ReferencingMinorId < 0 ||
                dependency.ReferencingClass == 0 ||
                dependency.ReferencedClass == 0 ||
                dependency.ReferencedId <= 0 ||
                dependency.ReferencedMinorId < 0 ||
                string.IsNullOrWhiteSpace(dependency.ReferencedEntityName) ||
                inconsistentSchemaBinding ||
                !dependencies.Add(dependency))
            {
                throw new SqlServerMigrationException(
                    "SQL Server returned duplicate or invalid expression-dependency metadata.");
            }
            budget.Add(dependency.ReferencingClassDescription);
            budget.Add(dependency.ReferencedClassDescription);
            budget.Add(dependency.ReferencedServerName, isName: true);
            budget.Add(dependency.ReferencedDatabaseName, isName: true);
            budget.Add(dependency.ReferencedSchemaName, isName: true);
            budget.Add(dependency.ReferencedEntityName, isName: true);
        }
    }

    private static void ValidateGlobalObjectIdOwnership(
        SqlServerCatalogSnapshot snapshot,
        CancellationToken cancellationToken)
    {
        var owners = new Dictionary<int, string>();
        var classifiedModuleIds = new HashSet<int>();

        void Register(int objectId, string objectType)
        {
            if (!owners.TryAdd(objectId, objectType))
            {
                throw new SqlServerMigrationException(
                    "SQL Server returned colliding global object identifiers.");
            }
        }

        foreach (SqlServerTableMetadata item in snapshot.Tables)
        {
            cancellationToken.ThrowIfCancellationRequested();
            Register(item.ObjectId, "table");
        }
        foreach (SqlServerViewMetadata item in snapshot.Views)
        {
            cancellationToken.ThrowIfCancellationRequested();
            Register(item.ObjectId, "view");
            classifiedModuleIds.Add(item.ObjectId);
        }
        foreach (SqlServerTriggerMetadata item in snapshot.Triggers)
        {
            cancellationToken.ThrowIfCancellationRequested();
            Register(item.ObjectId, "trigger");
            classifiedModuleIds.Add(item.ObjectId);
        }
        foreach (SqlServerRoutineMetadata item in snapshot.Routines)
        {
            cancellationToken.ThrowIfCancellationRequested();
            Register(item.ObjectId, "routine");
            classifiedModuleIds.Add(item.ObjectId);
        }
        foreach (SqlServerKeyMetadata item in snapshot.Keys)
        {
            cancellationToken.ThrowIfCancellationRequested();
            Register(item.ObjectId, "key");
        }
        foreach (SqlServerForeignKeyMetadata item in snapshot.ForeignKeys)
        {
            cancellationToken.ThrowIfCancellationRequested();
            Register(item.ObjectId, "foreign-key");
        }
        foreach (SqlServerCheckMetadata item in snapshot.Checks)
        {
            cancellationToken.ThrowIfCancellationRequested();
            Register(item.ObjectId, "check");
        }
        foreach (SqlServerSequenceMetadata item in snapshot.Sequences)
        {
            cancellationToken.ThrowIfCancellationRequested();
            Register(item.ObjectId, "sequence");
        }
        foreach (SqlServerModuleMetadata item in snapshot.Modules)
        {
            cancellationToken.ThrowIfCancellationRequested();
            if (!classifiedModuleIds.Contains(item.ObjectId))
                Register(item.ObjectId, "legacy-module");
        }
    }

    private static void ValidateParameterShape(
        SqlServerParameterMetadata parameter)
    {
        if (parameter.IsAssemblyType || parameter.IsTableType)
            return;

        string systemType = parameter.SystemTypeName.ToLowerInvariant();
        bool permitsMax =
            systemType is "varchar" or "nvarchar" or "varbinary" or "xml";
        if (parameter.MaxLength == -1 && !permitsMax)
        {
            throw new SqlServerMigrationException(
                "SQL Server returned invalid max-length metadata for a parameter.");
        }
        if (IsLengthType(systemType) &&
            (parameter.MaxLength == 0 ||
             systemType is "nchar" or "nvarchar" &&
             parameter.MaxLength != -1 &&
             parameter.MaxLength % 2 != 0))
        {
            throw new SqlServerMigrationException(
                "SQL Server returned invalid length metadata for a parameter.");
        }
        if (systemType is "decimal" or "numeric" &&
            (parameter.Precision is < 1 or > 38 ||
             parameter.Scale > parameter.Precision))
        {
            throw new SqlServerMigrationException(
                "SQL Server returned invalid decimal metadata for a parameter.");
        }
        if (systemType is "time" or "datetime2" or "datetimeoffset" &&
            parameter.Scale > 7)
        {
            throw new SqlServerMigrationException(
                "SQL Server returned invalid temporal metadata for a parameter.");
        }
    }

    private static IEnumerable<string?> ProgrammableSnapshotFields(
        SqlServerCatalogSnapshot snapshot)
    {
        foreach (SqlServerViewMetadata view in snapshot.Views
                     .OrderBy(static item => item.ObjectId))
        {
            yield return "view";
            yield return Invariant(view.ObjectId);
            yield return Invariant(view.SchemaId);
            yield return view.Name;
            yield return Boolean(view.IsReplicated);
            yield return Boolean(view.HasReplicationFilter);
            yield return Boolean(view.HasOpaqueMetadata);
            yield return Boolean(view.HasUncheckedAssemblyData);
            yield return Boolean(view.WithCheckOption);
            yield return Boolean(view.IsDateCorrelationView);
            yield return Boolean(view.IsIndexed);
            yield return NullableBoolean(view.HasViewDefinition);
            yield return view.LedgerViewType is null
                ? null
                : Invariant(view.LedgerViewType.Value);
            yield return view.LedgerViewTypeDescription;
            yield return NullableBoolean(view.IsDroppedLedgerView);
        }
        foreach (SqlServerViewColumnMetadata column in snapshot.ViewColumns
                     .OrderBy(static item => item.ObjectId)
                     .ThenBy(static item => item.ColumnId))
        {
            yield return "view-column";
            yield return Invariant(column.ObjectId);
            yield return Invariant(column.ColumnId);
            yield return column.Name;
            yield return column.TypeSchema;
            yield return column.TypeName;
            yield return column.SystemTypeName;
            yield return Invariant(column.MaxLength);
            yield return Invariant(column.Precision);
            yield return Invariant(column.Scale);
            yield return column.Collation;
            yield return Boolean(column.IsNullable);
            yield return Boolean(column.IsAnsiPadded);
            yield return Boolean(column.IsHidden);
            yield return Boolean(column.IsMasked);
            yield return column.EncryptionType;
            yield return Boolean(column.IsXmlDocument);
            yield return Invariant(column.XmlCollectionId);
        }
        foreach (SqlServerTriggerMetadata trigger in snapshot.Triggers
                     .OrderBy(static item => item.ObjectId))
        {
            yield return "trigger";
            yield return Invariant(trigger.ObjectId);
            yield return trigger.SchemaId is null
                ? null
                : Invariant(trigger.SchemaId.Value);
            yield return Invariant(trigger.ParentClass);
            yield return trigger.ParentClassDescription;
            yield return Invariant(trigger.ParentObjectId);
            yield return trigger.Name;
            yield return trigger.Type;
            yield return trigger.TypeDescription;
            yield return Boolean(trigger.IsDisabled);
            yield return Boolean(trigger.IsNotForReplication);
            yield return Boolean(trigger.IsInsteadOfTrigger);
            yield return NullableBoolean(trigger.IsInsert);
            yield return NullableBoolean(trigger.IsUpdate);
            yield return NullableBoolean(trigger.IsDelete);
            yield return NullableBoolean(trigger.IsFirstInsert);
            yield return NullableBoolean(trigger.IsLastInsert);
            yield return NullableBoolean(trigger.IsFirstUpdate);
            yield return NullableBoolean(trigger.IsLastUpdate);
            yield return NullableBoolean(trigger.IsFirstDelete);
            yield return NullableBoolean(trigger.IsLastDelete);
            yield return NullableBoolean(trigger.HasViewDefinition);
        }
        foreach (SqlServerTriggerEventMetadata triggerEvent in
                 snapshot.TriggerEvents
                     .OrderBy(static item => item.ObjectId)
                     .ThenBy(static item => item.Type))
        {
            yield return "trigger-event";
            yield return Invariant(triggerEvent.ObjectId);
            yield return Invariant(triggerEvent.Type);
            yield return triggerEvent.TypeDescription;
            yield return Boolean(triggerEvent.IsFirst);
            yield return Boolean(triggerEvent.IsLast);
            yield return triggerEvent.EventGroupType is null
                ? null
                : Invariant(triggerEvent.EventGroupType.Value);
            yield return triggerEvent.EventGroupTypeDescription;
        }
        foreach (SqlServerRoutineMetadata routine in snapshot.Routines
                     .OrderBy(static item => item.ObjectId))
        {
            yield return "routine";
            yield return Invariant(routine.ObjectId);
            yield return Invariant(routine.SchemaId);
            yield return routine.Name;
            yield return routine.Type;
            yield return routine.TypeDescription;
            yield return NullableBoolean(routine.IsAutoExecuted);
            yield return NullableBoolean(routine.IsExecutionReplicated);
            yield return NullableBoolean(
                routine.IsReplicationSerializableOnly);
            yield return NullableBoolean(routine.SkipsReplicationConstraints);
            yield return NullableBoolean(routine.HasViewDefinition);
        }
        foreach (SqlServerModuleMetadata module in snapshot.Modules
                     .OrderBy(static item => item.ObjectId))
        {
            yield return "module";
            yield return Invariant(module.ObjectId);
            yield return Invariant(module.SchemaId);
            yield return Invariant(module.ParentObjectId);
            yield return module.Name;
            yield return module.ObjectType;
            yield return module.ObjectTypeDescription;
            yield return module.DefinitionBytes is null
                ? null
                : Invariant(module.DefinitionBytes.Value);
            yield return module.Definition;
            yield return Boolean(module.UsesAnsiNulls);
            yield return Boolean(module.UsesQuotedIdentifier);
            yield return Boolean(module.IsSchemaBound);
            yield return Boolean(module.UsesDatabaseCollation);
            yield return Boolean(module.IsRecompiled);
            yield return Boolean(module.NullOnNullInput);
            yield return module.ExecuteAsPrincipalId is null
                ? null
                : Invariant(module.ExecuteAsPrincipalId.Value);
            yield return Boolean(module.UsesNativeCompilation);
            yield return Boolean(module.IsInlineable);
            yield return Boolean(module.InlineType);
            yield return NullableBoolean(module.IsEncrypted);
        }
        foreach (SqlServerParameterMetadata parameter in snapshot.Parameters
                     .OrderBy(static item => item.ObjectId)
                     .ThenBy(static item => item.ParameterId))
        {
            yield return "parameter";
            yield return Invariant(parameter.ObjectId);
            yield return Invariant(parameter.ParameterId);
            yield return parameter.Name;
            yield return parameter.TypeSchema;
            yield return parameter.TypeName;
            yield return parameter.SystemTypeName;
            yield return Invariant(parameter.MaxLength);
            yield return Invariant(parameter.Precision);
            yield return Invariant(parameter.Scale);
            yield return Boolean(parameter.IsOutput);
            yield return Boolean(parameter.IsCursorReference);
            yield return Boolean(parameter.HasDefaultValue);
            yield return Boolean(parameter.IsXmlDocument);
            yield return Invariant(parameter.XmlCollectionId);
            yield return Boolean(parameter.IsReadOnly);
            yield return Boolean(parameter.IsNullable);
            yield return parameter.EncryptionType;
            yield return Boolean(parameter.IsUserDefined);
            yield return Boolean(parameter.IsAssemblyType);
            yield return Boolean(parameter.IsTableType);
        }

        yield return "expression-dependency-audit";
        foreach (string? field in ExpressionDependencyAuditFields(
                     snapshot.ExpressionDependencyAudit))
        {
            yield return field;
        }
    }

    private static string ExpressionDependencyAuditDigest(
        SqlServerExpressionDependencyAuditMetadata audit) =>
        "sha256:" + SqlServerStableDigest.Sequence(
            "csharpdb-sqlserver-expression-dependency-audit/v1",
            ExpressionDependencyAuditFields(audit));

    private static IEnumerable<string?> ExpressionDependencyAuditFields(
        SqlServerExpressionDependencyAuditMetadata audit)
    {
        yield return Boolean(audit.Attempted);
        foreach (SqlServerExpressionDependencyMetadata dependency in
                 audit.Dependencies
                     .OrderBy(static item => item.ReferencingClass)
                     .ThenBy(static item => item.ReferencingId)
                     .ThenBy(static item => item.ReferencingMinorId)
                     .ThenBy(static item => item.ReferencedClass)
                     .ThenBy(static item => item.ReferencedServerName, StringComparer.Ordinal)
                     .ThenBy(static item => item.ReferencedDatabaseName, StringComparer.Ordinal)
                     .ThenBy(static item => item.ReferencedSchemaName, StringComparer.Ordinal)
                     .ThenBy(static item => item.ReferencedEntityName, StringComparer.Ordinal)
                     .ThenBy(static item => item.ReferencedId)
                     .ThenBy(static item => item.ReferencedMinorId)
                     .ThenBy(static item => item.IsCallerDependent)
                     .ThenBy(static item => item.IsAmbiguous))
        {
            yield return "dependency";
            foreach (string? field in ExpressionDependencyFields(dependency))
                yield return field;
        }
    }

    private static string ExpressionDependencyRowDigest(
        SqlServerExpressionDependencyMetadata dependency) =>
        SqlServerStableDigest.Sequence(
            "csharpdb-sqlserver-expression-dependency-row/v1",
            ExpressionDependencyFields(dependency));

    private static IEnumerable<string?> ExpressionDependencyFields(
        SqlServerExpressionDependencyMetadata dependency)
    {
        yield return Invariant(dependency.ReferencingId);
        yield return Invariant(dependency.ReferencingMinorId);
        yield return Invariant(dependency.ReferencingClass);
        yield return dependency.ReferencingClassDescription;
        yield return Boolean(dependency.IsSchemaBoundReference);
        yield return Invariant(dependency.ReferencedClass);
        yield return dependency.ReferencedClassDescription;
        yield return dependency.ReferencedServerName;
        yield return dependency.ReferencedDatabaseName;
        yield return dependency.ReferencedSchemaName;
        yield return dependency.ReferencedEntityName;
        yield return dependency.ReferencedId is null
            ? null
            : Invariant(dependency.ReferencedId.Value);
        yield return Invariant(dependency.ReferencedMinorId);
        yield return Boolean(dependency.IsCallerDependent);
        yield return Boolean(dependency.IsAmbiguous);
    }

    private sealed record ResolvedDependencyEndpointEdge(
        string From,
        string To,
        string RowDigest);

    private static void AddViewColumnLogicalFacets(
        ICollection<MigrationCatalogFacet> facets,
        SqlServerViewColumnMetadata column)
    {
        string type = column.SystemTypeName.ToLowerInvariant();
        if (type is "decimal" or "numeric" or "money" or "smallmoney")
        {
            facets.Add(Facet("precision", Invariant(column.Precision)));
            facets.Add(Facet("scale", Invariant(column.Scale)));
        }
        if (type is "time" or "datetime2" or "datetimeoffset")
            facets.Add(Facet("fractionalSeconds", Invariant(column.Scale)));
        if (IsLengthType(type))
        {
            facets.Add(Facet(
                "maxLength",
                column.MaxLength < 0
                    ? "max"
                    : Invariant(type is "nchar" or "nvarchar"
                        ? column.MaxLength / 2
                        : column.MaxLength)));
        }
    }

    private static string FormatViewColumnNativeType(
        SqlServerViewColumnMetadata column)
    {
        string type = $"{column.TypeSchema}.{column.TypeName}";
        if (!string.Equals(column.TypeSchema, "sys", StringComparison.Ordinal) ||
            !string.Equals(
                column.TypeName,
                column.SystemTypeName,
                StringComparison.Ordinal))
        {
            return type;
        }

        string systemType = column.SystemTypeName.ToLowerInvariant();
        if (systemType is "decimal" or "numeric")
        {
            return $"{type}({Invariant(column.Precision)},{Invariant(column.Scale)})";
        }
        if (systemType is "time" or "datetime2" or "datetimeoffset")
            return $"{type}({Invariant(column.Scale)})";
        if (IsLengthType(systemType))
        {
            string length = column.MaxLength < 0
                ? "max"
                : Invariant(systemType is "nchar" or "nvarchar"
                    ? column.MaxLength / 2
                    : column.MaxLength);
            return $"{type}({length})";
        }
        return type;
    }

    private static void ValidateViewColumnShape(
        SqlServerViewColumnMetadata column)
    {
        string systemType = column.SystemTypeName.ToLowerInvariant();
        if (IsLengthType(systemType))
        {
            bool permitsMax =
                systemType is "varchar" or "nvarchar" or "varbinary";
            if (column.MaxLength == 0 ||
                column.MaxLength < -1 ||
                column.MaxLength == -1 && !permitsMax ||
                systemType is "nchar" or "nvarchar" &&
                column.MaxLength != -1 &&
                column.MaxLength % 2 != 0)
            {
                throw new SqlServerMigrationException(
                    "SQL Server returned invalid length metadata for a view column.");
            }
        }
        if (systemType is "decimal" or "numeric" &&
            (column.Precision is < 1 or > 38 ||
             column.Scale > column.Precision))
        {
            throw new SqlServerMigrationException(
                "SQL Server returned invalid decimal metadata for a view column.");
        }
        if (systemType is "time" or "datetime2" or "datetimeoffset" &&
            column.Scale > 7)
        {
            throw new SqlServerMigrationException(
                "SQL Server returned invalid temporal metadata for a view column.");
        }
        if (column.XmlCollectionId < 0)
        {
            throw new SqlServerMigrationException(
                "SQL Server returned invalid XML metadata for a view column.");
        }
    }
}
