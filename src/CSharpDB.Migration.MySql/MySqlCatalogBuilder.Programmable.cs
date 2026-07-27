using CSharpDB.Migration;

namespace CSharpDB.Migration.MySql;

internal static partial class MySqlCatalogBuilder
{
    private static int ProgrammableObjectCapacity(
        MySqlCatalogSnapshot snapshot) =>
        checked(
            snapshot.Views.Count +
            snapshot.ViewColumns.Count +
            snapshot.Triggers.Count +
            snapshot.Routines.Count +
            snapshot.RoutineParameters.Count);

    private static void AddProgrammableObjects(
        MySqlCatalogSnapshot snapshot,
        string namespaceId,
        IReadOnlyDictionary<
            string,
            (MySqlTableMetadata Metadata, string Id)> tablesByIdentity,
        ICollection<MigrationCatalogObject> objects,
        ICollection<MigrationDiagnostic> diagnostics,
        CancellationToken cancellationToken)
    {
        int lowerCaseTableNames = snapshot.Server.LowerCaseTableNames;
        IReadOnlyDictionary<string, int> viewColumnCounts =
            snapshot.ViewColumns
                .GroupBy(column => ViewIdentity(
                    column.SchemaName,
                    column.ViewName,
                    lowerCaseTableNames))
                .ToDictionary(
                    static group => group.Key,
                    static group => group.Count(),
                    StringComparer.Ordinal);
        var viewsByIdentity =
            new Dictionary<
                string,
                (MySqlViewMetadata Metadata, string Id)>(
                StringComparer.Ordinal);
        foreach (MySqlViewMetadata view in OrderedViews(snapshot))
        {
            cancellationToken.ThrowIfCancellationRequested();
            string viewIdentity = ViewIdentity(
                view.SchemaName,
                view.Name,
                lowerCaseTableNames);
            string viewId = ObjectId(
                "view",
                view.SchemaName,
                view.Name);
            viewsByIdentity.Add(viewIdentity, (view, viewId));
            var facets = new List<MigrationCatalogFacet>
            {
                Facet("mysqlViewInventoryOnly", "true"),
                Facet(
                    "mysqlViewMetadataVisible",
                    Boolean(view.MetadataVisible)),
                Facet(
                    "mysqlViewDefinitionStatus",
                    view.Definition is null
                        ? "unavailable"
                        : "available"),
                Facet("mysqlViewCheckOption", view.CheckOption),
                Facet(
                    "mysqlViewUpdatable",
                    NullableBoolean(view.IsUpdatable)),
                Facet("mysqlViewSecurityType", view.SecurityType),
                Facet(
                    "mysqlViewCharacterSetClient",
                    view.CharacterSetClient),
                Facet(
                    "mysqlViewCollationConnection",
                    view.CollationConnection),
            };
            if (view.Definition is not null)
            {
                AddDefinitionDigestFacets(
                    facets,
                    "mysqlViewDefinition",
                    "csharpdb-mysql-view-definition/v1",
                    view.DefinitionBytes!.Value,
                    view.Definition);
            }
            objects.Add(new MigrationCatalogObject
            {
                ObjectId = viewId,
                Kind = MigrationObjectKind.View,
                ParentObjectId = namespaceId,
                SourceNamespace = view.SchemaName,
                SourceName = view.Name,
                Facets = facets.AsReadOnly(),
            });
            AddViewDiagnostics(
                view,
                viewId,
                viewColumnCounts.GetValueOrDefault(viewIdentity),
                diagnostics);
        }

        foreach (MySqlViewColumnMetadata column in
                 OrderedViewColumns(snapshot))
        {
            cancellationToken.ThrowIfCancellationRequested();
            string viewIdentity = ViewIdentity(
                column.SchemaName,
                column.ViewName,
                lowerCaseTableNames);
            (MySqlViewMetadata view, string viewId) =
                viewsByIdentity[viewIdentity];
            MySqlColumnMetadata typeShape = ViewColumnTypeShape(column);
            string logicalType = LogicalType(typeShape);
            var facets = new List<MigrationCatalogFacet>
            {
                Facet("logicalType", logicalType),
                Facet("nullable", Boolean(column.IsNullable)),
                Facet("identity", "false"),
                Facet(
                    "mysqlOrdinalPosition",
                    Invariant(column.OrdinalPosition)),
                Facet("mysqlDataType", column.DataType),
                Facet(
                    "mysqlColumnTypeBytes",
                    Invariant(column.ColumnTypeBytes)),
                Facet(
                    "mysqlColumnTypeDigest",
                    "sha256:" + MySqlStableDigest.Text(
                        "csharpdb-mysql-column-type/v1",
                        column.ColumnType)),
                Facet(
                    "mysqlUnsigned",
                    Boolean(typeShape.IsUnsigned)),
                Facet(
                    "mysqlZerofill",
                    Boolean(typeShape.IsZerofill)),
                Facet(
                    "mysqlTinyIntOne",
                    Boolean(typeShape.IsTinyIntOne)),
                Facet("mysqlCharacterSet", column.CharacterSetName),
                Facet("mysqlCollation", column.CollationName),
                Facet("mysqlViewOutputColumn", "true"),
            };
            AddLogicalFacets(facets, typeShape);
            objects.Add(new MigrationCatalogObject
            {
                ObjectId = ObjectId(
                    "column",
                    view.SchemaName,
                    view.Name,
                    column.Name),
                Kind = MigrationObjectKind.Column,
                ParentObjectId = viewId,
                SourceNamespace = column.SchemaName,
                SourceName = column.Name,
                NativeType = FormatNativeType(typeShape),
                Facets = facets.AsReadOnly(),
            });
        }

        foreach (MySqlTriggerMetadata trigger in OrderedTriggers(snapshot))
        {
            cancellationToken.ThrowIfCancellationRequested();
            string tableIdentity = TableIdentity(
                trigger.EventObjectSchema,
                trigger.EventObjectTable,
                lowerCaseTableNames);
            (_, string tableId) = tablesByIdentity[tableIdentity];
            string triggerId = ObjectId(
                "trigger",
                trigger.SchemaName,
                trigger.Name);
            var facets = new List<MigrationCatalogFacet>
            {
                Facet(
                    "mysqlTriggerEventManipulation",
                    trigger.EventManipulation),
                Facet(
                    "mysqlTriggerEventObjectSchema",
                    trigger.EventObjectSchema),
                Facet(
                    "mysqlTriggerEventObjectTable",
                    trigger.EventObjectTable),
                Facet(
                    "mysqlTriggerActionOrder",
                    Invariant(trigger.ActionOrder)),
                Facet(
                    "mysqlTriggerActionOrientation",
                    trigger.ActionOrientation),
                Facet(
                    "mysqlTriggerActionTiming",
                    trigger.ActionTiming),
                Facet("mysqlTriggerSqlMode", trigger.SqlMode),
                Facet(
                    "mysqlTriggerCharacterSetClient",
                    trigger.CharacterSetClient),
                Facet(
                    "mysqlTriggerCollationConnection",
                    trigger.CollationConnection),
                Facet(
                    "mysqlTriggerDatabaseCollation",
                    trigger.DatabaseCollation),
            };
            AddDefinitionDigestFacets(
                facets,
                "mysqlTriggerActionStatement",
                "csharpdb-mysql-trigger-action-statement/v1",
                trigger.ActionStatementBytes,
                trigger.ActionStatement);
            objects.Add(new MigrationCatalogObject
            {
                ObjectId = triggerId,
                Kind = MigrationObjectKind.Trigger,
                ParentObjectId = tableId,
                SourceNamespace = trigger.SchemaName,
                SourceName = trigger.Name,
                Facets = facets.AsReadOnly(),
            });
            diagnostics.Add(Diagnostic(
                triggerId,
                "MIG-MYSQL-TRIGGER-UNSUPPORTED-001",
                MigrationDiagnosticSeverity.Error,
                MigrationCompatibilityStatus.Unsupported,
                "MySQL triggers do not have an automatic target lowering.",
                "The trigger body is retained only as bounded digest evidence. Its timing, ordering, row context, SQL mode, side effects, and error behavior have not been parsed or proven equivalent.",
                "Reimplement and test the behavior explicitly against the target application contract.",
                canOverride: false));
        }

        var routinesByIdentity =
            new Dictionary<
                string,
                (MySqlRoutineMetadata Metadata, string Id)>(
                StringComparer.Ordinal);
        foreach (MySqlRoutineMetadata routine in OrderedRoutines(snapshot))
        {
            cancellationToken.ThrowIfCancellationRequested();
            string routineIdentity = RoutineIdentity(
                routine.SchemaName,
                routine.SpecificName,
                routine.RoutineType,
                lowerCaseTableNames);
            string routineId = ObjectId(
                "routine",
                routine.SchemaName,
                routine.RoutineType,
                routine.SpecificName);
            routinesByIdentity.Add(
                routineIdentity,
                (routine, routineId));
            var facets = new List<MigrationCatalogFacet>
            {
                Facet("mysqlRoutineSpecificName", routine.SpecificName),
                Facet("mysqlRoutineType", routine.RoutineType),
                Facet("mysqlRoutineDataType", routine.DataType),
                Facet("mysqlRoutineBody", routine.RoutineBody),
                Facet(
                    "mysqlRoutineDefinitionStatus",
                    routine.Definition is null
                        ? "unavailable"
                        : "available"),
                Facet(
                    "mysqlRoutineDeterministic",
                    Boolean(routine.IsDeterministic)),
                Facet(
                    "mysqlRoutineSqlDataAccess",
                    routine.SqlDataAccess),
                Facet(
                    "mysqlRoutineSecurityType",
                    routine.SecurityType),
                Facet("mysqlRoutineSqlMode", routine.SqlMode),
                Facet(
                    "mysqlRoutineCharacterSetClient",
                    routine.CharacterSetClient),
                Facet(
                    "mysqlRoutineCollationConnection",
                    routine.CollationConnection),
                Facet(
                    "mysqlRoutineDatabaseCollation",
                    routine.DatabaseCollation),
            };
            if (routine.DtdIdentifier is not null)
            {
                AddTextDigestFacets(
                    facets,
                    "mysqlRoutineDtdIdentifier",
                    "csharpdb-mysql-routine-dtd-identifier/v1",
                    routine.DtdIdentifierBytes!.Value,
                    routine.DtdIdentifier);
            }
            if (routine.Definition is not null)
            {
                AddDefinitionDigestFacets(
                    facets,
                    "mysqlRoutineDefinition",
                    "csharpdb-mysql-routine-definition/v1",
                    routine.DefinitionBytes!.Value,
                    routine.Definition);
            }
            objects.Add(new MigrationCatalogObject
            {
                ObjectId = routineId,
                Kind = MigrationObjectKind.Routine,
                ParentObjectId = namespaceId,
                SourceNamespace = routine.SchemaName,
                SourceName = routine.Name,
                Facets = facets.AsReadOnly(),
            });
            diagnostics.Add(Diagnostic(
                routineId,
                "MIG-MYSQL-ROUTINE-UNSUPPORTED-001",
                MigrationDiagnosticSeverity.Error,
                MigrationCompatibilityStatus.Unsupported,
                "MySQL routines do not have an automatic target lowering.",
                "Stored procedures and functions require explicit behavioral redesign; digest-only source evidence does not prove target execution semantics.",
                "Reimplement and test the required behavior against the target application contract.",
                canOverride: false));
            if (routine.Definition is null)
            {
                diagnostics.Add(Diagnostic(
                    routineId,
                    "MIG-MYSQL-ROUTINE-DEFINITION-UNAVAILABLE-001",
                    MigrationDiagnosticSeverity.Error,
                    MigrationCompatibilityStatus.Unknown,
                    "The MySQL routine definition is unavailable.",
                    "INFORMATION_SCHEMA returned no routine definition, so the routine body and its dependencies cannot be inspected. This can indicate incomplete metadata visibility.",
                    "Restore complete routine-definition visibility and inspect again.",
                    canOverride: false));
            }
        }

        foreach (MySqlRoutineParameterMetadata parameter in
                 OrderedRoutineParameters(snapshot))
        {
            cancellationToken.ThrowIfCancellationRequested();
            string routineIdentity = RoutineIdentity(
                parameter.SchemaName,
                parameter.SpecificName,
                parameter.RoutineType,
                lowerCaseTableNames);
            (MySqlRoutineMetadata routine, string routineId) =
                routinesByIdentity[routineIdentity];
            bool returnValue = parameter.OrdinalPosition == 0;
            var facets = new List<MigrationCatalogFacet>
            {
                Facet("mysqlObjectClass", "routine-parameter"),
                Facet("mysqlRoutineType", parameter.RoutineType),
                Facet(
                    "mysqlParameterOrdinalPosition",
                    Invariant(parameter.OrdinalPosition)),
                Facet(
                    "mysqlParameterReturnValue",
                    Boolean(returnValue)),
                Facet("mysqlParameterMode", parameter.Mode),
                Facet("mysqlParameterName", parameter.Name),
                Facet("mysqlParameterDataType", parameter.DataType),
                Facet(
                    "mysqlParameterCharacterSet",
                    parameter.CharacterSetName),
                Facet(
                    "mysqlParameterCollation",
                    parameter.CollationName),
                Facet(
                    "mysqlParameterCharacterMaximumLength",
                    NullableInvariant(
                        parameter.CharacterMaximumLength)),
                Facet(
                    "mysqlParameterNumericPrecision",
                    NullableInvariant(parameter.NumericPrecision)),
                Facet(
                    "mysqlParameterNumericScale",
                    NullableInvariant(parameter.NumericScale)),
                Facet(
                    "mysqlParameterDateTimePrecision",
                    NullableInvariant(parameter.DateTimePrecision)),
            };
            AddTextDigestFacets(
                facets,
                "mysqlParameterDtdIdentifier",
                "csharpdb-mysql-routine-parameter-dtd-identifier/v1",
                parameter.DtdIdentifierBytes,
                parameter.DtdIdentifier);
            objects.Add(new MigrationCatalogObject
            {
                ObjectId = ObjectId(
                    "routine-parameter",
                    parameter.SchemaName,
                    parameter.RoutineType,
                    parameter.SpecificName,
                    Invariant(parameter.OrdinalPosition)),
                Kind = MigrationObjectKind.Other,
                ParentObjectId = routineId,
                SourceNamespace = routine.SchemaName,
                SourceName = returnValue
                    ? "$return"
                    : parameter.Name!,
                Facets = facets.AsReadOnly(),
            });
        }
    }

    private static void AddDefaultFacets(
        ICollection<MigrationCatalogFacet> facets,
        MySqlColumnMetadata column)
    {
        bool hasAutomaticValueBehavior =
            column.DefaultValue is not null ||
            column.IsDefaultGenerated ||
            column.HasOnUpdateCurrentTimestamp;
        if (hasAutomaticValueBehavior)
        {
            facets.Add(Facet("hasDefault", "true"));
            facets.Add(Facet("defaultKind", "source-expression"));
        }
        facets.Add(Facet(
            "mysqlDefaultEvidence",
            column.DefaultValue is null
                ? column.IsDefaultGenerated
                    ? "information-schema-generated-text-unavailable"
                    : column.IsGenerated
                    ? "not-applicable-generated-column"
                    : column.IsNullable
                        ? "information-schema-null-ambiguous"
                        : "information-schema-null-no-explicit-null-shape"
                : "information-schema-non-null"));
        facets.Add(Facet(
            "mysqlDefaultGenerated",
            Boolean(column.IsDefaultGenerated)));
        facets.Add(Facet(
            "mysqlOnUpdateCurrentTimestamp",
            Boolean(column.HasOnUpdateCurrentTimestamp)));
        if (column.DefaultValue is null)
            return;
        AddTextDigestFacets(
            facets,
            "mysqlDefault",
            "csharpdb-mysql-column-default/v1",
            column.DefaultBytes!.Value,
            column.DefaultValue);
    }

    private static void AddDefaultDiagnostics(
        MySqlColumnMetadata column,
        string columnId,
        ICollection<MigrationDiagnostic> diagnostics)
    {
        if (column.DefaultValue is not null ||
            column.IsDefaultGenerated)
        {
            diagnostics.Add(Diagnostic(
                columnId,
                "MIG-MYSQL-DEFAULT-UNSUPPORTED-001",
                MigrationDiagnosticSeverity.Error,
                MigrationCompatibilityStatus.Unsupported,
                "The MySQL column default is not target-ready.",
                "Only bounded digest evidence is retained. The default has not been parsed, typed, bound, lowered, or scratch-executed, and it is deliberately classified as a source expression.",
                "Translate the default explicitly and validate its target type and execution semantics.",
                canOverride: false));
        }
        if (column.IsDefaultGenerated &&
            column.DefaultValue is null)
        {
            diagnostics.Add(Diagnostic(
                columnId,
                "MIG-MYSQL-DEFAULT-TEXT-UNAVAILABLE-001",
                MigrationDiagnosticSeverity.Error,
                MigrationCompatibilityStatus.Unknown,
                "The generated MySQL default text is unavailable.",
                "MySQL reported DEFAULT_GENERATED while INFORMATION_SCHEMA returned a NULL COLUMN_DEFAULT. A known Oracle MySQL metadata inconsistency can hide an expression that still executes, so the missing text cannot be treated as an absent default.",
                "Recover and review the exact default from independently bounded source evidence before designing target behavior.",
                canOverride: false));
        }
        if (column.HasOnUpdateCurrentTimestamp)
        {
            diagnostics.Add(Diagnostic(
                columnId,
                "MIG-MYSQL-ON-UPDATE-UNSUPPORTED-001",
                MigrationDiagnosticSeverity.Error,
                MigrationCompatibilityStatus.Unsupported,
                "Automatic ON UPDATE timestamp behavior is not supported.",
                "MySQL can mutate the column during updates without an explicit assigned value. This behavior is not represented by a CSharpDB column default.",
                "Move the update behavior into reviewed application logic or a separately validated target design.",
                canOverride: false));
        }
    }

    private static void AddViewDiagnostics(
        MySqlViewMetadata view,
        string viewId,
        int columnCount,
        ICollection<MigrationDiagnostic> diagnostics)
    {
        diagnostics.Add(Diagnostic(
            viewId,
            "MIG-MYSQL-VIEW-INVENTORY-ONLY-001",
            MigrationDiagnosticSeverity.Error,
            MigrationCompatibilityStatus.Unknown,
            "The MySQL view is inventory-only.",
            "The source definition is retained only as bounded digest evidence. It has not been parsed, dependency-bound, lowered, or scratch-executed, and no target SQL is emitted.",
            "Translate the view explicitly after bounded dependency and query-semantics analysis.",
            canOverride: false));
        if (!view.MetadataVisible ||
            view.Definition is null ||
            columnCount == 0)
        {
            diagnostics.Add(Diagnostic(
                viewId,
                "MIG-MYSQL-VIEW-METADATA-INCOMPLETE-001",
                MigrationDiagnosticSeverity.Error,
                MigrationCompatibilityStatus.Unknown,
                "The MySQL view metadata is incomplete.",
                "The view definition, INFORMATION_SCHEMA.VIEWS row, or output-column inventory is unavailable. Missing metadata cannot be interpreted as an empty definition or projection.",
                "Restore complete view metadata visibility and inspect again.",
                canOverride: false));
        }
        if (view.CheckOption is null ||
            !string.Equals(
                view.CheckOption,
                "NONE",
                StringComparison.Ordinal))
        {
            diagnostics.Add(Diagnostic(
                viewId,
                "MIG-MYSQL-VIEW-CHECK-OPTION-UNKNOWN-001",
                MigrationDiagnosticSeverity.Error,
                MigrationCompatibilityStatus.Unknown,
                "The MySQL view check-option shape needs explicit review.",
                "A missing, LOCAL, or CASCADED check option cannot be assumed equivalent to target view write semantics.",
                "Select and validate an explicit target write policy for the view.",
                canOverride: false,
                occurrenceKey: view.CheckOption));
        }
        if (view.SecurityType is not null)
        {
            diagnostics.Add(Diagnostic(
                viewId,
                "MIG-MYSQL-VIEW-SECURITY-SEMANTICS-UNKNOWN-001",
                MigrationDiagnosticSeverity.Error,
                MigrationCompatibilityStatus.Unknown,
                "The MySQL view security context needs explicit review.",
                "DEFINER and INVOKER execution contexts are not lowered automatically. Definer identities are intentionally omitted from the durable catalog.",
                "Choose and validate an explicit target authorization design.",
                canOverride: false,
                occurrenceKey: view.SecurityType));
        }
        if (view.IsUpdatable != false)
        {
            diagnostics.Add(Diagnostic(
                viewId,
                "MIG-MYSQL-VIEW-UPDATABLE-SHAPE-UNKNOWN-001",
                MigrationDiagnosticSeverity.Error,
                MigrationCompatibilityStatus.Unknown,
                "The MySQL view update shape needs explicit review.",
                "An updatable or unknown view shape can expose write behavior that is not represented by an inventory-only target view.",
                "Treat the view as read-only or validate an explicit write design.",
                canOverride: false,
                occurrenceKey: NullableBoolean(view.IsUpdatable)));
        }
    }

    private static void ValidateDefault(
        MySqlColumnMetadata column,
        MetadataBudget budget)
    {
        bool hasDefault = column.DefaultValue is not null;
        if (hasDefault != (column.DefaultBytes is not null) ||
            column.IsGenerated &&
            (hasDefault ||
             column.IsDefaultGenerated ||
             column.HasOnUpdateCurrentTimestamp) ||
            column.HasOnUpdateCurrentTimestamp &&
            !column.DataType.Equals(
                "timestamp",
                StringComparison.OrdinalIgnoreCase) &&
            !column.DataType.Equals(
                "datetime",
                StringComparison.OrdinalIgnoreCase))
        {
            throw InvalidSnapshot("inconsistent column-default metadata");
        }
        if (column.DefaultValue is not null)
        {
            budget.AddExpression(
                column.DefaultValue,
                column.DefaultBytes!.Value);
        }
    }

    private static void ValidateProgrammableCounts(
        MySqlCatalogSnapshot snapshot,
        MySqlInspectionLimits limits)
    {
        if (snapshot.Views.Count > limits.MaxViews)
            throw LimitExceeded("view count");
        if (snapshot.ViewColumns.Count > limits.MaxViewColumns)
            throw LimitExceeded("view-column count");
        if (snapshot.Triggers.Count > limits.MaxTriggers)
            throw LimitExceeded("trigger count");
        if (snapshot.Routines.Count > limits.MaxRoutines)
            throw LimitExceeded("routine count");
        if (snapshot.RoutineParameters.Count > limits.MaxRoutineParameters)
            throw LimitExceeded("routine-parameter count");

        long structuralRows = checked(
            (long)snapshot.Tables.Count +
            snapshot.Columns.Count +
            snapshot.TableDefinitions.Count +
            snapshot.Keys.Count +
            snapshot.KeyColumns.Count +
            snapshot.ForeignKeys.Count +
            snapshot.ForeignKeyColumns.Count +
            snapshot.Checks.Count +
            snapshot.IndexParts.Count +
            snapshot.Views.Count +
            snapshot.Views.Count(static view => view.MetadataVisible) +
            snapshot.ViewColumns.Count +
            snapshot.Triggers.Count +
            snapshot.Routines.Count +
            snapshot.RoutineParameters.Count);
        if (structuralRows > limits.MaxStructuralRowsTotal)
            throw LimitExceeded("aggregate structural-row count");
    }

    private static void ValidateProgrammableSnapshot(
        MySqlCatalogSnapshot snapshot,
        IReadOnlySet<string> tableIdentities,
        MetadataBudget budget,
        CancellationToken cancellationToken)
    {
        int lowerCaseTableNames = snapshot.Server.LowerCaseTableNames;
        var viewsByIdentity =
            new Dictionary<string, MySqlViewMetadata>(StringComparer.Ordinal);
        foreach (MySqlViewMetadata view in snapshot.Views)
        {
            cancellationToken.ThrowIfCancellationRequested();
            budget.AddRequired(view.SchemaName, isName: true);
            budget.AddRequired(view.Name, isName: true);
            string identity = ViewIdentity(
                view.SchemaName,
                view.Name,
                lowerCaseTableNames);
            bool hiddenShape =
                view.DefinitionBytes is null &&
                view.Definition is null &&
                view.CheckOption is null &&
                view.IsUpdatable is null &&
                view.SecurityType is null &&
                view.CharacterSetClient is null &&
                view.CollationConnection is null;
            bool visibleShape =
                view.CheckOption is not null &&
                view.IsUpdatable is not null &&
                view.SecurityType is not null &&
                view.CharacterSetClient is not null &&
                view.CollationConnection is not null;
            if (!DatabaseNamesEqual(
                    view.SchemaName,
                    snapshot.Database.Name,
                    lowerCaseTableNames) ||
                tableIdentities.Contains(identity) ||
                !viewsByIdentity.TryAdd(identity, view) ||
                (view.MetadataVisible
                    ? !visibleShape
                    : !hiddenShape))
            {
                throw InvalidSnapshot(
                    "duplicate, invalid, or out-of-scope view metadata");
            }
            if (view.CheckOption is not null &&
                view.CheckOption is not ("NONE" or "LOCAL" or "CASCADED") ||
                view.SecurityType is not null &&
                view.SecurityType is not ("DEFINER" or "INVOKER"))
            {
                throw InvalidSnapshot("invalid view shape metadata");
            }
            budget.Add(view.CheckOption);
            budget.Add(view.SecurityType);
            budget.Add(view.CharacterSetClient);
            budget.Add(view.CollationConnection);
            bool hasDefinition = view.Definition is not null;
            if (hasDefinition != (view.DefinitionBytes is not null) ||
                hasDefinition &&
                string.IsNullOrWhiteSpace(view.Definition))
            {
                throw InvalidSnapshot(
                    "inconsistent view-definition metadata");
            }
            if (view.Definition is not null)
            {
                budget.AddDefinition(
                    view.Definition,
                    view.DefinitionBytes!.Value);
            }
        }

        var viewColumnOrdinals = new HashSet<string>(StringComparer.Ordinal);
        var viewColumnNames = new HashSet<string>(StringComparer.Ordinal);
        var viewOrdinals =
            new Dictionary<string, List<int>>(StringComparer.Ordinal);
        foreach (MySqlViewColumnMetadata column in snapshot.ViewColumns)
        {
            cancellationToken.ThrowIfCancellationRequested();
            budget.AddRequired(column.SchemaName, isName: true);
            budget.AddRequired(column.ViewName, isName: true);
            budget.AddRequired(column.Name, isName: true);
            budget.AddRequired(column.DataType);
            if (string.IsNullOrWhiteSpace(column.ColumnType))
            {
                throw InvalidSnapshot(
                    "empty required view-column type metadata");
            }
            budget.AddColumnTypeText(
                column.ColumnType,
                column.ColumnTypeBytes);
            budget.Add(column.CharacterSetName);
            budget.Add(column.CollationName);
            string viewIdentity = ViewIdentity(
                column.SchemaName,
                column.ViewName,
                lowerCaseTableNames);
            string ordinalIdentity = string.Concat(
                viewIdentity,
                "\0",
                Invariant(column.OrdinalPosition));
            string nameIdentity = string.Concat(
                viewIdentity,
                "\0",
                column.Name.ToUpperInvariant());
            if (!viewsByIdentity.ContainsKey(viewIdentity) ||
                column.OrdinalPosition <= 0 ||
                !viewColumnOrdinals.Add(ordinalIdentity) ||
                !viewColumnNames.Add(nameIdentity) ||
                (column.CharacterSetName is null) !=
                (column.CollationName is null))
            {
                throw InvalidSnapshot(
                    "duplicate, invalid, or unowned view-column metadata");
            }
            ValidateTypeShape(
                column.CharacterMaximumLength,
                column.NumericPrecision,
                column.NumericScale,
                column.DateTimePrecision);
            if (!viewOrdinals.TryGetValue(
                    viewIdentity,
                    out List<int>? ordinals))
            {
                ordinals = [];
                viewOrdinals.Add(viewIdentity, ordinals);
            }
            ordinals.Add(column.OrdinalPosition);
        }
        foreach (List<int> ordinals in viewOrdinals.Values)
        {
            if (!ordinals
                    .Order()
                    .SequenceEqual(Enumerable.Range(1, ordinals.Count)))
            {
                throw InvalidSnapshot(
                    "noncontiguous view-column ordinal metadata");
            }
        }

        var triggerIdentities = new HashSet<string>(StringComparer.Ordinal);
        var triggerOrdinals =
            new Dictionary<string, List<int>>(StringComparer.Ordinal);
        foreach (MySqlTriggerMetadata trigger in snapshot.Triggers)
        {
            cancellationToken.ThrowIfCancellationRequested();
            budget.AddRequired(trigger.SchemaName, isName: true);
            budget.AddRequired(trigger.Name, isName: true);
            budget.AddRequired(trigger.EventManipulation);
            budget.AddRequired(trigger.EventObjectSchema, isName: true);
            budget.AddRequired(trigger.EventObjectTable, isName: true);
            budget.AddRequired(trigger.ActionOrientation);
            budget.AddRequired(trigger.ActionTiming);
            budget.Add(trigger.SqlMode);
            budget.AddRequired(trigger.CharacterSetClient);
            budget.AddRequired(trigger.CollationConnection);
            budget.AddRequired(trigger.DatabaseCollation);
            string tableIdentity = TableIdentity(
                trigger.EventObjectSchema,
                trigger.EventObjectTable,
                lowerCaseTableNames);
            string triggerIdentity = SchemaObjectIdentity(
                trigger.SchemaName,
                trigger.Name,
                lowerCaseTableNames);
            if (!DatabaseNamesEqual(
                    trigger.SchemaName,
                    snapshot.Database.Name,
                    lowerCaseTableNames) ||
                !DatabaseNamesEqual(
                    trigger.EventObjectSchema,
                    snapshot.Database.Name,
                    lowerCaseTableNames) ||
                !DatabaseNamesEqual(
                    trigger.SchemaName,
                    trigger.EventObjectSchema,
                    lowerCaseTableNames) ||
                !tableIdentities.Contains(tableIdentity) ||
                !triggerIdentities.Add(triggerIdentity) ||
                trigger.ActionOrder <= 0 ||
                trigger.EventManipulation is not ("INSERT" or "UPDATE" or
                    "DELETE") ||
                trigger.ActionOrientation is not "ROW" ||
                trigger.ActionTiming is not ("BEFORE" or "AFTER") ||
                string.IsNullOrWhiteSpace(trigger.ActionStatement))
            {
                throw InvalidSnapshot(
                    "duplicate, invalid, or unowned trigger metadata");
            }
            budget.AddDefinition(
                trigger.ActionStatement,
                trigger.ActionStatementBytes);
            string ordinalGroup = string.Concat(
                tableIdentity,
                "\0",
                trigger.EventManipulation,
                "\0",
                trigger.ActionTiming);
            if (!triggerOrdinals.TryGetValue(
                    ordinalGroup,
                    out List<int>? ordinals))
            {
                ordinals = [];
                triggerOrdinals.Add(ordinalGroup, ordinals);
            }
            ordinals.Add(trigger.ActionOrder);
        }
        foreach (List<int> ordinals in triggerOrdinals.Values)
        {
            if (ordinals.Count != ordinals.Distinct().Count() ||
                !ordinals
                    .Order()
                    .SequenceEqual(Enumerable.Range(1, ordinals.Count)))
            {
                throw InvalidSnapshot(
                    "duplicate or noncontiguous trigger-order metadata");
            }
        }

        var routinesByIdentity =
            new Dictionary<string, MySqlRoutineMetadata>(
                StringComparer.Ordinal);
        var routineNames = new HashSet<string>(StringComparer.Ordinal);
        foreach (MySqlRoutineMetadata routine in snapshot.Routines)
        {
            cancellationToken.ThrowIfCancellationRequested();
            budget.AddRequired(routine.SchemaName, isName: true);
            budget.AddRequired(routine.SpecificName, isName: true);
            budget.AddRequired(routine.Name, isName: true);
            budget.AddRequired(routine.RoutineType);
            budget.Add(routine.DataType);
            budget.AddRequired(routine.RoutineBody);
            budget.AddRequired(routine.SqlDataAccess);
            budget.AddRequired(routine.SecurityType);
            budget.Add(routine.SqlMode);
            budget.AddRequired(routine.CharacterSetClient);
            budget.AddRequired(routine.CollationConnection);
            budget.AddRequired(routine.DatabaseCollation);
            string identity = RoutineIdentity(
                routine.SchemaName,
                routine.SpecificName,
                routine.RoutineType,
                lowerCaseTableNames);
            string nameIdentity = RoutineIdentity(
                routine.SchemaName,
                routine.Name,
                routine.RoutineType,
                lowerCaseTableNames);
            bool function = routine.RoutineType == "FUNCTION";
            bool procedure = routine.RoutineType == "PROCEDURE";
            if (!DatabaseNamesEqual(
                    routine.SchemaName,
                    snapshot.Database.Name,
                    lowerCaseTableNames) ||
                !function && !procedure ||
                !routinesByIdentity.TryAdd(identity, routine) ||
                !routineNames.Add(nameIdentity) ||
                routine.RoutineBody is not "SQL" ||
                routine.SqlDataAccess is not ("CONTAINS SQL" or "NO SQL" or
                    "READS SQL DATA" or "MODIFIES SQL DATA") ||
                routine.SecurityType is not ("DEFINER" or "INVOKER") ||
                function &&
                (string.IsNullOrWhiteSpace(routine.DataType) ||
                 string.IsNullOrWhiteSpace(routine.DtdIdentifier) ||
                 routine.DtdIdentifierBytes is null) ||
                procedure &&
                (routine.DataType is not null ||
                 routine.DtdIdentifier is not null ||
                 routine.DtdIdentifierBytes is not null))
            {
                throw InvalidSnapshot(
                    "duplicate, invalid, or out-of-scope routine metadata");
            }
            if (routine.DtdIdentifier is not null)
            {
                AddBoundedTypeText(
                    budget,
                    routine.DtdIdentifier,
                    routine.DtdIdentifierBytes!.Value);
            }
            bool hasDefinition = routine.Definition is not null;
            if (hasDefinition != (routine.DefinitionBytes is not null) ||
                hasDefinition &&
                string.IsNullOrWhiteSpace(routine.Definition))
            {
                throw InvalidSnapshot(
                    "inconsistent routine-definition metadata");
            }
            if (routine.Definition is not null)
            {
                budget.AddDefinition(
                    routine.Definition,
                    routine.DefinitionBytes!.Value);
            }
        }

        var parameterOrdinals = new HashSet<string>(StringComparer.Ordinal);
        var parameterNames = new HashSet<string>(StringComparer.Ordinal);
        var routineParameterOrdinals =
            new Dictionary<string, List<int>>(StringComparer.Ordinal);
        foreach (MySqlRoutineParameterMetadata parameter in
                 snapshot.RoutineParameters)
        {
            cancellationToken.ThrowIfCancellationRequested();
            budget.AddRequired(parameter.SchemaName, isName: true);
            budget.AddRequired(parameter.SpecificName, isName: true);
            budget.AddRequired(parameter.RoutineType);
            budget.Add(parameter.Mode);
            budget.Add(parameter.Name, isName: true);
            budget.AddRequired(parameter.DataType);
            budget.Add(parameter.CharacterSetName);
            budget.Add(parameter.CollationName);
            AddBoundedTypeText(
                budget,
                parameter.DtdIdentifier,
                parameter.DtdIdentifierBytes);
            string routineIdentity = RoutineIdentity(
                parameter.SchemaName,
                parameter.SpecificName,
                parameter.RoutineType,
                lowerCaseTableNames);
            bool returnValue = parameter.OrdinalPosition == 0;
            bool validReturn =
                returnValue &&
                parameter.RoutineType == "FUNCTION" &&
                parameter.Mode is null &&
                parameter.Name is null;
            bool validParameter =
                parameter.OrdinalPosition > 0 &&
                parameter.Mode is "IN" or "OUT" or "INOUT" &&
                !string.IsNullOrWhiteSpace(parameter.Name);
            string ordinalIdentity = string.Concat(
                routineIdentity,
                "\0",
                Invariant(parameter.OrdinalPosition));
            string? nameIdentity = parameter.Name is null
                ? null
                : string.Concat(
                    routineIdentity,
                    "\0",
                    parameter.Name.ToUpperInvariant());
            if (!routinesByIdentity.TryGetValue(
                    routineIdentity,
                    out MySqlRoutineMetadata? routine) ||
                parameter.RoutineType != routine.RoutineType ||
                !validReturn && !validParameter ||
                string.IsNullOrWhiteSpace(parameter.DtdIdentifier) ||
                !parameterOrdinals.Add(ordinalIdentity) ||
                nameIdentity is not null &&
                !parameterNames.Add(nameIdentity) ||
                (parameter.CharacterSetName is null) !=
                (parameter.CollationName is null))
            {
                throw InvalidSnapshot(
                    "duplicate, invalid, or unowned routine-parameter metadata");
            }
            if (returnValue &&
                (!string.Equals(
                    parameter.DataType,
                    routine.DataType,
                    StringComparison.Ordinal) ||
                 !string.Equals(
                     parameter.DtdIdentifier,
                     routine.DtdIdentifier,
                     StringComparison.Ordinal) ||
                 parameter.DtdIdentifierBytes !=
                 routine.DtdIdentifierBytes))
            {
                throw InvalidSnapshot(
                    "inconsistent routine return metadata");
            }
            ValidateTypeShape(
                parameter.CharacterMaximumLength,
                parameter.NumericPrecision,
                parameter.NumericScale,
                parameter.DateTimePrecision);
            if (!routineParameterOrdinals.TryGetValue(
                    routineIdentity,
                    out List<int>? ordinals))
            {
                ordinals = [];
                routineParameterOrdinals.Add(
                    routineIdentity,
                    ordinals);
            }
            ordinals.Add(parameter.OrdinalPosition);
        }
        foreach ((string identity, MySqlRoutineMetadata routine) in
                 routinesByIdentity)
        {
            routineParameterOrdinals.TryGetValue(
                identity,
                out List<int>? ordinals);
            ordinals ??= [];
            int expectedStart = routine.RoutineType == "FUNCTION" ? 0 : 1;
            if (routine.RoutineType == "FUNCTION" && ordinals.Count == 0 ||
                !ordinals
                    .Order()
                    .SequenceEqual(
                        Enumerable.Range(expectedStart, ordinals.Count)))
            {
                throw InvalidSnapshot(
                    "incomplete or noncontiguous routine-parameter ordinal metadata");
            }
        }
    }

    private static void ValidateTypeShape(
        long? characterMaximumLength,
        int? numericPrecision,
        int? numericScale,
        int? dateTimePrecision)
    {
        ValidateNonNegative(characterMaximumLength);
        ValidateNonNegative(numericPrecision);
        ValidateNonNegative(numericScale);
        ValidateNonNegative(dateTimePrecision);
        if (numericPrecision is int precision &&
            numericScale is int scale &&
            scale > precision)
        {
            throw InvalidSnapshot("invalid type-shape metadata");
        }
    }

    private static void AddBoundedTypeText(
        MetadataBudget budget,
        string value,
        long sourceBytes) =>
        budget.AddColumnTypeText(value, sourceBytes);

    private static void AddTextDigestFacets(
        ICollection<MigrationCatalogFacet> facets,
        string facetPrefix,
        string digestDomain,
        long sourceBytes,
        string value)
    {
        facets.Add(Facet(
            facetPrefix + "SourceBytes",
            Invariant(sourceBytes)));
        facets.Add(Facet(
            facetPrefix + "Digest",
            "sha256:" + MySqlStableDigest.Text(
                digestDomain,
                value)));
        facets.Add(Facet(
            facetPrefix + "Length",
            Invariant(value.Length)));
    }

    private static IEnumerable<string?> ProgrammableSnapshotFields(
        MySqlCatalogSnapshot snapshot)
    {
        foreach (MySqlViewMetadata view in OrderedViews(snapshot))
        {
            yield return "view";
            yield return view.SchemaName;
            yield return view.Name;
            yield return Boolean(view.MetadataVisible);
            yield return NullableInvariant(view.DefinitionBytes);
            yield return view.Definition is null
                ? null
                : MySqlStableDigest.Text(
                    "csharpdb-mysql-view-definition/v1",
                    view.Definition);
            yield return view.CheckOption;
            yield return NullableBoolean(view.IsUpdatable);
            yield return view.SecurityType;
            yield return view.CharacterSetClient;
            yield return view.CollationConnection;
        }
        foreach (MySqlViewColumnMetadata column in
                 OrderedViewColumns(snapshot))
        {
            yield return "view-column";
            yield return column.SchemaName;
            yield return column.ViewName;
            yield return Invariant(column.OrdinalPosition);
            yield return column.Name;
            yield return column.DataType;
            yield return Boolean(column.IsNullable);
            yield return column.CharacterSetName;
            yield return column.CollationName;
            yield return NullableInvariant(column.CharacterMaximumLength);
            yield return NullableInvariant(column.NumericPrecision);
            yield return NullableInvariant(column.NumericScale);
            yield return NullableInvariant(column.DateTimePrecision);
            yield return Invariant(column.ColumnTypeBytes);
            yield return MySqlStableDigest.Text(
                "csharpdb-mysql-column-type/v1",
                column.ColumnType);
        }
        foreach (MySqlTriggerMetadata trigger in OrderedTriggers(snapshot))
        {
            yield return "trigger";
            yield return trigger.SchemaName;
            yield return trigger.Name;
            yield return trigger.EventManipulation;
            yield return trigger.EventObjectSchema;
            yield return trigger.EventObjectTable;
            yield return Invariant(trigger.ActionOrder);
            yield return Invariant(trigger.ActionStatementBytes);
            yield return MySqlStableDigest.Text(
                "csharpdb-mysql-trigger-action-statement/v1",
                trigger.ActionStatement);
            yield return trigger.ActionOrientation;
            yield return trigger.ActionTiming;
            yield return trigger.SqlMode;
            yield return trigger.CharacterSetClient;
            yield return trigger.CollationConnection;
            yield return trigger.DatabaseCollation;
        }
        foreach (MySqlRoutineMetadata routine in OrderedRoutines(snapshot))
        {
            yield return "routine";
            yield return routine.SchemaName;
            yield return routine.SpecificName;
            yield return routine.Name;
            yield return routine.RoutineType;
            yield return routine.DataType;
            yield return NullableInvariant(routine.DtdIdentifierBytes);
            yield return MySqlStableDigest.Text(
                "csharpdb-mysql-routine-dtd-identifier/v1",
                routine.DtdIdentifier);
            yield return routine.RoutineBody;
            yield return NullableInvariant(routine.DefinitionBytes);
            yield return routine.Definition is null
                ? null
                : MySqlStableDigest.Text(
                    "csharpdb-mysql-routine-definition/v1",
                    routine.Definition);
            yield return Boolean(routine.IsDeterministic);
            yield return routine.SqlDataAccess;
            yield return routine.SecurityType;
            yield return routine.SqlMode;
            yield return routine.CharacterSetClient;
            yield return routine.CollationConnection;
            yield return routine.DatabaseCollation;
        }
        foreach (MySqlRoutineParameterMetadata parameter in
                 OrderedRoutineParameters(snapshot))
        {
            yield return "routine-parameter";
            yield return parameter.SchemaName;
            yield return parameter.SpecificName;
            yield return parameter.RoutineType;
            yield return Invariant(parameter.OrdinalPosition);
            yield return parameter.Mode;
            yield return parameter.Name;
            yield return parameter.DataType;
            yield return Invariant(parameter.DtdIdentifierBytes);
            yield return MySqlStableDigest.Text(
                "csharpdb-mysql-routine-parameter-dtd-identifier/v1",
                parameter.DtdIdentifier);
            yield return parameter.CharacterSetName;
            yield return parameter.CollationName;
            yield return NullableInvariant(
                parameter.CharacterMaximumLength);
            yield return NullableInvariant(parameter.NumericPrecision);
            yield return NullableInvariant(parameter.NumericScale);
            yield return NullableInvariant(parameter.DateTimePrecision);
        }
    }

    private static IOrderedEnumerable<MySqlViewMetadata> OrderedViews(
        MySqlCatalogSnapshot snapshot) =>
        snapshot.Views
            .OrderBy(static item => item.SchemaName, StringComparer.Ordinal)
            .ThenBy(static item => item.Name, StringComparer.Ordinal);

    private static IOrderedEnumerable<MySqlViewColumnMetadata>
        OrderedViewColumns(MySqlCatalogSnapshot snapshot) =>
        snapshot.ViewColumns
            .OrderBy(static item => item.SchemaName, StringComparer.Ordinal)
            .ThenBy(static item => item.ViewName, StringComparer.Ordinal)
            .ThenBy(static item => item.OrdinalPosition)
            .ThenBy(static item => item.Name, StringComparer.Ordinal);

    private static IOrderedEnumerable<MySqlTriggerMetadata> OrderedTriggers(
        MySqlCatalogSnapshot snapshot) =>
        snapshot.Triggers
            .OrderBy(static item => item.SchemaName, StringComparer.Ordinal)
            .ThenBy(
                static item => item.EventObjectSchema,
                StringComparer.Ordinal)
            .ThenBy(
                static item => item.EventObjectTable,
                StringComparer.Ordinal)
            .ThenBy(
                static item => item.EventManipulation,
                StringComparer.Ordinal)
            .ThenBy(
                static item => item.ActionTiming,
                StringComparer.Ordinal)
            .ThenBy(static item => item.ActionOrder)
            .ThenBy(static item => item.Name, StringComparer.Ordinal);

    private static IOrderedEnumerable<MySqlRoutineMetadata> OrderedRoutines(
        MySqlCatalogSnapshot snapshot) =>
        snapshot.Routines
            .OrderBy(static item => item.SchemaName, StringComparer.Ordinal)
            .ThenBy(static item => item.RoutineType, StringComparer.Ordinal)
            .ThenBy(static item => item.SpecificName, StringComparer.Ordinal)
            .ThenBy(static item => item.Name, StringComparer.Ordinal);

    private static IOrderedEnumerable<MySqlRoutineParameterMetadata>
        OrderedRoutineParameters(MySqlCatalogSnapshot snapshot) =>
        snapshot.RoutineParameters
            .OrderBy(static item => item.SchemaName, StringComparer.Ordinal)
            .ThenBy(static item => item.RoutineType, StringComparer.Ordinal)
            .ThenBy(
                static item => item.SpecificName,
                StringComparer.Ordinal)
            .ThenBy(static item => item.OrdinalPosition)
            .ThenBy(static item => item.Name, StringComparer.Ordinal);

    private static string ViewIdentity(
        string schema,
        string view,
        int lowerCaseTableNames) =>
        TableIdentity(schema, view, lowerCaseTableNames);

    private static string SchemaObjectIdentity(
        string schema,
        string name,
        int lowerCaseTableNames) =>
        string.Concat(
            lowerCaseTableNames == 0
                ? schema
                : schema.ToUpperInvariant(),
            "\0",
            name.ToUpperInvariant());

    private static string RoutineIdentity(
        string schema,
        string specificName,
        string routineType,
        int lowerCaseTableNames) =>
        string.Concat(
            SchemaObjectIdentity(
                schema,
                specificName,
                lowerCaseTableNames),
            "\0",
            routineType.ToUpperInvariant());

    private static MySqlColumnMetadata ViewColumnTypeShape(
        MySqlViewColumnMetadata column)
    {
        bool numeric = IsViewNumericType(column.DataType);
        bool unsigned =
            numeric &&
            HasNativeTypeToken(column.ColumnType, "unsigned");
        bool zerofill =
            numeric &&
            HasNativeTypeToken(column.ColumnType, "zerofill");
        bool tinyIntOne = IsViewTinyIntOne(
            column.DataType,
            column.ColumnType);
        return new MySqlColumnMetadata(
            SchemaName: column.SchemaName,
            TableName: column.ViewName,
            OrdinalPosition: column.OrdinalPosition,
            Name: column.Name,
            DataType: column.DataType,
            ColumnTypeBytes: column.ColumnTypeBytes,
            ColumnType: column.ColumnType,
            IsNullable: column.IsNullable,
            CharacterSetName: column.CharacterSetName,
            CollationName: column.CollationName,
            CharacterMaximumLength: column.CharacterMaximumLength,
            NumericPrecision: column.NumericPrecision,
            NumericScale: column.NumericScale,
            DateTimePrecision: column.DateTimePrecision,
            IsUnsigned: unsigned,
            IsZerofill: zerofill,
            IsTinyIntOne: tinyIntOne,
            IsAutoIncrement: false,
            IsGenerated: false,
            GenerationKind: "NEVER",
            GenerationExpressionBytes: null,
            GenerationExpression: null,
            IsInvisible: false);
    }

    private static bool IsViewNumericType(string dataType) =>
        IsIntegerType(dataType) ||
        dataType.Equals("decimal", StringComparison.OrdinalIgnoreCase) ||
        dataType.Equals("numeric", StringComparison.OrdinalIgnoreCase) ||
        dataType.Equals("float", StringComparison.OrdinalIgnoreCase) ||
        dataType.Equals("double", StringComparison.OrdinalIgnoreCase) ||
        dataType.Equals("real", StringComparison.OrdinalIgnoreCase);

    private static bool HasNativeTypeToken(
        string value,
        string expected) =>
        value.Split(
                (char[]?)null,
                StringSplitOptions.RemoveEmptyEntries |
                StringSplitOptions.TrimEntries)
            .Any(token => string.Equals(
                token,
                expected,
                StringComparison.OrdinalIgnoreCase));

    private static bool IsViewTinyIntOne(
        string dataType,
        string columnType)
    {
        const string Prefix = "tinyint(1)";
        if (!dataType.Equals(
                "tinyint",
                StringComparison.OrdinalIgnoreCase) ||
            !columnType.StartsWith(
                Prefix,
                StringComparison.OrdinalIgnoreCase))
        {
            return false;
        }
        return columnType.Length == Prefix.Length ||
               char.IsWhiteSpace(columnType[Prefix.Length]);
    }
}
