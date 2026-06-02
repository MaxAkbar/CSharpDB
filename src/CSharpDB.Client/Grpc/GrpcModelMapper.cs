using CSharpDB.Client.Models;
using CSharpDB.Storage.Diagnostics;
using Google.Protobuf.WellKnownTypes;

namespace CSharpDB.Client.Grpc;

public static class GrpcModelMapper
{
    public static StringList ToStringList(IEnumerable<string> values)
    {
        var message = new StringList();
        message.Values.Add(values);
        return message;
    }

    public static IReadOnlyList<string> ToStringList(StringList values)
        => values.Values.ToList();

    public static VariantArrayList ToVariantArrayList(IEnumerable<object?[]> rows)
    {
        var message = new VariantArrayList();
        message.Items.Add(rows.Select(GrpcValueMapper.ToArray));
        return message;
    }

    public static List<object?[]> ToRows(VariantArrayList? rows)
        => rows?.Items.Select(GrpcValueMapper.ToArray).ToList() ?? [];

    public static DbTypeEnum ToMessage(DbType value)
        => value switch
        {
            DbType.Integer => DbTypeEnum.DbTypeInteger,
            DbType.Real => DbTypeEnum.DbTypeReal,
            DbType.Text => DbTypeEnum.DbTypeText,
            DbType.Blob => DbTypeEnum.DbTypeBlob,
            _ => throw new ArgumentOutOfRangeException(nameof(value), value, "Unsupported database type."),
        };

    public static DbType ToModel(DbTypeEnum value)
        => value switch
        {
            DbTypeEnum.DbTypeInteger => DbType.Integer,
            DbTypeEnum.DbTypeReal => DbType.Real,
            DbTypeEnum.DbTypeText => DbType.Text,
            DbTypeEnum.DbTypeBlob => DbType.Blob,
            _ => throw new ArgumentOutOfRangeException(nameof(value), value, "Unsupported database type enum."),
        };

    public static ForeignKeyOnDeleteActionEnum ToMessage(ForeignKeyOnDeleteAction value)
        => value switch
        {
            ForeignKeyOnDeleteAction.Restrict => ForeignKeyOnDeleteActionEnum.ForeignKeyOnDeleteActionRestrict,
            ForeignKeyOnDeleteAction.Cascade => ForeignKeyOnDeleteActionEnum.ForeignKeyOnDeleteActionCascade,
            _ => throw new ArgumentOutOfRangeException(nameof(value), value, "Unsupported foreign key ON DELETE action."),
        };

    public static ForeignKeyOnDeleteAction ToModel(ForeignKeyOnDeleteActionEnum value)
        => value switch
        {
            ForeignKeyOnDeleteActionEnum.ForeignKeyOnDeleteActionRestrict => ForeignKeyOnDeleteAction.Restrict,
            ForeignKeyOnDeleteActionEnum.ForeignKeyOnDeleteActionCascade => ForeignKeyOnDeleteAction.Cascade,
            ForeignKeyOnDeleteActionEnum.ForeignKeyOnDeleteActionUnspecified => ForeignKeyOnDeleteAction.Restrict,
            _ => throw new ArgumentOutOfRangeException(nameof(value), value, "Unsupported foreign key ON DELETE action enum."),
        };

    public static TriggerTimingEnum ToMessage(TriggerTiming value)
        => value switch
        {
            TriggerTiming.Before => TriggerTimingEnum.TriggerTimingBefore,
            TriggerTiming.After => TriggerTimingEnum.TriggerTimingAfter,
            _ => throw new ArgumentOutOfRangeException(nameof(value), value, "Unsupported trigger timing."),
        };

    public static TriggerTiming ToModel(TriggerTimingEnum value)
        => value switch
        {
            TriggerTimingEnum.TriggerTimingBefore => TriggerTiming.Before,
            TriggerTimingEnum.TriggerTimingAfter => TriggerTiming.After,
            _ => throw new ArgumentOutOfRangeException(nameof(value), value, "Unsupported trigger timing enum."),
        };

    public static TriggerEventEnum ToMessage(TriggerEvent value)
        => value switch
        {
            TriggerEvent.Insert => TriggerEventEnum.TriggerEventInsert,
            TriggerEvent.Update => TriggerEventEnum.TriggerEventUpdate,
            TriggerEvent.Delete => TriggerEventEnum.TriggerEventDelete,
            _ => throw new ArgumentOutOfRangeException(nameof(value), value, "Unsupported trigger event."),
        };

    public static TriggerEvent ToModel(TriggerEventEnum value)
        => value switch
        {
            TriggerEventEnum.TriggerEventInsert => TriggerEvent.Insert,
            TriggerEventEnum.TriggerEventUpdate => TriggerEvent.Update,
            TriggerEventEnum.TriggerEventDelete => TriggerEvent.Delete,
            _ => throw new ArgumentOutOfRangeException(nameof(value), value, "Unsupported trigger event enum."),
        };

    public static InspectSeverityEnum ToMessage(InspectSeverity value)
        => value switch
        {
            InspectSeverity.Info => InspectSeverityEnum.InspectSeverityInfo,
            InspectSeverity.Warning => InspectSeverityEnum.InspectSeverityWarning,
            InspectSeverity.Error => InspectSeverityEnum.InspectSeverityError,
            _ => throw new ArgumentOutOfRangeException(nameof(value), value, "Unsupported inspect severity."),
        };

    public static InspectSeverity ToModel(InspectSeverityEnum value)
        => value switch
        {
            InspectSeverityEnum.InspectSeverityInfo => InspectSeverity.Info,
            InspectSeverityEnum.InspectSeverityWarning => InspectSeverity.Warning,
            InspectSeverityEnum.InspectSeverityError => InspectSeverity.Error,
            _ => throw new ArgumentOutOfRangeException(nameof(value), value, "Unsupported inspect severity enum."),
        };

    public static ReindexScopeEnum ToMessage(ReindexScope value)
        => value switch
        {
            ReindexScope.All => ReindexScopeEnum.ReindexScopeAll,
            ReindexScope.Table => ReindexScopeEnum.ReindexScopeTable,
            ReindexScope.Index => ReindexScopeEnum.ReindexScopeIndex,
            _ => throw new ArgumentOutOfRangeException(nameof(value), value, "Unsupported reindex scope."),
        };

    public static ReindexScope ToModel(ReindexScopeEnum value)
        => value switch
        {
            ReindexScopeEnum.ReindexScopeAll => ReindexScope.All,
            ReindexScopeEnum.ReindexScopeTable => ReindexScope.Table,
            ReindexScopeEnum.ReindexScopeIndex => ReindexScope.Index,
            _ => throw new ArgumentOutOfRangeException(nameof(value), value, "Unsupported reindex scope enum."),
        };

    public static ColumnDefinitionMessage ToMessage(ColumnDefinition value)
        => new()
        {
            Name = value.Name,
            Type = ToMessage(value.Type),
            Nullable = value.Nullable,
            IsPrimaryKey = value.IsPrimaryKey,
            IsIdentity = value.IsIdentity,
            Collation = value.Collation ?? string.Empty,
        };

    public static ColumnDefinition ToModel(ColumnDefinitionMessage value)
        => new()
        {
            Name = value.Name,
            Type = ToModel(value.Type),
            Nullable = value.Nullable,
            IsPrimaryKey = value.IsPrimaryKey,
            IsIdentity = value.IsIdentity,
            Collation = string.IsNullOrEmpty(value.Collation) ? null : value.Collation,
        };

    public static ForeignKeyDefinitionMessage ToMessage(ForeignKeyDefinition value)
        => new()
        {
            ConstraintName = value.ConstraintName,
            ColumnName = value.ColumnName,
            ReferencedTableName = value.ReferencedTableName,
            ReferencedColumnName = value.ReferencedColumnName,
            OnDelete = ToMessage(value.OnDelete),
            SupportingIndexName = value.SupportingIndexName,
        };

    public static ForeignKeyDefinition ToModel(ForeignKeyDefinitionMessage value)
        => new()
        {
            ConstraintName = value.ConstraintName,
            ColumnName = value.ColumnName,
            ReferencedTableName = value.ReferencedTableName,
            ReferencedColumnName = value.ReferencedColumnName,
            OnDelete = ToModel(value.OnDelete),
            SupportingIndexName = value.SupportingIndexName,
        };

    public static TableSchemaMessage ToMessage(TableSchema value)
    {
        var message = new TableSchemaMessage
        {
            TableName = value.TableName,
        };
        message.Columns.Add(value.Columns.Select(ToMessage));
        message.ForeignKeys.Add(value.ForeignKeys.Select(ToMessage));
        return message;
    }

    public static TableSchema ToModel(TableSchemaMessage value)
        => new()
        {
            TableName = value.TableName,
            Columns = value.Columns.Select(ToModel).ToList(),
            ForeignKeys = value.ForeignKeys.Select(ToModel).ToList(),
        };

    public static IndexSchemaMessage ToMessage(IndexSchema value)
    {
        var message = new IndexSchemaMessage
        {
            IndexName = value.IndexName,
            TableName = value.TableName,
            IsUnique = value.IsUnique,
        };
        message.Columns.Add(value.Columns);
        message.ColumnCollations.Add(value.ColumnCollations.Select(static collation => collation ?? string.Empty));
        return message;
    }

    public static IndexSchema ToModel(IndexSchemaMessage value)
        => new()
        {
            IndexName = value.IndexName,
            TableName = value.TableName,
            Columns = value.Columns.ToList(),
            ColumnCollations = value.ColumnCollations
                .Select(static collation => string.IsNullOrEmpty(collation) ? null : collation)
                .ToList(),
            IsUnique = value.IsUnique,
        };

    public static ViewDefinitionMessage ToMessage(ViewDefinition value)
        => new()
        {
            Name = value.Name,
            Sql = value.Sql,
        };

    public static ViewDefinition ToModel(ViewDefinitionMessage value)
        => new()
        {
            Name = value.Name,
            Sql = value.Sql,
        };

    public static TriggerSchemaMessage ToMessage(TriggerSchema value)
        => new()
        {
            TriggerName = value.TriggerName,
            TableName = value.TableName,
            Timing = ToMessage(value.Timing),
            TriggerEvent = ToMessage(value.Event),
            BodySql = value.BodySql,
        };

    public static TriggerSchema ToModel(TriggerSchemaMessage value)
        => new()
        {
            TriggerName = value.TriggerName,
            TableName = value.TableName,
            Timing = ToModel(value.Timing),
            Event = ToModel(value.TriggerEvent),
            BodySql = value.BodySql,
        };

    public static TableBrowseResultMessage ToMessage(TableBrowseResult value)
    {
        var message = new TableBrowseResultMessage
        {
            TableName = value.TableName,
            Schema = ToMessage(value.Schema),
            TotalRows = value.TotalRows,
            Page = value.Page,
            PageSize = value.PageSize,
        };
        message.Rows.Add(value.Rows.Select(GrpcValueMapper.ToArray));
        return message;
    }

    public static TableBrowseResult ToModel(TableBrowseResultMessage value)
        => new()
        {
            TableName = value.TableName,
            Schema = ToModel(value.Schema),
            Rows = value.Rows.Select(GrpcValueMapper.ToArray).ToList(),
            TotalRows = value.TotalRows,
            Page = value.Page,
            PageSize = value.PageSize,
        };

    public static ViewBrowseResultMessage ToMessage(ViewBrowseResult value)
    {
        var message = new ViewBrowseResultMessage
        {
            ViewName = value.ViewName,
            TotalRows = value.TotalRows,
            Page = value.Page,
            PageSize = value.PageSize,
        };
        message.ColumnNames.Add(value.ColumnNames);
        message.Rows.Add(value.Rows.Select(GrpcValueMapper.ToArray));
        return message;
    }

    public static ViewBrowseResult ToModel(ViewBrowseResultMessage value)
        => new()
        {
            ViewName = value.ViewName,
            ColumnNames = value.ColumnNames.ToArray(),
            Rows = value.Rows.Select(GrpcValueMapper.ToArray).ToList(),
            TotalRows = value.TotalRows,
            Page = value.Page,
            PageSize = value.PageSize,
        };

    public static SqlExecutionResultMessage ToMessage(SqlExecutionResult value)
        => new()
        {
            IsQuery = value.IsQuery,
            ColumnNames = value.ColumnNames is null ? null : ToStringList(value.ColumnNames),
            Rows = value.Rows is null ? null : ToVariantArrayList(value.Rows),
            RowsAffected = value.RowsAffected,
            Error = value.Error,
            Elapsed = Duration.FromTimeSpan(value.Elapsed),
        };

    public static SqlExecutionResult ToModel(SqlExecutionResultMessage value)
        => new()
        {
            IsQuery = value.IsQuery,
            ColumnNames = value.ColumnNames?.Values.ToArray(),
            Rows = value.Rows is null ? null : ToRows(value.Rows),
            RowsAffected = value.RowsAffected,
            Error = value.Error,
            Elapsed = value.Elapsed.ToTimeSpan(),
        };

    public static DatabaseInfoMessage ToMessage(DatabaseInfo value)
        => new()
        {
            DataSource = value.DataSource,
            TableCount = value.TableCount,
            IndexCount = value.IndexCount,
            ViewCount = value.ViewCount,
            TriggerCount = value.TriggerCount,
            ProcedureCount = value.ProcedureCount,
            CollectionCount = value.CollectionCount,
            SavedQueryCount = value.SavedQueryCount,
        };

    public static DatabaseInfo ToModel(DatabaseInfoMessage value)
        => new()
        {
            DataSource = value.DataSource,
            TableCount = value.TableCount,
            IndexCount = value.IndexCount,
            ViewCount = value.ViewCount,
            TriggerCount = value.TriggerCount,
            ProcedureCount = value.ProcedureCount,
            CollectionCount = value.CollectionCount,
            SavedQueryCount = value.SavedQueryCount,
        };

    public static ShardDefinitionMessage ToMessage(CSharpDbShardDefinitionSnapshot value)
        => new()
        {
            ShardId = value.ShardId,
            Enabled = value.Enabled,
            Transport = value.Transport?.ToString(),
            Endpoint = value.Endpoint,
            DataSource = value.DataSource,
            HasConnectionString = value.HasConnectionString,
            HasApiKey = value.HasApiKey,
            ApiKeyHeaderName = value.ApiKeyHeaderName,
            Role = value.Role,
            PrimaryShardId = value.PrimaryShardId,
            PromotionEligible = value.PromotionEligible,
            ReplicationLagBytes = value.ReplicationLagBytes,
            LastReplicatedUtc = value.LastReplicatedUtc.HasValue
                ? Timestamp.FromDateTimeOffset(value.LastReplicatedUtc.Value)
                : null,
        };

    public static ShardTargetDefinitionMessage ToMessage(CSharpDbShardDefinition value)
        => new()
        {
            ShardId = value.ShardId,
            Enabled = value.Enabled,
            Transport = value.Transport?.ToString(),
            Endpoint = value.Endpoint,
            ConnectionString = value.ConnectionString,
            DataSource = value.DataSource,
            ApiKey = value.ApiKey,
            ApiKeyHeaderName = value.ApiKeyHeaderName,
            Role = value.Role,
            PrimaryShardId = value.PrimaryShardId,
            PromotionEligible = value.PromotionEligible,
            ReplicationLagBytes = value.ReplicationLagBytes,
            LastReplicatedUtc = value.LastReplicatedUtc.HasValue
                ? Timestamp.FromDateTimeOffset(value.LastReplicatedUtc.Value)
                : null,
        };

    public static CSharpDbShardDefinition ToModel(ShardTargetDefinitionMessage value)
        => new()
        {
            ShardId = value.ShardId,
            Enabled = value.Enabled,
            Transport = TryParseTransport(value.Transport),
            Endpoint = value.Endpoint,
            ConnectionString = value.ConnectionString,
            DataSource = value.DataSource,
            ApiKey = value.ApiKey,
            ApiKeyHeaderName = value.ApiKeyHeaderName,
            Role = string.IsNullOrWhiteSpace(value.Role) ? CSharpDbShardRoles.Primary : value.Role,
            PrimaryShardId = value.PrimaryShardId,
            PromotionEligible = value.PromotionEligible,
            ReplicationLagBytes = value.ReplicationLagBytes,
            LastReplicatedUtc = value.LastReplicatedUtc?.ToDateTimeOffset(),
        };

    public static CSharpDbShardDefinitionSnapshot ToModel(ShardDefinitionMessage value)
        => new()
        {
            ShardId = value.ShardId,
            Enabled = value.Enabled,
            Transport = TryParseTransport(value.Transport),
            Endpoint = value.Endpoint,
            DataSource = value.DataSource,
            HasConnectionString = value.HasConnectionString,
            HasApiKey = value.HasApiKey,
            ApiKeyHeaderName = value.ApiKeyHeaderName,
            Role = string.IsNullOrWhiteSpace(value.Role) ? CSharpDbShardRoles.Primary : value.Role,
            PrimaryShardId = value.PrimaryShardId,
            PromotionEligible = value.PromotionEligible,
            ReplicationLagBytes = value.ReplicationLagBytes,
            LastReplicatedUtc = value.LastReplicatedUtc?.ToDateTimeOffset(),
        };

    public static ShardBucketRangeMessage ToMessage(CSharpDbShardBucketRange value)
        => new()
        {
            StartBucketInclusive = value.StartBucketInclusive,
            EndBucketExclusive = value.EndBucketExclusive,
            ShardId = value.ShardId,
        };

    public static CSharpDbShardBucketRange ToModel(ShardBucketRangeMessage value)
        => new()
        {
            StartBucketInclusive = value.StartBucketInclusive,
            EndBucketExclusive = value.EndBucketExclusive,
            ShardId = value.ShardId,
        };

    public static ShardDirectoryDefinitionMessage ToMessage(CSharpDbShardDirectoryDefinition value)
        => new()
        {
            DirectoryName = value.DirectoryName,
            TargetKeyspace = value.TargetKeyspace,
            Description = value.Description,
            ReadOnly = value.ReadOnly,
            EntryCount = value.EntryCount,
        };

    public static CSharpDbShardDirectoryDefinition ToModel(ShardDirectoryDefinitionMessage value)
        => new()
        {
            DirectoryName = value.DirectoryName,
            TargetKeyspace = value.TargetKeyspace,
            Description = value.Description,
            ReadOnly = value.ReadOnly,
            EntryCount = value.EntryCount,
        };

    public static ShardDirectoryEntryMessage ToMessage(CSharpDbShardDirectoryEntry value)
        => new()
        {
            DirectoryName = value.DirectoryName,
            LookupKey = value.LookupKey,
            TargetKeyspace = value.TargetKeyspace,
            RouteKey = value.RouteKey,
            ShardId = value.ShardId,
            MapVersion = value.MapVersion,
            State = value.State,
        };

    public static CSharpDbShardDirectoryEntry ToModel(ShardDirectoryEntryMessage value)
        => new()
        {
            DirectoryName = value.DirectoryName,
            LookupKey = value.LookupKey,
            TargetKeyspace = value.TargetKeyspace,
            RouteKey = value.RouteKey,
            ShardId = value.ShardId,
            MapVersion = value.MapVersion,
            State = value.State,
        };

    public static ShardMapSnapshotMessage ToMessage(CSharpDbShardMapSnapshot value)
    {
        var message = new ShardMapSnapshotMessage
        {
            Keyspace = value.Keyspace,
            MapVersion = value.MapVersion,
            VirtualBucketCount = value.VirtualBucketCount,
        };

        message.Shards.Add(value.Shards.Select(ToMessage));
        message.BucketRanges.Add(value.BucketRanges.Select(ToMessage));
        message.ExactKeyPins.Add(value.ExactKeyPins);
        message.Directories.Add(value.Directories.Select(ToMessage));
        return message;
    }

    public static CSharpDbShardMapSnapshot ToModel(ShardMapSnapshotMessage value)
        => new()
        {
            Keyspace = value.Keyspace,
            MapVersion = value.MapVersion,
            VirtualBucketCount = value.VirtualBucketCount,
            Shards = value.Shards.Select(ToModel).ToList(),
            BucketRanges = value.BucketRanges.Select(ToModel).ToList(),
            ExactKeyPins = new Dictionary<string, string>(value.ExactKeyPins, StringComparer.Ordinal),
            Directories = value.Directories.Select(ToModel).ToList(),
        };

    public static ShardingOptionsMessage ToMessage(CSharpDbShardingOptions value)
    {
        var message = new ShardingOptionsMessage
        {
            Enabled = value.Enabled,
            Keyspace = value.Keyspace,
            MapVersion = value.MapVersion,
            VirtualBucketCount = value.VirtualBucketCount,
        };

        message.Shards.Add(value.Shards.Select(ToMessage));
        message.BucketRanges.Add(value.BucketRanges.Select(ToMessage));
        message.ExactKeyPins.Add(value.ExactKeyPins);
        message.Directories.Add(value.Directories.Select(ToMessage));
        message.DirectoryEntries.Add(value.DirectoryEntries.Select(ToMessage));
        return message;
    }

    public static CSharpDbShardingOptions ToModel(ShardingOptionsMessage value)
        => new()
        {
            Enabled = value.Enabled,
            Keyspace = value.Keyspace,
            MapVersion = value.MapVersion,
            VirtualBucketCount = value.VirtualBucketCount,
            Shards = value.Shards.Select(ToModel).ToArray(),
            BucketRanges = value.BucketRanges.Select(ToModel).ToArray(),
            ExactKeyPins = new Dictionary<string, string>(value.ExactKeyPins, StringComparer.Ordinal),
            Directories = value.Directories.Select(ToModel).ToArray(),
            DirectoryEntries = value.DirectoryEntries.Select(ToModel).ToArray(),
        };

    public static ShardRouteRequest ToMessage(CSharpDbRouteContext value)
        => new()
        {
            Keyspace = value.Keyspace,
            Key = value.Key,
        };

    public static CSharpDbRouteContext ToModel(ShardRouteRequest value)
        => new()
        {
            Keyspace = value.Keyspace,
            Key = value.Key,
        };

    public static ShardResolutionMessage ToMessage(CSharpDbShardResolution value)
        => new()
        {
            Keyspace = value.Keyspace,
            Key = value.Key,
            Token = value.Token,
            Bucket = value.Bucket,
            ShardId = value.ShardId,
            MapVersion = value.MapVersion,
        };

    public static CSharpDbShardResolution ToModel(ShardResolutionMessage value)
        => new()
        {
            Keyspace = value.Keyspace,
            Key = value.Key,
            Token = value.Token,
            Bucket = value.Bucket,
            ShardId = value.ShardId,
            MapVersion = value.MapVersion,
        };

    public static ShardStatusMessage ToMessage(CSharpDbShardStatus value)
        => new()
        {
            ShardId = value.ShardId,
            DataSource = value.DataSource,
            Enabled = value.Enabled,
            Healthy = value.Healthy,
            Error = value.Error,
            Info = value.Info is null ? null : ToMessage(value.Info),
            Role = value.Role,
            PrimaryShardId = value.PrimaryShardId,
            PromotionEligible = value.PromotionEligible,
            CanPromote = value.CanPromote,
            ReplicationLagBytes = value.ReplicationLagBytes,
            LastReplicatedUtc = value.LastReplicatedUtc.HasValue
                ? Timestamp.FromDateTimeOffset(value.LastReplicatedUtc.Value)
                : null,
        };

    public static CSharpDbShardStatus ToModel(ShardStatusMessage value)
        => new()
        {
            ShardId = value.ShardId,
            DataSource = value.DataSource,
            Enabled = value.Enabled,
            Healthy = value.Healthy,
            Error = value.Error,
            Info = value.Info is null ? null : ToModel(value.Info),
            Role = string.IsNullOrWhiteSpace(value.Role) ? CSharpDbShardRoles.Primary : value.Role,
            PrimaryShardId = value.PrimaryShardId,
            PromotionEligible = value.PromotionEligible,
            CanPromote = value.CanPromote,
            ReplicationLagBytes = value.ReplicationLagBytes,
            LastReplicatedUtc = value.LastReplicatedUtc?.ToDateTimeOffset(),
        };

    public static ShardSqlExecutionResultMessage ToMessage(CSharpDbShardSqlExecutionResult value)
        => new()
        {
            ShardId = value.ShardId,
            Result = value.Result is null ? null : ToMessage(value.Result),
            Error = value.Error,
        };

    public static CSharpDbShardSqlExecutionResult ToModel(ShardSqlExecutionResultMessage value)
        => new()
        {
            ShardId = value.ShardId,
            Result = value.Result is null ? null : ToModel(value.Result),
            Error = value.Error,
        };

    public static ShardCatalogHistoryEntryMessage ToMessage(CSharpDbShardCatalogHistoryEntry value)
        => new()
        {
            AppliedUtc = Timestamp.FromDateTimeOffset(value.AppliedUtc),
            MapVersion = value.MapVersion,
            Operator = value.Operator,
            Comment = value.Comment,
            MetadataOnlyOwnershipChange = value.MetadataOnlyOwnershipChange,
        };

    public static CSharpDbShardCatalogHistoryEntry ToModel(ShardCatalogHistoryEntryMessage value)
        => new()
        {
            AppliedUtc = value.AppliedUtc.ToDateTimeOffset(),
            MapVersion = value.MapVersion,
            Operator = value.Operator,
            Comment = value.Comment,
            MetadataOnlyOwnershipChange = value.MetadataOnlyOwnershipChange,
        };

    public static ShardCatalogIssueMessage ToMessage(CSharpDbShardCatalogIssue value)
        => new()
        {
            Severity = value.Severity.ToString(),
            Code = value.Code,
            Message = value.Message,
        };

    public static CSharpDbShardCatalogIssue ToModel(ShardCatalogIssueMessage value)
        => new()
        {
            Severity = System.Enum.TryParse(value.Severity, ignoreCase: true, out CSharpDbShardCatalogIssueSeverity severity)
                ? severity
                : CSharpDbShardCatalogIssueSeverity.Error,
            Code = value.Code,
            Message = value.Message,
        };

    public static ShardCatalogValidationResultMessage ToMessage(CSharpDbShardCatalogValidationResult value)
    {
        var message = new ShardCatalogValidationResultMessage
        {
            IsValid = value.IsValid,
            RequiresDataMigration = value.RequiresDataMigration,
            Preview = value.Preview is null ? null : ToMessage(value.Preview),
        };
        message.Issues.Add(value.Issues.Select(ToMessage));
        return message;
    }

    public static CSharpDbShardCatalogValidationResult ToModel(ShardCatalogValidationResultMessage value)
        => new()
        {
            IsValid = value.IsValid,
            RequiresDataMigration = value.RequiresDataMigration,
            Preview = value.Preview is null ? null : ToModel(value.Preview),
            Issues = value.Issues.Select(ToModel).ToList(),
        };

    public static ShardCatalogStateMessage ToMessage(CSharpDbShardCatalogState value)
    {
        var message = new ShardCatalogStateMessage
        {
            Source = value.Source,
            IsCatalogEnabled = value.IsCatalogEnabled,
            IsWritable = value.IsWritable,
            ActiveMap = ToMessage(value.ActiveMap),
            PendingMap = value.PendingMap is null ? null : ToMessage(value.PendingMap),
        };
        message.History.Add(value.History.Select(ToMessage));
        return message;
    }

    public static CSharpDbShardCatalogState ToModel(ShardCatalogStateMessage value)
        => new()
        {
            Source = value.Source,
            IsCatalogEnabled = value.IsCatalogEnabled,
            IsWritable = value.IsWritable,
            ActiveMap = ToModel(value.ActiveMap),
            PendingMap = value.PendingMap is null ? null : ToModel(value.PendingMap),
            History = value.History.Select(ToModel).ToList(),
        };

    public static ShardCatalogUpdateRequestMessage ToMessage(CSharpDbShardCatalogUpdateRequest value)
        => new()
        {
            Options = ToMessage(value.Options),
            ExpectedCurrentMapVersion = value.ExpectedCurrentMapVersion,
            AllowMetadataOnlyOwnershipChange = value.AllowMetadataOnlyOwnershipChange,
            Operator = value.Operator,
            Comment = value.Comment,
        };

    public static CSharpDbShardCatalogUpdateRequest ToModel(ShardCatalogUpdateRequestMessage value)
        => new()
        {
            Options = ToModel(value.Options),
            ExpectedCurrentMapVersion = value.ExpectedCurrentMapVersion,
            AllowMetadataOnlyOwnershipChange = value.AllowMetadataOnlyOwnershipChange,
            Operator = value.Operator,
            Comment = value.Comment,
        };

    public static ShardCatalogApplyResultMessage ToMessage(CSharpDbShardCatalogApplyResult value)
        => new()
        {
            Applied = value.Applied,
            RequiresRestart = value.RequiresRestart,
            Message = value.Message,
            Validation = ToMessage(value.Validation),
            PendingMap = value.PendingMap is null ? null : ToMessage(value.PendingMap),
        };

    public static CSharpDbShardCatalogApplyResult ToModel(ShardCatalogApplyResultMessage value)
        => new()
        {
            Applied = value.Applied,
            RequiresRestart = value.RequiresRestart,
            Message = value.Message,
            Validation = ToModel(value.Validation),
            PendingMap = value.PendingMap is null ? null : ToModel(value.PendingMap),
        };

    public static ShardMigrationTableManifestMessage ToMessage(CSharpDbShardMigrationTableManifest value)
        => new()
        {
            TableName = value.TableName,
            RouteKeyColumn = value.RouteKeyColumn,
            PrimaryKeyColumn = value.PrimaryKeyColumn,
        };

    public static CSharpDbShardMigrationTableManifest ToModel(ShardMigrationTableManifestMessage value)
        => new()
        {
            TableName = value.TableName,
            RouteKeyColumn = value.RouteKeyColumn,
            PrimaryKeyColumn = value.PrimaryKeyColumn,
        };

    public static ShardMigrationCollectionManifestMessage ToMessage(CSharpDbShardMigrationCollectionManifest value)
        => new()
        {
            CollectionName = value.CollectionName,
            RouteKeyPropertyName = value.RouteKeyPropertyName,
        };

    public static CSharpDbShardMigrationCollectionManifest ToModel(ShardMigrationCollectionManifestMessage value)
        => new()
        {
            CollectionName = value.CollectionName,
            RouteKeyPropertyName = value.RouteKeyPropertyName,
        };

    public static ShardMigrationManifestMessage ToMessage(CSharpDbShardMigrationManifest value)
    {
        var message = new ShardMigrationManifestMessage
        {
            PageSize = value.PageSize,
        };
        message.Tables.Add(value.Tables.Select(ToMessage));
        message.Collections.Add(value.Collections.Select(ToMessage));
        return message;
    }

    public static CSharpDbShardMigrationManifest ToModel(ShardMigrationManifestMessage value)
        => new()
        {
            PageSize = value.PageSize <= 0 ? 500 : value.PageSize,
            Tables = value.Tables.Select(ToModel).ToList(),
            Collections = value.Collections.Select(ToModel).ToList(),
        };

    public static ShardExactKeyMigrationRequestMessage ToMessage(CSharpDbShardExactKeyMigrationRequest value)
        => new()
        {
            Keyspace = value.Keyspace,
            RouteKey = value.RouteKey,
            DestinationShardId = value.DestinationShardId,
            Manifest = ToMessage(value.Manifest),
            ExpectedCurrentMapVersion = value.ExpectedCurrentMapVersion,
            OverwriteDestinationRows = value.OverwriteDestinationRows,
            DeleteSourceAfterVerification = value.DeleteSourceAfterVerification,
            Operator = value.Operator,
            Comment = value.Comment,
        };

    public static CSharpDbShardExactKeyMigrationRequest ToModel(ShardExactKeyMigrationRequestMessage value)
        => new()
        {
            Keyspace = value.Keyspace,
            RouteKey = value.RouteKey,
            DestinationShardId = value.DestinationShardId,
            Manifest = value.Manifest is null ? new CSharpDbShardMigrationManifest() : ToModel(value.Manifest),
            ExpectedCurrentMapVersion = value.ExpectedCurrentMapVersion,
            OverwriteDestinationRows = value.OverwriteDestinationRows,
            DeleteSourceAfterVerification = value.DeleteSourceAfterVerification,
            Operator = value.Operator,
            Comment = value.Comment,
        };

    public static ShardBucketRangeMigrationRequestMessage ToMessage(CSharpDbShardBucketRangeMigrationRequest value)
        => new()
        {
            Keyspace = value.Keyspace,
            SourceShardId = value.SourceShardId,
            DestinationShardId = value.DestinationShardId,
            StartBucketInclusive = value.StartBucketInclusive,
            EndBucketExclusive = value.EndBucketExclusive,
            Manifest = ToMessage(value.Manifest),
            ExpectedCurrentMapVersion = value.ExpectedCurrentMapVersion,
            OverwriteDestinationRows = value.OverwriteDestinationRows,
            DeleteSourceAfterVerification = value.DeleteSourceAfterVerification,
            Operator = value.Operator,
            Comment = value.Comment,
        };

    public static CSharpDbShardBucketRangeMigrationRequest ToModel(ShardBucketRangeMigrationRequestMessage value)
        => new()
        {
            Keyspace = value.Keyspace,
            SourceShardId = value.SourceShardId,
            DestinationShardId = value.DestinationShardId,
            StartBucketInclusive = value.StartBucketInclusive,
            EndBucketExclusive = value.EndBucketExclusive,
            Manifest = value.Manifest is null ? new CSharpDbShardMigrationManifest() : ToModel(value.Manifest),
            ExpectedCurrentMapVersion = value.ExpectedCurrentMapVersion,
            OverwriteDestinationRows = value.OverwriteDestinationRows,
            DeleteSourceAfterVerification = value.DeleteSourceAfterVerification,
            Operator = value.Operator,
            Comment = value.Comment,
        };

    public static ShardMigrationTableResultMessage ToMessage(CSharpDbShardMigrationTableResult value)
        => new()
        {
            TableName = value.TableName,
            SourceRows = value.SourceRows,
            DestinationRows = value.DestinationRows,
            RowsCopied = value.RowsCopied,
            SourceRowsDeleted = value.SourceRowsDeleted,
            Verified = value.Verified,
            SourceChecksum = value.SourceChecksum,
            DestinationChecksum = value.DestinationChecksum,
            Error = value.Error,
        };

    public static CSharpDbShardMigrationTableResult ToModel(ShardMigrationTableResultMessage value)
        => new()
        {
            TableName = value.TableName,
            SourceRows = value.SourceRows,
            DestinationRows = value.DestinationRows,
            RowsCopied = value.RowsCopied,
            SourceRowsDeleted = value.SourceRowsDeleted,
            Verified = value.Verified,
            SourceChecksum = value.SourceChecksum,
            DestinationChecksum = value.DestinationChecksum,
            Error = value.Error,
        };

    public static ShardMigrationCollectionResultMessage ToMessage(CSharpDbShardMigrationCollectionResult value)
        => new()
        {
            CollectionName = value.CollectionName,
            SourceDocuments = value.SourceDocuments,
            DestinationDocuments = value.DestinationDocuments,
            DocumentsCopied = value.DocumentsCopied,
            SourceDocumentsDeleted = value.SourceDocumentsDeleted,
            Verified = value.Verified,
            SourceChecksum = value.SourceChecksum,
            DestinationChecksum = value.DestinationChecksum,
            Error = value.Error,
        };

    public static CSharpDbShardMigrationCollectionResult ToModel(ShardMigrationCollectionResultMessage value)
        => new()
        {
            CollectionName = value.CollectionName,
            SourceDocuments = value.SourceDocuments,
            DestinationDocuments = value.DestinationDocuments,
            DocumentsCopied = value.DocumentsCopied,
            SourceDocumentsDeleted = value.SourceDocumentsDeleted,
            Verified = value.Verified,
            SourceChecksum = value.SourceChecksum,
            DestinationChecksum = value.DestinationChecksum,
            Error = value.Error,
        };

    public static ShardMigrationResultMessage ToMessage(CSharpDbShardMigrationResult value)
    {
        var message = new ShardMigrationResultMessage
        {
            MigrationId = value.MigrationId,
            StartedUtc = Timestamp.FromDateTimeOffset(value.StartedUtc),
            CompletedUtc = Timestamp.FromDateTimeOffset(value.CompletedUtc),
            Succeeded = value.Succeeded,
            Status = value.Status,
            Message = value.Message,
            Keyspace = value.Keyspace,
            RouteKey = value.RouteKey,
            SourceShardId = value.SourceShardId,
            DestinationShardId = value.DestinationShardId,
            MapVersion = value.MapVersion,
            PendingMapVersion = value.PendingMapVersion,
            RequiresRestart = value.RequiresRestart,
            RequiresOperatorRecovery = value.RequiresOperatorRecovery,
            RecoveryAction = value.RecoveryAction,
            CatalogApplyResult = value.CatalogApplyResult is null ? null : ToMessage(value.CatalogApplyResult),
        };
        message.Tables.Add(value.Tables.Select(ToMessage));
        message.Collections.Add(value.Collections.Select(ToMessage));
        message.Issues.Add(value.Issues.Select(ToMessage));
        return message;
    }

    public static CSharpDbShardMigrationResult ToModel(ShardMigrationResultMessage value)
        => new()
        {
            MigrationId = value.MigrationId,
            StartedUtc = value.StartedUtc.ToDateTimeOffset(),
            CompletedUtc = value.CompletedUtc.ToDateTimeOffset(),
            Succeeded = value.Succeeded,
            Status = value.Status,
            Message = value.Message,
            Keyspace = value.Keyspace,
            RouteKey = value.RouteKey,
            SourceShardId = value.SourceShardId,
            DestinationShardId = value.DestinationShardId,
            MapVersion = value.MapVersion,
            PendingMapVersion = value.PendingMapVersion,
            RequiresRestart = value.RequiresRestart,
            RequiresOperatorRecovery = value.RequiresOperatorRecovery,
            RecoveryAction = value.RecoveryAction,
            Tables = value.Tables.Select(ToModel).ToList(),
            Collections = value.Collections.Select(ToModel).ToList(),
            Issues = value.Issues.Select(ToModel).ToList(),
            CatalogApplyResult = value.CatalogApplyResult is null ? null : ToModel(value.CatalogApplyResult),
        };

    public static ShardMigrationHistoryEntryMessage ToMessage(CSharpDbShardMigrationHistoryEntry value)
    {
        var message = new ShardMigrationHistoryEntryMessage
        {
            MigrationId = value.MigrationId,
            MigrationType = value.MigrationType,
            StartedUtc = Timestamp.FromDateTimeOffset(value.StartedUtc),
            CompletedUtc = Timestamp.FromDateTimeOffset(value.CompletedUtc),
            RecordedUtc = Timestamp.FromDateTimeOffset(value.RecordedUtc),
            Succeeded = value.Succeeded,
            Status = value.Status,
            Message = value.Message,
            Keyspace = value.Keyspace,
            RouteKey = value.RouteKey,
            SourceShardId = value.SourceShardId,
            DestinationShardId = value.DestinationShardId,
            MapVersion = value.MapVersion,
            PendingMapVersion = value.PendingMapVersion,
            RequiresRestart = value.RequiresRestart,
            RequiresOperatorRecovery = value.RequiresOperatorRecovery,
            RecoveryAction = value.RecoveryAction,
            Operator = value.Operator,
            Comment = value.Comment,
        };
        message.Tables.Add(value.Tables.Select(ToMessage));
        message.Collections.Add(value.Collections.Select(ToMessage));
        message.Issues.Add(value.Issues.Select(ToMessage));
        return message;
    }

    public static CSharpDbShardMigrationHistoryEntry ToModel(ShardMigrationHistoryEntryMessage value)
        => new()
        {
            MigrationId = value.MigrationId,
            MigrationType = value.MigrationType,
            StartedUtc = value.StartedUtc.ToDateTimeOffset(),
            CompletedUtc = value.CompletedUtc.ToDateTimeOffset(),
            RecordedUtc = value.RecordedUtc.ToDateTimeOffset(),
            Succeeded = value.Succeeded,
            Status = value.Status,
            Message = value.Message,
            Keyspace = value.Keyspace,
            RouteKey = value.RouteKey,
            SourceShardId = value.SourceShardId,
            DestinationShardId = value.DestinationShardId,
            MapVersion = value.MapVersion,
            PendingMapVersion = value.PendingMapVersion,
            RequiresRestart = value.RequiresRestart,
            RequiresOperatorRecovery = value.RequiresOperatorRecovery,
            RecoveryAction = value.RecoveryAction,
            Operator = value.Operator,
            Comment = value.Comment,
            Tables = value.Tables.Select(ToModel).ToList(),
            Collections = value.Collections.Select(ToModel).ToList(),
            Issues = value.Issues.Select(ToModel).ToList(),
        };

    public static ProcedureParameterDefinitionMessage ToMessage(ProcedureParameterDefinition value)
        => new()
        {
            Name = value.Name,
            Type = ToMessage(value.Type),
            Required = value.Required,
            DefaultValue = value.Default is null ? null : GrpcValueMapper.ToMessage(value.Default),
            Description = value.Description,
        };

    public static ProcedureParameterDefinition ToModel(ProcedureParameterDefinitionMessage value)
        => new()
        {
            Name = value.Name,
            Type = ToModel(value.Type),
            Required = value.Required,
            Default = GrpcValueMapper.FromMessage(value.DefaultValue),
            Description = value.Description,
        };

    public static ProcedureDefinitionMessage ToMessage(ProcedureDefinition value)
    {
        var message = new ProcedureDefinitionMessage
        {
            Name = value.Name,
            BodySql = value.BodySql,
            Description = value.Description,
            IsEnabled = value.IsEnabled,
            CreatedUtc = ToTimestamp(value.CreatedUtc),
            UpdatedUtc = ToTimestamp(value.UpdatedUtc),
        };
        message.Parameters.Add(value.Parameters.Select(ToMessage));
        return message;
    }

    public static ProcedureDefinition ToModel(ProcedureDefinitionMessage value)
        => new()
        {
            Name = value.Name,
            BodySql = value.BodySql,
            Parameters = value.Parameters.Select(ToModel).ToList(),
            Description = value.Description,
            IsEnabled = value.IsEnabled,
            CreatedUtc = value.CreatedUtc.ToDateTime(),
            UpdatedUtc = value.UpdatedUtc.ToDateTime(),
        };

    public static ProcedureStatementExecutionResultMessage ToMessage(ProcedureStatementExecutionResult value)
        => new()
        {
            StatementIndex = value.StatementIndex,
            StatementText = value.StatementText,
            IsQuery = value.IsQuery,
            ColumnNames = value.ColumnNames is null ? null : ToStringList(value.ColumnNames),
            Rows = value.Rows is null ? null : ToVariantArrayList(value.Rows),
            RowsAffected = value.RowsAffected,
            Elapsed = Duration.FromTimeSpan(value.Elapsed),
        };

    public static ProcedureStatementExecutionResult ToModel(ProcedureStatementExecutionResultMessage value)
        => new()
        {
            StatementIndex = value.StatementIndex,
            StatementText = value.StatementText,
            IsQuery = value.IsQuery,
            ColumnNames = value.ColumnNames?.Values.ToArray(),
            Rows = value.Rows is null ? null : ToRows(value.Rows),
            RowsAffected = value.RowsAffected,
            Elapsed = value.Elapsed.ToTimeSpan(),
        };

    public static ProcedureExecutionResultMessage ToMessage(ProcedureExecutionResult value)
    {
        var message = new ProcedureExecutionResultMessage
        {
            ProcedureName = value.ProcedureName,
            Succeeded = value.Succeeded,
            Error = value.Error,
            FailedStatementIndex = value.FailedStatementIndex,
            Elapsed = Duration.FromTimeSpan(value.Elapsed),
        };
        message.Statements.Add(value.Statements.Select(ToMessage));
        return message;
    }

    public static ProcedureExecutionResult ToModel(ProcedureExecutionResultMessage value)
        => new()
        {
            ProcedureName = value.ProcedureName,
            Succeeded = value.Succeeded,
            Statements = value.Statements.Select(ToModel).ToList(),
            Error = value.Error,
            FailedStatementIndex = value.FailedStatementIndex,
            Elapsed = value.Elapsed.ToTimeSpan(),
        };

    public static SavedQueryDefinitionMessage ToMessage(SavedQueryDefinition value)
        => new()
        {
            Id = value.Id,
            Name = value.Name,
            SqlText = value.SqlText,
            CreatedUtc = ToTimestamp(value.CreatedUtc),
            UpdatedUtc = ToTimestamp(value.UpdatedUtc),
        };

    public static SavedQueryDefinition ToModel(SavedQueryDefinitionMessage value)
        => new()
        {
            Id = value.Id,
            Name = value.Name,
            SqlText = value.SqlText,
            CreatedUtc = value.CreatedUtc.ToDateTime(),
            UpdatedUtc = value.UpdatedUtc.ToDateTime(),
        };

    public static TransactionSessionInfoMessage ToMessage(TransactionSessionInfo value)
        => new()
        {
            TransactionId = value.TransactionId,
            ExpiresAtUtc = ToTimestamp(value.ExpiresAtUtc),
        };

    public static TransactionSessionInfo ToModel(TransactionSessionInfoMessage value)
        => new()
        {
            TransactionId = value.TransactionId,
            ExpiresAtUtc = value.ExpiresAtUtc.ToDateTime(),
        };

    public static CollectionDocumentMessage ToMessage(CollectionDocument value)
        => new()
        {
            Key = value.Key,
            Document = GrpcValueMapper.ToMessage(value.Document),
        };

    public static CollectionDocument ToModel(CollectionDocumentMessage value)
        => new()
        {
            Key = value.Key,
            Document = GrpcValueMapper.ToJsonElement(value.Document),
        };

    public static CollectionBrowseResultMessage ToMessage(CollectionBrowseResult value)
    {
        var message = new CollectionBrowseResultMessage
        {
            CollectionName = value.CollectionName,
            TotalCount = value.TotalCount,
            Page = value.Page,
            PageSize = value.PageSize,
        };
        message.Documents.Add(value.Documents.Select(ToMessage));
        return message;
    }

    public static CollectionBrowseResult ToModel(CollectionBrowseResultMessage value)
        => new()
        {
            CollectionName = value.CollectionName,
            Documents = value.Documents.Select(ToModel).ToList(),
            TotalCount = value.TotalCount,
            Page = value.Page,
            PageSize = value.PageSize,
        };

    public static ReindexRequestMessage ToMessage(ReindexRequest value)
        => new()
        {
            Scope = ToMessage(value.Scope),
            Name = value.Name,
            AllowCorruptIndexRecovery = value.AllowCorruptIndexRecovery,
        };

    public static ReindexRequest ToModel(ReindexRequestMessage value)
        => new()
        {
            Scope = ToModel(value.Scope),
            Name = value.Name,
            AllowCorruptIndexRecovery = value.AllowCorruptIndexRecovery,
        };

    public static BackupRequestMessage ToMessage(BackupRequest value)
        => new()
        {
            DestinationPath = value.DestinationPath,
            WithManifest = value.WithManifest,
        };

    public static BackupRequest ToModel(BackupRequestMessage value)
        => new()
        {
            DestinationPath = value.DestinationPath,
            WithManifest = value.WithManifest,
        };

    public static RestoreRequestMessage ToMessage(RestoreRequest value)
        => new()
        {
            SourcePath = value.SourcePath,
            ValidateOnly = value.ValidateOnly,
        };

    public static RestoreRequest ToModel(RestoreRequestMessage value)
        => new()
        {
            SourcePath = value.SourcePath,
            ValidateOnly = value.ValidateOnly,
        };

    public static ForeignKeyMigrationConstraintSpecMessage ToMessage(ForeignKeyMigrationConstraintSpec value)
        => new()
        {
            TableName = value.TableName,
            ColumnName = value.ColumnName,
            ReferencedTableName = value.ReferencedTableName,
            ReferencedColumnName = value.ReferencedColumnName ?? string.Empty,
            OnDelete = ToMessage(value.OnDelete),
        };

    public static ForeignKeyMigrationConstraintSpec ToModel(ForeignKeyMigrationConstraintSpecMessage value)
        => new()
        {
            TableName = value.TableName,
            ColumnName = value.ColumnName,
            ReferencedTableName = value.ReferencedTableName,
            ReferencedColumnName = string.IsNullOrWhiteSpace(value.ReferencedColumnName) ? null : value.ReferencedColumnName,
            OnDelete = ToModel(value.OnDelete),
        };

    public static ForeignKeyMigrationRequestMessage ToMessage(ForeignKeyMigrationRequest value)
    {
        var message = new ForeignKeyMigrationRequestMessage
        {
            ValidateOnly = value.ValidateOnly,
            BackupDestinationPath = value.BackupDestinationPath ?? string.Empty,
            ViolationSampleLimit = value.ViolationSampleLimit,
        };
        message.Constraints.Add(value.Constraints.Select(ToMessage));
        return message;
    }

    public static ForeignKeyMigrationRequest ToModel(ForeignKeyMigrationRequestMessage value)
        => new()
        {
            ValidateOnly = value.ValidateOnly,
            BackupDestinationPath = string.IsNullOrWhiteSpace(value.BackupDestinationPath) ? null : value.BackupDestinationPath,
            ViolationSampleLimit = value.ViolationSampleLimit,
            Constraints = value.Constraints.Select(ToModel).ToArray(),
        };

    public static SpaceUsageReportMessage ToMessage(SpaceUsageReport value)
        => new()
        {
            DatabaseFileBytes = value.DatabaseFileBytes,
            WalFileBytes = value.WalFileBytes,
            PageSizeBytes = value.PageSizeBytes,
            PhysicalPageCount = value.PhysicalPageCount,
            DeclaredPageCount = value.DeclaredPageCount,
            FreelistPageCount = value.FreelistPageCount,
            FreelistBytes = value.FreelistBytes,
        };

    public static SpaceUsageReport ToModel(SpaceUsageReportMessage value)
        => new()
        {
            DatabaseFileBytes = value.DatabaseFileBytes,
            WalFileBytes = value.WalFileBytes,
            PageSizeBytes = value.PageSizeBytes,
            PhysicalPageCount = value.PhysicalPageCount,
            DeclaredPageCount = value.DeclaredPageCount,
            FreelistPageCount = value.FreelistPageCount,
            FreelistBytes = value.FreelistBytes,
        };

    public static FragmentationReportMessage ToMessage(FragmentationReport value)
        => new()
        {
            BtreeFreeBytes = value.BTreeFreeBytes,
            PagesWithFreeSpace = value.PagesWithFreeSpace,
            TailFreelistPageCount = value.TailFreelistPageCount,
            TailFreelistBytes = value.TailFreelistBytes,
        };

    public static FragmentationReport ToModel(FragmentationReportMessage value)
        => new()
        {
            BTreeFreeBytes = value.BtreeFreeBytes,
            PagesWithFreeSpace = value.PagesWithFreeSpace,
            TailFreelistPageCount = value.TailFreelistPageCount,
            TailFreelistBytes = value.TailFreelistBytes,
        };

    public static DatabaseMaintenanceReportMessage ToMessage(DatabaseMaintenanceReport value)
    {
        var message = new DatabaseMaintenanceReportMessage
        {
            SchemaVersion = value.SchemaVersion,
            DatabasePath = value.DatabasePath,
            SpaceUsage = ToMessage(value.SpaceUsage),
            Fragmentation = ToMessage(value.Fragmentation),
        };

        foreach (KeyValuePair<string, int> entry in value.PageTypeHistogram)
            message.PageTypeHistogram[entry.Key] = entry.Value;

        return message;
    }

    public static DatabaseMaintenanceReport ToModel(DatabaseMaintenanceReportMessage value)
        => new()
        {
            SchemaVersion = value.SchemaVersion,
            DatabasePath = value.DatabasePath,
            SpaceUsage = ToModel(value.SpaceUsage),
            Fragmentation = ToModel(value.Fragmentation),
            PageTypeHistogram = value.PageTypeHistogram.ToDictionary(entry => entry.Key, entry => entry.Value),
        };

    public static ForeignKeyMigrationViolationMessage ToMessage(ForeignKeyMigrationViolation value)
        => new()
        {
            TableName = value.TableName,
            ColumnName = value.ColumnName,
            ReferencedTableName = value.ReferencedTableName,
            ReferencedColumnName = value.ReferencedColumnName,
            ChildKeyColumnName = value.ChildKeyColumnName,
            ChildKeyValue = GrpcValueMapper.ToMessage(value.ChildKeyValue),
            ChildValue = GrpcValueMapper.ToMessage(value.ChildValue),
            Reason = value.Reason,
        };

    public static ForeignKeyMigrationViolation ToModel(ForeignKeyMigrationViolationMessage value)
        => new()
        {
            TableName = value.TableName,
            ColumnName = value.ColumnName,
            ReferencedTableName = value.ReferencedTableName,
            ReferencedColumnName = value.ReferencedColumnName,
            ChildKeyColumnName = value.ChildKeyColumnName,
            ChildKeyValue = GrpcValueMapper.FromMessage(value.ChildKeyValue),
            ChildValue = GrpcValueMapper.FromMessage(value.ChildValue),
            Reason = value.Reason,
        };

    public static ForeignKeyMigrationAppliedConstraintMessage ToMessage(ForeignKeyMigrationAppliedConstraint value)
        => new()
        {
            TableName = value.TableName,
            ColumnName = value.ColumnName,
            ReferencedTableName = value.ReferencedTableName,
            ReferencedColumnName = value.ReferencedColumnName,
            ConstraintName = value.ConstraintName,
            SupportingIndexName = value.SupportingIndexName,
            OnDelete = ToMessage(value.OnDelete),
        };

    public static ForeignKeyMigrationAppliedConstraint ToModel(ForeignKeyMigrationAppliedConstraintMessage value)
        => new()
        {
            TableName = value.TableName,
            ColumnName = value.ColumnName,
            ReferencedTableName = value.ReferencedTableName,
            ReferencedColumnName = value.ReferencedColumnName,
            ConstraintName = value.ConstraintName,
            SupportingIndexName = value.SupportingIndexName,
            OnDelete = ToModel(value.OnDelete),
        };

    public static ForeignKeyMigrationResultMessage ToMessage(ForeignKeyMigrationResult value)
    {
        var message = new ForeignKeyMigrationResultMessage
        {
            ValidateOnly = value.ValidateOnly,
            Succeeded = value.Succeeded,
            BackupDestinationPath = value.BackupDestinationPath,
            AffectedTables = value.AffectedTables,
            AppliedForeignKeys = value.AppliedForeignKeys,
            CopiedRows = value.CopiedRows,
            ViolationCount = value.ViolationCount,
        };
        message.Violations.Add(value.Violations.Select(ToMessage));
        message.AppliedConstraints.Add(value.AppliedConstraints.Select(ToMessage));
        return message;
    }

    public static ForeignKeyMigrationResult ToModel(ForeignKeyMigrationResultMessage value)
        => new()
        {
            ValidateOnly = value.ValidateOnly,
            Succeeded = value.Succeeded,
            BackupDestinationPath = value.BackupDestinationPath,
            AffectedTables = value.AffectedTables,
            AppliedForeignKeys = value.AppliedForeignKeys,
            CopiedRows = value.CopiedRows,
            ViolationCount = value.ViolationCount,
            Violations = value.Violations.Select(ToModel).ToArray(),
            AppliedConstraints = value.AppliedConstraints.Select(ToModel).ToArray(),
        };

    public static ReindexResultMessage ToMessage(ReindexResult value)
        => new()
        {
            Scope = ToMessage(value.Scope),
            Name = value.Name,
            RebuiltIndexCount = value.RebuiltIndexCount,
            RecoveredCorruptIndexCount = value.RecoveredCorruptIndexCount,
        };

    public static ReindexResult ToModel(ReindexResultMessage value)
        => new()
        {
            Scope = ToModel(value.Scope),
            Name = value.Name,
            RebuiltIndexCount = value.RebuiltIndexCount,
            RecoveredCorruptIndexCount = value.RecoveredCorruptIndexCount,
        };

    public static VacuumResultMessage ToMessage(VacuumResult value)
        => new()
        {
            DatabaseFileBytesBefore = value.DatabaseFileBytesBefore,
            DatabaseFileBytesAfter = value.DatabaseFileBytesAfter,
            PhysicalPageCountBefore = value.PhysicalPageCountBefore,
            PhysicalPageCountAfter = value.PhysicalPageCountAfter,
        };

    public static VacuumResult ToModel(VacuumResultMessage value)
        => new()
        {
            DatabaseFileBytesBefore = value.DatabaseFileBytesBefore,
            DatabaseFileBytesAfter = value.DatabaseFileBytesAfter,
            PhysicalPageCountBefore = value.PhysicalPageCountBefore,
            PhysicalPageCountAfter = value.PhysicalPageCountAfter,
        };

    public static BackupResultMessage ToMessage(BackupResult value)
        => new()
        {
            SourcePath = value.SourcePath,
            DestinationPath = value.DestinationPath,
            ManifestPath = value.ManifestPath,
            DatabaseFileBytes = value.DatabaseFileBytes,
            PhysicalPageCount = value.PhysicalPageCount,
            DeclaredPageCount = value.DeclaredPageCount,
            ChangeCounter = value.ChangeCounter,
            WarningCount = value.WarningCount,
            ErrorCount = value.ErrorCount,
            Sha256 = value.Sha256,
        };

    public static BackupResult ToModel(BackupResultMessage value)
        => new()
        {
            SourcePath = value.SourcePath,
            DestinationPath = value.DestinationPath,
            ManifestPath = value.ManifestPath,
            DatabaseFileBytes = value.DatabaseFileBytes,
            PhysicalPageCount = value.PhysicalPageCount,
            DeclaredPageCount = value.DeclaredPageCount,
            ChangeCounter = value.ChangeCounter,
            WarningCount = value.WarningCount,
            ErrorCount = value.ErrorCount,
            Sha256 = value.Sha256,
        };

    public static RestoreResultMessage ToMessage(RestoreResult value)
        => new()
        {
            SourcePath = value.SourcePath,
            DestinationPath = value.DestinationPath,
            ValidateOnly = value.ValidateOnly,
            DatabaseFileBytes = value.DatabaseFileBytes,
            PhysicalPageCount = value.PhysicalPageCount,
            DeclaredPageCount = value.DeclaredPageCount,
            ChangeCounter = value.ChangeCounter,
            SourceWalExists = value.SourceWalExists,
            WarningCount = value.WarningCount,
            ErrorCount = value.ErrorCount,
        };

    public static RestoreResult ToModel(RestoreResultMessage value)
        => new()
        {
            SourcePath = value.SourcePath,
            DestinationPath = value.DestinationPath,
            ValidateOnly = value.ValidateOnly,
            DatabaseFileBytes = value.DatabaseFileBytes,
            PhysicalPageCount = value.PhysicalPageCount,
            DeclaredPageCount = value.DeclaredPageCount,
            ChangeCounter = value.ChangeCounter,
            SourceWalExists = value.SourceWalExists,
            WarningCount = value.WarningCount,
            ErrorCount = value.ErrorCount,
        };

    public static FileHeaderReportMessage ToMessage(FileHeaderReport value)
        => new()
        {
            FileLengthBytes = value.FileLengthBytes,
            PhysicalPageCount = value.PhysicalPageCount,
            Magic = value.Magic,
            MagicValid = value.MagicValid,
            Version = value.Version,
            VersionValid = value.VersionValid,
            PageSize = value.PageSize,
            PageSizeValid = value.PageSizeValid,
            DeclaredPageCount = value.DeclaredPageCount,
            DeclaredPageCountMatchesPhysical = value.DeclaredPageCountMatchesPhysical,
            SchemaRootPage = value.SchemaRootPage,
            FreelistHead = value.FreelistHead,
            ChangeCounter = value.ChangeCounter,
        };

    public static FileHeaderReport ToModel(FileHeaderReportMessage value)
        => new()
        {
            FileLengthBytes = value.FileLengthBytes,
            PhysicalPageCount = value.PhysicalPageCount,
            Magic = value.Magic,
            MagicValid = value.MagicValid,
            Version = value.Version,
            VersionValid = value.VersionValid,
            PageSize = value.PageSize,
            PageSizeValid = value.PageSizeValid,
            DeclaredPageCount = value.DeclaredPageCount,
            DeclaredPageCountMatchesPhysical = value.DeclaredPageCountMatchesPhysical,
            SchemaRootPage = value.SchemaRootPage,
            FreelistHead = value.FreelistHead,
            ChangeCounter = value.ChangeCounter,
        };

    public static IntegrityIssueMessage ToMessage(IntegrityIssue value)
        => new()
        {
            Code = value.Code,
            Severity = ToMessage(value.Severity),
            Message = value.Message,
            PageId = value.PageId,
            Offset = value.Offset,
        };

    public static IntegrityIssue ToModel(IntegrityIssueMessage value)
        => new()
        {
            Code = value.Code,
            Severity = ToModel(value.Severity),
            Message = value.Message,
            PageId = value.PageId,
            Offset = value.Offset,
        };

    public static LeafCellReportMessage ToMessage(LeafCellReport value)
        => new()
        {
            CellIndex = value.CellIndex,
            CellOffset = value.CellOffset,
            HeaderBytes = value.HeaderBytes,
            CellTotalBytes = value.CellTotalBytes,
            Key = value.Key,
            PayloadBytes = value.PayloadBytes,
        };

    public static LeafCellReport ToModel(LeafCellReportMessage value)
        => new()
        {
            CellIndex = value.CellIndex,
            CellOffset = value.CellOffset,
            HeaderBytes = value.HeaderBytes,
            CellTotalBytes = value.CellTotalBytes,
            Key = value.Key,
            PayloadBytes = value.PayloadBytes,
        };

    public static InteriorCellReportMessage ToMessage(InteriorCellReport value)
        => new()
        {
            CellIndex = value.CellIndex,
            CellOffset = value.CellOffset,
            HeaderBytes = value.HeaderBytes,
            CellTotalBytes = value.CellTotalBytes,
            LeftChildPage = value.LeftChildPage,
            Key = value.Key,
        };

    public static InteriorCellReport ToModel(InteriorCellReportMessage value)
        => new()
        {
            CellIndex = value.CellIndex,
            CellOffset = value.CellOffset,
            HeaderBytes = value.HeaderBytes,
            CellTotalBytes = value.CellTotalBytes,
            LeftChildPage = value.LeftChildPage,
            Key = value.Key,
        };

    public static PageReportMessage ToMessage(PageReport value)
    {
        var message = new PageReportMessage
        {
            PageId = value.PageId,
            PageTypeCode = value.PageTypeCode,
            PageTypeName = value.PageTypeName,
            BaseOffset = value.BaseOffset,
            CellCount = value.CellCount,
            CellContentStart = value.CellContentStart,
            RightChildOrNextLeaf = value.RightChildOrNextLeaf,
            FreeSpaceBytes = value.FreeSpaceBytes,
        };

        message.CellOffsets.Add(value.CellOffsets);

        if (value.LeafCells is not null)
        {
            message.LeafCells = new LeafCellReportList();
            message.LeafCells.Items.Add(value.LeafCells.Select(ToMessage));
        }

        if (value.InteriorCells is not null)
        {
            message.InteriorCells = new InteriorCellReportList();
            message.InteriorCells.Items.Add(value.InteriorCells.Select(ToMessage));
        }

        return message;
    }

    public static PageReport ToModel(PageReportMessage value)
        => new()
        {
            PageId = value.PageId,
            PageTypeCode = (byte)value.PageTypeCode,
            PageTypeName = value.PageTypeName,
            BaseOffset = value.BaseOffset,
            CellCount = value.CellCount,
            CellContentStart = value.CellContentStart,
            RightChildOrNextLeaf = value.RightChildOrNextLeaf,
            FreeSpaceBytes = value.FreeSpaceBytes,
            CellOffsets = value.CellOffsets.ToList(),
            LeafCells = value.LeafCells?.Items.Select(ToModel).ToList(),
            InteriorCells = value.InteriorCells?.Items.Select(ToModel).ToList(),
        };

    public static DatabaseInspectReportMessage ToMessage(DatabaseInspectReport value)
    {
        var message = new DatabaseInspectReportMessage
        {
            SchemaVersion = value.SchemaVersion,
            DatabasePath = value.DatabasePath,
            Header = ToMessage(value.Header),
            PageCountScanned = value.PageCountScanned,
        };

        foreach (KeyValuePair<string, int> entry in value.PageTypeHistogram)
            message.PageTypeHistogram[entry.Key] = entry.Value;

        message.Issues.Add(value.Issues.Select(ToMessage));

        if (value.Pages is not null)
        {
            message.Pages = new PageReportList();
            message.Pages.Items.Add(value.Pages.Select(ToMessage));
        }

        return message;
    }

    public static DatabaseInspectReport ToModel(DatabaseInspectReportMessage value)
        => new()
        {
            SchemaVersion = value.SchemaVersion,
            DatabasePath = value.DatabasePath,
            Header = ToModel(value.Header),
            PageTypeHistogram = value.PageTypeHistogram.ToDictionary(entry => entry.Key, entry => entry.Value),
            PageCountScanned = value.PageCountScanned,
            Pages = value.Pages?.Items.Select(ToModel).ToList(),
            Issues = value.Issues.Select(ToModel).ToList(),
        };

    public static PageInspectReportMessage ToMessage(PageInspectReport value)
    {
        var message = new PageInspectReportMessage
        {
            SchemaVersion = value.SchemaVersion,
            DatabasePath = value.DatabasePath,
            PageId = value.PageId,
            Exists = value.Exists,
            Page = value.Page is null ? null : ToMessage(value.Page),
            HexDump = value.HexDump,
        };
        message.Issues.Add(value.Issues.Select(ToMessage));
        return message;
    }

    public static PageInspectReport ToModel(PageInspectReportMessage value)
        => new()
        {
            SchemaVersion = value.SchemaVersion,
            DatabasePath = value.DatabasePath,
            PageId = value.PageId,
            Exists = value.Exists,
            Page = value.Page is null ? null : ToModel(value.Page),
            HexDump = value.HexDump,
            Issues = value.Issues.Select(ToModel).ToList(),
        };

    public static IndexCheckItemMessage ToMessage(IndexCheckItem value)
    {
        var message = new IndexCheckItemMessage
        {
            IndexName = value.IndexName,
            TableName = value.TableName,
            RootPage = value.RootPage,
            RootPageValid = value.RootPageValid,
            TableExists = value.TableExists,
            ColumnsExistInTable = value.ColumnsExistInTable,
            RootTreeReachable = value.RootTreeReachable,
        };
        message.Columns.Add(value.Columns);
        return message;
    }

    public static IndexCheckItem ToModel(IndexCheckItemMessage value)
        => new()
        {
            IndexName = value.IndexName,
            TableName = value.TableName,
            Columns = value.Columns.ToList(),
            RootPage = value.RootPage,
            RootPageValid = value.RootPageValid,
            TableExists = value.TableExists,
            ColumnsExistInTable = value.ColumnsExistInTable,
            RootTreeReachable = value.RootTreeReachable,
        };

    public static IndexInspectReportMessage ToMessage(IndexInspectReport value)
    {
        var message = new IndexInspectReportMessage
        {
            SchemaVersion = value.SchemaVersion,
            DatabasePath = value.DatabasePath,
            RequestedIndexName = value.RequestedIndexName,
            SampleSize = value.SampleSize,
        };
        message.Indexes.Add(value.Indexes.Select(ToMessage));
        message.Issues.Add(value.Issues.Select(ToMessage));
        return message;
    }

    public static IndexInspectReport ToModel(IndexInspectReportMessage value)
        => new()
        {
            SchemaVersion = value.SchemaVersion,
            DatabasePath = value.DatabasePath,
            RequestedIndexName = value.RequestedIndexName,
            SampleSize = value.SampleSize,
            Indexes = value.Indexes.Select(ToModel).ToList(),
            Issues = value.Issues.Select(ToModel).ToList(),
        };

    public static WalInspectReportMessage ToMessage(WalInspectReport value)
    {
        var message = new WalInspectReportMessage
        {
            SchemaVersion = value.SchemaVersion,
            DatabasePath = value.DatabasePath,
            WalPath = value.WalPath,
            Exists = value.Exists,
            FileLengthBytes = value.FileLengthBytes,
            FullFrameCount = value.FullFrameCount,
            CommitFrameCount = value.CommitFrameCount,
            TrailingBytes = value.TrailingBytes,
            Magic = value.Magic,
            MagicValid = value.MagicValid,
            Version = value.Version,
            VersionValid = value.VersionValid,
            PageSize = value.PageSize,
            PageSizeValid = value.PageSizeValid,
            Salt1 = value.Salt1,
            Salt2 = value.Salt2,
        };
        message.Issues.Add(value.Issues.Select(ToMessage));
        return message;
    }

    public static WalInspectReport ToModel(WalInspectReportMessage value)
        => new()
        {
            SchemaVersion = value.SchemaVersion,
            DatabasePath = value.DatabasePath,
            WalPath = value.WalPath,
            Exists = value.Exists,
            FileLengthBytes = value.FileLengthBytes,
            FullFrameCount = value.FullFrameCount,
            CommitFrameCount = value.CommitFrameCount,
            TrailingBytes = value.TrailingBytes,
            Magic = value.Magic,
            MagicValid = value.MagicValid,
            Version = value.Version,
            VersionValid = value.VersionValid,
            PageSize = value.PageSize,
            PageSizeValid = value.PageSizeValid,
            Salt1 = value.Salt1,
            Salt2 = value.Salt2,
            Issues = value.Issues.Select(ToModel).ToList(),
        };

    private static CSharpDbTransport? TryParseTransport(string? value)
        => string.IsNullOrWhiteSpace(value)
            ? null
            : System.Enum.TryParse(value, ignoreCase: true, out CSharpDbTransport transport)
                ? transport
                : throw new ArgumentOutOfRangeException(nameof(value), value, "Unsupported transport enum.");

    private static Timestamp ToTimestamp(DateTime value)
    {
        DateTime utc = value.Kind switch
        {
            DateTimeKind.Utc => value,
            DateTimeKind.Local => value.ToUniversalTime(),
            _ => DateTime.SpecifyKind(value, DateTimeKind.Utc),
        };

        return Timestamp.FromDateTime(utc);
    }
}
