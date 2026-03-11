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
        };

    public static ColumnDefinition ToModel(ColumnDefinitionMessage value)
        => new()
        {
            Name = value.Name,
            Type = ToModel(value.Type),
            Nullable = value.Nullable,
            IsPrimaryKey = value.IsPrimaryKey,
            IsIdentity = value.IsIdentity,
        };

    public static TableSchemaMessage ToMessage(TableSchema value)
    {
        var message = new TableSchemaMessage
        {
            TableName = value.TableName,
        };
        message.Columns.Add(value.Columns.Select(ToMessage));
        return message;
    }

    public static TableSchema ToModel(TableSchemaMessage value)
        => new()
        {
            TableName = value.TableName,
            Columns = value.Columns.Select(ToModel).ToList(),
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
        return message;
    }

    public static IndexSchema ToModel(IndexSchemaMessage value)
        => new()
        {
            IndexName = value.IndexName,
            TableName = value.TableName,
            Columns = value.Columns.ToList(),
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
        };

    public static ReindexRequest ToModel(ReindexRequestMessage value)
        => new()
        {
            Scope = ToModel(value.Scope),
            Name = value.Name,
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

    public static ReindexResultMessage ToMessage(ReindexResult value)
        => new()
        {
            Scope = ToMessage(value.Scope),
            Name = value.Name,
            RebuiltIndexCount = value.RebuiltIndexCount,
        };

    public static ReindexResult ToModel(ReindexResultMessage value)
        => new()
        {
            Scope = ToModel(value.Scope),
            Name = value.Name,
            RebuiltIndexCount = value.RebuiltIndexCount,
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
