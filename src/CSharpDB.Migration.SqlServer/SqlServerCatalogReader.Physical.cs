using System.Data;
using Microsoft.Data.SqlClient;

namespace CSharpDB.Migration.SqlServer;

internal sealed partial class SqlServerCatalogReader
{
    internal const string FullTextCatalogsQuery =
        """
        SELECT
            fc.fulltext_catalog_id,
            fc.name,
            fc.is_default,
            fc.is_accent_sensitivity_on,
            COALESCE(fc.data_space_id, 0)
        FROM sys.fulltext_catalogs AS fc
        ORDER BY fc.fulltext_catalog_id;
        """;

    internal const string FullTextStoplistsQuery =
        """
        SELECT
            stoplist.stoplist_id,
            stoplist.name
        FROM sys.fulltext_stoplists AS stoplist
        ORDER BY stoplist.stoplist_id;
        """;

    internal const string SearchPropertyListsQuery =
        """
        SELECT
            property_list.property_list_id,
            property_list.name
        FROM sys.registered_search_property_lists AS property_list
        ORDER BY property_list.property_list_id;
        """;

    internal const string FullTextIndexesQuery =
        """
        SELECT
            fulltext_index.object_id,
            fulltext_index.unique_index_id,
            CONVERT(int, NULL),
            fulltext_index.fulltext_catalog_id,
            fulltext_index.is_enabled,
            RTRIM(fulltext_index.change_tracking_state),
            fulltext_index.change_tracking_state_desc,
            fulltext_index.stoplist_id,
            COALESCE(fulltext_index.data_space_id, 0),
            fulltext_index.property_list_id
        FROM sys.fulltext_indexes AS fulltext_index
        INNER JOIN sys.objects AS o
            ON o.object_id = fulltext_index.object_id
        WHERE o.is_ms_shipped = 0
          AND o.type IN (N'U', N'V')
        ORDER BY fulltext_index.object_id;
        """;

    internal const string FullTextIndexesV17Query =
        """
        SELECT
            fulltext_index.object_id,
            fulltext_index.unique_index_id,
            fulltext_index.index_version,
            fulltext_index.fulltext_catalog_id,
            fulltext_index.is_enabled,
            RTRIM(fulltext_index.change_tracking_state),
            fulltext_index.change_tracking_state_desc,
            fulltext_index.stoplist_id,
            COALESCE(fulltext_index.data_space_id, 0),
            fulltext_index.property_list_id
        FROM sys.fulltext_indexes AS fulltext_index
        INNER JOIN sys.objects AS o
            ON o.object_id = fulltext_index.object_id
        WHERE o.is_ms_shipped = 0
          AND o.type IN (N'U', N'V')
        ORDER BY fulltext_index.object_id;
        """;

    internal const string FullTextIndexColumnsQuery =
        """
        SELECT
            fulltext_column.object_id,
            fulltext_column.column_id,
            fulltext_column.type_column_id,
            fulltext_column.language_id,
            fulltext_column.statistical_semantics
        FROM sys.fulltext_index_columns AS fulltext_column
        INNER JOIN sys.objects AS o
            ON o.object_id = fulltext_column.object_id
        WHERE o.is_ms_shipped = 0
          AND o.type IN (N'U', N'V')
        ORDER BY fulltext_column.object_id, fulltext_column.column_id;
        """;

    internal const string DataSpacesQuery =
        """
        SELECT
            data_space.data_space_id,
            data_space.name,
            RTRIM(data_space.type),
            data_space.type_desc,
            data_space.is_default,
            data_space.is_system,
            filegroup.is_read_only
        FROM sys.data_spaces AS data_space
        LEFT JOIN sys.filegroups AS filegroup
            ON filegroup.data_space_id = data_space.data_space_id
        ORDER BY data_space.data_space_id;
        """;

    internal const string PartitionSchemesQuery =
        """
        SELECT
            partition_scheme.data_space_id,
            partition_scheme.function_id
        FROM sys.partition_schemes AS partition_scheme
        ORDER BY partition_scheme.data_space_id;
        """;

    internal const string PartitionSchemeDestinationsQuery =
        """
        SELECT
            destination.partition_scheme_id,
            destination.destination_id,
            destination.data_space_id
        FROM sys.destination_data_spaces AS destination
        ORDER BY destination.partition_scheme_id, destination.destination_id;
        """;

    internal const string PartitionFunctionsQuery =
        """
        SELECT
            partition_function.function_id,
            partition_function.name,
            partition_function.fanout,
            partition_function.boundary_value_on_right,
            partition_function.is_system
        FROM sys.partition_functions AS partition_function
        ORDER BY partition_function.function_id;
        """;

    internal const string PartitionParametersQuery =
        """
        SELECT
            parameter.function_id,
            parameter.parameter_id,
            type_schema.name,
            user_type.name,
            COALESCE(system_type.name, user_type.name),
            parameter.max_length,
            parameter.precision,
            parameter.scale,
            parameter.collation_name
        FROM sys.partition_parameters AS parameter
        INNER JOIN sys.types AS user_type
            ON user_type.user_type_id = parameter.user_type_id
        INNER JOIN sys.schemas AS type_schema
            ON type_schema.schema_id = user_type.schema_id
        LEFT JOIN sys.types AS system_type
            ON system_type.user_type_id = parameter.system_type_id
           AND system_type.is_user_defined = 0
        ORDER BY parameter.function_id, parameter.parameter_id;
        """;

    internal const string PartitionRangeValuesQuery =
        """
        SELECT
            range_value.function_id,
            range_value.boundary_id,
            range_value.parameter_id,
            CONVERT(
                bit,
                CASE WHEN range_value.value IS NULL THEN 1 ELSE 0 END),
            CONVERT(
                nvarchar(128),
                SQL_VARIANT_PROPERTY(range_value.value, N'BaseType')),
            CONVERT(
                int,
                SQL_VARIANT_PROPERTY(range_value.value, N'MaxLength')),
            CONVERT(
                tinyint,
                SQL_VARIANT_PROPERTY(range_value.value, N'Precision')),
            CONVERT(
                tinyint,
                SQL_VARIANT_PROPERTY(range_value.value, N'Scale')),
            CONVERT(
                nvarchar(128),
                SQL_VARIANT_PROPERTY(range_value.value, N'Collation')),
            CONVERT(
                int,
                DATALENGTH(CONVERT(varbinary(max), range_value.value))),
            CONVERT(
                varchar(max),
                CONVERT(varbinary(max), range_value.value),
                2)
        FROM sys.partition_range_values AS range_value
        ORDER BY
            range_value.function_id,
            range_value.boundary_id,
            range_value.parameter_id;
        """;

    internal const string IndexPartitionsQuery =
        """
        SELECT
            partition.object_id,
            partition.index_id,
            partition.partition_number,
            partition.data_compression,
            partition.data_compression_desc,
            CONVERT(bit, NULL),
            CONVERT(varchar(3), NULL),
            NULLIF(index_definition.data_space_id, 0),
            CASE
                WHEN partition_scheme.data_space_id IS NOT NULL
                THEN destination.data_space_id
                ELSE NULLIF(index_definition.data_space_id, 0)
            END
        FROM sys.partitions AS partition
        INNER JOIN sys.indexes AS index_definition
            ON index_definition.object_id = partition.object_id
           AND index_definition.index_id = partition.index_id
        INNER JOIN sys.objects AS o
            ON o.object_id = partition.object_id
        LEFT JOIN sys.partition_schemes AS partition_scheme
            ON partition_scheme.data_space_id = index_definition.data_space_id
        LEFT JOIN sys.destination_data_spaces AS destination
            ON destination.partition_scheme_id = partition_scheme.data_space_id
           AND destination.destination_id = partition.partition_number
        WHERE o.is_ms_shipped = 0
          AND o.type IN (N'U', N'V')
        ORDER BY
            partition.object_id,
            partition.index_id,
            partition.partition_number;
        """;

    internal const string IndexPartitionsV16Query =
        """
        SELECT
            partition.object_id,
            partition.index_id,
            partition.partition_number,
            partition.data_compression,
            partition.data_compression_desc,
            partition.xml_compression,
            partition.xml_compression_desc,
            NULLIF(index_definition.data_space_id, 0),
            CASE
                WHEN partition_scheme.data_space_id IS NOT NULL
                THEN destination.data_space_id
                ELSE NULLIF(index_definition.data_space_id, 0)
            END
        FROM sys.partitions AS partition
        INNER JOIN sys.indexes AS index_definition
            ON index_definition.object_id = partition.object_id
           AND index_definition.index_id = partition.index_id
        INNER JOIN sys.objects AS o
            ON o.object_id = partition.object_id
        LEFT JOIN sys.partition_schemes AS partition_scheme
            ON partition_scheme.data_space_id = index_definition.data_space_id
        LEFT JOIN sys.destination_data_spaces AS destination
            ON destination.partition_scheme_id = partition_scheme.data_space_id
           AND destination.destination_id = partition.partition_number
        WHERE o.is_ms_shipped = 0
          AND o.type IN (N'U', N'V')
        ORDER BY
            partition.object_id,
            partition.index_id,
            partition.partition_number;
        """;

    private static async ValueTask<
        IReadOnlyList<SqlServerFullTextCatalogMetadata>>
        ReadFullTextCatalogsAsync(
            SqlConnection connection,
            ReaderBudget budget,
            SqlServerInspectionLimits limits,
            CancellationToken cancellationToken)
    {
        var catalogs = new List<SqlServerFullTextCatalogMetadata>();
        await using SqlCommand command = Command(connection, FullTextCatalogsQuery);
        await using SqlDataReader reader = await command.ExecuteReaderAsync(
                CommandBehavior.SequentialAccess | CommandBehavior.SingleResult,
                cancellationToken)
            .ConfigureAwait(false);
        while (await reader.ReadAsync(cancellationToken).ConfigureAwait(false))
        {
            if (catalogs.Count == limits.MaxFullTextCatalogs)
                throw LimitExceeded("full-text catalog count");
            budget.AddStructuralRow();
            catalogs.Add(new SqlServerFullTextCatalogMetadata(
                RequiredInt32(reader, 0),
                RequiredString(reader, 1, budget, isName: true),
                RequiredBoolean(reader, 2),
                RequiredBoolean(reader, 3),
                RequiredInt32(reader, 4)));
        }
        return catalogs.AsReadOnly();
    }

    private static async ValueTask<
        IReadOnlyList<SqlServerFullTextStoplistMetadata>>
        ReadFullTextStoplistsAsync(
            SqlConnection connection,
            ReaderBudget budget,
            SqlServerInspectionLimits limits,
            CancellationToken cancellationToken)
    {
        var stoplists = new List<SqlServerFullTextStoplistMetadata>();
        await using SqlCommand command = Command(connection, FullTextStoplistsQuery);
        await using SqlDataReader reader = await command.ExecuteReaderAsync(
                CommandBehavior.SequentialAccess | CommandBehavior.SingleResult,
                cancellationToken)
            .ConfigureAwait(false);
        while (await reader.ReadAsync(cancellationToken).ConfigureAwait(false))
        {
            if (stoplists.Count == limits.MaxFullTextStoplists)
                throw LimitExceeded("full-text stoplist count");
            budget.AddStructuralRow();
            stoplists.Add(new SqlServerFullTextStoplistMetadata(
                RequiredInt32(reader, 0),
                RequiredString(reader, 1, budget, isName: true)));
        }
        return stoplists.AsReadOnly();
    }

    private static async ValueTask<
        IReadOnlyList<SqlServerSearchPropertyListMetadata>>
        ReadSearchPropertyListsAsync(
            SqlConnection connection,
            ReaderBudget budget,
            SqlServerInspectionLimits limits,
            CancellationToken cancellationToken)
    {
        var propertyLists = new List<SqlServerSearchPropertyListMetadata>();
        await using SqlCommand command = Command(
            connection,
            SearchPropertyListsQuery);
        await using SqlDataReader reader = await command.ExecuteReaderAsync(
                CommandBehavior.SequentialAccess | CommandBehavior.SingleResult,
                cancellationToken)
            .ConfigureAwait(false);
        while (await reader.ReadAsync(cancellationToken).ConfigureAwait(false))
        {
            if (propertyLists.Count == limits.MaxSearchPropertyLists)
                throw LimitExceeded("search-property-list count");
            budget.AddStructuralRow();
            propertyLists.Add(new SqlServerSearchPropertyListMetadata(
                RequiredInt32(reader, 0),
                RequiredString(reader, 1, budget, isName: true)));
        }
        return propertyLists.AsReadOnly();
    }

    private static async ValueTask<
        IReadOnlyList<SqlServerFullTextIndexMetadata>>
        ReadFullTextIndexesAsync(
            SqlConnection connection,
            SqlServerInstanceMetadata instance,
            ReaderBudget budget,
            SqlServerInspectionLimits limits,
            CancellationToken cancellationToken)
    {
        var indexes = new List<SqlServerFullTextIndexMetadata>();
        string commandText = instance.ProductMajorVersion >= 17
            ? FullTextIndexesV17Query
            : FullTextIndexesQuery;
        await using SqlCommand command = Command(connection, commandText);
        await using SqlDataReader reader = await command.ExecuteReaderAsync(
                CommandBehavior.SequentialAccess | CommandBehavior.SingleResult,
                cancellationToken)
            .ConfigureAwait(false);
        while (await reader.ReadAsync(cancellationToken).ConfigureAwait(false))
        {
            if (indexes.Count == limits.MaxFullTextIndexes)
                throw LimitExceeded("full-text index count");
            budget.AddStructuralRow();
            indexes.Add(new SqlServerFullTextIndexMetadata(
                RequiredInt32(reader, 0),
                RequiredInt32(reader, 1),
                OptionalInt32(reader, 2),
                RequiredInt32(reader, 3),
                RequiredBoolean(reader, 4),
                RequiredString(reader, 5, budget),
                RequiredString(reader, 6, budget),
                OptionalInt32(reader, 7),
                RequiredInt32(reader, 8),
                OptionalInt32(reader, 9)));
        }
        return indexes.AsReadOnly();
    }

    private static async ValueTask<
        IReadOnlyList<SqlServerFullTextIndexColumnMetadata>>
        ReadFullTextIndexColumnsAsync(
            SqlConnection connection,
            ReaderBudget budget,
            SqlServerInspectionLimits limits,
            CancellationToken cancellationToken)
    {
        var columns = new List<SqlServerFullTextIndexColumnMetadata>();
        await using SqlCommand command = Command(
            connection,
            FullTextIndexColumnsQuery);
        await using SqlDataReader reader = await command.ExecuteReaderAsync(
                CommandBehavior.SequentialAccess | CommandBehavior.SingleResult,
                cancellationToken)
            .ConfigureAwait(false);
        while (await reader.ReadAsync(cancellationToken).ConfigureAwait(false))
        {
            if (columns.Count == limits.MaxFullTextIndexColumns)
                throw LimitExceeded("full-text index-column count");
            budget.AddStructuralRow();
            columns.Add(new SqlServerFullTextIndexColumnMetadata(
                RequiredInt32(reader, 0),
                RequiredInt32(reader, 1),
                OptionalInt32(reader, 2),
                RequiredInt32(reader, 3),
                RequiredBoolean(reader, 4)));
        }
        return columns.AsReadOnly();
    }

    private static async ValueTask<IReadOnlyList<SqlServerDataSpaceMetadata>>
        ReadDataSpacesAsync(
            SqlConnection connection,
            ReaderBudget budget,
            SqlServerInspectionLimits limits,
            CancellationToken cancellationToken)
    {
        var dataSpaces = new List<SqlServerDataSpaceMetadata>();
        await using SqlCommand command = Command(connection, DataSpacesQuery);
        await using SqlDataReader reader = await command.ExecuteReaderAsync(
                CommandBehavior.SequentialAccess | CommandBehavior.SingleResult,
                cancellationToken)
            .ConfigureAwait(false);
        while (await reader.ReadAsync(cancellationToken).ConfigureAwait(false))
        {
            if (dataSpaces.Count == limits.MaxDataSpaces)
                throw LimitExceeded("data-space count");
            budget.AddStructuralRow();
            dataSpaces.Add(new SqlServerDataSpaceMetadata(
                RequiredInt32(reader, 0),
                RequiredString(reader, 1, budget, isName: true),
                RequiredString(reader, 2, budget),
                RequiredString(reader, 3, budget),
                RequiredBoolean(reader, 4),
                RequiredBoolean(reader, 5),
                OptionalBoolean(reader, 6)));
        }
        return dataSpaces.AsReadOnly();
    }

    private static async ValueTask<
        IReadOnlyList<SqlServerPartitionSchemeMetadata>>
        ReadPartitionSchemesAsync(
            SqlConnection connection,
            ReaderBudget budget,
            SqlServerInspectionLimits limits,
            CancellationToken cancellationToken)
    {
        var schemes = new List<SqlServerPartitionSchemeMetadata>();
        await using SqlCommand command = Command(connection, PartitionSchemesQuery);
        await using SqlDataReader reader = await command.ExecuteReaderAsync(
                CommandBehavior.SequentialAccess | CommandBehavior.SingleResult,
                cancellationToken)
            .ConfigureAwait(false);
        while (await reader.ReadAsync(cancellationToken).ConfigureAwait(false))
        {
            if (schemes.Count == limits.MaxPartitionSchemes)
                throw LimitExceeded("partition-scheme count");
            budget.AddStructuralRow();
            schemes.Add(new SqlServerPartitionSchemeMetadata(
                RequiredInt32(reader, 0),
                RequiredInt32(reader, 1)));
        }
        return schemes.AsReadOnly();
    }

    private static async ValueTask<
        IReadOnlyList<SqlServerPartitionSchemeDestinationMetadata>>
        ReadPartitionSchemeDestinationsAsync(
            SqlConnection connection,
            ReaderBudget budget,
            SqlServerInspectionLimits limits,
            CancellationToken cancellationToken)
    {
        var destinations =
            new List<SqlServerPartitionSchemeDestinationMetadata>();
        await using SqlCommand command = Command(
            connection,
            PartitionSchemeDestinationsQuery);
        await using SqlDataReader reader = await command.ExecuteReaderAsync(
                CommandBehavior.SequentialAccess | CommandBehavior.SingleResult,
                cancellationToken)
            .ConfigureAwait(false);
        while (await reader.ReadAsync(cancellationToken).ConfigureAwait(false))
        {
            if (destinations.Count == limits.MaxPartitionSchemeDestinations)
                throw LimitExceeded("partition-scheme destination count");
            budget.AddStructuralRow();
            destinations.Add(new SqlServerPartitionSchemeDestinationMetadata(
                RequiredInt32(reader, 0),
                RequiredInt32(reader, 1),
                RequiredInt32(reader, 2)));
        }
        return destinations.AsReadOnly();
    }

    private static async ValueTask<
        IReadOnlyList<SqlServerPartitionFunctionMetadata>>
        ReadPartitionFunctionsAsync(
            SqlConnection connection,
            ReaderBudget budget,
            SqlServerInspectionLimits limits,
            CancellationToken cancellationToken)
    {
        var functions = new List<SqlServerPartitionFunctionMetadata>();
        await using SqlCommand command = Command(
            connection,
            PartitionFunctionsQuery);
        await using SqlDataReader reader = await command.ExecuteReaderAsync(
                CommandBehavior.SequentialAccess | CommandBehavior.SingleResult,
                cancellationToken)
            .ConfigureAwait(false);
        while (await reader.ReadAsync(cancellationToken).ConfigureAwait(false))
        {
            if (functions.Count == limits.MaxPartitionFunctions)
                throw LimitExceeded("partition-function count");
            budget.AddStructuralRow();
            functions.Add(new SqlServerPartitionFunctionMetadata(
                RequiredInt32(reader, 0),
                RequiredString(reader, 1, budget, isName: true),
                RequiredInt32(reader, 2),
                RequiredBoolean(reader, 3),
                RequiredBoolean(reader, 4)));
        }
        return functions.AsReadOnly();
    }

    private static async ValueTask<
        IReadOnlyList<SqlServerPartitionParameterMetadata>>
        ReadPartitionParametersAsync(
            SqlConnection connection,
            ReaderBudget budget,
            SqlServerInspectionLimits limits,
            CancellationToken cancellationToken)
    {
        var parameters = new List<SqlServerPartitionParameterMetadata>();
        await using SqlCommand command = Command(
            connection,
            PartitionParametersQuery);
        await using SqlDataReader reader = await command.ExecuteReaderAsync(
                CommandBehavior.SequentialAccess | CommandBehavior.SingleResult,
                cancellationToken)
            .ConfigureAwait(false);
        while (await reader.ReadAsync(cancellationToken).ConfigureAwait(false))
        {
            if (parameters.Count == limits.MaxPartitionParameters)
                throw LimitExceeded("partition-parameter count");
            budget.AddStructuralRow();
            parameters.Add(new SqlServerPartitionParameterMetadata(
                RequiredInt32(reader, 0),
                RequiredInt32(reader, 1),
                RequiredString(reader, 2, budget, isName: true),
                RequiredString(reader, 3, budget, isName: true),
                RequiredString(reader, 4, budget, isName: true),
                RequiredInt16(reader, 5),
                RequiredByte(reader, 6),
                RequiredByte(reader, 7),
                OptionalString(reader, 8, budget)));
        }
        return parameters.AsReadOnly();
    }

    private static async ValueTask<
        IReadOnlyList<SqlServerPartitionRangeValueMetadata>>
        ReadPartitionRangeValuesAsync(
            SqlConnection connection,
            ReaderBudget budget,
            SqlServerInspectionLimits limits,
            CancellationToken cancellationToken)
    {
        var values = new List<SqlServerPartitionRangeValueMetadata>();
        await using SqlCommand command = Command(
            connection,
            PartitionRangeValuesQuery);
        await using SqlDataReader reader = await command.ExecuteReaderAsync(
                CommandBehavior.SequentialAccess | CommandBehavior.SingleResult,
                cancellationToken)
            .ConfigureAwait(false);
        while (await reader.ReadAsync(cancellationToken).ConfigureAwait(false))
        {
            if (values.Count == limits.MaxPartitionRangeValues)
                throw LimitExceeded("partition range-value count");
            budget.AddStructuralRow();

            int? valueBytes = OptionalInt32(reader, 9);
            if (valueBytes is < 0)
                throw InvalidProviderMetadata();
            if (valueBytes > limits.MaxPartitionBoundaryBytes)
                throw LimitExceeded("partition boundary byte");

            values.Add(new SqlServerPartitionRangeValueMetadata(
                RequiredInt32(reader, 0),
                RequiredInt32(reader, 1),
                RequiredInt32(reader, 2),
                RequiredBoolean(reader, 3),
                OptionalString(reader, 4, budget),
                OptionalInt32(reader, 5),
                OptionalByte(reader, 6),
                OptionalByte(reader, 7),
                OptionalString(reader, 8, budget),
                valueBytes,
                OptionalString(reader, 10, budget)));
        }
        return values.AsReadOnly();
    }

    private static async ValueTask<
        IReadOnlyList<SqlServerIndexPartitionMetadata>>
        ReadIndexPartitionsAsync(
            SqlConnection connection,
            SqlServerInstanceMetadata instance,
            ReaderBudget budget,
            SqlServerInspectionLimits limits,
            CancellationToken cancellationToken)
    {
        var partitions = new List<SqlServerIndexPartitionMetadata>();
        string commandText = instance.ProductMajorVersion >= 16
            ? IndexPartitionsV16Query
            : IndexPartitionsQuery;
        await using SqlCommand command = Command(connection, commandText);
        await using SqlDataReader reader = await command.ExecuteReaderAsync(
                CommandBehavior.SequentialAccess | CommandBehavior.SingleResult,
                cancellationToken)
            .ConfigureAwait(false);
        while (await reader.ReadAsync(cancellationToken).ConfigureAwait(false))
        {
            if (partitions.Count == limits.MaxIndexPartitions)
                throw LimitExceeded("index-partition count");
            budget.AddStructuralRow();
            partitions.Add(new SqlServerIndexPartitionMetadata(
                RequiredInt32(reader, 0),
                RequiredInt32(reader, 1),
                RequiredInt32(reader, 2),
                RequiredByte(reader, 3),
                RequiredString(reader, 4, budget),
                OptionalBoolean(reader, 5),
                OptionalString(reader, 6, budget),
                OptionalInt32(reader, 7),
                OptionalInt32(reader, 8)));
        }
        return partitions.AsReadOnly();
    }
}
