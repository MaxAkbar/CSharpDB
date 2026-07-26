using System.Data;
using Microsoft.Data.SqlClient;

namespace CSharpDB.Migration.SqlServer;

internal sealed partial class SqlServerCatalogReader
{
    internal const string XmlIndexesQuery =
        """
        SELECT
            xi.object_id,
            xi.index_id,
            xi.using_xml_index_id,
            xi.secondary_type,
            xi.secondary_type_desc,
            xi.xml_index_type,
            xi.xml_index_type_description,
            xi.path_id
        FROM sys.xml_indexes AS xi
        INNER JOIN sys.tables AS t
            ON t.object_id = xi.object_id
        WHERE t.is_ms_shipped = 0
        ORDER BY xi.object_id, xi.index_id;
        """;

    internal const string SelectiveXmlIndexPathsQuery =
        """
        SELECT
            path.object_id,
            path.index_id,
            path.path_id,
            CONVERT(int, DATALENGTH(path.path)),
            path.path,
            path.name,
            path.path_type,
            path.path_type_desc,
            path.xml_component_id,
            path.xquery_type_description,
            path.is_xquery_type_inferred,
            path.xquery_max_length,
            path.is_xquery_max_length_inferred,
            path.is_node,
            path.system_type_id,
            path.user_type_id,
            path.max_length,
            path.precision,
            path.scale,
            path.collation_name,
            path.is_singleton
        FROM sys.selective_xml_index_paths AS path
        INNER JOIN sys.tables AS t
            ON t.object_id = path.object_id
        WHERE t.is_ms_shipped = 0
        ORDER BY path.object_id, path.index_id, path.path_id;
        """;

    internal const string SpatialIndexesQuery =
        """
        SELECT
            si.object_id,
            si.index_id,
            si.spatial_index_type,
            si.spatial_index_type_desc,
            si.tessellation_scheme
        FROM sys.spatial_indexes AS si
        INNER JOIN sys.tables AS t
            ON t.object_id = si.object_id
        WHERE t.is_ms_shipped = 0
        ORDER BY si.object_id, si.index_id;
        """;

    internal const string SpatialIndexTessellationsQuery =
        """
        SELECT
            tessellation.object_id,
            tessellation.index_id,
            tessellation.tessellation_scheme,
            tessellation.bounding_box_xmin,
            tessellation.bounding_box_ymin,
            tessellation.bounding_box_xmax,
            tessellation.bounding_box_ymax,
            tessellation.level_1_grid,
            tessellation.level_1_grid_desc,
            tessellation.level_2_grid,
            tessellation.level_2_grid_desc,
            tessellation.level_3_grid,
            tessellation.level_3_grid_desc,
            tessellation.level_4_grid,
            tessellation.level_4_grid_desc,
            tessellation.cells_per_object
        FROM sys.spatial_index_tessellations AS tessellation
        INNER JOIN sys.tables AS t
            ON t.object_id = tessellation.object_id
        WHERE t.is_ms_shipped = 0
        ORDER BY tessellation.object_id, tessellation.index_id;
        """;

    internal const string HashIndexesQuery =
        """
        SELECT
            hi.object_id,
            hi.index_id,
            hi.bucket_count
        FROM sys.hash_indexes AS hi
        INNER JOIN sys.tables AS t
            ON t.object_id = hi.object_id
        WHERE t.is_ms_shipped = 0
        ORDER BY hi.object_id, hi.index_id;
        """;

    internal const string JsonIndexesV17Query =
        """
        SELECT
            ji.object_id,
            ji.index_id,
            ji.optimize_for_array_search
        FROM sys.json_indexes AS ji
        INNER JOIN sys.tables AS t
            ON t.object_id = ji.object_id
        WHERE t.is_ms_shipped = 0
        ORDER BY ji.object_id, ji.index_id;
        """;

    internal const string JsonIndexPathsV17Query =
        """
        SELECT
            path.object_id,
            path.index_id,
            CONVERT(
                int,
                ROW_NUMBER() OVER (
                    PARTITION BY path.object_id, path.index_id
                    ORDER BY CONVERT(varbinary(8000), path.path))),
            CONVERT(int, DATALENGTH(path.path)),
            path.path
        FROM sys.json_index_paths AS path
        INNER JOIN sys.tables AS t
            ON t.object_id = path.object_id
        WHERE t.is_ms_shipped = 0
        ORDER BY
            path.object_id,
            path.index_id,
            CONVERT(varbinary(8000), path.path);
        """;

    private static async ValueTask<IReadOnlyList<SqlServerXmlIndexMetadata>>
        ReadXmlIndexesAsync(
            CatalogReadContext context,
            ReaderBudget budget,
            SqlServerInspectionLimits limits,
            CancellationToken cancellationToken)
    {
        var indexes = new List<SqlServerXmlIndexMetadata>();
        await using SqlCommand command = Command(context, XmlIndexesQuery);
        await using SqlDataReader reader = await command.ExecuteReaderAsync(
                CommandBehavior.SequentialAccess | CommandBehavior.SingleResult,
                cancellationToken)
            .ConfigureAwait(false);
        while (await reader.ReadAsync(cancellationToken).ConfigureAwait(false))
        {
            if (indexes.Count == limits.MaxXmlIndexes)
                throw LimitExceeded("XML index count");
            budget.AddStructuralRow();
            indexes.Add(new SqlServerXmlIndexMetadata(
                RequiredInt32(reader, 0),
                RequiredInt32(reader, 1),
                OptionalInt32(reader, 2),
                OptionalString(reader, 3, budget),
                OptionalString(reader, 4, budget),
                RequiredByte(reader, 5),
                RequiredString(reader, 6, budget),
                OptionalInt32(reader, 7)));
        }
        return indexes.AsReadOnly();
    }

    private static async ValueTask<
        IReadOnlyList<SqlServerSelectiveXmlIndexPathMetadata>>
        ReadSelectiveXmlIndexPathsAsync(
            CatalogReadContext context,
            ReaderBudget budget,
            SqlServerInspectionLimits limits,
            CancellationToken cancellationToken)
    {
        var paths = new List<SqlServerSelectiveXmlIndexPathMetadata>();
        await using SqlCommand command = Command(
            context,
            SelectiveXmlIndexPathsQuery);
        await using SqlDataReader reader = await command.ExecuteReaderAsync(
                CommandBehavior.SequentialAccess | CommandBehavior.SingleResult,
                cancellationToken)
            .ConfigureAwait(false);
        while (await reader.ReadAsync(cancellationToken).ConfigureAwait(false))
        {
            if (paths.Count == limits.MaxSelectiveXmlIndexPaths)
                throw LimitExceeded("selective XML index-path count");
            budget.AddStructuralRow();

            int objectId = RequiredInt32(reader, 0);
            int indexId = RequiredInt32(reader, 1);
            int pathId = RequiredInt32(reader, 2);
            int pathBytes = RequiredInt32(reader, 3);
            if (pathBytes < 0)
                throw InvalidProviderMetadata();
            if (pathBytes > limits.MaxIndexPathBytes)
                throw LimitExceeded("index-path byte");
            string path = RequiredString(reader, 4, budget);
            string name = RequiredString(reader, 5, budget, isName: true);
            byte pathType = RequiredByte(reader, 6);
            string pathTypeDescription = RequiredString(reader, 7, budget);
            int? xmlComponentId = OptionalInt32(reader, 8);
            string? xQueryTypeDescription = OptionalString(reader, 9, budget);
            bool? isXQueryTypeInferred = OptionalBoolean(reader, 10);
            short? xQueryMaximumLength = OptionalInt16(reader, 11);
            bool? isXQueryMaximumLengthInferred = OptionalBoolean(reader, 12);
            bool? isNode = OptionalBoolean(reader, 13);
            byte? systemTypeId = OptionalByte(reader, 14);
            int? userTypeId = OptionalInt32(reader, 15);
            short? maxLength = OptionalInt16(reader, 16);
            byte? precision = OptionalByte(reader, 17);
            byte? scale = OptionalByte(reader, 18);
            string? collation = OptionalString(reader, 19, budget);
            bool? isSingleton = OptionalBoolean(reader, 20);

            paths.Add(new SqlServerSelectiveXmlIndexPathMetadata(
                objectId,
                indexId,
                pathId,
                pathBytes,
                path,
                name,
                pathType,
                pathTypeDescription,
                xmlComponentId,
                xQueryTypeDescription,
                isXQueryTypeInferred,
                xQueryMaximumLength,
                isXQueryMaximumLengthInferred,
                isNode,
                systemTypeId,
                userTypeId,
                maxLength,
                precision,
                scale,
                collation,
                isSingleton));
        }
        return paths.AsReadOnly();
    }

    private static async ValueTask<IReadOnlyList<SqlServerSpatialIndexMetadata>>
        ReadSpatialIndexesAsync(
            CatalogReadContext context,
            ReaderBudget budget,
            SqlServerInspectionLimits limits,
            CancellationToken cancellationToken)
    {
        var indexes = new List<SqlServerSpatialIndexMetadata>();
        await using SqlCommand command = Command(context, SpatialIndexesQuery);
        await using SqlDataReader reader = await command.ExecuteReaderAsync(
                CommandBehavior.SequentialAccess | CommandBehavior.SingleResult,
                cancellationToken)
            .ConfigureAwait(false);
        while (await reader.ReadAsync(cancellationToken).ConfigureAwait(false))
        {
            if (indexes.Count == limits.MaxSpatialIndexes)
                throw LimitExceeded("spatial index count");
            budget.AddStructuralRow();
            indexes.Add(new SqlServerSpatialIndexMetadata(
                RequiredInt32(reader, 0),
                RequiredInt32(reader, 1),
                RequiredByte(reader, 2),
                RequiredString(reader, 3, budget),
                RequiredString(reader, 4, budget)));
        }
        return indexes.AsReadOnly();
    }

    private static async ValueTask<
        IReadOnlyList<SqlServerSpatialIndexTessellationMetadata>>
        ReadSpatialIndexTessellationsAsync(
            CatalogReadContext context,
            ReaderBudget budget,
            SqlServerInspectionLimits limits,
            CancellationToken cancellationToken)
    {
        var tessellations =
            new List<SqlServerSpatialIndexTessellationMetadata>();
        await using SqlCommand command = Command(
            context,
            SpatialIndexTessellationsQuery);
        await using SqlDataReader reader = await command.ExecuteReaderAsync(
                CommandBehavior.SequentialAccess | CommandBehavior.SingleResult,
                cancellationToken)
            .ConfigureAwait(false);
        while (await reader.ReadAsync(cancellationToken).ConfigureAwait(false))
        {
            if (tessellations.Count == limits.MaxSpatialIndexTessellations)
                throw LimitExceeded("spatial index-tessellation count");
            budget.AddStructuralRow();
            tessellations.Add(new SqlServerSpatialIndexTessellationMetadata(
                RequiredInt32(reader, 0),
                RequiredInt32(reader, 1),
                RequiredString(reader, 2, budget),
                OptionalDouble(reader, 3),
                OptionalDouble(reader, 4),
                OptionalDouble(reader, 5),
                OptionalDouble(reader, 6),
                OptionalInt16(reader, 7),
                OptionalString(reader, 8, budget),
                OptionalInt16(reader, 9),
                OptionalString(reader, 10, budget),
                OptionalInt16(reader, 11),
                OptionalString(reader, 12, budget),
                OptionalInt16(reader, 13),
                OptionalString(reader, 14, budget),
                OptionalInt32(reader, 15)));
        }
        return tessellations.AsReadOnly();
    }

    private static async ValueTask<IReadOnlyList<SqlServerHashIndexMetadata>>
        ReadHashIndexesAsync(
            CatalogReadContext context,
            ReaderBudget budget,
            SqlServerInspectionLimits limits,
            CancellationToken cancellationToken)
    {
        var indexes = new List<SqlServerHashIndexMetadata>();
        await using SqlCommand command = Command(context, HashIndexesQuery);
        await using SqlDataReader reader = await command.ExecuteReaderAsync(
                CommandBehavior.SequentialAccess | CommandBehavior.SingleResult,
                cancellationToken)
            .ConfigureAwait(false);
        while (await reader.ReadAsync(cancellationToken).ConfigureAwait(false))
        {
            if (indexes.Count == limits.MaxHashIndexes)
                throw LimitExceeded("hash index count");
            budget.AddStructuralRow();
            indexes.Add(new SqlServerHashIndexMetadata(
                RequiredInt32(reader, 0),
                RequiredInt32(reader, 1),
                RequiredInt32(reader, 2)));
        }
        return indexes.AsReadOnly();
    }

    private static async ValueTask<IReadOnlyList<SqlServerJsonIndexMetadata>>
        ReadJsonIndexesAsync(
            CatalogReadContext context,
            SqlServerInstanceMetadata instance,
            ReaderBudget budget,
            SqlServerInspectionLimits limits,
            CancellationToken cancellationToken)
    {
        if (instance.ProductMajorVersion < 17)
            return Array.Empty<SqlServerJsonIndexMetadata>();

        var indexes = new List<SqlServerJsonIndexMetadata>();
        await using SqlCommand command = Command(context, JsonIndexesV17Query);
        await using SqlDataReader reader = await command.ExecuteReaderAsync(
                CommandBehavior.SequentialAccess | CommandBehavior.SingleResult,
                cancellationToken)
            .ConfigureAwait(false);
        while (await reader.ReadAsync(cancellationToken).ConfigureAwait(false))
        {
            if (indexes.Count == limits.MaxJsonIndexes)
                throw LimitExceeded("JSON index count");
            budget.AddStructuralRow();
            indexes.Add(new SqlServerJsonIndexMetadata(
                RequiredInt32(reader, 0),
                RequiredInt32(reader, 1),
                RequiredBoolean(reader, 2)));
        }
        return indexes.AsReadOnly();
    }

    private static async ValueTask<IReadOnlyList<SqlServerJsonIndexPathMetadata>>
        ReadJsonIndexPathsAsync(
            CatalogReadContext context,
            SqlServerInstanceMetadata instance,
            ReaderBudget budget,
            SqlServerInspectionLimits limits,
            CancellationToken cancellationToken)
    {
        if (instance.ProductMajorVersion < 17)
            return Array.Empty<SqlServerJsonIndexPathMetadata>();

        var paths = new List<SqlServerJsonIndexPathMetadata>();
        await using SqlCommand command = Command(
            context,
            JsonIndexPathsV17Query);
        await using SqlDataReader reader = await command.ExecuteReaderAsync(
                CommandBehavior.SequentialAccess | CommandBehavior.SingleResult,
                cancellationToken)
            .ConfigureAwait(false);
        while (await reader.ReadAsync(cancellationToken).ConfigureAwait(false))
        {
            if (paths.Count == limits.MaxJsonIndexPaths)
                throw LimitExceeded("JSON index-path count");
            budget.AddStructuralRow();

            int objectId = RequiredInt32(reader, 0);
            int indexId = RequiredInt32(reader, 1);
            int pathOrdinal = RequiredInt32(reader, 2);
            int pathBytes = RequiredInt32(reader, 3);
            if (pathBytes < 0)
                throw InvalidProviderMetadata();
            if (pathBytes > limits.MaxIndexPathBytes)
                throw LimitExceeded("index-path byte");
            string path = RequiredString(reader, 4, budget);

            paths.Add(new SqlServerJsonIndexPathMetadata(
                objectId,
                indexId,
                pathOrdinal,
                pathBytes,
                path));
        }
        return paths.AsReadOnly();
    }
}
