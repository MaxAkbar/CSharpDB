using CSharpDB.Primitives;
using CSharpDB.Storage.Indexing;

namespace CSharpDB.Engine;

internal static class FullTextIndexCatalog
{
    public static IndexSchema CreateLogicalSchema(
        string indexName,
        string tableName,
        IReadOnlyList<string> columns,
        FullTextIndexOptions options)
        => new()
        {
            IndexName = indexName,
            TableName = tableName,
            Columns = columns.ToArray(),
            IsUnique = false,
            Kind = IndexKind.FullText,
            State = IndexState.Ready,
            OptionsJson = FullTextIndexOptionsCodec.Serialize(options),
        };

    public static IndexSchema[] CreateInternalSchemas(IndexSchema logicalIndex)
        => new[]
        {
            CreateInternalSchema(logicalIndex, FullTextIndexNaming.GetMetaIndexName(logicalIndex.IndexName)),
            CreateInternalSchema(logicalIndex, FullTextIndexNaming.GetTermsIndexName(logicalIndex.IndexName)),
            CreateInternalSchema(logicalIndex, FullTextIndexNaming.GetPostingsIndexName(logicalIndex.IndexName)),
            CreateInternalSchema(logicalIndex, FullTextIndexNaming.GetDocStatsIndexName(logicalIndex.IndexName)),
        };

    private static IndexSchema CreateInternalSchema(IndexSchema logicalIndex, string indexName)
        => new()
        {
            IndexName = indexName,
            TableName = logicalIndex.TableName,
            Columns = Array.Empty<string>(),
            IsUnique = false,
            Kind = IndexKind.FullTextInternal,
            State = IndexState.Ready,
            OwnerIndexName = logicalIndex.IndexName,
        };
}
