namespace CSharpDB.Storage.Indexing;

internal static class FullTextIndexNaming
{
    internal const string MetaSuffix = "__meta";
    internal const string TermsSuffix = "__terms";
    internal const string PostingsSuffix = "__postings";
    internal const string DocStatsSuffix = "__docstats";
    internal const string KGramsSuffix = "__kgrams";

    public static string GetMetaIndexName(string indexName) => indexName + MetaSuffix;

    public static string GetTermsIndexName(string indexName) => indexName + TermsSuffix;

    public static string GetPostingsIndexName(string indexName) => indexName + PostingsSuffix;

    public static string GetDocStatsIndexName(string indexName) => indexName + DocStatsSuffix;

    public static string GetKGramsIndexName(string indexName) => indexName + KGramsSuffix;

    public static string[] GetRequiredOwnedIndexNames(string indexName) =>
    [
        GetMetaIndexName(indexName),
        GetTermsIndexName(indexName),
        GetPostingsIndexName(indexName),
        GetDocStatsIndexName(indexName),
    ];
}
