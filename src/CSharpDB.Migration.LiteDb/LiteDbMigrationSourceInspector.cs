using System.Collections.ObjectModel;
using System.Diagnostics;
using System.Globalization;
using System.Reflection;
using System.Security.Cryptography;
using System.Text;
using System.Text.RegularExpressions;
using CSharpDB.Migration;
using LiteDB;

namespace CSharpDB.Migration.LiteDb;

/// <summary>
/// Deterministically inventories a LiteDB 5 database using only untyped
/// <see cref="BsonDocument"/> collections opened in explicit read-only,
/// non-upgrading mode.
/// </summary>
public sealed partial class LiteDbMigrationSourceInspector : IMigrationSourceInspector
{
    public const string CatalogContract = "csharpdb-litedb-catalog/v1";

    private const string MainNamespaceId = "litedb:namespace:main";

    private static readonly UTF8Encoding StrictUtf8 =
        new(encoderShouldEmitUTF8Identifier: false, throwOnInvalidBytes: true);

    private readonly string sourcePath;
    private readonly string? password;
    private readonly LiteDbInspectionLimits limits;

    public LiteDbMigrationSourceInspector(string sourcePath, string? password = null)
        : this(sourcePath, password, LiteDbInspectionLimits.Default)
    {
    }

    internal LiteDbMigrationSourceInspector(
        string sourcePath,
        string? password,
        LiteDbInspectionLimits limits)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(sourcePath);
        ArgumentNullException.ThrowIfNull(limits);
        limits.Validate();

        this.sourcePath = Path.GetFullPath(sourcePath);
        this.password = password;
        this.limits = limits;
    }

    public MigrationSourceKind SourceKind => MigrationSourceKind.LiteDb;

    public async ValueTask<MigrationCatalog> InspectAsync(
        MigrationInspectionRequest request,
        CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(request);
        cancellationToken.ThrowIfCancellationRequested();
        if (!string.Equals(
                request.TargetCSharpDbVersion,
                CSharpDbCapabilityCatalogLoader.CurrentTargetVersion,
                StringComparison.Ordinal))
        {
            throw new NotSupportedException(
                $"The LiteDB adapter is qualified for CSharpDB {CSharpDbCapabilityCatalogLoader.CurrentTargetVersion}.");
        }
        if (request.ProfileSampleSize <= 0)
        {
            throw new ArgumentOutOfRangeException(
                nameof(request),
                "Profile sample size must be positive.");
        }
        if (!File.Exists(sourcePath))
        {
            throw new FileNotFoundException(
                "The LiteDB source file does not exist.",
                sourcePath);
        }

        try
        {
            FileIdentity before = await ReadFileIdentityAsync(
                    sourcePath,
                    cancellationToken)
                .ConfigureAwait(false);
            MigrationCatalog catalog;
            using (var database = OpenReadOnlyDatabase())
            {
                catalog = BuildCatalog(
                    database,
                    before,
                    request,
                    limits,
                    cancellationToken);
            }

            FileIdentity after = await ReadFileIdentityAsync(
                    sourcePath,
                    cancellationToken)
                .ConfigureAwait(false);
            if (before != after)
            {
                throw new LiteDbMigrationException(
                    "The LiteDB source changed while it was being inspected.");
            }

            return catalog;
        }
        catch (LiteDbMigrationException)
        {
            throw;
        }
        catch (OperationCanceledException)
        {
            throw;
        }
        catch (Exception exception) when (
            exception is LiteException or IOException or
            UnauthorizedAccessException or InvalidOperationException or
            EncoderFallbackException)
        {
            throw new LiteDbMigrationException(
                "The LiteDB source could not be inspected safely in read-only mode.",
                exception);
        }
    }

    private LiteDatabase OpenReadOnlyDatabase()
    {
        var connection = new ConnectionString
        {
            Filename = sourcePath,
            Password = password,
            Connection = ConnectionType.Direct,
            ReadOnly = true,
            Upgrade = false,
        };
        return new LiteDatabase(connection, new BsonMapper());
    }

    private static MigrationCatalog BuildCatalog(
        LiteDatabase database,
        FileIdentity file,
        MigrationInspectionRequest request,
        LiteDbInspectionLimits limits,
        CancellationToken cancellationToken)
    {
        var metadataBudget = new MetadataBudget(limits);
        var collectionNameBuffer = new List<string>();
        foreach (string name in database.GetCollectionNames())
        {
            if (collectionNameBuffer.Count >= limits.MaxCollections)
                throw LimitExceeded("collection count");
            metadataBudget.AddString(name, "collection name");
            collectionNameBuffer.Add(name);
        }
        string[] collectionNames = collectionNameBuffer
            .Order(StringComparer.Ordinal)
            .ToArray();

        IReadOnlyList<LiteDbIndexMetadata> indexes = ReadIndexes(
            database,
            metadataBudget,
            limits,
            cancellationToken);
        var documentCounts = new Dictionary<string, long>(StringComparer.Ordinal);
        long totalDocuments = 0;
        foreach (string collectionName in collectionNames)
        {
            cancellationToken.ThrowIfCancellationRequested();
            long remaining = limits.MaxDocuments - totalDocuments;
            long documentCount = CountDocumentsBounded(
                database.GetCollection(collectionName, BsonAutoId.ObjectId),
                remaining,
                cancellationToken);
            totalDocuments += documentCount;
            documentCounts.Add(collectionName, documentCount);
        }
        var objects = new List<MigrationCatalogObject>(
            checked(1 + collectionNames.Length * 3 + indexes.Count));
        var diagnostics = new List<MigrationDiagnostic>(indexes.Count * 2);
        var namespaceFacets = new List<MigrationCatalogFacet>
        {
            Facet("isDefault", "true"),
            Facet("liteDbCatalogContract", CatalogContract),
            Facet("liteDbCollectionCount", Invariant(collectionNames.Length)),
            Facet("liteDbCollectionNameComparison", "ordinal-ignore-case"),
            Facet("liteDbDocumentCount", Invariant(totalDocuments)),
            Facet("liteDbIndexCount", Invariant(indexes.Count)),
            Facet("liteDbProfileIncluded", Boolean(request.IncludeProfile)),
            Facet(
                "liteDbProviderVersion",
                ProviderVersion()),
            Facet("liteDbUserVersion", Invariant(database.UserVersion)),
            Facet("liteDbUtcDate", Boolean(database.UtcDate)),
            Facet("liteDbCollationLcid", Invariant(database.Collation.LCID)),
            Facet(
                "liteDbCollationSortOptions",
                ((int)database.Collation.SortOptions).ToString(
                    CultureInfo.InvariantCulture)),
        };
        objects.Add(new MigrationCatalogObject
        {
            ObjectId = MainNamespaceId,
            Kind = MigrationObjectKind.Namespace,
            SourceName = "main",
            Facets = ReadOnly(namespaceFacets),
        });
        foreach (IGrouping<string, string> collision in collectionNames
                     .GroupBy(static name => name, StringComparer.OrdinalIgnoreCase)
                     .Where(static group => group.Count() > 1))
        {
            string[] names = collision.Order(StringComparer.Ordinal).ToArray();
            diagnostics.Add(
                Diagnostic(
                    MainNamespaceId,
                    "MIG-LITEDB-COLLECTION-NAME-COLLISION-001",
                    MigrationDiagnosticSeverity.Error,
                    MigrationCompatibilityStatus.Unsupported,
                    "LiteDB collection names collide under target case-insensitive naming.",
                    $"The source contains {names.Length.ToString(CultureInfo.InvariantCulture)} ordinally distinct collection names in one case-insensitive name group.",
                    canOverride: false,
                    occurrenceKey: string.Join("\0", names)));
        }

        var collectionIds = new Dictionary<string, string>(StringComparer.Ordinal);
        foreach (string collectionName in collectionNames)
        {
            cancellationToken.ThrowIfCancellationRequested();
            string collectionId = ObjectId("collection", collectionName);
            string keyColumnId = ObjectId("key", collectionName);
            string documentColumnId = ObjectId("document", collectionName);
            collectionIds.Add(collectionName, collectionId);

            ILiteCollection<BsonDocument> collection =
                database.GetCollection(collectionName, BsonAutoId.ObjectId);
            long documentCount = documentCounts[collectionName];

            var collectionFacets = new List<MigrationCatalogFacet>(
                MigrationLiteDbDocumentCollectionContract
                    .RequiredCollectionFacets.Count + 12);
            collectionFacets.AddRange(
                MigrationLiteDbDocumentCollectionContract.RequiredCollectionFacets);
            collectionFacets.Add(Facet("liteDbDocumentCount", Invariant(documentCount)));
            collectionFacets.Add(
                Facet(
                    "liteDbProfileCoverage",
                    request.IncludeProfile ? "full" : "none"));
            collectionFacets.Add(
                Facet(
                    "liteDbProfileValuesExamined",
                    request.IncludeProfile ? Invariant(documentCount) : "0"));

            if (request.IncludeProfile)
            {
                CollectionProfile profile = ProfileCollection(
                    collection,
                    documentCount,
                    metadataBudget,
                    limits,
                    cancellationToken);
                collectionFacets.Add(
                    Facet("liteDbProfileFieldPathCount", Invariant(profile.Fields.Count)));
                collectionFacets.Add(
                    Facet("liteDbIdTypeCounts", TypeCounts(profile.IdTypeCounts)));
                int fieldOrdinal = 0;
                foreach (FieldProfile field in profile.Fields.Values)
                {
                    collectionFacets.Add(
                        Facet(
                            $"liteDbProfileField{fieldOrdinal:D6}",
                            FieldFacet(field)));
                    fieldOrdinal++;
                }
            }

            foreach (MigrationCatalogFacet facet in collectionFacets)
                metadataBudget.AddFacet(facet);

            objects.Add(new MigrationCatalogObject
            {
                ObjectId = collectionId,
                Kind = MigrationObjectKind.Collection,
                ParentObjectId = MainNamespaceId,
                SourceNamespace = "main",
                SourceName = collectionName,
                Facets = ReadOnly(collectionFacets),
            });
            objects.Add(new MigrationCatalogObject
            {
                ObjectId = keyColumnId,
                Kind = MigrationObjectKind.Column,
                ParentObjectId = collectionId,
                SourceNamespace = "main",
                SourceName =
                    MigrationLiteDbDocumentCollectionContract.KeyColumnName,
                NativeType =
                    MigrationLiteDbDocumentCollectionContract.KeyNativeType,
                Facets =
                    MigrationLiteDbDocumentCollectionContract.CreateKeyFacets(),
            });
            objects.Add(new MigrationCatalogObject
            {
                ObjectId = documentColumnId,
                Kind = MigrationObjectKind.Column,
                ParentObjectId = collectionId,
                SourceNamespace = "main",
                SourceName =
                    MigrationLiteDbDocumentCollectionContract.DocumentColumnName,
                NativeType =
                    MigrationLiteDbDocumentCollectionContract.DocumentNativeType,
                Facets =
                    MigrationLiteDbDocumentCollectionContract.CreateDocumentFacets(),
            });
        }

        AddIndexes(
            indexes,
            database,
            collectionIds,
            objects,
            diagnostics,
            metadataBudget);

        var catalog = new MigrationCatalog
        {
            TargetCSharpDbVersion = request.TargetCSharpDbVersion,
            Source = new MigrationSourceIdentity
            {
                Kind = MigrationSourceKind.LiteDb,
                Identity = "litedb-file:" + HashText(
                    NormalizePathIdentity(file.Path)),
                Fingerprint = "sha256:" + file.Sha256,
                ProviderVersion = ProviderVersion(),
                SourceVersion = "5",
                Consistency = new MigrationConsistencyStrategy
                {
                    Kind = MigrationConsistencyKind.BestEffort,
                    Description =
                        "Direct LiteDB file inspection with ReadOnly=true and Upgrade=false; unchanged content is verified before and after inspection.",
                },
            },
            Objects = objects
                .OrderBy(static item => item.ObjectId, StringComparer.Ordinal)
                .ToArray(),
            Diagnostics = diagnostics
                .OrderBy(static item => item.DiagnosticId, StringComparer.Ordinal)
                .ToArray(),
        };
        MigrationContractValidator.ValidateCatalog(catalog);
        return catalog;
    }

    private static long CountDocumentsBounded(
        ILiteCollection<BsonDocument> collection,
        long remaining,
        CancellationToken cancellationToken)
    {
        long count = 0;
        foreach (BsonDocument _ in collection.FindAll())
        {
            cancellationToken.ThrowIfCancellationRequested();
            if (count == remaining)
                throw LimitExceeded("inspection-wide document count");
            count++;
        }
        return count;
    }

    private static CollectionProfile ProfileCollection(
        ILiteCollection<BsonDocument> collection,
        long expectedCount,
        MetadataBudget metadataBudget,
        LiteDbInspectionLimits limits,
        CancellationToken cancellationToken)
    {
        var fields = new SortedDictionary<string, MutableFieldProfile>(
            StringComparer.Ordinal);
        var idTypes = new SortedDictionary<string, long>(StringComparer.Ordinal);
        long documents = 0;

        foreach (BsonDocument document in collection.FindAll())
        {
            cancellationToken.ThrowIfCancellationRequested();
            documents++;
            if (documents > limits.MaxDocuments)
                throw LimitExceeded("document count");
            if (!document.TryGetValue("_id", out BsonValue? id) ||
                id is null ||
                id.IsNull)
            {
                throw new LiteDbMigrationException(
                    "A LiteDB document does not have a non-null _id.");
            }

            _ = LiteDbCanonicalBsonCodec.EncodeTypedKey(id, limits);
            _ = LiteDbCanonicalBsonCodec.EncodeDocument(document, limits);
            Increment(idTypes, LiteDbCanonicalBsonCodec.GetTypeLabel(id));

            var presentPaths = new HashSet<string>(StringComparer.Ordinal);
            VisitDocument(
                document,
                path: string.Empty,
                depth: 0,
                fields,
                presentPaths,
                metadataBudget,
                limits);
            foreach (string path in presentPaths)
                fields[path].DocumentsPresent++;
        }

        if (documents != expectedCount)
        {
            throw new LiteDbMigrationException(
                "The LiteDB collection changed while it was being profiled.");
        }

        var immutable = new SortedDictionary<string, FieldProfile>(
            StringComparer.Ordinal);
        foreach ((string path, MutableFieldProfile profile) in fields)
        {
            immutable.Add(
                path,
                new FieldProfile(
                    path,
                    profile.DocumentsPresent,
                    new ReadOnlyDictionary<string, long>(profile.TypeCounts)));
        }
        return new CollectionProfile(
            new ReadOnlyDictionary<string, FieldProfile>(immutable),
            new ReadOnlyDictionary<string, long>(idTypes));
    }

    private static void VisitDocument(
        BsonDocument document,
        string path,
        int depth,
        SortedDictionary<string, MutableFieldProfile> fields,
        HashSet<string> presentPaths,
        MetadataBudget metadataBudget,
        LiteDbInspectionLimits limits)
    {
        if (depth > limits.MaxDepth)
            throw LimitExceeded("profile nesting depth");

        foreach (KeyValuePair<string, BsonValue> field in document.OrderBy(
                     static item => item.Key,
                     StringComparer.Ordinal))
        {
            string nextPath = AppendPropertyPath(path, field.Key, limits);
            RecordField(
                nextPath,
                field.Value,
                fields,
                presentPaths,
                metadataBudget,
                limits);
            VisitChildren(
                field.Value,
                nextPath,
                depth + 1,
                fields,
                presentPaths,
                metadataBudget,
                limits);
        }
    }

    private static void VisitChildren(
        BsonValue value,
        string path,
        int depth,
        SortedDictionary<string, MutableFieldProfile> fields,
        HashSet<string> presentPaths,
        MetadataBudget metadataBudget,
        LiteDbInspectionLimits limits)
    {
        if (depth > limits.MaxDepth)
            throw LimitExceeded("profile nesting depth");

        if (value.IsDocument)
        {
            VisitDocument(
                value.AsDocument,
                path,
                depth,
                fields,
                presentPaths,
                metadataBudget,
                limits);
            return;
        }
        if (!value.IsArray)
            return;

        string elementPath = AppendArrayPath(path, limits);
        foreach (BsonValue element in value.AsArray)
        {
            RecordField(
                elementPath,
                element,
                fields,
                presentPaths,
                metadataBudget,
                limits);
            VisitChildren(
                element,
                elementPath,
                depth + 1,
                fields,
                presentPaths,
                metadataBudget,
                limits);
        }
    }

    private static void RecordField(
        string path,
        BsonValue value,
        SortedDictionary<string, MutableFieldProfile> fields,
        HashSet<string> presentPaths,
        MetadataBudget metadataBudget,
        LiteDbInspectionLimits limits)
    {
        if (!fields.TryGetValue(path, out MutableFieldProfile? profile))
        {
            if (fields.Count >= limits.MaxFieldPaths)
                throw LimitExceeded("profile field-path count");
            profile = new MutableFieldProfile();
            fields.Add(path, profile);
            metadataBudget.AddString(path, "profile path");
        }
        Increment(profile.TypeCounts, LiteDbCanonicalBsonCodec.GetTypeLabel(value));
        presentPaths.Add(path);
    }

    private static IReadOnlyList<LiteDbIndexMetadata> ReadIndexes(
        LiteDatabase database,
        MetadataBudget metadataBudget,
        LiteDbInspectionLimits limits,
        CancellationToken cancellationToken)
    {
        ILiteCollection<BsonDocument> systemIndexes =
            database.GetCollection("$indexes", BsonAutoId.Int32);
        var result = new List<LiteDbIndexMetadata>();
        int sourceOrdinal = 0;
        foreach (BsonDocument document in systemIndexes.FindAll())
        {
            cancellationToken.ThrowIfCancellationRequested();
            if (result.Count >= limits.MaxIndexes)
                throw LimitExceeded("index count");

            string? collection = OptionalString(document, "collection");
            string? name = OptionalString(document, "name");
            string? expression =
                OptionalString(document, "expression") ??
                OptionalString(document, "expr");
            bool? unique = OptionalBoolean(document, "unique");
            if (collection is not null)
                metadataBudget.AddString(collection, "index collection name");
            if (name is not null)
                metadataBudget.AddString(name, "index name");
            if (expression is not null)
                metadataBudget.AddString(expression, "index expression");

            result.Add(
                new LiteDbIndexMetadata(
                    collection,
                    name,
                    expression,
                    unique,
                    sourceOrdinal));
            sourceOrdinal++;
        }

        return result
            .OrderBy(static item => item.Collection, StringComparer.Ordinal)
            .ThenBy(static item => item.Name, StringComparer.Ordinal)
            .ThenBy(static item => item.Expression, StringComparer.Ordinal)
            .ThenBy(static item => item.Unique)
            .ThenBy(static item => item.SourceOrdinal)
            .ToArray();
    }

    private static void AddIndexes(
        IReadOnlyList<LiteDbIndexMetadata> indexes,
        LiteDatabase database,
        IReadOnlyDictionary<string, string> collectionIds,
        List<MigrationCatalogObject> objects,
        List<MigrationDiagnostic> diagnostics,
        MetadataBudget metadataBudget)
    {
        int duplicateOrdinal = 0;
        LiteDbIndexMetadata? previous = null;
        foreach (LiteDbIndexMetadata index in indexes)
        {
            if (previous is null ||
                !string.Equals(previous.Collection, index.Collection, StringComparison.Ordinal) ||
                !string.Equals(previous.Name, index.Name, StringComparison.Ordinal) ||
                !string.Equals(previous.Expression, index.Expression, StringComparison.Ordinal) ||
                previous.Unique != index.Unique)
            {
                duplicateOrdinal = 0;
            }
            else
            {
                duplicateOrdinal++;
            }
            previous = index;

            string? collectionId = null;
            bool knownCollection = index.Collection is not null &&
                collectionIds.TryGetValue(index.Collection, out collectionId);
            string parentId = knownCollection ? collectionId! : MainNamespaceId;
            string sourceName = string.IsNullOrWhiteSpace(index.Name)
                ? $"untranslated-{index.SourceOrdinal:D8}"
                : index.Name;
            bool idIndex = knownCollection &&
                string.Equals(index.Name, "_id", StringComparison.Ordinal) &&
                string.Equals(index.Expression, "$._id", StringComparison.Ordinal) &&
                index.Unique == true;
            bool simple = index.Expression is not null &&
                SimplePathExpression().IsMatch(index.Expression);
            string indexId = ObjectId(
                "index",
                string.Join(
                    "\0",
                    index.Collection,
                    index.Name,
                    index.Expression,
                    index.Unique?.ToString(CultureInfo.InvariantCulture),
                    duplicateOrdinal.ToString(CultureInfo.InvariantCulture)));

            var facets = new List<MigrationCatalogFacet>
            {
                Facet("liteDbIndexInventoryContract", "litedb-system-indexes/v1"),
                Facet("liteDbIndexUnique", index.Unique is null ? "unknown" : Boolean(index.Unique.Value)),
                Facet("unique", index.Unique is null ? "unknown" : Boolean(index.Unique.Value)),
                Facet(
                    "liteDbIndexExpressionDigest",
                    index.Expression is null ? "unavailable" : HashText(index.Expression)),
                Facet(
                    "liteDbIndexExpression",
                    index.Expression ?? "unavailable"),
                Facet(
                    "liteDbIndexShape",
                    idIndex ? "id" :
                    index.Expression is null ? "untranslated" :
                    simple ? "simple-path" : "expression"),
                Facet("kind", "standard"),
            };
            foreach (MigrationCatalogFacet facet in facets)
                metadataBudget.AddFacet(facet);

            var indexObject = new MigrationCatalogObject
            {
                ObjectId = indexId,
                Kind = MigrationObjectKind.Index,
                ParentObjectId = parentId,
                SourceNamespace = "main",
                SourceName = sourceName,
                Facets = ReadOnly(facets),
            };
            objects.Add(indexObject);

            if (idIndex)
            {
                diagnostics.Add(
                    Diagnostic(
                        indexId,
                        "MIG-LITEDB-INDEX-ID-001",
                        MigrationDiagnosticSeverity.Information,
                        MigrationCompatibilityStatus.CompatibleWithRewrite,
                        "The LiteDB _id index is subsumed by the typed collection key.",
                        "The built-in _id index is retained in inventory without a structural target-index dependency so it is not emitted as a redundant secondary index.",
                        canOverride: false,
                        remediation: null));
                continue;
            }

            if (!knownCollection || index.Expression is null || index.Unique is null)
            {
                diagnostics.Add(
                    Diagnostic(
                        indexId,
                        "MIG-LITEDB-INDEX-UNTRANSLATED-001",
                        MigrationDiagnosticSeverity.Error,
                        MigrationCompatibilityStatus.Unknown,
                        "The LiteDB index metadata cannot be translated.",
                        "The $indexes row is retained in inventory, but its collection, expression, or uniqueness metadata is incomplete.",
                        canOverride: false));
            }
            else if (!simple)
            {
                diagnostics.Add(
                    Diagnostic(
                        indexId,
                        "MIG-LITEDB-INDEX-EXPRESSION-001",
                        MigrationDiagnosticSeverity.Error,
                        MigrationCompatibilityStatus.Unsupported,
                        "The LiteDB expression index is not translated automatically.",
                        "The tagged-document projection cannot prove equivalent target semantics for this BSON expression.",
                        canOverride: false));
            }
            else if (index.Unique == true)
            {
                diagnostics.Add(
                    Diagnostic(
                        indexId,
                        "MIG-LITEDB-INDEX-UNIQUE-001",
                        MigrationDiagnosticSeverity.Error,
                        MigrationCompatibilityStatus.Unsupported,
                        "The LiteDB unique document-path index requires manual migration.",
                        "Uniqueness over a BSON path cannot be represented as an index over the tagged document bridge column.",
                        canOverride: false));
            }
            else
            {
                diagnostics.Add(
                    Diagnostic(
                        indexId,
                        "MIG-LITEDB-INDEX-SIMPLE-001",
                        MigrationDiagnosticSeverity.Error,
                        MigrationCompatibilityStatus.Unsupported,
                        "The LiteDB document-path index requires manual migration.",
                        "A simple BSON path is inventoried, but the tagged document bridge does not expose it as a target column.",
                        canOverride: false));
            }

            if (knownCollection && index.Expression is not null)
            {
                diagnostics.Add(
                    Diagnostic(
                        indexId,
                        "MIG-LITEDB-INDEX-COLLATION-001",
                        MigrationDiagnosticSeverity.Error,
                        MigrationCompatibilityStatus.Unsupported,
                        "LiteDB index collation semantics are not translated automatically.",
                        $"The source uses LiteDB collation LCID {database.Collation.LCID.ToString(CultureInfo.InvariantCulture)} with sort options {((int)database.Collation.SortOptions).ToString(CultureInfo.InvariantCulture)}.",
                        canOverride: false));
            }
        }
    }

    private static string AppendPropertyPath(
        string path,
        string propertyName,
        LiteDbInspectionLimits limits)
    {
        byte[] bytes;
        try
        {
            bytes = StrictUtf8.GetBytes(propertyName);
        }
        catch (EncoderFallbackException exception)
        {
            throw new LiteDbMigrationException(
                "A LiteDB document property name is not valid Unicode.",
                exception);
        }
        if (bytes.Length > limits.MaxPropertyNameBytes)
            throw LimitExceeded("profile property-name bytes");
        string result = string.Concat(path, "/p:", Base64UrlEncode(bytes));
        if (StrictUtf8.GetByteCount(result) > limits.MaxPathBytes)
            throw LimitExceeded("profile path bytes");
        return result;
    }

    private static string AppendArrayPath(
        string path,
        LiteDbInspectionLimits limits)
    {
        string result = path + "/a";
        if (StrictUtf8.GetByteCount(result) > limits.MaxPathBytes)
            throw LimitExceeded("profile path bytes");
        return result;
    }

    private static string FieldFacet(FieldProfile field) =>
        string.Concat(
            "path=",
            field.Path,
            ";present=",
            Invariant(field.DocumentsPresent),
            ";types=",
            TypeCounts(field.TypeCounts));

    private static string TypeCounts(IReadOnlyDictionary<string, long> counts) =>
        string.Join(
            ",",
            counts.Select(pair =>
                string.Concat(pair.Key, ":", Invariant(pair.Value))));

    private static void Increment(
        IDictionary<string, long> counts,
        string key)
    {
        counts.TryGetValue(key, out long value);
        counts[key] = checked(value + 1);
    }

    private static string? OptionalString(BsonDocument document, string name) =>
        document.TryGetValue(name, out BsonValue? value) &&
        value is not null &&
        value.IsString
            ? value.AsString
            : null;

    private static bool? OptionalBoolean(BsonDocument document, string name) =>
        document.TryGetValue(name, out BsonValue? value) &&
        value is not null &&
        value.IsBoolean
            ? value.AsBoolean
            : null;

    private static MigrationDiagnostic Diagnostic(
        string objectId,
        string ruleId,
        MigrationDiagnosticSeverity severity,
        MigrationCompatibilityStatus status,
        string summary,
        string explanation,
        bool canOverride,
        string? occurrenceKey = null,
        string? remediation =
            "Recreate the index after migration using reviewed CSharpDB semantics.") =>
        new()
        {
            DiagnosticId = string.Concat(
                "litedb:diag:",
                ruleId.ToLowerInvariant(),
                ":",
                HashText(string.Concat(objectId, "\0", occurrenceKey))),
            RuleId = ruleId,
            Severity = severity,
            Status = status,
            Evidence = MigrationEvidenceLevel.Parsed,
            Summary = summary,
            Explanation = explanation,
            ObjectId = objectId,
            Remediation = status == MigrationCompatibilityStatus.Compatible
                ? null
                : remediation,
            CanOverride = canOverride,
        };

    private static MigrationCatalogFacet Facet(string name, string? value) =>
        new()
        {
            Name = name,
            Value = value,
        };

    private static ReadOnlyCollection<MigrationCatalogFacet> ReadOnly(
        List<MigrationCatalogFacet> facets) =>
        Array.AsReadOnly(facets.ToArray());

    private static string ObjectId(string kind, string sourceName) =>
        string.Concat("litedb:", kind, ":", HashText(sourceName));

    private static string HashText(string value) =>
        Convert.ToHexString(
                SHA256.HashData(StrictUtf8.GetBytes(value)))
            .ToLowerInvariant();

    private static string Base64UrlEncode(ReadOnlySpan<byte> bytes) =>
        Convert.ToBase64String(bytes)
            .TrimEnd('=')
            .Replace('+', '-')
            .Replace('/', '_');

    private static string ProviderVersion()
    {
        Assembly assembly = typeof(LiteDatabase).Assembly;
        return assembly.GetCustomAttribute<AssemblyInformationalVersionAttribute>()
                   ?.InformationalVersion ??
            FileVersionInfo.GetVersionInfo(assembly.Location).FileVersion ??
            assembly.GetName().Version?.ToString() ??
            "5.0.21";
    }

    private static string NormalizePathIdentity(string path)
    {
        string fullPath = Path.GetFullPath(path);
        return OperatingSystem.IsWindows()
            ? fullPath.ToUpperInvariant()
            : fullPath;
    }

    private static async ValueTask<FileIdentity> ReadFileIdentityAsync(
        string path,
        CancellationToken cancellationToken)
    {
        var info = new FileInfo(path);
        await using var stream = new FileStream(
            path,
            FileMode.Open,
            FileAccess.Read,
            FileShare.Read,
            bufferSize: 128 * 1024,
            FileOptions.Asynchronous | FileOptions.SequentialScan);
        byte[] digest = await SHA256.HashDataAsync(stream, cancellationToken)
            .ConfigureAwait(false);
        info.Refresh();
        return new FileIdentity(
            path,
            info.Length,
            info.LastWriteTimeUtc.Ticks,
            Convert.ToHexString(digest).ToLowerInvariant());
    }

    private static string Invariant(long value) =>
        value.ToString(CultureInfo.InvariantCulture);

    private static string Boolean(bool value) => value ? "true" : "false";

    private static LiteDbMigrationException LimitExceeded(string subject) =>
        new($"The LiteDB {subject} exceeds the fixed inspection limit.");

    [GeneratedRegex(
        @"^\$\.[A-Za-z_][A-Za-z0-9_]*(?:\.[A-Za-z_][A-Za-z0-9_]*)*$",
        RegexOptions.CultureInvariant)]
    private static partial Regex SimplePathExpression();

    private sealed class MetadataBudget(LiteDbInspectionLimits limits)
    {
        private long bytes;

        public void AddFacet(MigrationCatalogFacet facet)
        {
            AddString(facet.Name, "facet name");
            if (facet.Value is not null)
                AddString(facet.Value, "facet value");
        }

        public void AddString(string value, string subject)
        {
            int count;
            try
            {
                count = StrictUtf8.GetByteCount(value);
            }
            catch (EncoderFallbackException exception)
            {
                throw new LiteDbMigrationException(
                    $"LiteDB {subject} is not valid Unicode.",
                    exception);
            }
            bytes = checked(bytes + count);
            if (bytes > limits.MaxMetadataBytes)
                throw LimitExceeded("catalog metadata bytes");
        }
    }

    private sealed class MutableFieldProfile
    {
        public long DocumentsPresent { get; set; }

        public SortedDictionary<string, long> TypeCounts { get; } =
            new(StringComparer.Ordinal);
    }

    private sealed record FieldProfile(
        string Path,
        long DocumentsPresent,
        IReadOnlyDictionary<string, long> TypeCounts);

    private sealed record CollectionProfile(
        IReadOnlyDictionary<string, FieldProfile> Fields,
        IReadOnlyDictionary<string, long> IdTypeCounts);

    private sealed record LiteDbIndexMetadata(
        string? Collection,
        string? Name,
        string? Expression,
        bool? Unique,
        int SourceOrdinal);

    private sealed record FileIdentity(
        string Path,
        long Length,
        long LastWriteTicks,
        string Sha256);
}
