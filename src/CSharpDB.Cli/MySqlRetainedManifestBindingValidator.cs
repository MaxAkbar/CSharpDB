using System.Globalization;
using CSharpDB.Migration;
using CSharpDB.Migration.Retained;

namespace CSharpDB.Cli;

/// <summary>
/// CLI-side copy of the MySQL retained v1 binding contract. The generic CLI
/// intentionally does not reference the optional MySQL provider assembly.
/// </summary>
internal static class MySqlRetainedManifestBindingValidator
{
    private const string CatalogContract =
        "csharpdb-mysql-retained-catalog/v1";
    private const string AnalyzerContract =
        "csharpdb-mysql-catalog/v3";
    private const string DataContract =
        "csharpdb-mysql-retained-data/v1";
    private const string RowOrderContract =
        "csharpdb-mysql-integer-key-order/v1";
    private const string ScalarContract =
        "csharpdb-mysql-scalar/v1";
    private const string SnapshotPrefix =
        "mysql-retained:";

    internal static void Validate(
        MigrationCatalog catalog,
        RetainedMigrationPackageManifest manifest)
    {
        ArgumentNullException.ThrowIfNull(catalog);
        ArgumentNullException.ThrowIfNull(manifest);
        try
        {
            ValidateCore(catalog, manifest);
        }
        catch (InvalidDataException)
        {
            throw;
        }
        catch (Exception exception) when (
            exception is ArgumentException or
                InvalidOperationException or
                OverflowException)
        {
            throw InvalidBinding();
        }
    }

    private static void ValidateCore(
        MigrationCatalog catalog,
        RetainedMigrationPackageManifest manifest)
    {
        MigrationContractValidator.ValidateCatalog(catalog);
        if (catalog.Source.Kind != MigrationSourceKind.MySql ||
            catalog.Source.Consistency.Kind !=
                MigrationConsistencyKind.Snapshot ||
            manifest.SourceKind != MigrationSourceKind.MySql ||
            !string.Equals(
                manifest.Format,
                RetainedMigrationPackageContract.Format,
                StringComparison.Ordinal) ||
            !string.Equals(
                manifest.SourceIdentity,
                catalog.Source.Identity,
                StringComparison.Ordinal) ||
            !string.Equals(
                manifest.CatalogDigest,
                MigrationArtifactSerializer
                    .ComputeCatalogDigest(catalog),
                StringComparison.Ordinal))
        {
            throw InvalidBinding();
        }

        MigrationCatalogObject database =
            SingleDatabase(catalog);
        string contentDigest =
            Facet(
                database,
                "mysqlRetainedContentDigest");
        string snapshotIdentity =
            Facet(
                database,
                "mysqlRetainedSnapshotIdentity");
        if (!string.Equals(
                Facet(
                    database,
                    "mysqlCatalogContract"),
                CatalogContract,
                StringComparison.Ordinal) ||
            !string.Equals(
                Facet(
                    database,
                    "mysqlAnalyzerCatalogContract"),
                AnalyzerContract,
                StringComparison.Ordinal) ||
            !string.Equals(
                Facet(
                    database,
                    "mysqlDataContract"),
                DataContract,
                StringComparison.Ordinal) ||
            !string.Equals(
                Facet(
                    database,
                    "mysqlRetainedMetadataScope"),
                "ordinary-base-tables",
                StringComparison.Ordinal) ||
            !IsTrue(
                Facet(
                    database,
                    "mysqlRetainedDirectSchemaSelectProven")) ||
            !IsSha256(contentDigest) ||
            !string.Equals(
                contentDigest,
                catalog.Source.Fingerprint,
                StringComparison.Ordinal) ||
            !string.Equals(
                contentDigest,
                manifest.SourceFingerprint,
                StringComparison.Ordinal) ||
            !string.Equals(
                contentDigest,
                manifest.ContentDigest,
                StringComparison.Ordinal) ||
            !string.Equals(
                snapshotIdentity,
                SnapshotPrefix + contentDigest,
                StringComparison.Ordinal) ||
            !string.Equals(
                snapshotIdentity,
                manifest.SnapshotIdentity,
                StringComparison.Ordinal))
        {
            throw InvalidBinding();
        }

        IReadOnlyDictionary<string, MigrationCatalogObject>
            objects = catalog.Objects.ToDictionary(
                static item => item.ObjectId,
                StringComparer.Ordinal);
        var availableTableIds =
            new HashSet<string>(StringComparer.Ordinal);
        foreach (MigrationCatalogObject table in
                 catalog.Objects.Where(static item =>
                     item.Kind == MigrationObjectKind.Table))
        {
            string available = Facet(
                table,
                MigrationDataAvailabilityContract
                    .AvailableFacet);
            if (IsTrue(available))
            {
                availableTableIds.Add(table.ObjectId);
            }
            else if (!string.Equals(
                         available,
                         "false",
                         StringComparison.Ordinal) ||
                     string.IsNullOrWhiteSpace(
                         Facet(
                             table,
                             MigrationDataAvailabilityContract
                                 .UnavailableReasonFacet)))
            {
                throw InvalidBinding();
            }
        }

        RetainedMigrationPackageTableManifest[] tables =
            manifest.Tables?.ToArray() ??
            throw InvalidBinding();
        if (tables.Any(static table =>
                table is null ||
                table.Descriptor is null) ||
            tables.Select(static table =>
                    table.Descriptor.SourceObjectId)
                .Distinct(StringComparer.Ordinal)
                .Count() != tables.Length ||
            !availableTableIds.SetEquals(
                tables.Select(static table =>
                    table.Descriptor.SourceObjectId)))
        {
            throw InvalidBinding();
        }
        foreach (RetainedMigrationPackageTableManifest table
                 in tables)
        {
            ValidateTable(
                table,
                catalog,
                objects);
        }
    }

    private static void ValidateTable(
        RetainedMigrationPackageTableManifest manifest,
        MigrationCatalog catalog,
        IReadOnlyDictionary<string, MigrationCatalogObject>
            objects)
    {
        RetainedMigrationTableDescriptor descriptor =
            manifest.Descriptor ??
            throw InvalidBinding();
        if (!objects.TryGetValue(
                descriptor.SourceObjectId,
                out MigrationCatalogObject? table) ||
            table.Kind != MigrationObjectKind.Table ||
            manifest.RowCount < 0 ||
            manifest.SectionLength < 0 ||
            !IsSha256(manifest.SectionDigest) ||
            ParseNonNegativeInt64(
                Facet(
                    table,
                    "mysqlRetainedRowCount")) !=
                manifest.RowCount ||
            !string.Equals(
                Facet(
                    table,
                    "mysqlRetainedSectionDigest"),
                manifest.SectionDigest,
                StringComparison.Ordinal) ||
            !string.Equals(
                Facet(
                    table,
                    "mysqlRowOrderContract"),
                RowOrderContract,
                StringComparison.Ordinal))
        {
            throw InvalidBinding();
        }

        MigrationCatalogObject[] columns = catalog.Objects
            .Where(item =>
                item.Kind == MigrationObjectKind.Column &&
                string.Equals(
                    item.ParentObjectId,
                    table.ObjectId,
                    StringComparison.Ordinal))
            .OrderBy(item => ParsePositiveInt32(
                Facet(
                    item,
                    "mysqlOrdinalPosition")))
            .ThenBy(static item =>
                item.ObjectId,
                StringComparer.Ordinal)
            .ToArray();
        if (columns.Length == 0 ||
            descriptor.ColumnObjectIds is null ||
            !columns.Select(static column =>
                    column.ObjectId)
                .SequenceEqual(
                    descriptor.ColumnObjectIds,
                    StringComparer.Ordinal))
        {
            throw InvalidBinding();
        }
        foreach (MigrationCatalogObject column in columns)
            ValidateColumn(column);

        string orderObjectId =
            Facet(
                table,
                "mysqlRowOrderObjectId");
        string orderKind =
            Facet(
                table,
                "mysqlRowOrderKind");
        if (!objects.TryGetValue(
                orderObjectId,
                out MigrationCatalogObject? key) ||
            key.Kind != MigrationObjectKind.Key ||
            !string.Equals(
                key.ParentObjectId,
                table.ObjectId,
                StringComparison.Ordinal) ||
            orderKind is not ("primary" or "unique") ||
            !string.Equals(
                Facet(key, "kind"),
                orderKind,
                StringComparison.Ordinal) ||
            !IsTrue(
                Facet(
                    key,
                    "mysqlMembershipComplete")) ||
            !IsTrue(
                Facet(
                    key,
                    "mysqlBackingIndexMatched")))
        {
            throw InvalidBinding();
        }

        MigrationObjectReference[] members = key.Members
            .Where(static member =>
                member.Role ==
                MigrationObjectReferenceRoles.Column)
            .OrderBy(static member =>
                member.Ordinal)
            .ToArray();
        if (members.Length == 0 ||
            members.Length != key.Members.Count ||
            !members.Select(static member =>
                    member.Ordinal)
                .SequenceEqual(
                    Enumerable.Range(0, members.Length)) ||
            descriptor.OrderingKeyColumnObjectIds is null ||
            !members.Select(static member =>
                    member.ObjectId)
                .SequenceEqual(
                    descriptor
                        .OrderingKeyColumnObjectIds,
                    StringComparer.Ordinal))
        {
            throw InvalidBinding();
        }
        foreach (MigrationObjectReference member in members)
        {
            if (!objects.TryGetValue(
                    member.ObjectId,
                    out MigrationCatalogObject?
                        orderColumn) ||
                orderColumn.Kind !=
                    MigrationObjectKind.Column ||
                !string.Equals(
                    orderColumn.ParentObjectId,
                    table.ObjectId,
                    StringComparison.Ordinal) ||
                !string.Equals(
                    Facet(
                        orderColumn,
                        "nullable"),
                    "false",
                    StringComparison.Ordinal) ||
                Facet(
                    orderColumn,
                    "mysqlScalarCodec") is not (
                    "signed-integer" or
                    "unsigned-integer"))
            {
                throw InvalidBinding();
            }
        }
    }

    private static void ValidateColumn(
        MigrationCatalogObject column)
    {
        string codec = Facet(
            column,
            "mysqlScalarCodec");
        string logicalType = Facet(
            column,
            "logicalType");
        if (!IsTrue(
                Facet(
                    column,
                    "mysqlColumnDataAvailable")) ||
            !string.Equals(
                Facet(
                    column,
                    "mysqlScalarCodecContract"),
                ScalarContract,
                StringComparison.Ordinal) ||
            !CodecMatchesLogicalType(
                codec,
                logicalType))
        {
            throw InvalidBinding();
        }
    }

    private static bool CodecMatchesLogicalType(
        string codec,
        string logicalType) =>
        (codec, logicalType) switch
        {
            ("signed-integer", "signedInteger") => true,
            ("unsigned-integer", "unsignedInteger") => true,
            ("decimal", "decimal") => true,
            ("binary32", "floatingPoint") => true,
            ("binary64", "floatingPoint") => true,
            ("text", "text") => true,
            ("binary", "binary") => true,
            ("date", "date") => true,
            ("datetime", "dateTime") => true,
            _ => false,
        };

    private static MigrationCatalogObject SingleDatabase(
        MigrationCatalog catalog)
    {
        MigrationCatalogObject[] databases = catalog.Objects
            .Where(static item =>
                item.Kind == MigrationObjectKind.Database)
            .ToArray();
        return databases.Length == 1
            ? databases[0]
            : throw InvalidBinding();
    }

    private static string Facet(
        MigrationCatalogObject item,
        string name)
    {
        MigrationCatalogFacet[] matches = item.Facets
            .Where(facet => string.Equals(
                facet.Name,
                name,
                StringComparison.Ordinal))
            .ToArray();
        return matches.Length == 1 &&
               matches[0].Value is string value
            ? value
            : throw InvalidBinding();
    }

    private static int ParsePositiveInt32(
        string value)
    {
        if (int.TryParse(
                value,
                NumberStyles.None,
                CultureInfo.InvariantCulture,
                out int parsed) &&
            parsed > 0 &&
            string.Equals(
                parsed.ToString(
                    CultureInfo.InvariantCulture),
                value,
                StringComparison.Ordinal))
        {
            return parsed;
        }
        throw InvalidBinding();
    }

    private static long ParseNonNegativeInt64(
        string value)
    {
        if (long.TryParse(
                value,
                NumberStyles.None,
                CultureInfo.InvariantCulture,
                out long parsed) &&
            parsed >= 0 &&
            string.Equals(
                parsed.ToString(
                    CultureInfo.InvariantCulture),
                value,
                StringComparison.Ordinal))
        {
            return parsed;
        }
        throw InvalidBinding();
    }

    private static bool IsTrue(string value) =>
        string.Equals(
            value,
            "true",
            StringComparison.Ordinal);

    private static bool IsSha256(string value) =>
        value.Length == 71 &&
        value.StartsWith(
            "sha256:",
            StringComparison.Ordinal) &&
        value.AsSpan(7).IndexOfAnyExcept(
            "0123456789abcdef".AsSpan()) < 0;

    private static InvalidDataException InvalidBinding() =>
        new(
            "The retained MySQL package manifest does not match its provider catalog bindings.");
}
