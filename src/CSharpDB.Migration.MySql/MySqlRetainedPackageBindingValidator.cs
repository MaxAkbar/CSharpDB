using System.Globalization;
using CSharpDB.Migration.Retained;

namespace CSharpDB.Migration.MySql;

/// <summary>
/// Validates the provider-specific binding between a retained MySQL catalog
/// and the generic retained package manifest before any target mutation.
/// </summary>
public static class MySqlRetainedPackageBindingValidator
{
    public static void Validate(
        MigrationCatalog catalog,
        RetainedMigrationPackageManifest manifest)
    {
        ArgumentNullException.ThrowIfNull(catalog);
        ArgumentNullException.ThrowIfNull(manifest);

        try
        {
            ValidateCore(catalog, manifest);
        }
        catch (MySqlMigrationException)
        {
            throw;
        }
        catch (Exception exception) when (
            exception is ArgumentException or
                InvalidDataException or
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

        MigrationCatalogObject database = SingleObject(
            catalog,
            MigrationObjectKind.Database);
        string contentDigest = RequiredFacet(
            database,
            MySqlRetainedCatalog.ContentDigestFacet);
        string snapshotIdentity = RequiredFacet(
            database,
            MySqlRetainedCatalog.SnapshotIdentityFacet);
        if (!string.Equals(
                RequiredFacet(
                    database,
                    "mysqlCatalogContract"),
                MySqlRetainedDataContract.CatalogContract,
                StringComparison.Ordinal) ||
            !string.Equals(
                RequiredFacet(
                    database,
                    MySqlRetainedCatalog
                        .AnalyzerCatalogContractFacet),
                MySqlCatalogBuilder.CatalogContract,
                StringComparison.Ordinal) ||
            !string.Equals(
                RequiredFacet(
                    database,
                    MySqlRetainedCatalog.DataContractFacet),
                MySqlRetainedDataContract.DataContract,
                StringComparison.Ordinal) ||
            !string.Equals(
                RequiredFacet(
                    database,
                    MySqlRetainedCatalog.MetadataScopeFacet),
                MySqlRetainedCatalog.MetadataScope,
                StringComparison.Ordinal) ||
            !string.Equals(
                RequiredFacet(
                    database,
                    MySqlRetainedCatalog
                        .DirectSchemaSelectProvenFacet),
                "true",
                StringComparison.Ordinal) ||
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
                MySqlRetainedDataContract
                    .SnapshotIdentityPrefix +
                contentDigest,
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
        MigrationCatalogObject[] tables = catalog.Objects
            .Where(static item =>
                item.Kind == MigrationObjectKind.Table)
            .ToArray();
        var availableTableIds =
            new HashSet<string>(StringComparer.Ordinal);
        foreach (MigrationCatalogObject table in tables)
        {
            string available = RequiredFacet(
                table,
                MySqlRetainedDataContract
                    .DataAvailableFacet);
            if (string.Equals(
                    available,
                    "true",
                    StringComparison.Ordinal))
            {
                availableTableIds.Add(table.ObjectId);
            }
            else if (!string.Equals(
                         available,
                         "false",
                         StringComparison.Ordinal) ||
                     string.IsNullOrWhiteSpace(
                         RequiredFacet(
                             table,
                             MySqlRetainedDataContract
                                 .DataUnavailableReasonFacet)))
            {
                throw InvalidBinding();
            }
        }

        RetainedMigrationPackageTableManifest[]
            manifestTables = manifest.Tables?.ToArray() ??
            throw InvalidBinding();
        if (manifestTables.Any(static item =>
                item is null ||
                item.Descriptor is null) ||
            manifestTables.Select(static item =>
                    item.Descriptor.SourceObjectId)
                .Distinct(StringComparer.Ordinal)
                .Count() != manifestTables.Length ||
            !availableTableIds.SetEquals(
                manifestTables.Select(static item =>
                    item.Descriptor.SourceObjectId)))
        {
            throw InvalidBinding();
        }

        foreach (RetainedMigrationPackageTableManifest
                 tableManifest in manifestTables)
        {
            ValidateTable(
                tableManifest,
                objects,
                catalog);
        }
    }

    private static void ValidateTable(
        RetainedMigrationPackageTableManifest manifest,
        IReadOnlyDictionary<string, MigrationCatalogObject>
            objects,
        MigrationCatalog catalog)
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
            ParseCanonicalNonNegativeInt64(
                RequiredFacet(
                    table,
                    MySqlRetainedCatalog.RowCountFacet)) !=
                manifest.RowCount ||
            !string.Equals(
                RequiredFacet(
                    table,
                    MySqlRetainedCatalog.SectionDigestFacet),
                manifest.SectionDigest,
                StringComparison.Ordinal) ||
            !string.Equals(
                RequiredFacet(
                    table,
                    MySqlRetainedCatalog
                        .RowOrderContractFacet),
                MySqlRetainedDataContract.RowOrderContract,
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
            .OrderBy(item => ParseCanonicalPositiveInt32(
                RequiredFacet(
                    item,
                    "mysqlOrdinalPosition")))
            .ThenBy(static item =>
                item.ObjectId,
                StringComparer.Ordinal)
            .ToArray();
        if (columns.Length == 0 ||
            descriptor.ColumnObjectIds is null ||
            !columns.Select(static item => item.ObjectId)
                .SequenceEqual(
                    descriptor.ColumnObjectIds,
                    StringComparer.Ordinal))
        {
            throw InvalidBinding();
        }
        foreach (MigrationCatalogObject column in columns)
            ValidateColumn(column);

        string orderObjectId = RequiredFacet(
            table,
            MySqlRetainedCatalog.RowOrderObjectIdFacet);
        string orderKind = RequiredFacet(
            table,
            MySqlRetainedCatalog.RowOrderKindFacet);
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
                RequiredFacet(key, "kind"),
                orderKind,
                StringComparison.Ordinal) ||
            !IsTrue(
                RequiredFacet(
                    key,
                    "mysqlMembershipComplete")) ||
            !IsTrue(
                RequiredFacet(
                    key,
                    "mysqlBackingIndexMatched")))
        {
            throw InvalidBinding();
        }

        MigrationObjectReference[] members = key.Members
            .Where(static member =>
                member.Role ==
                MigrationObjectReferenceRoles.Column)
            .OrderBy(static member => member.Ordinal)
            .ToArray();
        if (members.Length == 0 ||
            members.Length != key.Members.Count ||
            !members.Select(static member => member.Ordinal)
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
                    RequiredFacet(
                        orderColumn,
                        "nullable"),
                    "false",
                    StringComparison.Ordinal) ||
                RequiredFacet(
                    orderColumn,
                    MySqlRetainedCatalog
                        .ScalarCodecFacet) is not (
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
        string codec = RequiredFacet(
            column,
            MySqlRetainedCatalog.ScalarCodecFacet);
        string logicalType = RequiredFacet(
            column,
            "logicalType");
        if (!IsTrue(
                RequiredFacet(
                    column,
                    MySqlRetainedCatalog
                        .ColumnDataAvailableFacet)) ||
            !string.Equals(
                RequiredFacet(
                    column,
                    MySqlRetainedCatalog
                        .ScalarCodecContractFacet),
                MySqlRetainedDataContract
                    .ScalarCodecContract,
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

    private static MigrationCatalogObject SingleObject(
        MigrationCatalog catalog,
        MigrationObjectKind kind)
    {
        MigrationCatalogObject[] matches = catalog.Objects
            .Where(item => item.Kind == kind)
            .ToArray();
        return matches.Length == 1
            ? matches[0]
            : throw InvalidBinding();
    }

    private static string RequiredFacet(
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

    private static int ParseCanonicalPositiveInt32(
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

    private static long ParseCanonicalNonNegativeInt64(
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

    private static MySqlMigrationException
        InvalidBinding() => new(
            "The retained MySQL package binding is invalid.");
}
