using System.Globalization;
using CSharpDB.Migration;
using CSharpDB.Migration.Retained;

namespace CSharpDB.Migration.Access;

/// <summary>
/// Validates the Access-specific binding between a verified generic retained
/// package manifest and its embedded catalog before any target mutation.
/// </summary>
public static class AccessRetainedPackageBindingValidator
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
        catch (AccessMigrationException)
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
        MigrationContractValidator.ValidateCatalog(
            catalog);
        if (catalog.Source.Kind !=
                MigrationSourceKind.Access ||
            catalog.Source.Consistency.Kind !=
                MigrationConsistencyKind.Snapshot ||
            manifest.SourceKind !=
                MigrationSourceKind.Access ||
            !string.Equals(
                manifest.Format,
                RetainedMigrationPackageContract
                    .Format,
                StringComparison.Ordinal) ||
            !string.Equals(
                manifest.SourceIdentity,
                catalog.Source.Identity,
                StringComparison.Ordinal) ||
            !string.Equals(
                manifest.SourceFingerprint,
                catalog.Source.Fingerprint,
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
            SingleObject(
                catalog,
                MigrationObjectKind.Database);
        string sourceDigest = RequiredFacet(
            database,
            "accessSourceContentDigest");
        string contentDigest = RequiredFacet(
            database,
            "accessRetainedContentDigest");
        string snapshotIdentity = RequiredFacet(
            database,
            "accessRetainedSnapshotIdentity");
        string expectedSnapshotDigest =
            AccessStableDigest.Text(
                "csharpdb-access-retained-snapshot/v1",
                sourceDigest,
                contentDigest);
        string expectedSnapshot =
            AccessRetainedDataContract
                .SnapshotIdentityPrefix +
            expectedSnapshotDigest[
                "sha256:".Length..];
        if (!string.Equals(
                RequiredFacet(
                    database,
                    "accessCatalogContract"),
                AccessMigrationSourceInspector
                    .CatalogContract,
                StringComparison.Ordinal) ||
            !string.Equals(
                RequiredFacet(
                    database,
                    "accessRetainedDataContract"),
                AccessRetainedDataContract
                    .DataContract,
                StringComparison.Ordinal) ||
            RequiredFacet(
                database,
                "accessProviderId") is not (
                AccessProviderIds.Ace16 or
                AccessProviderIds.Ace12) ||
            RequiredFacet(
                database,
                "accessSourceExtension") is not (
                ".mdb" or ".accdb") ||
            !string.Equals(
                RequiredFacet(
                    database,
                    "accessSourceLease"),
                "share-deny-write-delete",
                StringComparison.Ordinal) ||
            !string.Equals(
                RequiredFacet(
                    database,
                    "accessEncryptedSource"),
                "false",
                StringComparison.Ordinal) ||
            !IsSha256(sourceDigest) ||
            !IsSha256(contentDigest) ||
            !string.Equals(
                sourceDigest,
                catalog.Source.Fingerprint,
                StringComparison.Ordinal) ||
            !string.Equals(
                contentDigest,
                manifest.ContentDigest,
                StringComparison.Ordinal) ||
            !string.Equals(
                snapshotIdentity,
                expectedSnapshot,
                StringComparison.Ordinal) ||
            !string.Equals(
                snapshotIdentity,
                manifest.SnapshotIdentity,
                StringComparison.Ordinal))
        {
            throw InvalidBinding();
        }

        MigrationDiagnostic? qualification =
            catalog.Diagnostics.SingleOrDefault(
                static item =>
                    item.RuleId ==
                    AccessCatalogBuilder
                        .LiveQualificationRule);
        if (qualification is null ||
            qualification.ObjectId !=
                database.ObjectId ||
            qualification.Severity !=
                MigrationDiagnosticSeverity.Error ||
            qualification.Status !=
                MigrationCompatibilityStatus.Unknown ||
            qualification.CanOverride)
        {
            throw InvalidBinding();
        }

        IReadOnlyDictionary<string,
                MigrationCatalogObject>
            objects =
                catalog.Objects.ToDictionary(
                    static item => item.ObjectId,
                    StringComparer.Ordinal);
        var availableTableIds =
            new HashSet<string>(
                StringComparer.Ordinal);
        foreach (MigrationCatalogObject table in
                 catalog.Objects.Where(
                     static item =>
                         item.Kind ==
                         MigrationObjectKind.Table))
        {
            string available = RequiredFacet(
                table,
                AccessRetainedDataContract
                    .DataAvailableFacet);
            if (string.Equals(
                    available,
                    "true",
                    StringComparison.Ordinal))
            {
                availableTableIds.Add(
                    table.ObjectId);
            }
            else if (!string.Equals(
                         available,
                         "false",
                         StringComparison.Ordinal) ||
                     string.IsNullOrWhiteSpace(
                         RequiredFacet(
                             table,
                             AccessRetainedDataContract
                                 .DataUnavailableReasonFacet)))
            {
                throw InvalidBinding();
            }
        }

        RetainedMigrationPackageTableManifest[]
            manifestTables =
                manifest.Tables?.ToArray() ??
                throw InvalidBinding();
        if (manifestTables.Any(
                static table =>
                    table is null ||
                    table.Descriptor is null) ||
            manifestTables.Select(
                    static table =>
                        table.Descriptor
                            .SourceObjectId)
                .Distinct(
                    StringComparer.Ordinal)
                .Count() !=
                manifestTables.Length ||
            !availableTableIds.SetEquals(
                manifestTables.Select(
                    static table =>
                        table.Descriptor
                            .SourceObjectId)))
        {
            throw InvalidBinding();
        }

        foreach (
            RetainedMigrationPackageTableManifest
                table in manifestTables)
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
        IReadOnlyDictionary<string,
            MigrationCatalogObject> objects)
    {
        RetainedMigrationTableDescriptor descriptor =
            manifest.Descriptor ??
            throw InvalidBinding();
        if (!objects.TryGetValue(
                descriptor.SourceObjectId,
                out MigrationCatalogObject? table) ||
            table.Kind !=
                MigrationObjectKind.Table ||
            manifest.RowCount < 0 ||
            manifest.SectionLength < 0 ||
            !IsSha256(manifest.SectionDigest) ||
            ParseCanonicalNonNegativeInt64(
                RequiredFacet(
                    table,
                    "accessRetainedRowCount")) !=
                manifest.RowCount ||
            !string.Equals(
                RequiredFacet(
                    table,
                    "accessRetainedSectionDigest"),
                manifest.SectionDigest,
                StringComparison.Ordinal) ||
            !string.Equals(
                RequiredFacet(
                    table,
                    "accessRowOrderContract"),
                AccessRetainedDataContract
                    .RowOrderContract,
                StringComparison.Ordinal))
        {
            throw InvalidBinding();
        }

        MigrationCatalogObject[] columns =
            catalog.Objects.Where(item =>
                    item.Kind ==
                        MigrationObjectKind.Column &&
                    string.Equals(
                        item.ParentObjectId,
                        table.ObjectId,
                        StringComparison.Ordinal))
                .OrderBy(item =>
                    ParseCanonicalPositiveInt32(
                        RequiredFacet(
                            item,
                            "accessOrdinal")))
                .ThenBy(
                    static item =>
                        item.ObjectId,
                    StringComparer.Ordinal)
                .ToArray();
        if (columns.Length == 0 ||
            descriptor.ColumnObjectIds is null ||
            !columns.Select(
                    static item =>
                        item.ObjectId)
                .SequenceEqual(
                    descriptor.ColumnObjectIds,
                    StringComparer.Ordinal))
        {
            throw InvalidBinding();
        }
        foreach (MigrationCatalogObject column in
                 columns)
        {
            ValidateColumn(column);
        }

        MigrationCatalogObject key =
            catalog.Objects.SingleOrDefault(item =>
                item.Kind ==
                    MigrationObjectKind.Key &&
                string.Equals(
                    item.ParentObjectId,
                    table.ObjectId,
                    StringComparison.Ordinal) &&
                string.Equals(
                    OptionalFacet(item, "kind"),
                    "primary",
                    StringComparison.Ordinal)) ??
            throw InvalidBinding();
        MigrationObjectReference[] members =
            key.Members
                .Where(static member =>
                    member.Role ==
                    MigrationObjectReferenceRoles
                        .Column)
                .OrderBy(
                    static member =>
                        member.Ordinal)
                .ToArray();
        if (members.Length == 0 ||
            members.Length != key.Members.Count ||
            !members.Select(
                    static member =>
                        member.Ordinal)
                .SequenceEqual(
                    Enumerable.Range(
                        0,
                        members.Length)) ||
            descriptor
                .OrderingKeyColumnObjectIds is null ||
            !members.Select(
                    static member =>
                        member.ObjectId)
                .SequenceEqual(
                    descriptor
                        .OrderingKeyColumnObjectIds,
                    StringComparer.Ordinal))
        {
            throw InvalidBinding();
        }
        foreach (
            MigrationObjectReference member in members)
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
                    StringComparison.Ordinal))
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
            "accessScalarCodec");
        string logicalType = RequiredFacet(
            column,
            "logicalType");
        if (!string.Equals(
                RequiredFacet(
                    column,
                    "accessDataAvailable"),
                "true",
                StringComparison.Ordinal) ||
            !string.Equals(
                RequiredFacet(
                    column,
                    "accessScalarCodecContract"),
                AccessRetainedDataContract
                    .ScalarCodecContract,
                StringComparison.Ordinal) ||
            !Enum.TryParse(
                codec,
                ignoreCase: false,
                out AccessScalarCodecKind kind) ||
            !string.Equals(
                logicalType,
                LogicalType(kind),
                StringComparison.Ordinal))
        {
            throw InvalidBinding();
        }
    }

    private static string LogicalType(
        AccessScalarCodecKind kind) =>
        kind switch
        {
            AccessScalarCodecKind.SignedInteger =>
                "SignedInteger",
            AccessScalarCodecKind.UnsignedInteger =>
                "UnsignedInteger",
            AccessScalarCodecKind.Boolean =>
                "Boolean",
            AccessScalarCodecKind.Decimal =>
                "Decimal",
            AccessScalarCodecKind.FloatingPoint =>
                "FloatingPoint",
            AccessScalarCodecKind.Text => "Text",
            AccessScalarCodecKind.Binary =>
                "Binary",
            AccessScalarCodecKind.Guid => "Guid",
            AccessScalarCodecKind.DateTime =>
                "DateTime",
            _ => throw InvalidBinding(),
        };

    private static MigrationCatalogObject SingleObject(
        MigrationCatalog catalog,
        MigrationObjectKind kind) =>
        catalog.Objects.SingleOrDefault(item =>
            item.Kind == kind) ??
        throw InvalidBinding();

    private static string RequiredFacet(
        MigrationCatalogObject item,
        string name) =>
        OptionalFacet(item, name) ??
        throw InvalidBinding();

    private static string? OptionalFacet(
        MigrationCatalogObject item,
        string name) =>
        item.Facets.SingleOrDefault(facet =>
            string.Equals(
                facet.Name,
                name,
                StringComparison.Ordinal))?.Value;

    private static long
        ParseCanonicalNonNegativeInt64(
        string value)
    {
        if (value.Length == 0 ||
            (value.Length > 1 &&
             value[0] == '0') ||
            !long.TryParse(
                value,
                NumberStyles.None,
                CultureInfo.InvariantCulture,
                out long parsed) ||
            parsed < 0)
        {
            throw InvalidBinding();
        }
        return parsed;
    }

    private static int ParseCanonicalPositiveInt32(
        string value)
    {
        if (value.Length == 0 ||
            value[0] == '0' ||
            !int.TryParse(
                value,
                NumberStyles.None,
                CultureInfo.InvariantCulture,
                out int parsed) ||
            parsed <= 0)
        {
            throw InvalidBinding();
        }
        return parsed;
    }

    private static bool IsSha256(string value)
    {
        if (value.Length != 71 ||
            !value.StartsWith(
                "sha256:",
                StringComparison.Ordinal))
        {
            return false;
        }
        foreach (char character in value.AsSpan(7))
        {
            if (character is not (
                    >= '0' and <= '9') and not (
                    >= 'a' and <= 'f'))
            {
                return false;
            }
        }
        return true;
    }

    private static AccessMigrationException
        InvalidBinding() =>
        new(
            AccessMigrationErrorCode
                .InvalidRetainedPackage,
            "The retained Microsoft Access package does not match its provider-specific catalog contract.");
}
