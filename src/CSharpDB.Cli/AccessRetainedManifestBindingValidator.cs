using System.Buffers.Binary;
using System.Globalization;
using System.Security.Cryptography;
using System.Text;
using CSharpDB.Migration;
using CSharpDB.Migration.Retained;

namespace CSharpDB.Cli;

/// <summary>
/// CLI-side copy of the Access retained-v1 binding contract. The generic CLI
/// intentionally does not reference the optional Windows/OleDb adapter.
/// </summary>
internal static class
    AccessRetainedManifestBindingValidator
{
    private const string CatalogContract =
        "csharpdb-access-catalog/v1";
    private const string DataContract =
        "csharpdb-access-retained-data/v1";
    private const string ScalarContract =
        "csharpdb-access-scalar/v1";
    private const string RowOrderContract =
        "csharpdb-access-primary-key-order/v1";
    private const string SnapshotPrefix =
        "access-retained:";
    private const string QualificationRule =
        "MIG-ACCESS-LIVE-QUALIFICATION-PENDING-001";

    private static readonly UTF8Encoding StrictUtf8 =
        new(
            encoderShouldEmitUTF8Identifier: false,
            throwOnInvalidBytes: true);

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
                RetainedMigrationPackageContract.Format,
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
        string sourceDigest =
            RequiredFacet(
                database,
                "accessSourceContentDigest");
        string contentDigest =
            RequiredFacet(
                database,
                "accessRetainedContentDigest");
        string snapshotIdentity =
            RequiredFacet(
                database,
                "accessRetainedSnapshotIdentity");
        string expectedSnapshot =
            SnapshotPrefix +
            StableDigest(
                "csharpdb-access-retained-snapshot/v1",
                sourceDigest,
                contentDigest)["sha256:".Length..];

        if (!string.Equals(
                RequiredFacet(
                    database,
                    "accessCatalogContract"),
                CatalogContract,
                StringComparison.Ordinal) ||
            !string.Equals(
                RequiredFacet(
                    database,
                    "accessRetainedDataContract"),
                DataContract,
                StringComparison.Ordinal) ||
            RequiredFacet(
                database,
                "accessProviderId") is not (
                    "Microsoft.ACE.OLEDB.16.0" or
                    "Microsoft.ACE.OLEDB.12.0") ||
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
                        QualificationRule);
        if (qualification is null ||
            !string.Equals(
                qualification.ObjectId,
                database.ObjectId,
                StringComparison.Ordinal) ||
            qualification.Severity !=
                MigrationDiagnosticSeverity.Error ||
            qualification.Status !=
                MigrationCompatibilityStatus.Unknown ||
            qualification.Evidence !=
                MigrationEvidenceLevel.Parsed ||
            qualification.CanOverride)
        {
            throw InvalidBinding();
        }

        IReadOnlyDictionary<string,
                MigrationCatalogObject> objects =
            catalog.Objects.ToDictionary(
                static item => item.ObjectId,
                StringComparer.Ordinal);
        var availableTables =
            new HashSet<string>(
                StringComparer.Ordinal);
        foreach (MigrationCatalogObject table in
                 catalog.Objects.Where(
                     static item =>
                         item.Kind ==
                         MigrationObjectKind.Table))
        {
            string available =
                RequiredFacet(
                    table,
                    "migrationDataAvailable");
            if (string.Equals(
                    available,
                    "true",
                    StringComparison.Ordinal))
            {
                availableTables.Add(
                    table.ObjectId);
            }
            else if (!string.Equals(
                         available,
                         "false",
                         StringComparison.Ordinal) ||
                     string.IsNullOrWhiteSpace(
                         RequiredFacet(
                             table,
                             "migrationDataUnavailableReason")))
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
                .Distinct(StringComparer.Ordinal)
                .Count() !=
                manifestTables.Length ||
            !availableTables.SetEquals(
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
        IReadOnlyDictionary<
            string,
            MigrationCatalogObject> objects)
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
                RowOrderContract,
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
                    static item => item.ObjectId)
                .SequenceEqual(
                    descriptor.ColumnObjectIds,
                    StringComparer.Ordinal))
        {
            throw InvalidBinding();
        }
        foreach (MigrationCatalogObject column
                 in columns)
        {
            ValidateColumn(column);
        }

        MigrationCatalogObject key =
            catalog.Objects.SingleOrDefault(item =>
                item.Kind == MigrationObjectKind.Key &&
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
                    static member => member.Ordinal)
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
            descriptor.OrderingKeyColumnObjectIds
                is null ||
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

        foreach (MigrationObjectReference member
                 in members)
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
        string codec =
            RequiredFacet(
                column,
                "accessScalarCodec");
        string logicalType =
            RequiredFacet(
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
                ScalarContract,
                StringComparison.Ordinal) ||
            !TryLogicalType(
                codec,
                out string? expectedType) ||
            !string.Equals(
                logicalType,
                expectedType,
                StringComparison.Ordinal))
        {
            throw InvalidBinding();
        }
    }

    private static bool TryLogicalType(
        string codec,
        out string? logicalType)
    {
        logicalType = codec switch
        {
            "SignedInteger" => "SignedInteger",
            "UnsignedInteger" => "UnsignedInteger",
            "Boolean" => "Boolean",
            "Decimal" => "Decimal",
            "FloatingPoint" => "FloatingPoint",
            "Text" => "Text",
            "Binary" => "Binary",
            "Guid" => "Guid",
            "DateTime" => "DateTime",
            _ => null,
        };
        return logicalType is not null;
    }

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
            value.Length > 1 &&
                value[0] == '0' ||
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
        return value.AsSpan(7).ContainsAnyExcept(
                "0123456789abcdef") ==
            false;
    }

    private static string StableDigest(
        string domain,
        params string?[] values)
    {
        using IncrementalHash hash =
            IncrementalHash.CreateHash(
                HashAlgorithmName.SHA256);
        Append(hash, domain);
        foreach (string? value in values)
            Append(hash, value);
        return "sha256:" +
            Convert.ToHexString(
                    hash.GetHashAndReset())
                .ToLowerInvariant();
    }

    private static void Append(
        IncrementalHash hash,
        string? value)
    {
        Span<byte> length =
            stackalloc byte[sizeof(int)];
        if (value is null)
        {
            BinaryPrimitives.WriteInt32BigEndian(
                length,
                -1);
            hash.AppendData(length);
            return;
        }
        byte[] bytes =
            StrictUtf8.GetBytes(value);
        BinaryPrimitives.WriteInt32BigEndian(
            length,
            bytes.Length);
        hash.AppendData(length);
        hash.AppendData(bytes);
        CryptographicOperations.ZeroMemory(bytes);
    }

    private static InvalidDataException
        InvalidBinding() =>
        new(
            "The retained Microsoft Access package does not match its provider-specific catalog contract.");
}
