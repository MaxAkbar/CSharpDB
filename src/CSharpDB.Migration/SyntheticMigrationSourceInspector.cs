using System.Security.Cryptography;
using System.Text;

namespace CSharpDB.Migration;

/// <summary>
/// Project-authored source used to exercise the complete planning grammar
/// without a provider or external fixture license.
/// </summary>
public sealed class SyntheticMigrationSourceInspector : IMigrationSourceInspector
{
    public const string FixtureIdentity = "synthetic:awkward-v1";

    public MigrationSourceKind SourceKind => MigrationSourceKind.Synthetic;

    public ValueTask<MigrationCatalog> InspectAsync(
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
                $"The synthetic fixture is qualified for CSharpDB {CSharpDbCapabilityCatalogLoader.CurrentTargetVersion}.");
        }
        if (request.ProfileSampleSize <= 0)
            throw new ArgumentOutOfRangeException(nameof(request), "Profile sample size must be positive.");

        string longTableName = "Quarterly archive " + new string('x', 128) + " 🚀";
        var objects = new List<MigrationCatalogObject>
        {
            Object("syn:ns:main", MigrationObjectKind.Namespace, "main",
                facets: [Facet("isDefault", "true")]),
            Object("syn:ns:sales", MigrationObjectKind.Namespace, "sales"),

            Object("syn:table:customers-upper", MigrationObjectKind.Table, "Customers", "syn:ns:main"),
            Column("syn:column:customers-upper:id", "Id", "INT64", "signedInteger", "syn:table:customers-upper",
                Facet("nullable", "false"), Facet("primaryKey", "true")),
            Column("syn:column:customers-upper:name", "Display Name", "NVARCHAR(200)", "text", "syn:table:customers-upper",
                Facet("nullable", "false"), Facet("maxLength", "200")),
            Column("syn:column:customers-upper:payload", "Payload", "VARBINARY(MAX)", "binary", "syn:table:customers-upper",
                Facet("nullable", "true")),
            Column("syn:column:customers-upper:enabled", "Enabled?", "BOOLEAN", "boolean", "syn:table:customers-upper",
                Facet("nullable", "false")),
            Column("syn:column:customers-upper:external-id", "External Id", "UUID", "guid", "syn:table:customers-upper",
                Facet("nullable", "false")),
            Key("syn:key:customers-upper:pk", "PK Customers", "syn:table:customers-upper",
                "syn:column:customers-upper:id"),

            Object("syn:table:customers-lower", MigrationObjectKind.Table, "customers", "syn:ns:main"),
            Column("syn:column:customers-lower:code-upper", "Code", "TEXT", "text", "syn:table:customers-lower",
                Facet("nullable", "false")),
            Column("syn:column:customers-lower:code-lower", "code", "TEXT", "text", "syn:table:customers-lower",
                Facet("nullable", "true")),

            Object("syn:table:reserved", MigrationObjectKind.Table, "sys_tables", "syn:ns:main"),
            Column("syn:column:reserved:value", "value", "TEXT", "text", "syn:table:reserved",
                Facet("nullable", "true")),

            Object("syn:table:orders", MigrationObjectKind.Table, "Order \"Lines\" 🚀", "syn:ns:sales"),
            Column("syn:column:orders:id", "Order-ID", "INT64", "signedInteger", "syn:table:orders",
                Facet("nullable", "false")),
            Column("syn:column:orders:customer-id", "Customer Id", "INT64", "signedInteger", "syn:table:orders",
                Facet("nullable", "false")),
            Column("syn:column:orders:amount", "Gross Amount", "DECIMAL(38,9)", "decimal", "syn:table:orders",
                Facet("nullable", "false"), Facet("precision", "38"), Facet("scale", "9"),
                ProfileFacets(request.IncludeProfile, MigrationCoverageKind.Full, 12, 12)),
            Column("syn:column:orders:tax", "Tax", "DECIMAL(12,2)", "decimal", "syn:table:orders",
                Facet("nullable", "false"), Facet("precision", "12"), Facet("scale", "2"),
                ProfileFacets(request.IncludeProfile, MigrationCoverageKind.Full, 12, 12)),
            Column("syn:column:orders:ordered-at", "Ordered At", "DATETIMEOFFSET(7)", "dateTimeOffset", "syn:table:orders",
                Facet("nullable", "false"), Facet("fractionalSeconds", "7")),
            Column("syn:column:orders:source-counter", "Source Counter", "UINT64", "unsignedInteger", "syn:table:orders",
                Facet("nullable", "false"),
                ProfileFacets(request.IncludeProfile, MigrationCoverageKind.Sample,
                    Math.Min(request.ProfileSampleSize, 5), 12)),
            Key("syn:key:orders:pk", "PK Order Lines", "syn:table:orders", "syn:column:orders:id"),
            Object("syn:index:orders:amount", MigrationObjectKind.Index, "IX Amount", "syn:table:orders",
                members: [Member("syn:column:orders:amount", MigrationObjectReferenceRoles.Column, 0)],
                dependsOn: ["syn:column:orders:amount"]),
            Object("syn:fk:orders:customer", MigrationObjectKind.ForeignKey, "FK Order Customer", "syn:table:orders",
                members:
                [
                    Member("syn:column:orders:customer-id", MigrationObjectReferenceRoles.SourceColumn, 0),
                    Member("syn:key:customers-upper:pk", MigrationObjectReferenceRoles.ReferencedKey, 0),
                ],
                dependsOn: ["syn:column:orders:customer-id", "syn:key:customers-upper:pk"]),

            Object("syn:table:spatial", MigrationObjectKind.Table, longTableName, "syn:ns:sales"),
            Column("syn:column:spatial:shape", "Shape", "GEOGRAPHY", "geography", "syn:table:spatial",
                Facet("srid", "4326"), Facet("nullable", "false")),

            Object("syn:view:recent-orders", MigrationObjectKind.View, "Recent Orders", "syn:ns:sales",
                facets: [Facet("sourceSql", "SELECT OrderId FROM Orders")],
                dependsOn: ["syn:table:orders"]),
            Object("syn:trigger:orders-audit", MigrationObjectKind.Trigger, "Audit Orders", "syn:table:orders",
                facets: [Facet("timing", "after"), Facet("event", "update"), Facet("hasWhen", "true")],
                dependsOn: ["syn:table:orders"]),
            Object("syn:sequence:orders", MigrationObjectKind.Sequence, "Order Number Sequence", "syn:ns:sales"),
            Object("syn:routine:reprice", MigrationObjectKind.Routine, "Reprice Orders", "syn:ns:sales",
                dependsOn: ["syn:table:orders"]),
        };

        var catalog = new MigrationCatalog
        {
            TargetCSharpDbVersion = request.TargetCSharpDbVersion,
            Source = new MigrationSourceIdentity
            {
                Kind = MigrationSourceKind.Synthetic,
                Identity = FixtureIdentity,
                Fingerprint = "sha256:" + StableSha256("csharpdb-synthetic-awkward-v1"),
                ProviderVersion = "1.0.0",
                SourceVersion = "awkward-v1",
                Consistency = new MigrationConsistencyStrategy
                {
                    Kind = MigrationConsistencyKind.Immutable,
                    Description = "Project-authored immutable synthetic fixture.",
                },
            },
            Objects = objects,
            Diagnostics =
            [
                new MigrationDiagnostic
                {
                    DiagnosticId = "syn:diag:trigger-when",
                    RuleId = "SYN-SOURCE-TRIGGER-001",
                    Severity = MigrationDiagnosticSeverity.Warning,
                    Status = MigrationCompatibilityStatus.Unsupported,
                    Evidence = MigrationEvidenceLevel.Parsed,
                    Summary = "The source trigger contains a WHEN clause.",
                    Explanation = "The clause is retained as a source facet and must not be silently dropped.",
                    ObjectId = "syn:trigger:orders-audit",
                    Remediation = "Rewrite the trigger condition after scratch validation.",
                    CanOverride = false,
                },
            ],
        };

        MigrationContractValidator.ValidateCatalog(catalog);
        return ValueTask.FromResult(catalog);
    }

    private static MigrationCatalogObject Object(
        string id,
        MigrationObjectKind kind,
        string name,
        string? parentId = null,
        IReadOnlyList<MigrationCatalogFacet>? facets = null,
        IReadOnlyList<MigrationObjectReference>? members = null,
        IReadOnlyList<string>? dependsOn = null) => new()
    {
        ObjectId = id,
        Kind = kind,
        ParentObjectId = parentId,
        SourceNamespace = parentId?.StartsWith("syn:ns:", StringComparison.Ordinal) == true
            ? parentId[7..]
            : null,
        SourceName = name,
        Facets = facets ?? [],
        Members = members ?? [],
        DependsOn = dependsOn ?? [],
    };

    private static MigrationCatalogObject Column(
        string id,
        string name,
        string nativeType,
        string logicalType,
        string parentId,
        params object[] facets)
    {
        var flattened = new List<MigrationCatalogFacet> { Facet("logicalType", logicalType) };
        foreach (object value in facets)
        {
            if (value is MigrationCatalogFacet facet)
                flattened.Add(facet);
            else if (value is IEnumerable<MigrationCatalogFacet> group)
                flattened.AddRange(group);
        }

        return new MigrationCatalogObject
        {
            ObjectId = id,
            Kind = MigrationObjectKind.Column,
            ParentObjectId = parentId,
            SourceName = name,
            NativeType = nativeType,
            Facets = flattened,
        };
    }

    private static MigrationCatalogObject Key(
        string id,
        string name,
        string parentId,
        params string[] columnIds) => new()
    {
        ObjectId = id,
        Kind = MigrationObjectKind.Key,
        ParentObjectId = parentId,
        SourceName = name,
        Facets = [Facet("kind", "primary")],
        Members = columnIds
            .Select((columnId, ordinal) => Member(
                columnId,
                MigrationObjectReferenceRoles.Column,
                ordinal))
            .ToArray(),
        DependsOn = columnIds,
    };

    private static MigrationObjectReference Member(string objectId, string role, int ordinal) => new()
    {
        ObjectId = objectId,
        Role = role,
        Ordinal = ordinal,
    };

    private static MigrationCatalogFacet Facet(string name, string value) => new()
    {
        Name = name,
        Value = value,
    };

    private static IReadOnlyList<MigrationCatalogFacet> ProfileFacets(
        bool include,
        MigrationCoverageKind kind,
        long examined,
        long total) => include
        ?
        [
            Facet("profileKind", kind.ToString()),
            Facet("profileValuesExamined", examined.ToString(System.Globalization.CultureInfo.InvariantCulture)),
            Facet("profileTotalValues", total.ToString(System.Globalization.CultureInfo.InvariantCulture)),
        ]
        : [];

    private static string StableSha256(string value) =>
        Convert.ToHexString(SHA256.HashData(Encoding.UTF8.GetBytes(value))).ToLowerInvariant();
}
