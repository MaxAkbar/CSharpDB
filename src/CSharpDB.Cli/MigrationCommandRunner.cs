using System.Buffers;
using System.ComponentModel;
using System.Globalization;
using System.Runtime.ExceptionServices;
using System.Runtime.InteropServices;
using System.Runtime.Versioning;
using System.Security.AccessControl;
using System.Security.Cryptography;
using System.Security.Principal;
using System.Text;
using System.Text.Json;
using System.Text.Json.Serialization;
using CSharpDB.Engine;
using CSharpDB.Migration;
using CSharpDB.Migration.CSharpDb;
using CSharpDB.Migration.Compatibility;
using CSharpDB.Migration.Files.Csv;
using CSharpDB.Migration.Files.Json;
using CSharpDB.Migration.LiteDb;
using CSharpDB.Migration.Retained;
using CSharpDB.Migration.Sqlite;
using CSharpDB.Migration.Validation;
using CSharpDB.Primitives;
using CSharpDB.Sql;

namespace CSharpDB.Cli;

internal static class MigrationCommandRunner
{
    internal const string Usage =
        "Usage: csharpdb migrate inspect --source synthetic --out <catalog.json>\n" +
        "       csharpdb migrate inspect --source csv --input <source.csv> --package <source.csdbcsv> --out <catalog.json> [--delimiter auto|comma|semicolon|tab|pipe|<character>] [--no-header] [--table <name>] [--sample-rows <count>] [--null-token <text>] [--source-id <label>] [--workspace <directory>] [--max-source-bytes <count>]\n" +
        "       csharpdb migrate inspect --source json --input <source.json|source.ndjson> --package <source.csdbjson> --out <catalog.json> [--framing root-array|ndjson] [--table <name>] [--sample-rows <count>] [--source-id <label>] [--workspace <directory>] [--max-source-bytes <count>] [--typed-intent <source.csdbjson-intent.json> --expected-intent-manifest-digest <sha256:...>]\n" +
        "       csharpdb migrate inspect --source sqlite --input <source.db> --package <snapshot.csdbsqlite> --out <catalog.json> [--profile-sample-size <count>] [--max-source-bytes <count>]\n" +
        "       csharpdb migrate inspect --source litedb --input <source.db> --package <snapshot.csdblitedb> --out <catalog.json> [--profile-sample-size <count>] [--max-source-bytes <count>]\n" +
        "       csharpdb migrate inspect --source access --input <source.mdb|source.accdb> --package <snapshot.csdbaccess> --out <catalog.json> [--provider ace16|ace12] [--allow-ace12-fallback] [--command-timeout-seconds <1..3600>] [--max-source-bytes <count>] [--max-package-bytes <count>]\n" +
        "       csharpdb migrate inspect --source sqlserver --connection-env <name> --out <catalog.json> [--package <snapshot.csdbsqlserver> --max-source-bytes <count> --table-timeout-seconds <1..86400>]\n" +
        "       csharpdb migrate inspect --source mysql --connection-env <name> --out <catalog.json> [--package <snapshot.csdbmysql> --max-source-bytes <count> --table-timeout-seconds <1..86400>]\n" +
        "       csharpdb migrate ddl-check <file.sql> --dialect csharpdb|tsql [--format text|json]\n" +
        "       csharpdb migrate type-map <catalog.json> --out <report> [--profile preserve|queryable|custom --custom-map <map.json>] [--format text|json]\n" +
        "       csharpdb migrate query-check <query.sql> --dialect csharpdb|tsql|mysql|sqlite|access --out <report> [--query-id <id>] [--compatibility-level 150|160|170] [--format text|json]\n" +
        "       csharpdb migrate plan <catalog.json> --out <plan.json> [--profile preserve|queryable] [--accept-exclusions all|<id,...>] [--accept-diagnostics <id,...>] [--reject-mode fail-fast|deterministic --reject-rules all|<id,...> --max-rejected-rows-per-batch <count> --max-rejected-rows-per-run <count> --max-reject-evidence-value-bytes <count> --max-reject-evidence-bytes-per-batch <count> --max-reject-evidence-bytes-per-run <count> --max-reject-artifact-bytes <count>]\n" +
        "       csharpdb migrate preview <plan.json> --catalog <catalog.json> [--ddl|--scratch] [--format text|json]\n" +
        "       csharpdb migrate apply <plan.json> --catalog <catalog.json> [--source-package <source.csdbcsv|source.csdbjson|source.csdbsqlite|source.csdblitedb|source.csdbaccess|source.csdbsqlserver|source.csdbmysql> --expected-manifest-digest <sha256:...> --workspace <directory> --max-source-bytes <count>] --target <staged.csdb> --out <run.json> [--resume] [--allow-deterministic-rejects --reject-artifact <absolute-normalized-rejects.jsonl>] [--format text|json]\n" +
        "       csharpdb migrate validate <plan.json> --catalog <catalog.json> [--source-package <source.csdbcsv|source.csdbjson|source.csdbsqlite|source.csdblitedb|source.csdbaccess|source.csdbsqlserver|source.csdbmysql> --expected-manifest-digest <sha256:...> --workspace <directory> --max-source-bytes <count>] --target <staged.csdb> --out <validation.json> [--level schema|count|checksum] [--spill-dir <directory>] [--allow-deterministic-rejects --reject-artifact <absolute-normalized-rejects.jsonl>] [--format text|json]\n" +
        "       csharpdb migrate snapshot <source.csdb> --out <retained-snapshot.db> --offline [--workspace <directory>] [--max-database-bytes <count>] [--max-wal-bytes <count>] [--max-snapshot-bytes <count>] [--json]\n" +
        "       csharpdb migrate export <retained-snapshot.db> --format csv --table <physical-table> --out <table.csv> --manifest <table.manifest.json> --expected-snapshot-identity <csharpdb-retained-snapshot/v1:<bytes>:sha256:<64-lowercase-hex>> [--profile lossless-v1|spreadsheet-safe-lossy-v1] [--max-data-bytes <count>] [--max-decoded-blob-bytes <count>] [--checkpoint-row-interval <count>] [--json]\n" +
        "       csharpdb migrate export <retained-snapshot.db> --format json|ndjson --table <physical-table> --out <table.json|table.ndjson> --manifest <table.manifest.json> --expected-snapshot-identity <csharpdb-retained-snapshot/v1:<bytes>:sha256:<64-lowercase-hex>> [--profile lossless-v1] [--max-data-bytes <count>] [--max-decoded-blob-bytes <count>] [--checkpoint-row-interval <count>] [--json]";

    private static readonly JsonSerializerOptions JsonOptions = CreateJsonOptions();
    private static readonly UTF8Encoding Utf8NoBom = new(encoderShouldEmitUTF8Identifier: false);
    private static readonly UTF8Encoding Utf8NoBomStrict = new(
        encoderShouldEmitUTF8Identifier: false,
        throwOnInvalidBytes: true);
    private const string SqlServerRetainedCatalogFacet =
        "sqlServerCatalogContract";
    private const string SqlServerRetainedCatalogContract =
        "csharpdb-sqlserver-retained-catalog/v1";
    private const string SqlServerRetainedDataFacet =
        "sqlServerDataContract";
    private const string SqlServerRetainedDataContract =
        "csharpdb-sqlserver-retained-data/v1";
    private const string MySqlRetainedCatalogFacet =
        "mysqlCatalogContract";
    private const string MySqlRetainedCatalogContract =
        "csharpdb-mysql-retained-catalog/v1";
    private const string MySqlAnalyzerCatalogFacet =
        "mysqlAnalyzerCatalogContract";
    private const string MySqlAnalyzerCatalogContract =
        "csharpdb-mysql-catalog/v3";
    private const string MySqlRetainedDataFacet =
        "mysqlDataContract";
    private const string MySqlRetainedDataContract =
        "csharpdb-mysql-retained-data/v1";
    private const string MySqlRetainedContentDigestFacet =
        "mysqlRetainedContentDigest";
    private const string MySqlRetainedSnapshotIdentityFacet =
        "mysqlRetainedSnapshotIdentity";
    private const string MySqlRetainedSnapshotIdentityPrefix =
        "mysql-retained:";
    private const string MySqlRetainedMetadataScopeFacet =
        "mysqlRetainedMetadataScope";
    private const string MySqlRetainedMetadataScope =
        "ordinary-base-tables";
    private const string MySqlRetainedDirectSelectFacet =
        "mysqlRetainedDirectSchemaSelectProven";
    private const string MySqlRetainedScopeRule =
        "MIG-MYSQL-RETAINED-SCOPE-001";
    private const string MySqlRetainedQualificationRule =
        "MIG-MYSQL-RETAINED-LIVE-QUALIFICATION-DEFERRED-001";
    private static readonly StringComparison PathComparison =
        OperatingSystem.IsWindows() || OperatingSystem.IsMacOS()
        ? StringComparison.OrdinalIgnoreCase
        : StringComparison.Ordinal;
    private const StringComparison PortableArtifactPathComparison =
        StringComparison.OrdinalIgnoreCase;
    private static readonly string[] CsvDeterministicRejectRuleIds =
    [
        CsvMigrationDataRules.MissingField,
        CsvMigrationDataRules.NullNotAllowed,
        CsvMigrationDataRules.TypeMismatch,
    ];
    private static readonly string[] JsonDeterministicRejectRuleIds =
    [
        JsonMigrationDataRules.MissingProperty,
        JsonMigrationDataRules.NullNotAllowed,
        JsonMigrationDataRules.NonObjectRow,
        JsonMigrationDataRules.TypeMismatch,
    ];
    private const string DeferredDeterministicRejectRuleId =
        "MIG-CLI-REJECT-RULE-001";
    private const string JsonCatalogRouteOnlyCode =
        "MIG-JSON-CLI-SOURCE-VERSION-001";
    private const string JsonCatalogRouteOnlyMessage =
        "This CLI route supports only untyped retained JSON package v1 or explicitly typed retained JSON package v2 catalogs.";
    private const string SqliteCatalogContractFacet =
        "sqliteCatalogContract";
    private const string SqliteCatalogContractV1 =
        "csharpdb-sqlite-catalog-v1";
    private const string SqliteCatalogRouteOnlyMessage =
        "This CLI route supports only SQLite catalog contract v1.";
    private const string LiteDbCatalogContractFacet =
        "liteDbCatalogContract";
    private const string LiteDbCatalogRouteOnlyMessage =
        "This CLI route supports only LiteDB catalog contract v1.";
    private const string AccessCatalogFacet =
        "accessCatalogContract";
    private const string AccessCatalogContract =
        "csharpdb-access-catalog/v1";
    private const string AccessRetainedDataFacet =
        "accessRetainedDataContract";
    private const string AccessRetainedDataContract =
        "csharpdb-access-retained-data/v1";
    private const string AccessRetainedContentDigestFacet =
        "accessRetainedContentDigest";
    private const string AccessRetainedSnapshotIdentityFacet =
        "accessRetainedSnapshotIdentity";
    private const string AccessRetainedSnapshotIdentityPrefix =
        "access-retained:";
    private const string AccessLiveQualificationRule =
        "MIG-ACCESS-LIVE-QUALIFICATION-PENDING-001";
    private const long MaxMigrationContractArtifactBytes =
        64L * 1024 * 1024;

    public static bool IsKnownCommand(string? arg) =>
        string.Equals(arg, "migrate", StringComparison.OrdinalIgnoreCase);

    public static async ValueTask<int> RunAsync(
        string[] args,
        TextWriter output,
        TextWriter error,
        CancellationToken ct = default) =>
        await RunAsync(
            args,
            output,
            error,
            MigrationCommandDependencies.Default,
            ct);

    internal static async ValueTask<int> RunAsync(
        string[] args,
        TextWriter output,
        TextWriter error,
        MigrationCommandDependencies dependencies,
        CancellationToken ct = default)
    {
        ArgumentNullException.ThrowIfNull(args);
        ArgumentNullException.ThrowIfNull(output);
        ArgumentNullException.ThrowIfNull(error);
        ArgumentNullException.ThrowIfNull(dependencies);
        ArgumentNullException.ThrowIfNull(
            dependencies.InspectSqlServerAsync);
        ArgumentNullException.ThrowIfNull(
            dependencies.InspectMySqlAsync);
        ArgumentNullException.ThrowIfNull(
            dependencies.CaptureMySqlAsync);
        ArgumentNullException.ThrowIfNull(
            dependencies.CaptureAccessAsync);
        ArgumentNullException.ThrowIfNull(
            dependencies.BuildCSharpDbDdlPreview);
        ArgumentNullException.ThrowIfNull(
            dependencies.AnalyzeCSharpDbDdlAsync);
        ArgumentNullException.ThrowIfNull(
            dependencies.AnalyzeTsqlDdlAsync);
        ArgumentNullException.ThrowIfNull(
            dependencies.SealCSharpDbMigrationPlan);
        ArgumentNullException.ThrowIfNull(
            dependencies.SerializeMigrationPlan);

        if (args.Length < 2 || !IsKnownCommand(args[0]))
            return await UsageAsync(error);

        try
        {
            return args[1].ToLowerInvariant() switch
            {
                "inspect" => await RunInspectAsync(
                    args,
                    output,
                    error,
                    dependencies,
                    ct),
                "plan" => await RunPlanAsync(
                    args,
                    output,
                    error,
                    dependencies,
                    ct),
                "preview" => await RunPreviewAsync(
                    args,
                    output,
                    error,
                    dependencies,
                    ct),
                "ddl-check" => await RunDdlCheckAsync(
                    args,
                    output,
                    error,
                    dependencies,
                    ct),
                "type-map" => await RunTypeMapAsync(
                    args,
                    output,
                    error,
                    ct),
                "query-check" => await RunQueryCheckAsync(
                    args,
                    output,
                    error,
                    dependencies,
                    ct),
                "apply" => await RunApplyAsync(args, output, error, ct),
                "validate" => await RunValidateAsync(args, output, error, ct),
                "snapshot" => await RunSnapshotAsync(args, output, error, ct),
                "export" => await RunExportAsync(args, output, error, ct),
                _ => await UnsupportedVerbAsync(args[1], error),
            };
        }
        catch (OperationCanceledException) when (ct.IsCancellationRequested)
        {
            throw;
        }
        catch (Exception ex)
        {
            string message = ex switch
            {
                MigrationCliSafeException safe =>
                    $"{safe.Code}: {safe.Message}",
                CsvSnapshotPackageException packageError =>
                    $"{packageError.RuleId}: {packageError.Message}",
                CsvSourceSnapshotException snapshotError =>
                    $"{snapshotError.RuleId}: {snapshotError.Message}",
                JsonSnapshotPackageException packageError =>
                    $"{packageError.RuleId}: {packageError.Message}",
                JsonSourceSnapshotException snapshotError =>
                    $"{snapshotError.RuleId}: {snapshotError.Message}",
                JsonTableSchemaInferenceException schemaError =>
                    $"{schemaError.RuleId}: {schemaError.Message}",
                JsonTypedTableSchemaException typedSchemaError =>
                    $"{typedSchemaError.RuleId}: {typedSchemaError.Message}",
                JsonTypedIntentException intentError =>
                    $"{intentError.RuleId}: {intentError.Message}",
                JsonReadException readError =>
                    $"{readError.Diagnostic.RuleId}: {readError.Message}",
                _ => ex.Message,
            };
            await error.WriteLineAsync($"Error: {message}");
            return InspectorCommandRunner.ExitError;
        }
    }

    private static async ValueTask<int> RunSnapshotAsync(
        string[] args,
        TextWriter output,
        TextWriter error,
        CancellationToken ct)
    {
        if (args.Length < 3 ||
            args[2].StartsWith("--", StringComparison.Ordinal) ||
            string.IsNullOrWhiteSpace(args[2]))
        {
            return await OptionErrorAsync(
                "Missing source CSharpDB database path.",
                error);
        }
        if (!TryParseOptions(
                args,
                3,
                ["--offline", "--json"],
                out Dictionary<string, string> options,
                out string? parseError))
        {
            return await OptionErrorAsync(parseError!, error);
        }
        if (!RequireOnly(
                options,
                [
                    "--out",
                    "--offline",
                    "--workspace",
                    "--max-database-bytes",
                    "--max-wal-bytes",
                    "--max-snapshot-bytes",
                    "--json",
                ],
                out parseError))
        {
            return await OptionErrorAsync(parseError!, error);
        }
        if (!options.ContainsKey("--offline"))
        {
            return await OptionErrorAsync(
                "Snapshot capture is offline. Close every writer, then pass --offline to confirm the source is quiesced.",
                error);
        }
        if (!options.TryGetValue("--out", out string? destinationValue) ||
            string.IsNullOrWhiteSpace(destinationValue))
        {
            return await OptionErrorAsync(
                "Missing required option --out.",
                error);
        }

        long maxDatabaseBytes =
            RetainedDatabaseSnapshotOptions.DefaultMaxDatabaseBytes;
        if (options.TryGetValue(
                "--max-database-bytes",
                out string? maxDatabaseValue) &&
            !TryParsePositiveLong(maxDatabaseValue, out maxDatabaseBytes))
        {
            return await OptionErrorAsync(
                "The snapshot database-byte limit must be a positive 64-bit integer.",
                error);
        }

        long maxWalBytes =
            RetainedDatabaseSnapshotOptions.DefaultMaxWalBytes;
        if (options.TryGetValue(
                "--max-wal-bytes",
                out string? maxWalValue) &&
            !TryParsePositiveLong(maxWalValue, out maxWalBytes))
        {
            return await OptionErrorAsync(
                "The snapshot WAL-byte limit must be a positive 64-bit integer.",
                error);
        }

        long maxSnapshotBytes =
            RetainedDatabaseSnapshotOptions.DefaultMaxSnapshotBytes;
        if (options.TryGetValue(
                "--max-snapshot-bytes",
                out string? maxSnapshotValue) &&
            !TryParsePositiveLong(maxSnapshotValue, out maxSnapshotBytes))
        {
            return await OptionErrorAsync(
                "The retained snapshot-byte limit must be a positive 64-bit integer.",
                error);
        }

        string sourcePath = Path.GetFullPath(args[2]);
        string destinationPath = Path.GetFullPath(destinationValue);
        string? workspacePath = null;
        if (options.TryGetValue("--workspace", out string? workspaceValue))
        {
            if (string.IsNullOrWhiteSpace(workspaceValue))
            {
                return await OptionErrorAsync(
                    "The snapshot workspace path cannot be blank.",
                    error);
            }
            workspacePath = Path.GetFullPath(workspaceValue);
        }

        if (HasWindowsDosAliasSegment(sourcePath) ||
            HasWindowsDosAliasSegment(destinationPath) ||
            (workspacePath is not null &&
             HasWindowsDosAliasSegment(workspacePath)))
        {
            return await OptionErrorAsync(
                "Windows DOS short-name aliases cannot be used for snapshot capture paths.",
                error);
        }
        if (ContainsEquivalentResolvedPaths([sourcePath, destinationPath]))
        {
            return await OptionErrorAsync(
                "Source database and retained snapshot must use different files.",
                error);
        }

        RetainedDatabaseSnapshotReceipt receipt =
            await RetainedDatabaseSnapshot.CaptureAsync(
                sourcePath,
                destinationPath,
                databaseOptions: null,
                new RetainedDatabaseSnapshotOptions
                {
                    WorkspacePath = workspacePath,
                    MaxDatabaseBytes = maxDatabaseBytes,
                    MaxWalBytes = maxWalBytes,
                    MaxSnapshotBytes = maxSnapshotBytes,
                },
                ct)
                .ConfigureAwait(false);

        if (options.ContainsKey("--json"))
        {
            var report = new
            {
                Format = "csharpdb-migration-snapshot-result/v1",
                Status = "complete",
                SourcePath = sourcePath,
                receipt.SnapshotPath,
                receipt.SnapshotIdentity,
                receipt.ByteLength,
                receipt.Sha256,
                SourceState = "offline-confirmed",
                PublicationState = "published",
            };
            await output.WriteLineAsync(
                JsonSerializer.Serialize(report, JsonOptions));
        }
        else
        {
            await output.WriteLineAsync(
                $"Status: OK | sourceState=offline-confirmed | " +
                $"snapshot={receipt.SnapshotPath} | " +
                $"snapshotIdentity={receipt.SnapshotIdentity} | " +
                $"bytes={receipt.ByteLength} | digest={receipt.Sha256} | " +
                "publicationState=published");
        }

        return InspectorCommandRunner.ExitOk;
    }

    private static async ValueTask<int> RunExportAsync(
        string[] args,
        TextWriter output,
        TextWriter error,
        CancellationToken ct)
    {
        if (args.Length < 3 || args[2].StartsWith("--", StringComparison.Ordinal))
            return await OptionErrorAsync("Missing retained snapshot path.", error);
        if (!TryParseOptions(
                args,
                3,
                ["--json"],
                out Dictionary<string, string> options,
                out string? parseError))
        {
            return await OptionErrorAsync(parseError!, error);
        }
        if (!RequireOnly(
                options,
                [
                    "--format",
                    "--table",
                    "--out",
                    "--manifest",
                    "--expected-snapshot-identity",
                    "--profile",
                    "--max-data-bytes",
                    "--max-decoded-blob-bytes",
                    "--checkpoint-row-interval",
                    "--json",
                ],
                out parseError))
        {
            return await OptionErrorAsync(parseError!, error);
        }
        if (!options.TryGetValue("--format", out string? formatValue))
            return await OptionErrorAsync("Missing required option --format.", error);
        string exportFormat = formatValue.ToLowerInvariant();
        if (exportFormat is not ("csv" or "json" or "ndjson"))
            return await OptionErrorAsync($"Unsupported export format '{formatValue}'.", error);
        if (!options.TryGetValue("--table", out string? tableName))
            return await OptionErrorAsync("Missing required option --table.", error);
        if (!options.TryGetValue("--out", out string? outputValue))
            return await OptionErrorAsync("Missing required option --out.", error);
        if (!options.TryGetValue("--manifest", out string? manifestValue))
            return await OptionErrorAsync("Missing required option --manifest.", error);
        if (!options.TryGetValue(
                "--expected-snapshot-identity",
                out string? snapshotIdentityValue))
        {
            return await OptionErrorAsync(
                "Missing required option --expected-snapshot-identity.",
                error);
        }
        if (string.IsNullOrWhiteSpace(tableName) ||
            string.IsNullOrWhiteSpace(outputValue) ||
            string.IsNullOrWhiteSpace(manifestValue))
        {
            return await OptionErrorAsync(
                "Export table, data, and manifest values cannot be blank.",
                error);
        }
        if (!TryParseRetainedSnapshotIdentity(
                snapshotIdentityValue,
                out RetainedDatabaseSnapshotIdentity snapshotIdentity))
        {
            return await OptionErrorAsync(
                "The expected snapshot identity must use canonical " +
                "'csharpdb-retained-snapshot/v1:<positive-bytes>:sha256:<64 lowercase hex>' form.",
                error);
        }

        long maxDataBytes = 1L << 40;
        if (options.TryGetValue("--max-data-bytes", out string? maxDataValue) &&
            !TryParsePositiveLong(maxDataValue, out maxDataBytes))
        {
            return await OptionErrorAsync(
                "The export data-byte limit must be a positive 64-bit integer.",
                error);
        }

        string snapshotPath = Path.GetFullPath(args[2]);
        string destinationPath = Path.GetFullPath(outputValue);
        string manifestPath = Path.GetFullPath(manifestValue);
        if (HasWindowsDosAliasSegment(snapshotPath))
        {
            return await OptionErrorAsync(
                "Windows DOS short-name aliases cannot be used for retained export snapshots.",
                error);
        }
        if (ContainsEquivalentResolvedPaths(
                [snapshotPath, destinationPath, manifestPath]))
        {
            return await OptionErrorAsync(
                "Retained snapshot, data output, and manifest must use different files.",
                error);
        }
        if (!TryGetExportPublicationCapability(
                OperatingSystem.IsWindows(),
                out string capabilityDiagnostic))
        {
            await error.WriteLineAsync(
                $"Error: MIG-EXPORT-PLATFORM-001: {capabilityDiagnostic}");
            return InspectorCommandRunner.ExitError;
        }

        return exportFormat == "csv"
            ? await RunCsvExportAsync(
                options,
                snapshotPath,
                snapshotIdentity,
                tableName,
                destinationPath,
                manifestPath,
                maxDataBytes,
                output,
                error,
                ct)
            : await RunJsonExportAsync(
                options,
                exportFormat,
                snapshotPath,
                snapshotIdentity,
                tableName,
                destinationPath,
                manifestPath,
                maxDataBytes,
                output,
                error,
                ct);
    }

    internal static bool TryGetExportPublicationCapability(
        bool isWindows,
        out string diagnostic)
    {
        if (isWindows)
        {
            diagnostic = string.Empty;
            return true;
        }

        diagnostic =
            "CSV/JSON export publication currently requires Windows. " +
            "The qualified resumable publisher depends on handle-bound parent-directory validation " +
            "and atomic no-replace pair publication; an equivalent Unix substrate has not been qualified.";
        return false;
    }

    private static async ValueTask<int> RunCsvExportAsync(
        IReadOnlyDictionary<string, string> options,
        string snapshotPath,
        RetainedDatabaseSnapshotIdentity snapshotIdentity,
        string tableName,
        string destinationPath,
        string manifestPath,
        long maxDataBytes,
        TextWriter output,
        TextWriter error,
        CancellationToken ct)
    {
        CsvExportProfile profile = CsvExportProfile.LosslessV1;
        if (options.TryGetValue("--profile", out string? profileValue))
        {
            profile = profileValue.ToLowerInvariant() switch
            {
                "lossless-v1" => CsvExportProfile.LosslessV1,
                "spreadsheet-safe-lossy-v1" =>
                    CsvExportProfile.SpreadsheetSafeLossyV1,
                _ => (CsvExportProfile)(-1),
            };
            if (!Enum.IsDefined(profile))
            {
                return await OptionErrorAsync(
                    $"Unsupported CSV export profile '{profileValue}'.",
                    error);
            }
        }

        int maxDecodedBlobBytes =
            CsvExportContracts.MaximumSupportedDecodedBlobBytes;
        if (options.TryGetValue(
                "--max-decoded-blob-bytes",
                out string? maxBlobValue) &&
            (!TryParsePositiveInt(maxBlobValue, out maxDecodedBlobBytes) ||
             maxDecodedBlobBytes >
             CsvExportContracts.MaximumSupportedDecodedBlobBytes))
        {
            return await OptionErrorAsync(
                "The CSV export decoded BLOB limit must be a positive 32-bit integer " +
                $"no greater than {CsvExportContracts.MaximumSupportedDecodedBlobBytes}.",
                error);
        }

        long checkpointRowInterval = 10_000;
        if (options.TryGetValue(
                "--checkpoint-row-interval",
                out string? checkpointValue) &&
            !TryParsePositiveLong(checkpointValue, out checkpointRowInterval))
        {
            return await OptionErrorAsync(
                "The CSV export checkpoint row interval must be a positive 64-bit integer.",
                error);
        }

        if (SnapshotUsesReservedExportNamespace(
                snapshotPath,
                destinationPath,
                ".csharpdb-csv-export-"))
        {
            return await OptionErrorAsync(
                "The retained snapshot cannot occupy the CSV export's reserved private namespace.",
                error);
        }

        CsvExportPreparedOutputPublisher.ValidatePaths(
            destinationPath,
            manifestPath);
        var request = new CSharpDbRetainedCsvExportRequest
        {
            SnapshotPath = snapshotPath,
            SnapshotIdentity = snapshotIdentity,
            TableName = tableName,
            DestinationPath = destinationPath,
            Profile = profile,
            MaxDataBytes = maxDataBytes,
            MaximumDecodedBlobBytes = maxDecodedBlobBytes,
            CheckpointRowInterval = checkpointRowInterval,
        };
        CsvExportPublicationResult result =
            await new CSharpDbCsvExportAdapter()
                .WriteResumableAndPublishTableAsync(
                    request,
                    manifestPath,
                    ct)
                .ConfigureAwait(false);

        if (options.ContainsKey("--json"))
        {
            var report = new
            {
                Format = "csharpdb-migration-export-result/v1",
                Status = "complete",
                ExportFormat = "csv",
                SnapshotIdentity = snapshotIdentity.SnapshotIdentity,
                Table = result.Manifest.Table.Name,
                Profile = result.Manifest.Profile,
                DataPath = result.DestinationPath,
                ManifestPath = result.ManifestPath,
                result.ManifestDigest,
                RowCount = result.Manifest.Content.RowCount,
                DataByteLength = result.Manifest.Content.DataByteLength,
                DataDigest = result.Manifest.Content.DataDigest,
                result.ReusedData,
                result.ReusedManifest,
            };
            await output.WriteLineAsync(
                JsonSerializer.Serialize(report, JsonOptions));
        }
        else
        {
            await output.WriteLineAsync(
                $"Status: OK | format=csv | table={result.Manifest.Table.Name} | " +
                $"profile={FormatCsvExportProfile(result.Manifest.Profile)} | " +
                $"csv={result.DestinationPath} | manifest={result.ManifestPath} | " +
                $"manifestDigest={result.ManifestDigest} | " +
                $"dataDigest={result.Manifest.Content.DataDigest.Algorithm}:" +
                $"{result.Manifest.Content.DataDigest.Value} | " +
                $"rows={result.Manifest.Content.RowCount} | " +
                $"bytes={result.Manifest.Content.DataByteLength} | " +
                $"dataState={(result.ReusedData ? "reused" : "published")} | " +
                $"manifestState={(result.ReusedManifest ? "reused" : "published")}");
        }

        return InspectorCommandRunner.ExitOk;
    }

    private static async ValueTask<int> RunJsonExportAsync(
        IReadOnlyDictionary<string, string> options,
        string exportFormat,
        string snapshotPath,
        RetainedDatabaseSnapshotIdentity snapshotIdentity,
        string tableName,
        string destinationPath,
        string manifestPath,
        long maxDataBytes,
        TextWriter output,
        TextWriter error,
        CancellationToken ct)
    {
        long checkpointRowInterval = 10_000;
        if (options.TryGetValue(
                "--checkpoint-row-interval",
                out string? checkpointValue) &&
            !TryParsePositiveLong(
                checkpointValue,
                out checkpointRowInterval))
        {
            return await OptionErrorAsync(
                $"The {exportFormat.ToUpperInvariant()} export checkpoint row interval must be a positive 64-bit integer.",
                error);
        }
        if (options.TryGetValue(
                "--profile",
                out string? profileValue) &&
            !string.Equals(
                profileValue,
                "lossless-v1",
                StringComparison.OrdinalIgnoreCase))
        {
            return await OptionErrorAsync(
                $"Unsupported {exportFormat.ToUpperInvariant()} export profile '{profileValue}'.",
                error);
        }

        int maxDecodedBlobBytes =
            JsonExportContracts.MaximumSupportedDecodedBlobBytes;
        if (options.TryGetValue(
                "--max-decoded-blob-bytes",
                out string? maxBlobValue) &&
            (!TryParsePositiveInt(
                 maxBlobValue,
                 out maxDecodedBlobBytes) ||
             maxDecodedBlobBytes >
             JsonExportContracts
                 .MaximumSupportedDecodedBlobBytes))
        {
            return await OptionErrorAsync(
                $"The {exportFormat.ToUpperInvariant()} export decoded BLOB limit must be a positive 32-bit integer " +
                $"no greater than {JsonExportContracts.MaximumSupportedDecodedBlobBytes}.",
                error);
        }

        if (SnapshotUsesReservedExportNamespace(
                snapshotPath,
                destinationPath,
                ".csharpdb-json-export-"))
        {
            return await OptionErrorAsync(
                $"The retained snapshot cannot occupy the {exportFormat.ToUpperInvariant()} export's reserved private namespace.",
                error);
        }

        JsonExportPublisher
            .ValidatePreparedPublicationPaths(
                destinationPath,
                manifestPath);
        var request = new CSharpDbRetainedJsonExportRequest
        {
            SnapshotPath = snapshotPath,
            SnapshotIdentity = snapshotIdentity,
            TableName = tableName,
            DestinationPath = destinationPath,
            Profile = JsonExportProfile.LosslessV1,
            Framing = exportFormat == "json"
                ? JsonExportFraming.RootArray
                : JsonExportFraming.Ndjson,
            MaxDataBytes = maxDataBytes,
            MaximumDecodedBlobBytes = maxDecodedBlobBytes,
            CheckpointRowInterval = checkpointRowInterval,
        };
        JsonExportPublicationResult result =
            await new CSharpDbJsonExportAdapter()
                .WriteResumableAndPublishTableAsync(
                    request,
                    manifestPath,
                    ct)
                .ConfigureAwait(false);

        if (options.ContainsKey("--json"))
        {
            var report = new
            {
                Format =
                    "csharpdb-migration-export-result/v1",
                Status = "complete",
                ExportFormat = exportFormat,
                SnapshotIdentity =
                    snapshotIdentity.SnapshotIdentity,
                Table = result.Manifest.Table.Name,
                Profile = result.Manifest.Profile,
                DataPath = result.DestinationPath,
                ManifestPath = result.ManifestPath,
                result.ManifestDigest,
                RowCount =
                    result.Manifest.Content.RowCount,
                DataByteLength =
                    result.Manifest.Content.DataByteLength,
                DataDigest =
                    result.Manifest.Content.DataDigest,
                result.ReusedData,
                result.ReusedManifest,
            };
            await output.WriteLineAsync(
                JsonSerializer.Serialize(
                    report,
                    JsonOptions));
        }
        else
        {
            await output.WriteLineAsync(
                $"Status: OK | format={exportFormat} | " +
                $"table={result.Manifest.Table.Name} | " +
                "profile=lossless-v1 | " +
                $"data={result.DestinationPath} | " +
                $"manifest={result.ManifestPath} | " +
                $"manifestDigest={result.ManifestDigest} | " +
                $"dataDigest={result.Manifest.Content.DataDigest.Algorithm}:" +
                $"{result.Manifest.Content.DataDigest.Value} | " +
                $"rows={result.Manifest.Content.RowCount} | " +
                $"bytes={result.Manifest.Content.DataByteLength} | " +
                $"dataState={(result.ReusedData ? "reused" : "published")} | " +
                $"manifestState={(result.ReusedManifest ? "reused" : "published")}");
        }

        return InspectorCommandRunner.ExitOk;
    }

    private static async ValueTask<int> RunInspectAsync(
        string[] args,
        TextWriter output,
        TextWriter error,
        MigrationCommandDependencies dependencies,
        CancellationToken ct)
    {
        if (!TryParseOptions(
                args,
                2,
                ["--no-header", "--allow-ace12-fallback"],
                out Dictionary<string, string> options,
                out string? parseError))
        {
            return await OptionErrorAsync(
                SafeInspectOptionError(parseError!),
                error);
        }
        if (!options.TryGetValue("--source", out string? source))
            return await OptionErrorAsync("Missing required option --source.", error);
        if (!options.TryGetValue("--out", out string? outputValue))
            return await OptionErrorAsync("Missing required option --out.", error);

        if (string.Equals(source, "synthetic", StringComparison.OrdinalIgnoreCase))
        {
            if (!RequireOnly(options, ["--source", "--out"], out parseError))
                return await OptionErrorAsync(parseError!, error);
            return await RunSyntheticInspectAsync(outputValue, output, ct);
        }
        if (string.Equals(source, "csv", StringComparison.OrdinalIgnoreCase))
        {
            return await RunCsvInspectAsync(
                options,
                outputValue,
                output,
                error,
                ct);
        }
        if (string.Equals(source, "json", StringComparison.OrdinalIgnoreCase))
        {
            return await RunJsonInspectAsync(
                options,
                outputValue,
                output,
                error,
                ct);
        }
        if (string.Equals(source, "sqlite", StringComparison.OrdinalIgnoreCase))
        {
            return await RunSqliteInspectAsync(
                options,
                outputValue,
                output,
                error,
                ct);
        }
        if (string.Equals(source, "litedb", StringComparison.OrdinalIgnoreCase))
        {
            return await RunLiteDbInspectAsync(
                options,
                outputValue,
                output,
                error,
                ct);
        }
        if (string.Equals(source, "access", StringComparison.OrdinalIgnoreCase))
        {
            return await RunAccessInspectAsync(
                options,
                outputValue,
                output,
                error,
                dependencies,
                ct);
        }
        if (string.Equals(
                source,
                "sqlserver",
                StringComparison.OrdinalIgnoreCase))
        {
            return await RunSqlServerInspectAsync(
                options,
                outputValue,
                output,
                error,
                dependencies,
                ct);
        }
        if (string.Equals(
                source,
                "mysql",
                StringComparison.OrdinalIgnoreCase))
        {
            return await RunMySqlInspectAsync(
                options,
                outputValue,
                output,
                error,
                dependencies,
                ct);
        }

        return await OptionErrorAsync(
            "Unsupported migration source.",
            error);
    }

    private static async ValueTask<int> RunSqlServerInspectAsync(
        IReadOnlyDictionary<string, string> options,
        string outputValue,
        TextWriter output,
        TextWriter error,
        MigrationCommandDependencies dependencies,
        CancellationToken ct)
    {
        if (!RequireOnly(
                options,
                [
                    "--source",
                    "--connection-env",
                    "--package",
                    "--out",
                    "--max-source-bytes",
                    "--table-timeout-seconds",
                ],
                out string? parseError))
        {
            return await OptionErrorAsync(
                "The SQL Server inspect command contains an unsupported option.",
                error);
        }
        if (!options.TryGetValue(
                "--connection-env",
                out string? environmentVariableName))
        {
            return await OptionErrorAsync(
                "Missing required option --connection-env.",
                error);
        }
        if (!IsSafeEnvironmentVariableName(environmentVariableName))
        {
            return await OptionErrorAsync(
                "The SQL Server connection environment variable name is invalid.",
                error);
        }
        bool hasPackage = options.TryGetValue(
            "--package",
            out string? packageValue);
        if (!hasPackage &&
            options.ContainsKey("--max-source-bytes"))
        {
            return await OptionErrorAsync(
                "The SQL Server source byte limit requires --package.",
                error);
        }
        if (!hasPackage &&
            options.ContainsKey(
                "--table-timeout-seconds"))
        {
            return await OptionErrorAsync(
                "The SQL Server table timeout requires --package.",
                error);
        }
        if (hasPackage &&
            string.IsNullOrWhiteSpace(packageValue))
        {
            return await OptionErrorAsync(
                "The SQL Server retained package path cannot be blank.",
                error);
        }
        if (string.IsNullOrWhiteSpace(outputValue))
        {
            return await OptionErrorAsync(
                "The SQL Server catalog path cannot be blank.",
                error);
        }

        string outputPath;
        try
        {
            outputPath = Path.GetFullPath(outputValue);
        }
        catch (Exception pathError) when (
            pathError is ArgumentException or NotSupportedException or
                PathTooLongException)
        {
            throw new MigrationCliSafeException(
                "MIG-SQLSERVER-CLI-PATH-001",
                "The SQL Server catalog path is invalid.",
                pathError);
        }
        if (hasPackage)
        {
            return await RunSqlServerCaptureInspectAsync(
                options,
                environmentVariableName,
                packageValue!,
                outputPath,
                output,
                error,
                dependencies,
                ct);
        }
        if (File.Exists(outputPath) || Directory.Exists(outputPath))
        {
            return await OptionErrorAsync(
                "The SQL Server catalog destination already exists.",
                error);
        }

        try
        {
            ct.ThrowIfCancellationRequested();
            string targetCSharpDbVersion =
                CSharpDbCapabilityCatalogLoader.CurrentTargetVersion;
            SqlServerWorkerResult result =
                await dependencies.InspectSqlServerAsync(
                        environmentVariableName,
                        targetCSharpDbVersion,
                        ct)
                    .ConfigureAwait(false);
            ArgumentNullException.ThrowIfNull(result);

            if (result.Status is SqlServerWorkerStatus.Missing or
                SqlServerWorkerStatus.Incompatible)
            {
                throw new MigrationCliSafeException(
                    "MIG-SQLSERVER-CLI-ADAPTER-001",
                    "The optional SQL Server inspection adapter is unavailable or incompatible.",
                    new InvalidOperationException(
                        "The SQL Server worker boundary is unavailable."));
            }
            if (result.Status == SqlServerWorkerStatus.ConnectionUnavailable)
            {
                throw new MigrationCliSafeException(
                    "MIG-SQLSERVER-CLI-CONNECTION-001",
                    "The SQL Server connection could not be acquired by the optional adapter.",
                    new InvalidOperationException(
                        "SQL Server connection material was unavailable."));
            }
            if (result.Status == SqlServerWorkerStatus.InspectionFailed)
            {
                throw new MigrationCliSafeException(
                    "MIG-SQLSERVER-CLI-INSPECT-001",
                    "The SQL Server schema could not be inspected or published safely.",
                    new InvalidOperationException(
                        "The SQL Server worker could not inspect the source."));
            }
            if (result.Status != SqlServerWorkerStatus.Success ||
                result.Catalog is null ||
                result.Catalog.Source.Kind != MigrationSourceKind.SqlServer ||
                !string.Equals(
                    result.Catalog.TargetCSharpDbVersion,
                    targetCSharpDbVersion,
                    StringComparison.Ordinal))
            {
                throw new MigrationCliSafeException(
                    "MIG-SQLSERVER-CLI-ADAPTER-001",
                    "The optional SQL Server inspection adapter is unavailable or incompatible.",
                    new InvalidDataException(
                        "The SQL Server worker returned an invalid contract."));
            }

            MigrationCatalog catalog = result.Catalog;

            string serialized =
                MigrationArtifactSerializer.SerializeCatalog(catalog);
            await WriteNewArtifactAsync(outputPath, serialized, ct);

            int exitCode = catalog.Diagnostics.Count == 0
                ? InspectorCommandRunner.ExitOk
                : InspectorCommandRunner.ExitWarn;
            await output.WriteLineAsync(
                $"Status: {StatusLabel(exitCode)} | catalog={outputPath} | objects={catalog.Objects.Count} | diagnostics={catalog.Diagnostics.Count}");
            return exitCode;
        }
        catch (OperationCanceledException) when (ct.IsCancellationRequested)
        {
            throw;
        }
        catch (MigrationCliSafeException)
        {
            throw;
        }
        catch (Exception inspectionError) when (
            IsRecoverableCliException(inspectionError))
        {
            throw new MigrationCliSafeException(
                "MIG-SQLSERVER-CLI-INSPECT-001",
                "The SQL Server schema could not be inspected or published safely.",
                inspectionError);
        }
    }

    private static async ValueTask<int> RunSqlServerCaptureInspectAsync(
        IReadOnlyDictionary<string, string> options,
        string environmentVariableName,
        string packageValue,
        string outputPath,
        TextWriter output,
        TextWriter error,
        MigrationCommandDependencies dependencies,
        CancellationToken ct)
    {
        long maxSourceBytes =
            new RetainedMigrationPackageWriteOptions().MaxPackageBytes;
        if (options.TryGetValue(
                "--max-source-bytes",
                out string? maxSourceBytesValue) &&
            (!TryParseSourceByteLimit(
                 maxSourceBytesValue,
                 out maxSourceBytes) ||
             maxSourceBytes <= 0 ||
             maxSourceBytes >
                 SqlServerWorkerClient
                     .HardMaxCapturePackageBytes))
        {
            return await OptionErrorAsync(
                "The SQL Server source byte limit must be a positive 64-bit integer no larger than 256 GiB.",
                error);
        }
        int tableTimeoutSeconds =
            SqlServerWorkerClient
                .DefaultCaptureTableTimeoutSeconds;
        if (options.TryGetValue(
                "--table-timeout-seconds",
                out string? tableTimeoutValue) &&
            (!int.TryParse(
                 tableTimeoutValue,
                 System.Globalization
                     .NumberStyles.None,
                 System.Globalization
                     .CultureInfo.InvariantCulture,
                 out tableTimeoutSeconds) ||
             tableTimeoutSeconds <= 0 ||
             tableTimeoutSeconds >
                 SqlServerWorkerClient
                     .MaxCaptureTableTimeoutSeconds))
        {
            return await OptionErrorAsync(
                "The SQL Server table timeout must be an integer from 1 through 86400 seconds.",
                error);
        }

        string packagePath;
        try
        {
            packagePath = Path.GetFullPath(packageValue);
        }
        catch (Exception pathError) when (
            pathError is ArgumentException or NotSupportedException or
                PathTooLongException)
        {
            throw new MigrationCliSafeException(
                "MIG-SQLSERVER-CLI-PATH-001",
                "The SQL Server retained package path is invalid.",
                pathError);
        }

        bool pathsCollide;
        try
        {
            pathsCollide = ContainsEquivalentResolvedPaths(
                [packagePath, outputPath]);
        }
        catch (Exception pathError) when (
            pathError is IOException or UnauthorizedAccessException or
                ArgumentException or NotSupportedException)
        {
            throw new MigrationCliSafeException(
                "MIG-SQLSERVER-CLI-PATH-001",
                "The SQL Server migration paths could not be verified safely.",
                pathError);
        }
        if (pathsCollide)
        {
            return await OptionErrorAsync(
                "The SQL Server retained package and catalog output must use different files.",
                error);
        }
        if (File.Exists(packagePath) ||
            Directory.Exists(packagePath))
        {
            return await OptionErrorAsync(
                "The SQL Server retained package destination already exists.",
                error);
        }
        if (File.Exists(outputPath) ||
            Directory.Exists(outputPath))
        {
            return await OptionErrorAsync(
                "The SQL Server catalog destination already exists.",
                error);
        }

        string? packageParent = Path.GetDirectoryName(packagePath);
        if (string.IsNullOrEmpty(packageParent) ||
            !Directory.Exists(packageParent))
        {
            return await OptionErrorAsync(
                "The SQL Server retained package parent must be an existing caller-controlled directory.",
                error);
        }
        try
        {
            FileAttributes parentAttributes =
                File.GetAttributes(packageParent);
            if ((parentAttributes &
                (FileAttributes.ReparsePoint |
                 FileAttributes.Device)) != 0)
            {
                return await OptionErrorAsync(
                    "The SQL Server retained package parent cannot be a link, reparse point, or device.",
                    error);
            }
        }
        catch (Exception pathError) when (
            pathError is IOException or UnauthorizedAccessException or
                ArgumentException or NotSupportedException)
        {
            throw new MigrationCliSafeException(
                "MIG-SQLSERVER-CLI-PATH-001",
                "The SQL Server retained package parent could not be verified safely.",
                pathError);
        }

        string? catalogParent = Path.GetDirectoryName(outputPath);
        if (string.IsNullOrEmpty(catalogParent) ||
            !Directory.Exists(catalogParent))
        {
            return await OptionErrorAsync(
                "The SQL Server catalog parent must be an existing caller-controlled directory.",
                error);
        }

        RetainedCaptureDirectoryLease packageParentLease;
        RetainedCaptureDirectoryLease catalogParentLease;
        try
        {
            packageParentLease =
                RetainedCaptureDirectoryLease.Open(packageParent);
            try
            {
                catalogParentLease =
                    RetainedCaptureDirectoryLease.Open(catalogParent);
            }
            catch
            {
                packageParentLease.Dispose();
                throw;
            }
        }
        catch (Exception pathError) when (
            pathError is IOException or
                UnauthorizedAccessException or
                ArgumentException or
                NotSupportedException)
        {
            throw new MigrationCliSafeException(
                "MIG-SQLSERVER-CLI-PATH-001",
                "The SQL Server retained capture output directories are not safe caller-controlled local directories.",
                pathError);
        }

        using (packageParentLease)
        using (catalogParentLease)
        {
            packageParentLease.AssertUnchanged();
            catalogParentLease.AssertUnchanged();
            if (File.Exists(packagePath) ||
                Directory.Exists(packagePath) ||
                File.Exists(outputPath) ||
                Directory.Exists(outputPath))
            {
                return await OptionErrorAsync(
                    "A SQL Server retained capture destination appeared while its output directories were being secured.",
                    error);
            }

            bool packagePublished = false;
            bool catalogPublished = false;
            SqlServerCaptureWorkspace? workspace = null;
            try
            {
                try
                {
                    workspace =
                        SqlServerCaptureWorkspace.Create(
                            packageParent,
                            packageParentLease);
                    packageParentLease.AssertUnchanged();
                    catalogParentLease.AssertUnchanged();
                    string targetCSharpDbVersion =
                        CSharpDbCapabilityCatalogLoader.CurrentTargetVersion;
                    SqlServerCaptureWorkerResult workerResult =
                        await dependencies.CaptureSqlServerAsync(
                                environmentVariableName,
                                targetCSharpDbVersion,
                                workspace.CapturePath,
                                maxSourceBytes,
                                tableTimeoutSeconds,
                                ct)
                            .ConfigureAwait(false);
                    ArgumentNullException.ThrowIfNull(workerResult);
                    packageParentLease.AssertUnchanged();
                    workspace.AssertUnchanged();

                    switch (workerResult.Status)
                    {
                        case SqlServerCaptureWorkerStatus.Missing:
                        case SqlServerCaptureWorkerStatus.Incompatible:
                            throw new MigrationCliSafeException(
                                "MIG-SQLSERVER-CLI-ADAPTER-001",
                                "The optional SQL Server capture adapter is unavailable or incompatible.",
                                new InvalidOperationException(
                                    "The SQL Server capture worker boundary is unavailable."));
                        case SqlServerCaptureWorkerStatus.ConnectionUnavailable:
                            throw new MigrationCliSafeException(
                                "MIG-SQLSERVER-CLI-CONNECTION-001",
                                "The SQL Server connection could not be acquired by the optional adapter.",
                                new InvalidOperationException(
                                    "SQL Server connection material was unavailable."));
                        case SqlServerCaptureWorkerStatus.LimitExceeded:
                            throw new MigrationCliSafeException(
                                "MIG-SQLSERVER-CLI-CAPTURE-LIMIT-001",
                                "The SQL Server retained capture exceeded a configured safety limit.",
                                new InvalidDataException(
                                    "The SQL Server capture worker crossed a retained-source limit."));
                        case SqlServerCaptureWorkerStatus.CaptureFailed:
                            throw new MigrationCliSafeException(
                                "MIG-SQLSERVER-CLI-CAPTURE-001",
                                "The SQL Server rows could not be captured safely.",
                                new InvalidOperationException(
                                    "The SQL Server capture worker could not retain the source."));
                    }
                    if (workerResult.Status !=
                            SqlServerCaptureWorkerStatus.Success ||
                        workerResult.Receipt is null)
                    {
                        throw new MigrationCliSafeException(
                            "MIG-SQLSERVER-CLI-ADAPTER-001",
                            "The optional SQL Server capture adapter is unavailable or incompatible.",
                            new InvalidDataException(
                                "The SQL Server capture worker returned an invalid contract."));
                    }

                    SqlServerCaptureReceipt receipt =
                        workerResult.Receipt;
                    MigrationCatalog catalog;
                    await using (
                        RetainedMigrationPackageSession session =
                            await RetainedMigrationPackageSession
                                .OpenAsync(
                                    workspace.CapturePath,
                                    new RetainedMigrationPackageOpenOptions
                                    {
                                        ExpectedPackageDigest =
                                            receipt.PackageDigest,
                                        WorkspacePath =
                                            workspace
                                                .VerificationWorkspacePath,
                                        MaxPackageBytes =
                                            maxSourceBytes,
                                    },
                                    ct)
                                .ConfigureAwait(false))
                    {
                        ValidateSqlServerCaptureSession(
                            session,
                            receipt,
                            targetCSharpDbVersion);
                        catalog = session.Catalog;
                    }

                    ct.ThrowIfCancellationRequested();
                    packageParentLease.AssertUnchanged();
                    workspace.AssertUnchanged();
                    File.Move(
                        workspace.CapturePath,
                        packagePath,
                        overwrite: false);
                    packagePublished = true;
                    packageParentLease.AssertUnchanged();

                    workspace.Dispose();
                    workspace = null;
                    packageParentLease.AssertUnchanged();

                    catalogParentLease.AssertUnchanged();
                    await WriteNewArtifactAsync(
                        outputPath,
                        MigrationArtifactSerializer.SerializeCatalog(
                            catalog),
                        ct);
                    catalogPublished = true;
                    catalogParentLease.AssertUnchanged();

                    int exitCode = catalog.Diagnostics.Count == 0
                        ? InspectorCommandRunner.ExitOk
                        : InspectorCommandRunner.ExitWarn;
                    await output.WriteLineAsync(
                        $"Status: {StatusLabel(exitCode)} | catalog={outputPath} | package={packagePath} | manifestDigest={receipt.PackageDigest} | tables={receipt.TableCount} | rows={receipt.RowCount} | objects={catalog.Objects.Count} | diagnostics={catalog.Diagnostics.Count}");
                    return exitCode;
                }
                catch (Exception operationFailure)
                {
                    if (workspace is null)
                        throw;

                    try
                    {
                        workspace.Dispose();
                        workspace = null;
                    }
                    catch (
                        RetainedCaptureWorkspaceCleanupException
                            cleanupFailure)
                    {
                        throw new RetainedCaptureWorkspaceCleanupException(
                            operationFailure,
                            cleanupFailure);
                    }

                    throw;
                }
            }
            catch (
                RetainedCaptureWorkspaceCleanupException
                    cleanupFailure)
            {
                string message = packagePublished
                    ? "The SQL Server retained package was published, but private capture workspace cleanup failed; the package was preserved and the catalog was not published."
                    : "SQL Server capture failed and its private workspace could not be cleaned safely; no final artifacts were published.";
                throw new MigrationCliSafeException(
                    "MIG-SQLSERVER-CLI-CLEANUP-001",
                    message,
                    cleanupFailure);
            }
            catch (Exception operationFailure) when (
                packagePublished &&
                !catalogPublished)
            {
                throw new MigrationCliSafeException(
                    "MIG-SQLSERVER-CLI-CATALOG-001",
                    "SQL Server catalog publication failed after the retained package was published; the package was preserved.",
                    operationFailure);
            }
            catch (MigrationCliSafeException)
            {
                throw;
            }
            catch (OperationCanceledException) when (
                ct.IsCancellationRequested)
            {
                throw;
            }
            catch (RetainedMigrationPackageException packageFailure)
            {
                throw new MigrationCliSafeException(
                    "MIG-SQLSERVER-CLI-PACKAGE-001",
                    "The SQL Server retained package could not be verified safely.",
                    packageFailure);
            }
            catch (Exception captureError) when (
                IsRecoverableCliException(captureError))
            {
                throw new MigrationCliSafeException(
                    "MIG-SQLSERVER-CLI-CAPTURE-001",
                    "The SQL Server rows could not be captured or published safely.",
                    captureError);
            }
        }
    }

    private static async ValueTask<int> RunMySqlInspectAsync(
        IReadOnlyDictionary<string, string> options,
        string outputValue,
        TextWriter output,
        TextWriter error,
        MigrationCommandDependencies dependencies,
        CancellationToken ct)
    {
        if (!RequireOnly(
                options,
                [
                    "--source",
                    "--connection-env",
                    "--package",
                    "--out",
                    "--max-source-bytes",
                    "--table-timeout-seconds",
                ],
                out string? parseError))
        {
            return await OptionErrorAsync(
                "The MySQL inspect command contains an unsupported option.",
                error);
        }
        if (!options.TryGetValue(
                "--connection-env",
                out string? environmentVariableName))
        {
            return await OptionErrorAsync(
                "Missing required option --connection-env.",
                error);
        }
        if (!IsSafeEnvironmentVariableName(environmentVariableName))
        {
            return await OptionErrorAsync(
                "The MySQL connection environment variable name is invalid.",
                error);
        }
        bool hasPackage = options.TryGetValue(
            "--package",
            out string? packageValue);
        if (!hasPackage &&
            options.ContainsKey("--max-source-bytes"))
        {
            return await OptionErrorAsync(
                "The MySQL source byte limit requires --package.",
                error);
        }
        if (!hasPackage &&
            options.ContainsKey(
                "--table-timeout-seconds"))
        {
            return await OptionErrorAsync(
                "The MySQL table timeout requires --package.",
                error);
        }
        if (hasPackage &&
            string.IsNullOrWhiteSpace(packageValue))
        {
            return await OptionErrorAsync(
                "The MySQL retained package path cannot be blank.",
                error);
        }
        if (string.IsNullOrWhiteSpace(outputValue))
        {
            return await OptionErrorAsync(
                "The MySQL catalog path cannot be blank.",
                error);
        }

        string outputPath;
        try
        {
            outputPath = Path.GetFullPath(outputValue);
        }
        catch (Exception pathError) when (
            pathError is ArgumentException or NotSupportedException or
                PathTooLongException)
        {
            throw new MigrationCliSafeException(
                "MIG-MYSQL-CLI-PATH-001",
                "The MySQL catalog path is invalid.",
                pathError);
        }
        if (hasPackage)
        {
            return await RunMySqlCaptureInspectAsync(
                options,
                environmentVariableName,
                packageValue!,
                outputPath,
                output,
                error,
                dependencies,
                ct);
        }
        if (File.Exists(outputPath) || Directory.Exists(outputPath))
        {
            return await OptionErrorAsync(
                "The MySQL catalog destination already exists.",
                error);
        }

        try
        {
            ct.ThrowIfCancellationRequested();
            string targetCSharpDbVersion =
                CSharpDbCapabilityCatalogLoader.CurrentTargetVersion;
            MySqlWorkerResult result =
                await dependencies.InspectMySqlAsync(
                        environmentVariableName,
                        targetCSharpDbVersion,
                        ct)
                    .ConfigureAwait(false);
            ArgumentNullException.ThrowIfNull(result);

            if (result.Status is MySqlWorkerStatus.Missing or
                MySqlWorkerStatus.Incompatible)
            {
                throw new MigrationCliSafeException(
                    "MIG-MYSQL-CLI-ADAPTER-001",
                    "The optional MySQL inspection adapter is unavailable or incompatible.",
                    new InvalidOperationException(
                        "The MySQL worker boundary is unavailable."));
            }
            if (result.Status == MySqlWorkerStatus.ConnectionUnavailable)
            {
                throw new MigrationCliSafeException(
                    "MIG-MYSQL-CLI-CONNECTION-001",
                    "The MySQL connection could not be acquired by the optional adapter.",
                    new InvalidOperationException(
                        "MySQL connection material was unavailable."));
            }
            if (result.Status == MySqlWorkerStatus.InspectionFailed)
            {
                throw new MigrationCliSafeException(
                    "MIG-MYSQL-CLI-INSPECT-001",
                    "The MySQL schema could not be inspected or published safely.",
                    new InvalidOperationException(
                        "The MySQL worker could not inspect the source."));
            }
            if (result.Status != MySqlWorkerStatus.Success ||
                result.Catalog is null ||
                result.Catalog.Source.Kind != MigrationSourceKind.MySql ||
                !string.Equals(
                    result.Catalog.TargetCSharpDbVersion,
                    targetCSharpDbVersion,
                    StringComparison.Ordinal))
            {
                throw new MigrationCliSafeException(
                    "MIG-MYSQL-CLI-ADAPTER-001",
                    "The optional MySQL inspection adapter is unavailable or incompatible.",
                    new InvalidDataException(
                        "The MySQL worker returned an invalid contract."));
            }

            MigrationCatalog catalog = result.Catalog;

            string serialized =
                MigrationArtifactSerializer.SerializeCatalog(catalog);
            await WriteNewArtifactAsync(outputPath, serialized, ct);

            int exitCode = catalog.Diagnostics.Count == 0
                ? InspectorCommandRunner.ExitOk
                : InspectorCommandRunner.ExitWarn;
            await output.WriteLineAsync(
                $"Status: {StatusLabel(exitCode)} | catalog={outputPath} | objects={catalog.Objects.Count} | diagnostics={catalog.Diagnostics.Count}");
            return exitCode;
        }
        catch (OperationCanceledException) when (ct.IsCancellationRequested)
        {
            throw;
        }
        catch (MigrationCliSafeException)
        {
            throw;
        }
        catch (Exception inspectionError) when (
            IsRecoverableCliException(inspectionError))
        {
            throw new MigrationCliSafeException(
                "MIG-MYSQL-CLI-INSPECT-001",
                "The MySQL schema could not be inspected or published safely.",
                inspectionError);
        }
    }

    private static async ValueTask<int>
        RunMySqlCaptureInspectAsync(
        IReadOnlyDictionary<string, string> options,
        string environmentVariableName,
        string packageValue,
        string outputPath,
        TextWriter output,
        TextWriter error,
        MigrationCommandDependencies dependencies,
        CancellationToken ct)
    {
        long maxSourceBytes =
            new RetainedMigrationPackageWriteOptions()
                .MaxPackageBytes;
        if (options.TryGetValue(
                "--max-source-bytes",
                out string? maxSourceBytesValue) &&
            (!TryParseSourceByteLimit(
                 maxSourceBytesValue,
                 out maxSourceBytes) ||
             maxSourceBytes <= 0 ||
             maxSourceBytes >
                 MySqlWorkerClient
                     .HardMaxCapturePackageBytes))
        {
            return await OptionErrorAsync(
                "The MySQL source byte limit must be a positive 64-bit integer no larger than 256 GiB.",
                error);
        }
        int tableTimeoutSeconds =
            MySqlWorkerClient
                .DefaultCaptureTableTimeoutSeconds;
        if (options.TryGetValue(
                "--table-timeout-seconds",
                out string? tableTimeoutValue) &&
            (!int.TryParse(
                 tableTimeoutValue,
                 NumberStyles.None,
                 CultureInfo.InvariantCulture,
                 out tableTimeoutSeconds) ||
             tableTimeoutSeconds <= 0 ||
             tableTimeoutSeconds >
                 MySqlWorkerClient
                     .MaxCaptureTableTimeoutSeconds))
        {
            return await OptionErrorAsync(
                "The MySQL table timeout must be an integer from 1 through 86400 seconds.",
                error);
        }

        string packagePath;
        try
        {
            packagePath =
                Path.GetFullPath(packageValue);
        }
        catch (Exception pathError) when (
            pathError is ArgumentException or
                NotSupportedException or
                PathTooLongException)
        {
            throw new MigrationCliSafeException(
                "MIG-MYSQL-CLI-PATH-001",
                "The MySQL retained package path is invalid.",
                pathError);
        }

        bool pathsCollide;
        try
        {
            pathsCollide =
                ContainsEquivalentResolvedPaths(
                    [packagePath, outputPath]);
        }
        catch (Exception pathError) when (
            pathError is IOException or
                UnauthorizedAccessException or
                ArgumentException or
                NotSupportedException)
        {
            throw new MigrationCliSafeException(
                "MIG-MYSQL-CLI-PATH-001",
                "The MySQL migration paths could not be verified safely.",
                pathError);
        }
        if (pathsCollide)
        {
            return await OptionErrorAsync(
                "The MySQL retained package and catalog output must use different files.",
                error);
        }
        if (File.Exists(packagePath) ||
            Directory.Exists(packagePath))
        {
            return await OptionErrorAsync(
                "The MySQL retained package destination already exists.",
                error);
        }
        if (File.Exists(outputPath) ||
            Directory.Exists(outputPath))
        {
            return await OptionErrorAsync(
                "The MySQL catalog destination already exists.",
                error);
        }

        string? packageParent =
            Path.GetDirectoryName(packagePath);
        if (string.IsNullOrEmpty(packageParent) ||
            !Directory.Exists(packageParent))
        {
            return await OptionErrorAsync(
                "The MySQL retained package parent must be an existing caller-controlled directory.",
                error);
        }
        try
        {
            FileAttributes parentAttributes =
                File.GetAttributes(packageParent);
            if ((parentAttributes &
                (FileAttributes.ReparsePoint |
                 FileAttributes.Device)) != 0)
            {
                return await OptionErrorAsync(
                    "The MySQL retained package parent cannot be a link, reparse point, or device.",
                    error);
            }
        }
        catch (Exception pathError) when (
            pathError is IOException or
                UnauthorizedAccessException or
                ArgumentException or
                NotSupportedException)
        {
            throw new MigrationCliSafeException(
                "MIG-MYSQL-CLI-PATH-001",
                "The MySQL retained package parent could not be verified safely.",
                pathError);
        }

        string? catalogParent =
            Path.GetDirectoryName(outputPath);
        if (string.IsNullOrEmpty(catalogParent) ||
            !Directory.Exists(catalogParent))
        {
            return await OptionErrorAsync(
                "The MySQL catalog parent must be an existing caller-controlled directory.",
                error);
        }

        RetainedCaptureDirectoryLease packageParentLease;
        RetainedCaptureDirectoryLease catalogParentLease;
        try
        {
            packageParentLease =
                RetainedCaptureDirectoryLease.Open(packageParent);
            try
            {
                catalogParentLease =
                    RetainedCaptureDirectoryLease.Open(catalogParent);
            }
            catch
            {
                packageParentLease.Dispose();
                throw;
            }
        }
        catch (Exception pathError) when (
            pathError is IOException or
                UnauthorizedAccessException or
                ArgumentException or
                NotSupportedException)
        {
            throw new MigrationCliSafeException(
                "MIG-MYSQL-CLI-PATH-001",
                "The MySQL retained capture output directories are not safe caller-controlled local directories.",
                pathError);
        }

        using (packageParentLease)
        using (catalogParentLease)
        {
            packageParentLease.AssertUnchanged();
            catalogParentLease.AssertUnchanged();
            if (File.Exists(packagePath) ||
                Directory.Exists(packagePath) ||
                File.Exists(outputPath) ||
                Directory.Exists(outputPath))
            {
                return await OptionErrorAsync(
                    "A MySQL retained capture destination appeared while its output directories were being secured.",
                    error);
            }

            bool packagePublished = false;
            bool catalogPublished = false;
            SqlServerCaptureWorkspace? workspace =
                null;
            try
            {
                try
                {
                    workspace =
                        SqlServerCaptureWorkspace.Create(
                            packageParent,
                            MySqlWorkerClient
                                .CaptureWorkspacePrefix,
                            MySqlWorkerClient
                                .CaptureOutputFileName,
                            packageParentLease);
                    packageParentLease.AssertUnchanged();
                    catalogParentLease.AssertUnchanged();
                    string targetCSharpDbVersion =
                        CSharpDbCapabilityCatalogLoader
                            .CurrentTargetVersion;
                    MySqlCaptureWorkerResult workerResult =
                        await dependencies.CaptureMySqlAsync(
                                environmentVariableName,
                                targetCSharpDbVersion,
                                workspace.CapturePath,
                                maxSourceBytes,
                                tableTimeoutSeconds,
                                ct)
                            .ConfigureAwait(false);
                    ArgumentNullException.ThrowIfNull(
                        workerResult);
                    packageParentLease.AssertUnchanged();
                    workspace.AssertUnchanged();

                    switch (workerResult.Status)
                    {
                        case MySqlCaptureWorkerStatus
                            .Missing:
                        case MySqlCaptureWorkerStatus
                            .Incompatible:
                            throw new MigrationCliSafeException(
                                "MIG-MYSQL-CLI-ADAPTER-001",
                                "The optional MySQL capture adapter is unavailable or incompatible.",
                                new InvalidOperationException(
                                    "The MySQL capture worker boundary is unavailable."));
                        case MySqlCaptureWorkerStatus
                            .ConnectionUnavailable:
                            throw new MigrationCliSafeException(
                                "MIG-MYSQL-CLI-CONNECTION-001",
                                "The MySQL connection could not be acquired by the optional adapter.",
                                new InvalidOperationException(
                                    "MySQL connection material was unavailable."));
                        case MySqlCaptureWorkerStatus
                            .LimitExceeded:
                            throw new MigrationCliSafeException(
                                "MIG-MYSQL-CLI-CAPTURE-LIMIT-001",
                                "The MySQL retained capture exceeded a configured safety limit.",
                                new InvalidDataException(
                                    "The MySQL capture worker crossed a retained-source limit."));
                        case MySqlCaptureWorkerStatus
                            .CaptureFailed:
                            throw new MigrationCliSafeException(
                                "MIG-MYSQL-CLI-CAPTURE-001",
                                "The MySQL rows could not be captured safely.",
                                new InvalidOperationException(
                                    "The MySQL capture worker could not retain the source."));
                    }
                    if (workerResult.Status !=
                            MySqlCaptureWorkerStatus
                                .Success ||
                        workerResult.Receipt is null)
                    {
                        throw new MigrationCliSafeException(
                            "MIG-MYSQL-CLI-ADAPTER-001",
                            "The optional MySQL capture adapter is unavailable or incompatible.",
                            new InvalidDataException(
                                "The MySQL capture worker returned an invalid contract."));
                    }

                    MySqlCaptureReceipt receipt =
                        workerResult.Receipt;
                    long capturedPackageBytes =
                        new FileInfo(
                            workspace.CapturePath)
                            .Length;
                    MigrationCatalog catalog;
                    await using (
                        RetainedMigrationPackageSession session =
                            await RetainedMigrationPackageSession
                                .OpenAsync(
                                    workspace.CapturePath,
                                    new RetainedMigrationPackageOpenOptions
                                    {
                                        ExpectedPackageDigest =
                                            receipt
                                                .PackageDigest,
                                        WorkspacePath =
                                            workspace
                                                .VerificationWorkspacePath,
                                        MaxPackageBytes =
                                            maxSourceBytes,
                                    },
                                    ct)
                                .ConfigureAwait(false))
                    {
                        ValidateMySqlCaptureSession(
                            session,
                            receipt,
                            capturedPackageBytes,
                            targetCSharpDbVersion);
                        catalog = session.Catalog;
                    }

                    ct.ThrowIfCancellationRequested();
                    packageParentLease.AssertUnchanged();
                    workspace.AssertUnchanged();
                    File.Move(
                        workspace.CapturePath,
                        packagePath,
                        overwrite: false);
                    packagePublished = true;
                    packageParentLease.AssertUnchanged();

                    workspace.Dispose();
                    workspace = null;
                    packageParentLease.AssertUnchanged();

                    catalogParentLease.AssertUnchanged();
                    await WriteNewArtifactAsync(
                        outputPath,
                        MigrationArtifactSerializer
                            .SerializeCatalog(catalog),
                        ct);
                    catalogPublished = true;
                    catalogParentLease.AssertUnchanged();

                    int exitCode =
                        catalog.Diagnostics.Count == 0
                            ? InspectorCommandRunner.ExitOk
                            : InspectorCommandRunner.ExitWarn;
                    await output.WriteLineAsync(
                        $"Status: {StatusLabel(exitCode)} | catalog={outputPath} | package={packagePath} | manifestDigest={receipt.PackageDigest} | tables={receipt.TableCount} | rows={receipt.RowCount} | objects={catalog.Objects.Count} | diagnostics={catalog.Diagnostics.Count}");
                    return exitCode;
                }
                catch (Exception operationFailure)
                {
                    if (workspace is null)
                        throw;

                    try
                    {
                        workspace.Dispose();
                        workspace = null;
                    }
                    catch (
                        RetainedCaptureWorkspaceCleanupException
                            cleanupFailure)
                    {
                        throw new RetainedCaptureWorkspaceCleanupException(
                            operationFailure,
                            cleanupFailure);
                    }

                    throw;
                }
            }
            catch (
                RetainedCaptureWorkspaceCleanupException
                    cleanupFailure)
            {
                string message = packagePublished
                    ? "The MySQL retained package was published, but private capture workspace cleanup failed; the package was preserved and the catalog was not published."
                    : "MySQL capture failed and its private workspace could not be cleaned safely; no final artifacts were published.";
                throw new MigrationCliSafeException(
                    "MIG-MYSQL-CLI-CLEANUP-001",
                    message,
                    cleanupFailure);
            }
            catch (Exception operationFailure) when (
                packagePublished &&
                !catalogPublished)
            {
                throw new MigrationCliSafeException(
                    "MIG-MYSQL-CLI-CATALOG-001",
                    "MySQL catalog publication failed after the retained package was published; the package was preserved.",
                    operationFailure);
            }
            catch (MigrationCliSafeException)
            {
                throw;
            }
            catch (OperationCanceledException) when (
                ct.IsCancellationRequested)
            {
                throw;
            }
            catch (
                RetainedMigrationPackageException
                    packageFailure)
            {
                throw new MigrationCliSafeException(
                    "MIG-MYSQL-CLI-PACKAGE-001",
                    "The MySQL retained package could not be verified safely.",
                    packageFailure);
            }
            catch (Exception captureError) when (
                IsRecoverableCliException(captureError))
            {
                throw new MigrationCliSafeException(
                    "MIG-MYSQL-CLI-CAPTURE-001",
                    "The MySQL rows could not be captured or published safely.",
                    captureError);
            }
        }
    }

    private static bool IsSafeEnvironmentVariableName(string? value)
    {
        if (value is not { Length: > 0 and <= 128 } ||
            value[0] is not (>= 'A' and <= 'Z') and
                not (>= 'a' and <= 'z') and
                not '_')
        {
            return false;
        }

        foreach (char character in value.AsSpan(1))
        {
            if (character is not (>= 'A' and <= 'Z') and
                not (>= 'a' and <= 'z') and
                not (>= '0' and <= '9') and
                not '_')
            {
                return false;
            }
        }

        return true;
    }

    private static string SafeInspectOptionError(string error)
    {
        if (error.StartsWith(
                "Duplicate option",
                StringComparison.Ordinal))
        {
            return "Duplicate option in the migration inspection command.";
        }
        if (error.StartsWith(
                "Missing value for",
                StringComparison.Ordinal))
        {
            return "An inspection option is missing its value.";
        }
        if (error.StartsWith(
                "Unexpected positional argument",
                StringComparison.Ordinal))
        {
            return "The migration inspection command contains an unexpected positional argument.";
        }

        return "The migration inspection options are invalid.";
    }

    private static bool IsRecoverableCliException(Exception exception) =>
        exception is not OutOfMemoryException and
        not StackOverflowException and
        not AccessViolationException;

    private static async ValueTask<int> RunSyntheticInspectAsync(
        string outputValue,
        TextWriter output,
        CancellationToken ct)
    {
        string outputPath = Path.GetFullPath(outputValue);
        MigrationCatalog catalog = await new SyntheticMigrationSourceInspector().InspectAsync(
            new MigrationInspectionRequest
            {
                TargetCSharpDbVersion = CSharpDbCapabilityCatalogLoader.CurrentTargetVersion,
                IncludeProfile = true,
            },
            ct);
        await WriteArtifactAsync(
            outputPath,
            MigrationArtifactSerializer.SerializeCatalog(catalog),
            ct);

        int exitCode = catalog.Diagnostics.Count == 0
            ? InspectorCommandRunner.ExitOk
            : InspectorCommandRunner.ExitWarn;
        await output.WriteLineAsync(
            $"Status: {StatusLabel(exitCode)} | catalog={outputPath} | objects={catalog.Objects.Count} | diagnostics={catalog.Diagnostics.Count}");
        return exitCode;
    }

    private static async ValueTask<int> RunSqliteInspectAsync(
        IReadOnlyDictionary<string, string> options,
        string outputValue,
        TextWriter output,
        TextWriter error,
        CancellationToken ct)
    {
        if (!RequireOnly(
                options,
                [
                    "--source",
                    "--input",
                    "--package",
                    "--out",
                    "--profile-sample-size",
                    "--max-source-bytes",
                ],
                out string? parseError))
        {
            return await OptionErrorAsync(parseError!, error);
        }
        if (!options.TryGetValue("--input", out string? inputValue))
            return await OptionErrorAsync("Missing required option --input.", error);
        if (!options.TryGetValue("--package", out string? packageValue))
            return await OptionErrorAsync("Missing required option --package.", error);
        if (string.IsNullOrWhiteSpace(inputValue) ||
            string.IsNullOrWhiteSpace(packageValue) ||
            string.IsNullOrWhiteSpace(outputValue))
        {
            return await OptionErrorAsync(
                "SQLite input, retained package, and catalog paths cannot be blank.",
                error);
        }

        var inspectionRequest = new MigrationInspectionRequest
        {
            TargetCSharpDbVersion =
                CSharpDbCapabilityCatalogLoader.CurrentTargetVersion,
            IncludeProfile = true,
        };
        if (options.TryGetValue(
                "--profile-sample-size",
                out string? sampleSizeValue))
        {
            if (!TryParsePositiveInt(
                    sampleSizeValue,
                    out int profileSampleSize))
            {
                return await OptionErrorAsync(
                    "The SQLite profile sample size must be a positive 32-bit integer.",
                    error);
            }

            inspectionRequest = inspectionRequest with
            {
                ProfileSampleSize = profileSampleSize,
            };
        }

        long maxSourceBytes =
            SqliteBackupSnapshot.DefaultMaxSnapshotBytes;
        if (options.TryGetValue(
                "--max-source-bytes",
                out string? maxSourceBytesValue) &&
            !TryParseSourceByteLimit(maxSourceBytesValue, out maxSourceBytes))
        {
            return await OptionErrorAsync(
                "The SQLite source byte limit must be a non-negative 64-bit integer below Int64.MaxValue.",
                error);
        }

        string inputPath;
        string packagePath;
        string outputPath;
        try
        {
            inputPath = Path.GetFullPath(inputValue);
            packagePath = Path.GetFullPath(packageValue);
            outputPath = Path.GetFullPath(outputValue);
        }
        catch (Exception pathError) when (
            pathError is ArgumentException or NotSupportedException or
                PathTooLongException)
        {
            throw new MigrationCliSafeException(
                "MIG-SQLITE-CLI-PATH-001",
                "The SQLite migration paths are invalid.",
                pathError);
        }

        bool pathsCollide;
        try
        {
            pathsCollide = ContainsEquivalentResolvedPaths(
                [inputPath, packagePath, outputPath]);
        }
        catch (Exception pathError) when (
            pathError is IOException or UnauthorizedAccessException or
                ArgumentException or NotSupportedException)
        {
            throw new MigrationCliSafeException(
                "MIG-SQLITE-CLI-PATH-001",
                "The SQLite migration paths could not be verified safely.",
                pathError);
        }
        if (pathsCollide)
        {
            return await OptionErrorAsync(
                "SQLite input, retained package, and catalog output must use different files.",
                error);
        }
        if (File.Exists(packagePath) || Directory.Exists(packagePath))
        {
            return await OptionErrorAsync(
                "The SQLite retained package destination already exists.",
                error);
        }
        if (File.Exists(outputPath) || Directory.Exists(outputPath))
        {
            return await OptionErrorAsync(
                "The SQLite catalog destination already exists.",
                error);
        }

        bool packagePublished = false;
        bool catalogPublished = false;
        try
        {
            SqliteBackupSnapshot snapshot =
                await SqliteBackupSnapshot.CreateAsync(
                    inputPath,
                    packagePath,
                    maxSourceBytes,
                    ct);
            packagePublished = true;
            MigrationCatalog catalog =
                await new SqliteMigrationSourceInspector(snapshot)
                    .InspectAsync(inspectionRequest, ct);
            if (!IsSupportedSqliteV1Catalog(catalog))
            {
                throw new InvalidDataException(
                    "The SQLite inspector produced an unsupported catalog contract.");
            }
            await WriteNewArtifactAsync(
                outputPath,
                MigrationArtifactSerializer.SerializeCatalog(catalog),
                ct);
            catalogPublished = true;

            int exitCode = catalog.Diagnostics.Count == 0
                ? InspectorCommandRunner.ExitOk
                : InspectorCommandRunner.ExitWarn;
            await output.WriteLineAsync(
                $"Status: {StatusLabel(exitCode)} | catalog={outputPath} | package={packagePath} | manifestDigest={snapshot.ContentDigest} | objects={catalog.Objects.Count} | diagnostics={catalog.Diagnostics.Count}");
            return exitCode;
        }
        catch (Exception operationFailure) when (
            packagePublished &&
            !catalogPublished)
        {
            throw new MigrationCliSafeException(
                "MIG-SQLITE-CLI-CATALOG-001",
                "SQLite catalog publication failed after the retained package was published; the package was preserved.",
                operationFailure);
        }
        catch (SqliteMigrationException sqliteFailure)
        {
            throw new MigrationCliSafeException(
                "MIG-SQLITE-CLI-INSPECT-001",
                sqliteFailure.Message,
                sqliteFailure);
        }
    }

    private static async ValueTask<int> RunLiteDbInspectAsync(
        IReadOnlyDictionary<string, string> options,
        string outputValue,
        TextWriter output,
        TextWriter error,
        CancellationToken ct)
    {
        if (!RequireOnly(
                options,
                [
                    "--source",
                    "--input",
                    "--package",
                    "--out",
                    "--profile-sample-size",
                    "--max-source-bytes",
                ],
                out string? parseError))
        {
            return await OptionErrorAsync(parseError!, error);
        }
        if (!options.TryGetValue("--input", out string? inputValue))
            return await OptionErrorAsync("Missing required option --input.", error);
        if (!options.TryGetValue("--package", out string? packageValue))
            return await OptionErrorAsync("Missing required option --package.", error);
        if (string.IsNullOrWhiteSpace(inputValue) ||
            string.IsNullOrWhiteSpace(packageValue) ||
            string.IsNullOrWhiteSpace(outputValue))
        {
            return await OptionErrorAsync(
                "LiteDB input, retained package, and catalog paths cannot be blank.",
                error);
        }

        var inspectionRequest = new MigrationInspectionRequest
        {
            TargetCSharpDbVersion =
                CSharpDbCapabilityCatalogLoader.CurrentTargetVersion,
            IncludeProfile = true,
        };
        if (options.TryGetValue(
                "--profile-sample-size",
                out string? sampleSizeValue))
        {
            if (!TryParsePositiveInt(
                    sampleSizeValue,
                    out int profileSampleSize))
            {
                return await OptionErrorAsync(
                    "The LiteDB profile sample size must be a positive 32-bit integer.",
                    error);
            }

            inspectionRequest = inspectionRequest with
            {
                ProfileSampleSize = profileSampleSize,
            };
        }

        long maxSourceBytes =
            LiteDbRetainedSnapshot.DefaultMaxSnapshotBytes;
        if (options.TryGetValue(
                "--max-source-bytes",
                out string? maxSourceBytesValue) &&
            !TryParseSourceByteLimit(maxSourceBytesValue, out maxSourceBytes))
        {
            return await OptionErrorAsync(
                "The LiteDB source byte limit must be a non-negative 64-bit integer below Int64.MaxValue.",
                error);
        }

        string inputPath;
        string packagePath;
        string outputPath;
        try
        {
            inputPath = Path.GetFullPath(inputValue);
            packagePath = Path.GetFullPath(packageValue);
            outputPath = Path.GetFullPath(outputValue);
        }
        catch (Exception pathError) when (
            pathError is ArgumentException or NotSupportedException or
                PathTooLongException)
        {
            throw new MigrationCliSafeException(
                "MIG-LITEDB-CLI-PATH-001",
                "The LiteDB migration paths are invalid.",
                pathError);
        }

        bool pathsCollide;
        try
        {
            pathsCollide = ContainsEquivalentResolvedPaths(
                [inputPath, packagePath, outputPath]);
        }
        catch (Exception pathError) when (
            pathError is IOException or UnauthorizedAccessException or
                ArgumentException or NotSupportedException)
        {
            throw new MigrationCliSafeException(
                "MIG-LITEDB-CLI-PATH-001",
                "The LiteDB migration paths could not be verified safely.",
                pathError);
        }
        if (pathsCollide)
        {
            return await OptionErrorAsync(
                "LiteDB input, retained package, and catalog output must use different files.",
                error);
        }
        if (File.Exists(packagePath) || Directory.Exists(packagePath))
        {
            return await OptionErrorAsync(
                "The LiteDB retained package destination already exists.",
                error);
        }
        if (File.Exists(outputPath) || Directory.Exists(outputPath))
        {
            return await OptionErrorAsync(
                "The LiteDB catalog destination already exists.",
                error);
        }

        bool packagePublished = false;
        bool catalogPublished = false;
        try
        {
            LiteDbRetainedSnapshot snapshot =
                await LiteDbRetainedSnapshot.CreateAsync(
                    inputPath,
                    packagePath,
                    maxSourceBytes,
                    ct);
            packagePublished = true;
            MigrationCatalog catalog =
                await new LiteDbMigrationSourceInspector(snapshot)
                    .InspectAsync(inspectionRequest, ct);
            if (!IsSupportedLiteDbV1Catalog(catalog))
            {
                throw new InvalidDataException(
                    "The LiteDB inspector produced an unsupported catalog contract.");
            }
            await WriteNewArtifactAsync(
                outputPath,
                MigrationArtifactSerializer.SerializeCatalog(catalog),
                ct);
            catalogPublished = true;

            int exitCode = catalog.Diagnostics.Count == 0
                ? InspectorCommandRunner.ExitOk
                : InspectorCommandRunner.ExitWarn;
            await output.WriteLineAsync(
                $"Status: {StatusLabel(exitCode)} | catalog={outputPath} | package={packagePath} | manifestDigest={snapshot.ContentDigest} | objects={catalog.Objects.Count} | diagnostics={catalog.Diagnostics.Count}");
            return exitCode;
        }
        catch (Exception operationFailure) when (
            packagePublished &&
            !catalogPublished)
        {
            throw new MigrationCliSafeException(
                "MIG-LITEDB-CLI-CATALOG-001",
                "LiteDB catalog publication failed after the retained package was published; the package was preserved.",
                operationFailure);
        }
        catch (LiteDbMigrationException liteDbFailure)
        {
            throw new MigrationCliSafeException(
                "MIG-LITEDB-CLI-INSPECT-001",
                liteDbFailure.Message,
                liteDbFailure);
        }
    }

    private static async ValueTask<int> RunAccessInspectAsync(
        IReadOnlyDictionary<string, string> options,
        string outputValue,
        TextWriter output,
        TextWriter error,
        MigrationCommandDependencies dependencies,
        CancellationToken ct)
    {
        if (!RequireOnly(
                options,
                [
                    "--source",
                    "--input",
                    "--package",
                    "--out",
                    "--provider",
                    "--allow-ace12-fallback",
                    "--command-timeout-seconds",
                    "--max-source-bytes",
                    "--max-package-bytes",
                ],
                out string? parseError))
        {
            return await OptionErrorAsync(
                "The Microsoft Access inspect command contains an unsupported option.",
                error);
        }
        if (!options.TryGetValue("--input", out string? inputValue))
        {
            return await OptionErrorAsync(
                "Missing required option --input.",
                error);
        }
        if (!options.TryGetValue("--package", out string? packageValue))
        {
            return await OptionErrorAsync(
                "Missing required option --package.",
                error);
        }
        if (string.IsNullOrWhiteSpace(inputValue) ||
            string.IsNullOrWhiteSpace(packageValue) ||
            string.IsNullOrWhiteSpace(outputValue))
        {
            return await OptionErrorAsync(
                "Microsoft Access input, retained package, and catalog paths cannot be blank.",
                error);
        }

        string provider =
            options.GetValueOrDefault("--provider", "ace16")
                .ToLowerInvariant() switch
            {
                "ace16" => "ace16",
                "ace12" => "ace12",
                _ => string.Empty,
            };
        if (provider.Length == 0)
        {
            return await OptionErrorAsync(
                "Microsoft Access provider must be ace16 or ace12.",
                error);
        }
        if (provider == "ace12" &&
            options.ContainsKey("--allow-ace12-fallback"))
        {
            return await OptionErrorAsync(
                "--allow-ace12-fallback applies only when ace16 is selected.",
                error);
        }

        int commandTimeoutSeconds =
            AccessWorkerClient
                .DefaultCommandTimeoutSeconds;
        if (options.TryGetValue(
                "--command-timeout-seconds",
                out string? timeoutValue) &&
            (!TryParsePositiveInt(
                 timeoutValue,
                 out commandTimeoutSeconds) ||
             commandTimeoutSeconds >
                 AccessWorkerClient
                     .MaxCommandTimeoutSeconds))
        {
            return await OptionErrorAsync(
                "The Microsoft Access command timeout must be an integer from 1 through 3600.",
                error);
        }

        long maxSourceBytes =
            AccessWorkerClient
                .DefaultMaxSourceBytes;
        if (options.TryGetValue(
                "--max-source-bytes",
                out string? sourceLimitValue) &&
            (!TryParsePositiveLong(
                 sourceLimitValue,
                 out maxSourceBytes) ||
             maxSourceBytes >
                 AccessWorkerClient
                     .HardMaxSourceBytes))
        {
            return await OptionErrorAsync(
                "The Microsoft Access source byte limit must be a positive integer no larger than 64 GiB.",
                error);
        }
        long maxPackageBytes =
            AccessWorkerClient
                .DefaultMaxPackageBytes;
        if (options.TryGetValue(
                "--max-package-bytes",
                out string? packageLimitValue) &&
            (!TryParsePositiveLong(
                 packageLimitValue,
                 out maxPackageBytes) ||
             maxPackageBytes >
                 AccessWorkerClient
                     .HardMaxPackageBytes))
        {
            return await OptionErrorAsync(
                "The Microsoft Access package byte limit must be a positive integer no larger than 256 GiB.",
                error);
        }

        string inputPath;
        string packagePath;
        string outputPath;
        try
        {
            inputPath = Path.GetFullPath(inputValue);
            packagePath = Path.GetFullPath(packageValue);
            outputPath = Path.GetFullPath(outputValue);
        }
        catch (Exception pathError) when (
            pathError is ArgumentException or
                NotSupportedException or
                PathTooLongException)
        {
            throw new MigrationCliSafeException(
                "MIG-ACCESS-CLI-PATH-001",
                "The Microsoft Access migration paths are invalid.",
                pathError);
        }

        bool pathsCollide;
        try
        {
            pathsCollide = ContainsEquivalentResolvedPaths(
                [inputPath, packagePath, outputPath]);
        }
        catch (Exception pathError) when (
            pathError is IOException or
                UnauthorizedAccessException or
                ArgumentException or
                NotSupportedException)
        {
            throw new MigrationCliSafeException(
                "MIG-ACCESS-CLI-PATH-001",
                "The Microsoft Access migration paths could not be verified safely.",
                pathError);
        }
        if (pathsCollide)
        {
            return await OptionErrorAsync(
                "Microsoft Access input, retained package, and catalog output must use different files.",
                error);
        }
        if (File.Exists(packagePath) ||
            Directory.Exists(packagePath))
        {
            return await OptionErrorAsync(
                "The Microsoft Access retained package destination already exists.",
                error);
        }
        if (File.Exists(outputPath) ||
            Directory.Exists(outputPath))
        {
            return await OptionErrorAsync(
                "The Microsoft Access catalog destination already exists.",
                error);
        }

        string? packageParent =
            Path.GetDirectoryName(packagePath);
        if (string.IsNullOrEmpty(packageParent) ||
            !Directory.Exists(packageParent))
        {
            return await OptionErrorAsync(
                "The Microsoft Access retained package parent must be an existing caller-controlled directory.",
                error);
        }

        RetainedCaptureDirectoryLease
            packageParentLease;
        try
        {
            packageParentLease =
                RetainedCaptureDirectoryLease.Open(
                    packageParent);
        }
        catch (Exception pathError) when (
            pathError is IOException or
                UnauthorizedAccessException or
                ArgumentException or
                NotSupportedException)
        {
            throw new MigrationCliSafeException(
                "MIG-ACCESS-CLI-PATH-001",
                "The Microsoft Access retained package directory is not a safe caller-controlled local directory.",
                pathError);
        }

        using (packageParentLease)
        {
            bool packagePublished = false;
            bool catalogPublished = false;
            SqlServerCaptureWorkspace? workspace =
                null;
            try
            {
                packageParentLease.AssertUnchanged();
                workspace =
                    SqlServerCaptureWorkspace.Create(
                        packageParent,
                        AccessWorkerClient
                            .CaptureWorkspacePrefix,
                        AccessWorkerClient
                            .CaptureOutputFileName,
                        packageParentLease);
                AccessCaptureWorkerResult workerResult =
                    await dependencies.CaptureAccessAsync(
                            inputPath,
                            CSharpDbCapabilityCatalogLoader
                                .CurrentTargetVersion,
                            workspace.CapturePath,
                            provider,
                            options.ContainsKey(
                                "--allow-ace12-fallback"),
                            commandTimeoutSeconds,
                            maxSourceBytes,
                            maxPackageBytes,
                            ct)
                        .ConfigureAwait(false);
                ArgumentNullException.ThrowIfNull(
                    workerResult);
                packageParentLease.AssertUnchanged();
                workspace.AssertUnchanged();

                switch (workerResult.Status)
                {
                    case AccessCaptureWorkerStatus
                        .Missing:
                    case AccessCaptureWorkerStatus
                        .Incompatible:
                        throw new MigrationCliSafeException(
                            "MIG-ACCESS-CLI-ADAPTER-001",
                            "The optional Microsoft Access capture adapter is unavailable or incompatible.",
                            new InvalidOperationException(
                                "The Microsoft Access capture worker boundary is unavailable."));
                    case AccessCaptureWorkerStatus
                        .UnsupportedPlatform:
                        throw new MigrationCliSafeException(
                            "MIG-ACCESS-CLI-INSPECT-001",
                            "Microsoft Access capture requires Windows.",
                            new PlatformNotSupportedException());
                    case AccessCaptureWorkerStatus
                        .ProviderUnavailable:
                        throw new MigrationCliSafeException(
                            "MIG-ACCESS-CLI-INSPECT-001",
                            "The selected process-matched ACE OLE DB provider is unavailable.",
                            new InvalidOperationException(
                                "The Access provider is unavailable."));
                    case AccessCaptureWorkerStatus
                        .LimitExceeded:
                        throw new MigrationCliSafeException(
                            "MIG-ACCESS-CLI-CAPTURE-LIMIT-001",
                            "The Microsoft Access retained capture exceeded a configured safety limit.",
                            new InvalidDataException(
                                "The Access worker crossed a retained-source limit."));
                    case AccessCaptureWorkerStatus
                        .CaptureFailed:
                        throw new MigrationCliSafeException(
                            "MIG-ACCESS-CLI-INSPECT-001",
                            "The Microsoft Access source could not be captured or inspected safely.",
                            new InvalidOperationException(
                                "The Access worker could not retain the source."));
                }
                if (workerResult.Status !=
                        AccessCaptureWorkerStatus
                            .Success ||
                    workerResult.Receipt is null)
                {
                    throw new MigrationCliSafeException(
                        "MIG-ACCESS-CLI-ADAPTER-001",
                        "The optional Microsoft Access capture adapter is unavailable or incompatible.",
                        new InvalidDataException(
                            "The Access capture worker returned an invalid contract."));
                }

                AccessCaptureReceipt receipt =
                    workerResult.Receipt;
                MigrationCatalog catalog;
                await using (
                    RetainedMigrationPackageSession
                        session =
                        await RetainedMigrationPackageSession
                            .OpenAsync(
                                workspace.CapturePath,
                                new RetainedMigrationPackageOpenOptions
                                {
                                    ExpectedPackageDigest =
                                        receipt
                                            .PackageDigest,
                                    WorkspacePath =
                                        workspace
                                            .VerificationWorkspacePath,
                                    MaxPackageBytes =
                                        maxPackageBytes,
                                },
                                ct)
                            .ConfigureAwait(false))
                {
                    ValidateAccessCaptureSession(
                        session,
                        receipt);
                    catalog = session.Catalog;
                }

                ct.ThrowIfCancellationRequested();
                packageParentLease.AssertUnchanged();
                workspace.AssertUnchanged();
                File.Move(
                    workspace.CapturePath,
                    packagePath,
                    overwrite: false);
                packagePublished = true;
                packageParentLease.AssertUnchanged();

                workspace.Dispose();
                workspace = null;
                await WriteNewArtifactAsync(
                    outputPath,
                    MigrationArtifactSerializer
                        .SerializeCatalog(catalog),
                    ct);
                catalogPublished = true;

                int exitCode =
                    catalog.Diagnostics.Count == 0
                        ? InspectorCommandRunner.ExitOk
                        : InspectorCommandRunner.ExitWarn;
                await output.WriteLineAsync(
                    $"Status: {StatusLabel(exitCode)} | catalog={outputPath} | package={packagePath} | packageDigest={receipt.PackageDigest} | objects={catalog.Objects.Count} | tables={receipt.TableCount} | rows={receipt.RowCount} | diagnostics={catalog.Diagnostics.Count} | applyReady=false");
                return exitCode;
            }
            catch (Exception operationFailure)
            {
                Exception failure = operationFailure;
                if (workspace is not null)
                {
                    try
                    {
                        workspace.Dispose();
                        workspace = null;
                    }
                    catch (
                        RetainedCaptureWorkspaceCleanupException
                            cleanupFailure)
                    {
                        failure = new
                            RetainedCaptureWorkspaceCleanupException(
                                operationFailure,
                                cleanupFailure);
                    }
                }

                if (failure is
                    RetainedCaptureWorkspaceCleanupException)
                {
                    throw new MigrationCliSafeException(
                        "MIG-ACCESS-CLI-CLEANUP-001",
                        packagePublished
                            ? "The Microsoft Access retained package was published, but private capture workspace cleanup failed; the package was preserved and the catalog was not published."
                            : "Microsoft Access capture failed and its private workspace could not be cleaned safely; no final artifacts were published.",
                        failure);
                }
                if (packagePublished &&
                    !catalogPublished)
                {
                    throw new MigrationCliSafeException(
                        "MIG-ACCESS-CLI-CATALOG-001",
                        "Microsoft Access catalog publication failed after the retained package was published; the package was preserved.",
                        failure);
                }
                if (failure is
                    MigrationCliSafeException or
                    OperationCanceledException)
                {
                    ExceptionDispatchInfo.Capture(
                        failure).Throw();
                }
                if (failure is
                    RetainedMigrationPackageException)
                {
                    throw new MigrationCliSafeException(
                        "MIG-ACCESS-CLI-PACKAGE-001",
                        "The Microsoft Access retained package could not be verified safely.",
                        failure);
                }
                if (IsRecoverableCliException(
                        failure))
                {
                    throw new MigrationCliSafeException(
                        "MIG-ACCESS-CLI-INSPECT-001",
                        "The Microsoft Access source could not be captured or inspected safely.",
                        failure);
                }
                throw;
            }
        }
    }

    private static async ValueTask<int> RunCsvInspectAsync(
        IReadOnlyDictionary<string, string> options,
        string outputValue,
        TextWriter output,
        TextWriter error,
        CancellationToken ct)
    {
        if (!RequireOnly(
                options,
                [
                    "--source",
                    "--input",
                    "--package",
                    "--out",
                    "--delimiter",
                    "--no-header",
                    "--table",
                    "--sample-rows",
                    "--null-token",
                    "--source-id",
                    "--workspace",
                    "--max-source-bytes",
                ],
                out string? parseError))
        {
            return await OptionErrorAsync(parseError!, error);
        }
        if (!options.TryGetValue("--input", out string? inputValue))
            return await OptionErrorAsync("Missing required option --input.", error);
        if (!options.TryGetValue("--package", out string? packageValue))
            return await OptionErrorAsync("Missing required option --package.", error);
        if (string.IsNullOrWhiteSpace(inputValue) || string.IsNullOrWhiteSpace(packageValue))
            return await OptionErrorAsync("CSV input and package paths cannot be blank.", error);
        if (options.TryGetValue("--workspace", out string? workspaceValue) &&
            string.IsNullOrWhiteSpace(workspaceValue))
        {
            return await OptionErrorAsync("The CSV workspace path cannot be blank.", error);
        }

        int sampleRows = 1_000;
        if (options.TryGetValue("--sample-rows", out string? sampleRowsValue) &&
            !TryParsePositiveInt(sampleRowsValue, out sampleRows))
        {
            return await OptionErrorAsync(
                "The CSV sample row count must be a positive 32-bit integer.",
                error);
        }

        long maxSourceBytes = new CsvSourceSnapshotOptions().MaxSourceBytes;
        if (options.TryGetValue("--max-source-bytes", out string? maxSourceBytesValue) &&
            !TryParseSourceByteLimit(maxSourceBytesValue, out maxSourceBytes))
        {
            return await OptionErrorAsync(
                "The CSV source byte limit must be a non-negative 64-bit integer below Int64.MaxValue.",
                error);
        }

        if (!TryResolveDelimiter(
                options.GetValueOrDefault("--delimiter", "auto"),
                out string configuredDelimiter,
                out IReadOnlyList<string> delimiterCandidates))
        {
            return await OptionErrorAsync(
                "The CSV delimiter must be auto, comma, semicolon, tab, pipe, or one character.",
                error);
        }

        string inputPath = Path.GetFullPath(inputValue);
        string packagePath = Path.GetFullPath(packageValue);
        string outputPath = Path.GetFullPath(outputValue);
        if (ContainsEquivalentResolvedPaths([inputPath, packagePath, outputPath]))
        {
            return await OptionErrorAsync(
                "CSV input, retained package, and catalog output must use different files.",
                error);
        }
        if (!ValidateWorkspacePath(
                options,
                [inputPath, packagePath, outputPath],
                out parseError))
        {
            return await OptionErrorAsync(parseError!, error);
        }

        string? packageDirectory = Path.GetDirectoryName(packagePath);
        if (string.IsNullOrEmpty(packageDirectory))
            return await OptionErrorAsync("The CSV package path must have a parent directory.", error);
        if (!Directory.Exists(packageDirectory))
        {
            return await OptionErrorAsync(
                "The CSV package parent must be an existing caller-controlled directory.",
                error);
        }
        FileAttributes packageParentAttributes = File.GetAttributes(packageDirectory);
        if ((packageParentAttributes & (FileAttributes.ReparsePoint | FileAttributes.Device)) != 0)
        {
            return await OptionErrorAsync(
                "The CSV package parent cannot be a link, reparse point, or device.",
                error);
        }

        var readerOptions = new CsvReaderOptions
        {
            HasHeaderRecord = !options.ContainsKey("--no-header"),
            Delimiter = configuredDelimiter,
            NullToken = options.GetValueOrDefault("--null-token"),
        };
        var snapshotOptions = new CsvSourceSnapshotOptions
        {
            WorkspacePath = options.GetValueOrDefault("--workspace"),
            MaxSourceBytes = maxSourceBytes,
        };
        await using CsvSourceSnapshot snapshot = await CsvSourceSnapshot.CreateFromFileAsync(
            inputPath,
            snapshotOptions,
            ct);
        CsvFormatInspection inspection = await CsvFormatInspector.InspectAsync(
            snapshot,
            readerOptions,
            new CsvInspectionOptions { DelimiterCandidates = delimiterCandidates },
            ct);
        if (inspection.Delimiter.Resolution != CsvInspectionResolution.Resolved)
        {
            throw new InvalidDataException(
                $"CSV delimiter inspection was {inspection.Delimiter.Resolution}. " +
                "Specify one delimiter with --delimiter and inspect again.");
        }
        CsvSourceBinding binding = await CsvSourceBinding.CreateAsync(
            snapshot,
            inspection,
            options.GetValueOrDefault("--source-id"),
            ct);
        CsvSchemaInferenceResult schema = await CsvSchemaInferer.InferAsync(
            binding,
            snapshot,
            sampleRows,
            new CsvSchemaInferenceOptions
            {
                TableName = options.GetValueOrDefault("--table", "csv_data"),
            },
            ct);
        MigrationCatalog catalog = schema.CreateCatalog(
            CSharpDbCapabilityCatalogLoader.CurrentTargetVersion);

        bool packagePublished = false;
        bool catalogPublished = false;
        try
        {
            CsvSnapshotPackageManifest manifest = await CsvSnapshotPackage.WriteAsync(
                packagePath,
                snapshot,
                schema,
                catalog.TargetCSharpDbVersion,
                ct);
            packagePublished = true;
            await WriteArtifactAsync(
                outputPath,
                MigrationArtifactSerializer.SerializeCatalog(catalog),
                ct);
            catalogPublished = true;

            int exitCode = catalog.Diagnostics.Count == 0
                ? InspectorCommandRunner.ExitOk
                : InspectorCommandRunner.ExitWarn;
            await output.WriteLineAsync(
                $"Status: {StatusLabel(exitCode)} | catalog={outputPath} | package={packagePath} | manifestDigest={manifest.ManifestDigest} | objects={catalog.Objects.Count} | diagnostics={catalog.Diagnostics.Count}");
            return exitCode;
        }
        catch (Exception operationFailure) when (packagePublished && !catalogPublished)
        {
            throw new IOException(
                $"Catalog publication failed after the retained CSV package was published. " +
                $"The package was preserved at '{packagePath}'.",
                operationFailure);
        }
    }

    private static async ValueTask<int> RunJsonInspectAsync(
        IReadOnlyDictionary<string, string> options,
        string outputValue,
        TextWriter output,
        TextWriter error,
        CancellationToken ct)
    {
        if (!RequireOnly(
                options,
                [
                    "--source",
                    "--input",
                    "--package",
                    "--out",
                    "--framing",
                    "--table",
                    "--sample-rows",
                    "--source-id",
                    "--workspace",
                    "--max-source-bytes",
                    "--typed-intent",
                    "--expected-intent-manifest-digest",
                ],
                out string? parseError))
        {
            return await OptionErrorAsync(parseError!, error);
        }
        if (!options.TryGetValue("--input", out string? inputValue))
            return await OptionErrorAsync("Missing required option --input.", error);
        if (!options.TryGetValue("--package", out string? packageValue))
            return await OptionErrorAsync("Missing required option --package.", error);
        if (string.IsNullOrWhiteSpace(inputValue) || string.IsNullOrWhiteSpace(packageValue))
        {
            return await OptionErrorAsync(
                "JSON input and package paths cannot be blank.",
                error);
        }
        bool hasTypedIntent = options.TryGetValue(
            "--typed-intent",
            out string? typedIntentValue);
        bool hasExpectedIntentDigest = options.TryGetValue(
            "--expected-intent-manifest-digest",
            out string? expectedIntentDigest);
        if (hasTypedIntent != hasExpectedIntentDigest)
        {
            return await OptionErrorAsync(
                "Options --typed-intent and --expected-intent-manifest-digest must be supplied together.",
                error);
        }
        if (hasTypedIntent &&
            string.IsNullOrWhiteSpace(typedIntentValue))
        {
            return await OptionErrorAsync(
                "The typed JSON intent path cannot be blank.",
                error);
        }
        if (hasExpectedIntentDigest &&
            !IsCanonicalSha256(expectedIntentDigest!))
        {
            return await OptionErrorAsync(
                "The expected typed JSON intent manifest digest must be canonical lowercase sha256:<64-hex>.",
                error);
        }
        if (options.TryGetValue("--workspace", out string? workspaceValue) &&
            string.IsNullOrWhiteSpace(workspaceValue))
        {
            return await OptionErrorAsync(
                "The retained-source workspace path cannot be blank.",
                error);
        }

        string framingValue = options.GetValueOrDefault("--framing", "root-array");
        JsonInputFraming framing = framingValue.ToLowerInvariant() switch
        {
            "root-array" => JsonInputFraming.RootArray,
            "ndjson" => JsonInputFraming.MultipleValues,
            _ => (JsonInputFraming)(-1),
        };
        if (!Enum.IsDefined(framing))
        {
            return await OptionErrorAsync(
                $"Unsupported JSON framing '{framingValue}'.",
                error);
        }

        int sampleRows = 1_000;
        if (options.TryGetValue("--sample-rows", out string? sampleRowsValue) &&
            (!TryParsePositiveInt(sampleRowsValue, out sampleRows) ||
             sampleRows >
             JsonTableSchemaInferenceOptions.MaximumSupportedProfileRecords))
        {
            return await OptionErrorAsync(
                "The JSON sample row count must be a positive 32-bit integer no greater than " +
                $"{JsonTableSchemaInferenceOptions.MaximumSupportedProfileRecords}.",
                error);
        }

        long maxSourceBytes = new JsonSourceSnapshotOptions().MaxSourceBytes;
        if (options.TryGetValue("--max-source-bytes", out string? maxSourceBytesValue) &&
            !TryParseSourceByteLimit(maxSourceBytesValue, out maxSourceBytes))
        {
            return await OptionErrorAsync(
                "The retained source byte limit must be a non-negative 64-bit integer below Int64.MaxValue.",
                error);
        }

        string inputPath = Path.GetFullPath(inputValue);
        string packagePath = Path.GetFullPath(packageValue);
        string outputPath = Path.GetFullPath(outputValue);
        string? typedIntentPath = hasTypedIntent
            ? Path.GetFullPath(typedIntentValue!)
            : null;
        string[] protectedPaths = typedIntentPath is null
            ? [inputPath, packagePath, outputPath]
            : [inputPath, typedIntentPath, packagePath, outputPath];
        if (ContainsEquivalentResolvedPaths(protectedPaths))
        {
            return await OptionErrorAsync(
                typedIntentPath is null
                    ? "JSON input, retained package, and catalog output must use different files."
                    : "JSON input, typed intent, retained package, and catalog output must use different files.",
                error);
        }
        if (!ValidateWorkspacePath(
                options,
                protectedPaths,
                out parseError))
        {
            return await OptionErrorAsync(parseError!, error);
        }

        string? packageDirectory = Path.GetDirectoryName(packagePath);
        if (string.IsNullOrEmpty(packageDirectory))
        {
            return await OptionErrorAsync(
                "The JSON package path must have a parent directory.",
                error);
        }
        if (!Directory.Exists(packageDirectory))
        {
            return await OptionErrorAsync(
                "The JSON package parent must be an existing caller-controlled directory.",
                error);
        }
        FileAttributes packageParentAttributes = File.GetAttributes(packageDirectory);
        if ((packageParentAttributes & (FileAttributes.ReparsePoint | FileAttributes.Device)) != 0)
        {
            return await OptionErrorAsync(
                "The JSON package parent cannot be a link, reparse point, or device.",
                error);
        }

        await using JsonSourceSnapshot snapshot =
            await JsonSourceSnapshot.CreateFromFileAsync(
                inputPath,
                new JsonSourceSnapshotOptions
                {
                    WorkspacePath = options.GetValueOrDefault("--workspace"),
                    MaxSourceBytes = maxSourceBytes,
                },
                ct);
        JsonSourceBinding binding = await JsonSourceBinding.CreateAsync(
            snapshot,
            new JsonStreamingReaderOptions { Framing = framing },
            options.GetValueOrDefault("--source-id"),
            ct);
        if (typedIntentPath is not null)
        {
            JsonTypedIntentManifest intentManifest =
                await JsonTypedIntentSidecar.OpenAsync(
                    typedIntentPath,
                    binding,
                    new JsonTypedIntentOpenOptions
                    {
                        ExpectedManifestDigest =
                            expectedIntentDigest,
                    },
                    ct);
            JsonTypedTableSchemaInferenceResult typedSchema =
                await JsonTypedTableSchemaInferer.InferAsync(
                    binding,
                    snapshot,
                    intentManifest,
                    sampleRows,
                    new JsonTableSchemaInferenceOptions
                    {
                        TableName = options.GetValueOrDefault(
                            "--table",
                            "json_data"),
                    },
                    ct);
            MigrationCatalog typedCatalog =
                typedSchema.CreateCatalog(
                    CSharpDbCapabilityCatalogLoader
                        .CurrentTargetVersion);
            string serializedTypedCatalog =
                MigrationArtifactSerializer.SerializeCatalog(
                    typedCatalog);

            bool typedPackagePublished = false;
            bool typedCatalogPublished = false;
            try
            {
                JsonTypedSnapshotPackageManifest manifest =
                    await JsonTypedSnapshotPackage.WriteAsync(
                        packagePath,
                        snapshot,
                        typedSchema,
                        typedCatalog.TargetCSharpDbVersion,
                        ct);
                typedPackagePublished = true;
                await WriteArtifactAsync(
                    outputPath,
                    serializedTypedCatalog,
                    ct);
                typedCatalogPublished = true;

                int exitCode = typedCatalog.Diagnostics.Count == 0
                    ? InspectorCommandRunner.ExitOk
                    : InspectorCommandRunner.ExitWarn;
                await output.WriteLineAsync(
                    $"Status: {StatusLabel(exitCode)} | catalog={outputPath} | package={packagePath} | manifestDigest={manifest.ManifestDigest} | intentManifestDigest={manifest.IntentManifestDigest} | objects={typedCatalog.Objects.Count} | diagnostics={typedCatalog.Diagnostics.Count}");
                return exitCode;
            }
            catch (Exception operationFailure) when (
                typedPackagePublished &&
                !typedCatalogPublished)
            {
                throw new IOException(
                    $"Catalog publication failed after the retained typed JSON package was published. " +
                    $"The package was preserved at '{packagePath}'.",
                    operationFailure);
            }
        }

        JsonTableSchemaInferenceResult schema = await JsonTableSchemaInferer.InferAsync(
            binding,
            snapshot,
            sampleRows,
            new JsonTableSchemaInferenceOptions
            {
                TableName = options.GetValueOrDefault("--table", "json_data"),
            },
            ct);
        MigrationCatalog catalog = schema.CreateCatalog(
            CSharpDbCapabilityCatalogLoader.CurrentTargetVersion);

        bool packagePublished = false;
        bool catalogPublished = false;
        try
        {
            JsonSnapshotPackageManifest manifest = await JsonSnapshotPackage.WriteAsync(
                packagePath,
                snapshot,
                schema,
                catalog.TargetCSharpDbVersion,
                ct);
            packagePublished = true;
            await WriteArtifactAsync(
                outputPath,
                MigrationArtifactSerializer.SerializeCatalog(catalog),
                ct);
            catalogPublished = true;

            int exitCode = catalog.Diagnostics.Count == 0
                ? InspectorCommandRunner.ExitOk
                : InspectorCommandRunner.ExitWarn;
            await output.WriteLineAsync(
                $"Status: {StatusLabel(exitCode)} | catalog={outputPath} | package={packagePath} | manifestDigest={manifest.ManifestDigest} | objects={catalog.Objects.Count} | diagnostics={catalog.Diagnostics.Count}");
            return exitCode;
        }
        catch (Exception operationFailure) when (packagePublished && !catalogPublished)
        {
            throw new IOException(
                $"Catalog publication failed after the retained JSON package was published. " +
                $"The package was preserved at '{packagePath}'.",
                operationFailure);
        }
    }

    private static async ValueTask<int> RunPlanAsync(
        string[] args,
        TextWriter output,
        TextWriter error,
        MigrationCommandDependencies dependencies,
        CancellationToken ct)
    {
        if (args.Length < 3 || args[2].StartsWith("--", StringComparison.Ordinal))
            return await OptionErrorAsync("Missing catalog artifact path.", error);
        if (!TryParseOptions(args, 3, out Dictionary<string, string> options, out string? parseError))
            return await OptionErrorAsync(parseError!, error);
        if (!RequireOnly(
                options,
                [
                    "--out",
                    "--profile",
                    "--accept-exclusions",
                    "--accept-diagnostics",
                    "--reject-mode",
                    "--reject-rules",
                    "--max-rejected-rows-per-batch",
                    "--max-rejected-rows-per-run",
                    "--max-reject-evidence-value-bytes",
                    "--max-reject-evidence-bytes-per-batch",
                    "--max-reject-evidence-bytes-per-run",
                    "--max-reject-artifact-bytes",
                ],
                out parseError))
            return await OptionErrorAsync(parseError!, error);
        if (!options.TryGetValue("--out", out string? outputValue))
            return await OptionErrorAsync("Missing required option --out.", error);

        MigrationMappingProfile profile = MigrationMappingProfile.Preserve;
        if (options.TryGetValue("--profile", out string? profileValue))
        {
            profile = profileValue.ToLowerInvariant() switch
            {
                "preserve" => MigrationMappingProfile.Preserve,
                "queryable" => MigrationMappingProfile.Queryable,
                _ => (MigrationMappingProfile)(-1),
            };
            if (!Enum.IsDefined(profile))
                return await OptionErrorAsync($"Unsupported mapping profile '{profileValue}'.", error);
        }

        string catalogPath = Path.GetFullPath(args[2]);
        string outputPath = Path.GetFullPath(outputValue);
        if (PathsAreEquivalent(catalogPath, outputPath))
        {
            return await OptionErrorAsync(
                "Catalog input and plan output must be different files.",
                error);
        }

        IReadOnlyList<string> acceptedDiagnosticIds = [];
        if (options.TryGetValue("--accept-diagnostics", out string? acceptedDiagnosticsValue) &&
            !TryParseIdList(acceptedDiagnosticsValue, out acceptedDiagnosticIds, out parseError))
        {
            return await OptionErrorAsync(parseError!, error);
        }

        bool acceptAllExclusions = false;
        IReadOnlyList<string> acceptedExclusionObjectIds = [];
        if (options.TryGetValue("--accept-exclusions", out string? acceptedExclusionsValue))
        {
            acceptAllExclusions = string.Equals(
                acceptedExclusionsValue,
                "all",
                StringComparison.OrdinalIgnoreCase);
            if (!acceptAllExclusions &&
                !TryParseIdList(acceptedExclusionsValue, out acceptedExclusionObjectIds, out parseError))
            {
                return await OptionErrorAsync(parseError!, error);
            }
        }

        if (!TryParsePlanLoadPolicy(
                options,
                out ParsedPlanLoadPolicy parsedLoad,
                out parseError))
        {
            return await OptionErrorAsync(parseError!, error);
        }
        MigrationCatalog catalog;
        try
        {
            catalog = MigrationArtifactSerializer.DeserializeCatalog(
                await ReadBoundedMigrationContractArtifactAsync(
                    catalogPath,
                    ct));
        }
        catch (OperationCanceledException) when (ct.IsCancellationRequested)
        {
            throw;
        }
        catch (Exception artifactError) when (
            IsRecoverableCliException(artifactError))
        {
            throw new MigrationCliSafeException(
                "MIG-CSHARPDB-PLAN-ARTIFACT-001",
                "The migration catalog could not be loaded safely for planning.",
                artifactError);
        }
        if (catalog.Source.Kind == MigrationSourceKind.Json &&
            ClassifyJsonCatalog(
                catalog,
                out _) == JsonCatalogRoute.Unsupported)
        {
            return await OptionErrorAsync(
                JsonCatalogRouteOnlyMessage,
                error);
        }
        if (catalog.Source.Kind == MigrationSourceKind.Sqlite &&
            !IsSupportedSqliteV1Catalog(catalog))
        {
            return await OptionErrorAsync(
                SqliteCatalogRouteOnlyMessage,
                error);
        }
        if (catalog.Source.Kind == MigrationSourceKind.LiteDb &&
            !IsSupportedLiteDbV1Catalog(catalog))
        {
            return await OptionErrorAsync(
                LiteDbCatalogRouteOnlyMessage,
                error);
        }
        if (!TryBindPlanLoadPolicy(
                parsedLoad,
                catalog,
                out MigrationLoadPolicy load,
                out parseError))
        {
            return await OptionErrorAsync(parseError!, error);
        }
        MigrationPlan plan = new MigrationPlanner().CreatePlan(
            catalog,
            new MigrationPlanningOptions
            {
                MappingProfile = profile,
                Load = load,
                AcceptedDiagnosticIds = acceptedDiagnosticIds,
                AcceptedExclusionObjectIds = acceptedExclusionObjectIds,
                AcceptAllExclusions = acceptAllExclusions,
            });
        try
        {
            plan = dependencies.SealCSharpDbMigrationPlan(
                plan,
                catalog,
                ct);
        }
        catch (OperationCanceledException) when (ct.IsCancellationRequested)
        {
            throw;
        }
        catch (CSharpDbDdlPreviewLimitException limitError)
        {
            throw new MigrationCliSafeException(
                "MIG-CSHARPDB-PLAN-DDL-LIMIT-001",
                "The migration plan's CSharpDB DDL exceeded a production safety limit.",
                limitError);
        }
        catch (Exception sealingError) when (
            IsRecoverableCliException(sealingError))
        {
            throw new MigrationCliSafeException(
                "MIG-CSHARPDB-PLAN-DDL-001",
                "The migration plan could not be sealed to its CSharpDB DDL safely.",
                sealingError);
        }
        string serializedPlan;
        try
        {
            serializedPlan =
                dependencies.SerializeMigrationPlan(plan, catalog) ??
                throw new InvalidDataException(
                    "Migration plan serialization returned no content.");
            ct.ThrowIfCancellationRequested();
        }
        catch (OperationCanceledException) when (ct.IsCancellationRequested)
        {
            throw;
        }
        catch (Exception serializationError) when (
            IsRecoverableCliException(serializationError))
        {
            throw new MigrationCliSafeException(
                "MIG-CSHARPDB-PLAN-ARTIFACT-001",
                "The sealed migration plan could not be serialized safely.",
                serializationError);
        }

        bool serializedPlanExceedsLimit;
        try
        {
            serializedPlanExceedsLimit =
                ExceedsStrictUtf8ByteLimit(
                    serializedPlan,
                    MaxMigrationContractArtifactBytes,
                    ct);
        }
        catch (EncoderFallbackException encodingError)
        {
            throw new MigrationCliSafeException(
                "MIG-CSHARPDB-PLAN-ARTIFACT-001",
                "The sealed migration plan could not be encoded safely.",
                encodingError);
        }
        ct.ThrowIfCancellationRequested();
        if (serializedPlanExceedsLimit)
        {
            throw new MigrationCliSafeException(
                "MIG-CSHARPDB-PLAN-ARTIFACT-LIMIT-001",
                "The sealed migration plan exceeds the migration contract artifact limit.",
                new InvalidDataException(
                    "The sealed migration plan exceeds its byte limit."));
        }

        await WriteArtifactAsync(
            outputPath,
            serializedPlan,
            ct);

        MigrationPlanReadiness readiness = MigrationPlanReadinessValidator.Evaluate(plan, catalog);
        int exitCode = HasReviewFindings(plan, readiness)
            ? InspectorCommandRunner.ExitWarn
            : InspectorCommandRunner.ExitOk;
        string rejectStatus = plan.Load.RejectMode == MigrationRejectMode.DeterministicRejects
            ? " | rejectMode=deterministic"
            : string.Empty;
        await output.WriteLineAsync(
            $"Status: {StatusLabel(exitCode)} | plan={outputPath} | included={plan.Objects.Count(item => item.Included)} | excluded={plan.Objects.Count(item => !item.Included)} | diagnostics={plan.Diagnostics.Count}{rejectStatus}");
        return exitCode;
    }

    private static async ValueTask<int> RunPreviewAsync(
        string[] args,
        TextWriter output,
        TextWriter error,
        MigrationCommandDependencies dependencies,
        CancellationToken ct)
    {
        if (args.Length < 3 || args[2].StartsWith("--", StringComparison.Ordinal))
            return await OptionErrorAsync("Missing plan artifact path.", error);
        bool explicitPreviewRequested = args
            .Skip(3)
            .Any(IsExplicitPreviewOptionToken);
        if (!TryParseOptions(
                args,
                3,
                ["--ddl", "--scratch"],
                out Dictionary<string, string> options,
                out string? parseError))
        {
            return await OptionErrorAsync(
                explicitPreviewRequested
                    ? "The explicit CSharpDB preview options are invalid."
                    : parseError!,
                error);
        }
        if (!RequireOnly(
                options,
                ["--catalog", "--format", "--ddl", "--scratch"],
                out parseError))
        {
            return await OptionErrorAsync(
                explicitPreviewRequested
                    ? "The explicit CSharpDB preview command contains an unsupported option."
                    : parseError!,
                error);
        }
        if (!options.TryGetValue("--catalog", out string? catalogValue))
            return await OptionErrorAsync("Missing required option --catalog.", error);
        bool includeDdl = options.ContainsKey("--ddl");
        bool validateScratch = options.ContainsKey("--scratch");
        if (includeDdl && validateScratch)
        {
            return await OptionErrorAsync(
                "Options --ddl and --scratch cannot be combined.",
                error);
        }

        string format = options.GetValueOrDefault("--format", "text");
        if (!string.Equals(format, "text", StringComparison.OrdinalIgnoreCase) &&
            !string.Equals(format, "json", StringComparison.OrdinalIgnoreCase))
        {
            return await OptionErrorAsync(
                explicitPreviewRequested
                    ? "Unsupported explicit preview format."
                    : $"Unsupported preview format '{format}'.",
                error);
        }

        if (!includeDdl && !validateScratch)
        {
            return await RunDefaultPreviewAsync(
                args[2],
                catalogValue,
                format,
                output,
                error,
                ct);
        }

        try
        {
            return await RunExplicitPreviewAsync(
                args[2],
                catalogValue,
                format,
                includeDdl,
                output,
                error,
                dependencies,
                ct);
        }
        catch (OperationCanceledException) when (ct.IsCancellationRequested)
        {
            throw;
        }
        catch (CSharpDbDdlPreviewLimitException limitError)
        {
            throw new MigrationCliSafeException(
                "MIG-CSHARPDB-DDL-PREVIEW-LIMIT-001",
                "The CSharpDB DDL preview exceeded a production safety limit.",
                limitError);
        }
        catch (Exception previewError) when (
            IsRecoverableCliException(previewError))
        {
            throw new MigrationCliSafeException(
                "MIG-CSHARPDB-DDL-PREVIEW-001",
                "The explicit CSharpDB preview could not be produced safely.",
                previewError);
        }
    }

    private static async ValueTask<int> RunDdlCheckAsync(
        string[] args,
        TextWriter output,
        TextWriter error,
        MigrationCommandDependencies dependencies,
        CancellationToken ct)
    {
        if (args.Length < 3 ||
            args[2].StartsWith("--", StringComparison.Ordinal) ||
            string.IsNullOrWhiteSpace(args[2]))
        {
            return await OptionErrorAsync(
                "Missing DDL script path.",
                error);
        }
        if (!TryParseOptions(
                args,
                3,
                out Dictionary<string, string> options,
                out string? parseError))
        {
            return await OptionErrorAsync(
                SafeDdlCheckOptionError(parseError!),
                error);
        }
        if (!RequireOnly(
                options,
                ["--dialect", "--format"],
                out parseError))
        {
            return await OptionErrorAsync(
                "The DDL compatibility command contains an unsupported option.",
                error);
        }
        if (!options.TryGetValue("--dialect", out string? dialect) ||
            string.IsNullOrWhiteSpace(dialect))
        {
            return await OptionErrorAsync(
                "Missing required option --dialect.",
                error);
        }
        bool isCSharpDbDialect = string.Equals(
            dialect,
            "csharpdb",
            StringComparison.OrdinalIgnoreCase);
        bool isTsqlDialect = string.Equals(
            dialect,
            "tsql",
            StringComparison.OrdinalIgnoreCase);
        if (!isCSharpDbDialect && !isTsqlDialect)
        {
            return await OptionErrorAsync(
                "This DDL compatibility command supports only the csharpdb and tsql dialects.",
                error);
        }

        string format = options.GetValueOrDefault("--format", "text");
        if (!string.Equals(
                format,
                "text",
                StringComparison.OrdinalIgnoreCase) &&
            !string.Equals(
                format,
                "json",
                StringComparison.OrdinalIgnoreCase))
        {
            return await OptionErrorAsync(
                "Unsupported DDL compatibility output format.",
                error);
        }

        string script;
        try
        {
            script = await ReadBoundedDdlScriptAsync(
                args[2],
                ct);
        }
        catch (OperationCanceledException) when (ct.IsCancellationRequested)
        {
            throw;
        }
        catch (DdlScriptLimitException limitError)
        {
            throw new MigrationCliSafeException(
                "MIG-CSHARPDB-DDL-CHECK-LIMIT-001",
                "The DDL script exceeds a production size limit.",
                limitError);
        }
        catch (DecoderFallbackException encodingError)
        {
            throw new MigrationCliSafeException(
                "MIG-CSHARPDB-DDL-CHECK-ENCODING-001",
                "The DDL script is not valid UTF-8.",
                encodingError);
        }
        catch (Exception readError) when (
            IsRecoverableCliException(readError))
        {
            throw new MigrationCliSafeException(
                "MIG-CSHARPDB-DDL-CHECK-READ-001",
                "The DDL script could not be read safely.",
                readError);
        }

        CSharpDbDdlCompatibilityReport report;
        try
        {
            if (isCSharpDbDialect)
            {
                report = await dependencies.AnalyzeCSharpDbDdlAsync(
                    script,
                    ct);
            }
            else
            {
                string targetCSharpDbVersion =
                    CSharpDbCapabilityCatalogLoader.CurrentTargetVersion;
                SqlServerDdlWorkerResult result =
                    await dependencies.AnalyzeTsqlDdlAsync(
                        script,
                        targetCSharpDbVersion,
                        ct);
                ArgumentNullException.ThrowIfNull(result);
                if (result.Status is
                    SqlServerDdlWorkerStatus.Missing or
                    SqlServerDdlWorkerStatus.Incompatible)
                {
                    throw new MigrationCliSafeException(
                        "MIG-TSQL-CLI-ADAPTER-001",
                        "The optional T-SQL DDL analyzer is unavailable or incompatible.",
                        new InvalidOperationException(
                            "The T-SQL worker boundary is unavailable."));
                }
                if (result.Status ==
                    SqlServerDdlWorkerStatus.AnalysisFailed)
                {
                    throw new MigrationCliSafeException(
                        "MIG-TSQL-CLI-DDL-CHECK-001",
                        "The T-SQL DDL compatibility proof could not be produced safely.",
                        new InvalidOperationException(
                            "The T-SQL worker could not analyze the source."));
                }
                if (result.Status !=
                        SqlServerDdlWorkerStatus.Success ||
                    !SqlServerWorkerClient.TrySanitizeDdlReport(
                        result.Report,
                        targetCSharpDbVersion,
                        script,
                        out CSharpDbDdlCompatibilityReport?
                            sanitizedReport))
                {
                    throw new MigrationCliSafeException(
                        "MIG-TSQL-CLI-ADAPTER-001",
                        "The optional T-SQL DDL analyzer is unavailable or incompatible.",
                        new InvalidDataException(
                            "The T-SQL worker returned an invalid report contract."));
                }

                report = sanitizedReport!;
            }
            ArgumentNullException.ThrowIfNull(report);
        }
        catch (OperationCanceledException) when (ct.IsCancellationRequested)
        {
            throw;
        }
        catch (MigrationCliSafeException)
        {
            throw;
        }
        catch (Exception analysisError) when (
            IsRecoverableCliException(analysisError))
        {
            throw new MigrationCliSafeException(
                isTsqlDialect
                    ? "MIG-TSQL-CLI-DDL-CHECK-001"
                    : "MIG-CSHARPDB-DDL-CHECK-001",
                isTsqlDialect
                    ? "The T-SQL DDL compatibility proof could not be produced safely."
                    : "The DDL compatibility proof could not be produced safely.",
                analysisError);
        }

        if (string.Equals(
                format,
                "json",
                StringComparison.OrdinalIgnoreCase))
        {
            ct.ThrowIfCancellationRequested();
            string json = JsonSerializer.Serialize(report, JsonOptions);
            ct.ThrowIfCancellationRequested();
            await output.WriteLineAsync(
                json);
        }
        else
        {
            await WriteTextDdlCompatibilityAsync(
                output,
                report,
                ct);
        }
        ct.ThrowIfCancellationRequested();

        return DdlCompatibilityExitCode(report.Status);
    }

    private static async ValueTask<int> RunTypeMapAsync(
        string[] args,
        TextWriter output,
        TextWriter error,
        CancellationToken ct)
    {
        if (args.Length < 3 ||
            args[2].StartsWith("--", StringComparison.Ordinal) ||
            string.IsNullOrWhiteSpace(args[2]))
        {
            return await OptionErrorAsync(
                "Missing migration catalog path.",
                error);
        }
        if (!TryParseOptions(
                args,
                3,
                out Dictionary<string, string> options,
                out string? parseError))
        {
            return await OptionErrorAsync(parseError!, error);
        }
        if (!RequireOnly(
                options,
                ["--out", "--profile", "--custom-map", "--format"],
                out parseError))
        {
            return await OptionErrorAsync(
                "The data type mapping command contains an unsupported option.",
                error);
        }
        if (!options.TryGetValue("--out", out string? outputValue) ||
            string.IsNullOrWhiteSpace(outputValue))
        {
            return await OptionErrorAsync(
                "Missing required option --out.",
                error);
        }

        MigrationMappingProfile profile =
            options.GetValueOrDefault("--profile", "preserve")
                .ToLowerInvariant() switch
            {
                "preserve" => MigrationMappingProfile.Preserve,
                "queryable" => MigrationMappingProfile.Queryable,
                "custom" => MigrationMappingProfile.Custom,
                _ => (MigrationMappingProfile)(-1),
            };
        if (!Enum.IsDefined(profile))
        {
            return await OptionErrorAsync(
                "Unsupported data type mapping profile.",
                error);
        }

        bool hasCustomMap = options.TryGetValue(
            "--custom-map",
            out string? customMapValue);
        if (profile == MigrationMappingProfile.Custom && !hasCustomMap)
        {
            return await OptionErrorAsync(
                "The custom data type mapping profile requires --custom-map.",
                error);
        }
        if (profile != MigrationMappingProfile.Custom && hasCustomMap)
        {
            return await OptionErrorAsync(
                "--custom-map can be used only with the custom data type mapping profile.",
                error);
        }

        string format = options.GetValueOrDefault("--format", "text");
        if (!IsCompatibilityOutputFormat(format))
        {
            return await OptionErrorAsync(
                "Unsupported data type mapping output format.",
                error);
        }

        string catalogPath = Path.GetFullPath(args[2]);
        string outputPath = Path.GetFullPath(outputValue);
        string? customMapPath = hasCustomMap
            ? Path.GetFullPath(customMapValue!)
            : null;
        var protectedPaths = new List<string>
        {
            catalogPath,
            outputPath,
        };
        if (customMapPath is not null)
            protectedPaths.Add(customMapPath);
        if (ContainsEquivalentPaths(protectedPaths))
        {
            return await OptionErrorAsync(
                "The catalog, custom map, and report must use different files.",
                error);
        }

        MigrationCatalog catalog;
        try
        {
            catalog = MigrationArtifactSerializer.DeserializeCatalog(
                await ReadBoundedMigrationContractArtifactAsync(
                    catalogPath,
                    ct));
        }
        catch (OperationCanceledException) when (ct.IsCancellationRequested)
        {
            throw;
        }
        catch (Exception artifactError) when (
            IsRecoverableCliException(artifactError))
        {
            throw new MigrationCliSafeException(
                "MIG-TYPE-MAP-CATALOG-001",
                "The migration catalog could not be loaded safely for data type mapping.",
                artifactError);
        }

        IReadOnlyDictionary<string, DbType> customTargetTypes =
            new Dictionary<string, DbType>(StringComparer.Ordinal);
        if (customMapPath is not null)
        {
            try
            {
                customTargetTypes = ParseCustomTypeMap(
                    await ReadBoundedMigrationContractArtifactAsync(
                        customMapPath,
                        ct));
            }
            catch (OperationCanceledException) when (
                ct.IsCancellationRequested)
            {
                throw;
            }
            catch (Exception mapError) when (
                IsRecoverableCliException(mapError))
            {
                throw new MigrationCliSafeException(
                    "MIG-TYPE-MAP-CUSTOM-001",
                    "The custom data type map could not be loaded safely.",
                    mapError);
            }
        }

        DataTypeMappingReport report;
        try
        {
            report = new DataTypeMappingReportService().Create(
                catalog,
                new DataTypeMappingReportOptions
                {
                    Profile = profile,
                    CustomTargetTypes = customTargetTypes,
                });
        }
        catch (Exception mappingError) when (
            IsRecoverableCliException(mappingError))
        {
            throw new MigrationCliSafeException(
                "MIG-TYPE-MAP-ANALYSIS-001",
                "The data type mapping report could not be produced safely.",
                mappingError);
        }

        string rendered = string.Equals(
                format,
                "json",
                StringComparison.OrdinalIgnoreCase)
            ? CompatibilityReportFormatter.ToJson(report)
            : CompatibilityReportFormatter.ToText(report);
        await WriteNewCompatibilityArtifactAsync(
            outputPath,
            rendered,
            "MIG-TYPE-MAP-OUTPUT-001",
            "The data type mapping report could not be published without overwriting an existing file.",
            ct);
        await output.WriteLineAsync(
            $"Data type mapping report: total={report.Summary.Total} | exact={report.Summary.Exact} | lossless-reencoded={report.Summary.LosslessReencoded} | lossy={report.Summary.Lossy} | unsupported={report.Summary.Unsupported} | full-stream-validation={report.Summary.RequiresFullStreamValidation}");

        if (report.Summary.Unsupported != 0)
            return InspectorCommandRunner.ExitError;
        if (report.Summary.Lossy != 0 ||
            report.Summary.RequiresFullStreamValidation != 0)
        {
            return InspectorCommandRunner.ExitWarn;
        }
        return InspectorCommandRunner.ExitOk;
    }

    private static async ValueTask<int> RunQueryCheckAsync(
        string[] args,
        TextWriter output,
        TextWriter error,
        MigrationCommandDependencies dependencies,
        CancellationToken ct)
    {
        if (args.Length < 3 ||
            args[2].StartsWith("--", StringComparison.Ordinal) ||
            string.IsNullOrWhiteSpace(args[2]))
        {
            return await OptionErrorAsync(
                "Missing query file path.",
                error);
        }
        if (!TryParseOptions(
                args,
                3,
                out Dictionary<string, string> options,
                out string? parseError))
        {
            return await OptionErrorAsync(parseError!, error);
        }
        if (!RequireOnly(
                options,
                [
                    "--dialect",
                    "--out",
                    "--query-id",
                    "--compatibility-level",
                    "--format",
                ],
                out parseError))
        {
            return await OptionErrorAsync(
                "The query compatibility command contains an unsupported option.",
                error);
        }
        if (!options.TryGetValue("--dialect", out string? dialectValue) ||
            string.IsNullOrWhiteSpace(dialectValue))
        {
            return await OptionErrorAsync(
                "Missing required option --dialect.",
                error);
        }
        if (!options.TryGetValue("--out", out string? outputValue) ||
            string.IsNullOrWhiteSpace(outputValue))
        {
            return await OptionErrorAsync(
                "Missing required option --out.",
                error);
        }

        QuerySourceDialect dialect =
            dialectValue.ToLowerInvariant() switch
            {
                "csharpdb" => QuerySourceDialect.CSharpDb,
                "tsql" => QuerySourceDialect.SqlServerTsql,
                "mysql" => QuerySourceDialect.MySql,
                "sqlite" => QuerySourceDialect.Sqlite,
                "access" => QuerySourceDialect.Access,
                _ => (QuerySourceDialect)(-1),
            };
        if (!Enum.IsDefined(dialect))
        {
            return await OptionErrorAsync(
                "Unsupported query source dialect.",
                error);
        }

        int compatibilityLevel = 160;
        if (options.TryGetValue(
                "--compatibility-level",
                out string? compatibilityValue))
        {
            if (dialect != QuerySourceDialect.SqlServerTsql)
            {
                return await OptionErrorAsync(
                    "--compatibility-level can be used only with the tsql dialect.",
                    error);
            }
            if (!int.TryParse(
                    compatibilityValue,
                    NumberStyles.None,
                    CultureInfo.InvariantCulture,
                    out compatibilityLevel) ||
                compatibilityLevel is not (150 or 160 or 170))
            {
                return await OptionErrorAsync(
                    "SQL Server compatibility level must be 150, 160, or 170.",
                    error);
            }
        }

        string format = options.GetValueOrDefault("--format", "text");
        if (!IsCompatibilityOutputFormat(format))
        {
            return await OptionErrorAsync(
                "Unsupported query compatibility output format.",
                error);
        }

        string queryPath = Path.GetFullPath(args[2]);
        string outputPath = Path.GetFullPath(outputValue);
        if (PathsAreEquivalent(queryPath, outputPath))
        {
            return await OptionErrorAsync(
                "The query input and compatibility report must use different files.",
                error);
        }
        string queryId = options.GetValueOrDefault(
            "--query-id",
            Path.GetFileNameWithoutExtension(queryPath));
        if (string.IsNullOrWhiteSpace(queryId))
        {
            return await OptionErrorAsync(
                "The query id must not be empty.",
                error);
        }

        string query;
        try
        {
            query = await ReadBoundedDdlScriptAsync(
                queryPath,
                ct);
        }
        catch (OperationCanceledException) when (ct.IsCancellationRequested)
        {
            throw;
        }
        catch (DdlScriptLimitException limitError)
        {
            throw new MigrationCliSafeException(
                "MIG-QUERY-CLI-LIMIT-001",
                "The query exceeds a production size limit.",
                limitError);
        }
        catch (DecoderFallbackException encodingError)
        {
            throw new MigrationCliSafeException(
                "MIG-QUERY-CLI-ENCODING-001",
                "The query is not valid UTF-8.",
                encodingError);
        }
        catch (Exception readError) when (
            IsRecoverableCliException(readError))
        {
            throw new MigrationCliSafeException(
                "MIG-QUERY-CLI-READ-001",
                "The query could not be read safely.",
                readError);
        }

        QueryCompatibilityReport report;
        try
        {
            if (dialect == QuerySourceDialect.SqlServerTsql)
            {
                string targetVersion =
                    CSharpDbCapabilityCatalogLoader
                        .CurrentTargetVersion;
                SqlServerQueryWorkerResult result =
                    await dependencies.AnalyzeTsqlQueryAsync(
                        query,
                        queryId,
                        compatibilityLevel,
                        targetVersion,
                        ct);
                ArgumentNullException.ThrowIfNull(result);
                if (result.Status is
                    SqlServerQueryWorkerStatus.Missing or
                    SqlServerQueryWorkerStatus.Incompatible)
                {
                    throw new MigrationCliSafeException(
                        "MIG-TSQL-CLI-ADAPTER-001",
                        "The optional T-SQL query analyzer is unavailable or incompatible.",
                        new InvalidOperationException(
                            "The T-SQL worker boundary is unavailable."));
                }
                if (result.Status ==
                    SqlServerQueryWorkerStatus.AnalysisFailed)
                {
                    throw new MigrationCliSafeException(
                        "MIG-TSQL-CLI-QUERY-CHECK-001",
                        "The T-SQL query compatibility report could not be produced safely.",
                        new InvalidOperationException(
                            "The T-SQL worker could not analyze the source."));
                }
                if (result.Status !=
                        SqlServerQueryWorkerStatus.Success ||
                    !SqlServerWorkerClient.TrySanitizeQueryReport(
                        result.Report,
                        targetVersion,
                        queryId,
                        query,
                        out QueryCompatibilityReport?
                            sanitizedReport))
                {
                    throw new MigrationCliSafeException(
                        "MIG-TSQL-CLI-ADAPTER-001",
                        "The optional T-SQL query analyzer is unavailable or incompatible.",
                        new InvalidDataException(
                            "The T-SQL worker returned an invalid report contract."));
                }

                report = sanitizedReport!;
            }
            else
            {
                report = new QueryCompatibilityAnalyzer().Analyze(
                    new QueryCompatibilityRequest
                    {
                        SqlServerCompatibilityLevel =
                            compatibilityLevel,
                        Queries =
                        [
                            new QueryCompatibilityInput
                            {
                                QueryId = queryId,
                                SourceDialect = dialect,
                                Sql = query,
                            },
                        ],
                    },
                    ct);
            }
        }
        catch (OperationCanceledException) when (ct.IsCancellationRequested)
        {
            throw;
        }
        catch (MigrationCliSafeException)
        {
            throw;
        }
        catch (Exception analysisError) when (
            IsRecoverableCliException(analysisError))
        {
            throw new MigrationCliSafeException(
                dialect == QuerySourceDialect.SqlServerTsql
                    ? "MIG-TSQL-CLI-QUERY-CHECK-001"
                    : "MIG-QUERY-CLI-ANALYSIS-001",
                dialect == QuerySourceDialect.SqlServerTsql
                    ? "The T-SQL query compatibility report could not be produced safely."
                    : "The query compatibility report could not be produced safely.",
                analysisError);
        }

        string rendered = string.Equals(
                format,
                "json",
                StringComparison.OrdinalIgnoreCase)
            ? CompatibilityReportFormatter.ToJson(report)
            : CompatibilityReportFormatter.ToText(report);
        await WriteNewCompatibilityArtifactAsync(
            outputPath,
            rendered,
            "MIG-QUERY-CLI-OUTPUT-001",
            "The query compatibility report could not be published without overwriting an existing file.",
            ct);
        await output.WriteLineAsync(
            $"Query compatibility report: total={report.Summary.Total} | compatible={report.Summary.Compatible} | rewrite={report.Summary.CompatibleWithRewrite} | conditional={report.Summary.Conditional} | unsupported={report.Summary.Unsupported} | unknown={report.Summary.Unknown}");

        if (report.Summary.Unsupported != 0 ||
            report.Summary.Unknown != 0)
        {
            return InspectorCommandRunner.ExitError;
        }
        if (report.Summary.CompatibleWithRewrite != 0 ||
            report.Summary.Conditional != 0)
        {
            return InspectorCommandRunner.ExitWarn;
        }
        return InspectorCommandRunner.ExitOk;
    }

    private static bool IsCompatibilityOutputFormat(string format) =>
        string.Equals(
            format,
            "text",
            StringComparison.OrdinalIgnoreCase) ||
        string.Equals(
            format,
            "json",
            StringComparison.OrdinalIgnoreCase);

    private static IReadOnlyDictionary<string, DbType>
        ParseCustomTypeMap(string payload)
    {
        using JsonDocument document = JsonDocument.Parse(
            payload,
            new JsonDocumentOptions
            {
                AllowTrailingCommas = false,
                CommentHandling = JsonCommentHandling.Disallow,
                MaxDepth = 32,
            });
        if (document.RootElement.ValueKind != JsonValueKind.Object)
        {
            throw new InvalidDataException(
                "The custom data type map must be a JSON object.");
        }

        var mappings =
            new Dictionary<string, DbType>(StringComparer.Ordinal);
        foreach (JsonProperty property in
                 document.RootElement.EnumerateObject())
        {
            if (mappings.Count >= 100_000)
            {
                throw new InvalidDataException(
                    "The custom data type map exceeds its entry limit.");
            }
            if (property.Value.ValueKind != JsonValueKind.String ||
                !Enum.TryParse(
                    property.Value.GetString(),
                    ignoreCase: true,
                    out DbType targetType) ||
                !Enum.IsDefined(targetType) ||
                targetType is not (
                    DbType.Integer or
                    DbType.Real or
                    DbType.Decimal or
                    DbType.Text or
                    DbType.Blob))
            {
                throw new InvalidDataException(
                    $"Custom target type for '{property.Name}' is invalid.");
            }
            if (!mappings.TryAdd(property.Name, targetType))
            {
                throw new InvalidDataException(
                    $"The custom data type map contains duplicate object id '{property.Name}'.");
            }
        }
        return mappings;
    }

    private static async ValueTask WriteNewCompatibilityArtifactAsync(
        string outputPath,
        string content,
        string code,
        string message,
        CancellationToken ct)
    {
        try
        {
            await WriteNewArtifactAsync(
                outputPath,
                content,
                ct);
        }
        catch (OperationCanceledException) when (ct.IsCancellationRequested)
        {
            throw;
        }
        catch (Exception outputError) when (
            IsRecoverableCliException(outputError))
        {
            throw new MigrationCliSafeException(
                code,
                message,
                outputError);
        }
    }

    private static string SafeDdlCheckOptionError(string error)
    {
        if (error.StartsWith(
                "Duplicate option",
                StringComparison.Ordinal))
        {
            return "Duplicate option in the DDL compatibility command.";
        }
        if (error.StartsWith(
                "Missing value for",
                StringComparison.Ordinal))
        {
            return "A DDL compatibility option is missing its value.";
        }
        if (error.StartsWith(
                "Unexpected positional argument",
                StringComparison.Ordinal))
        {
            return "The DDL compatibility command contains an unexpected positional argument.";
        }

        return "The DDL compatibility options are invalid.";
    }

    private static async ValueTask<string> ReadBoundedDdlScriptAsync(
        string path,
        CancellationToken ct)
    {
        ct.ThrowIfCancellationRequested();
        await using var stream = new FileStream(
            path,
            FileMode.Open,
            FileAccess.Read,
            FileShare.Read,
            bufferSize: 4096,
            FileOptions.Asynchronous | FileOptions.SequentialScan);
        if (stream.CanSeek &&
            stream.Length > SqlScriptParserOptions.HardMaxScriptUtf8Bytes)
        {
            throw new DdlScriptLimitException();
        }

        int maximumBytes =
            SqlScriptParserOptions.HardMaxScriptUtf8Bytes;
        byte[] payload =
            ArrayPool<byte>.Shared.Rent(maximumBytes);
        byte[] overflowProbe = new byte[1];
        int totalBytes = 0;
        try
        {
            while (totalBytes < maximumBytes)
            {
                ct.ThrowIfCancellationRequested();
                int read = await stream.ReadAsync(
                    payload.AsMemory(
                        totalBytes,
                        Math.Min(
                            64 * 1024,
                            maximumBytes - totalBytes)),
                    ct);
                if (read == 0)
                    break;

                totalBytes += read;
            }
            if (totalBytes == maximumBytes)
            {
                ct.ThrowIfCancellationRequested();
                int extra = await stream.ReadAsync(
                    overflowProbe.AsMemory(),
                    ct);
                if (extra != 0)
                    throw new DdlScriptLimitException();
            }

            ct.ThrowIfCancellationRequested();
            ReadOnlySpan<byte> bytes =
                payload.AsSpan(0, totalBytes);
            if (bytes.Length >= 3 &&
                bytes[0] == 0xEF &&
                bytes[1] == 0xBB &&
                bytes[2] == 0xBF)
            {
                bytes = bytes[3..];
            }
            if (Utf8NoBomStrict.GetCharCount(bytes) >
                SqlScriptParserOptions.HardMaxScriptCharacters)
            {
                throw new DdlScriptLimitException();
            }
            return Utf8NoBomStrict.GetString(bytes);
        }
        finally
        {
            CryptographicOperations.ZeroMemory(overflowProbe);
            ArrayPool<byte>.Shared.Return(
                payload,
                clearArray: true);
        }
    }

    private static async ValueTask WriteTextDdlCompatibilityAsync(
        TextWriter output,
        CSharpDbDdlCompatibilityReport report,
        CancellationToken ct)
    {
        ct.ThrowIfCancellationRequested();
        await output.WriteLineAsync($"Format: {report.Format}");
        await output.WriteLineAsync(
            $"Status: {CliToken(report.Status)}");
        await output.WriteLineAsync($"Dialect: {report.Dialect}");
        await output.WriteLineAsync(
            $"Source grammar: {report.SourceGrammar}");
        await output.WriteLineAsync(
            $"Target CSharpDB version: {report.TargetCSharpDbVersion}");
        await output.WriteLineAsync(
            $"Highest evidence: {CliToken(report.HighestEvidence)}");
        await output.WriteLineAsync($"Rule ID: {report.RuleId}");
        await output.WriteLineAsync(
            $"Statements: total={report.StatementCount} | proven={report.ProvenStatementCount} | candidate-actions={report.CandidateActionCount}");
        await output.WriteLineAsync(
            $"Capability digest: {report.CapabilityDigest}");
        await output.WriteLineAsync(
            $"Script digest: {report.ScriptDigest}");
        await output.WriteLineAsync(
            $"Catalog digest: {DigestOrNone(report.CatalogDigest)}");
        await output.WriteLineAsync(
            $"Plan contract digest: {DigestOrNone(report.PlanContractDigest)}");
        await output.WriteLineAsync(
            $"Generated DDL digest: {DigestOrNone(report.GeneratedDdlDigest)}");
        await output.WriteLineAsync(
            $"Expected schema digest: {DigestOrNone(report.ExpectedSchemaDigest)}");
        await output.WriteLineAsync(
            $"Actual schema digest: {DigestOrNone(report.ActualSchemaDigest)}");

        foreach (var statement in report.Statements)
        {
            ct.ThrowIfCancellationRequested();
            await output.WriteLineAsync(
                $"Statement: index={statement.Index} | kind={statement.Kind} | status={CliToken(statement.Status)} | evidence={CliToken(statement.Evidence)} | rule={statement.RuleId} | span={FormatSourceSpan(statement.Span)}");
        }
        foreach (var diagnostic in report.Diagnostics)
        {
            ct.ThrowIfCancellationRequested();
            await output.WriteLineAsync(
                $"Diagnostic: ordinal={diagnostic.Ordinal} | id={diagnostic.DiagnosticId} | rule={diagnostic.RuleId} | severity={CliToken(diagnostic.Severity)} | status={CliToken(diagnostic.Status)} | evidence={CliToken(diagnostic.Evidence)} | statement={NullableIntOrNone(diagnostic.StatementIndex)} | span={FormatSourceSpan(diagnostic.SourceSpan)} | summary={diagnostic.Summary} | remediation={diagnostic.Remediation ?? "none"}");
        }
        foreach (var difference in report.Differences)
        {
            ct.ThrowIfCancellationRequested();
            await output.WriteLineAsync(
                $"Difference: ordinal={difference.Ordinal} | object={difference.ObjectIdentityDigest} | kind={CliToken(difference.Kind)} | expected={DigestOrNone(difference.ExpectedDefinitionDigest)} | actual={DigestOrNone(difference.ActualDefinitionDigest)}");
        }
    }

    private static int DdlCompatibilityExitCode(
        MigrationCompatibilityStatus status) =>
        status switch
        {
            MigrationCompatibilityStatus.Compatible =>
                InspectorCommandRunner.ExitOk,
            MigrationCompatibilityStatus.CompatibleWithRewrite or
            MigrationCompatibilityStatus.Conditional =>
                InspectorCommandRunner.ExitWarn,
            MigrationCompatibilityStatus.Unsupported or
            MigrationCompatibilityStatus.Unknown =>
                InspectorCommandRunner.ExitError,
            _ => throw new InvalidDataException(
                "The DDL compatibility report has an unknown status."),
        };

    private static string DigestOrNone(string? digest) =>
        digest ?? "none";

    private static string NullableIntOrNone(int? value) =>
        value?.ToString(CultureInfo.InvariantCulture) ?? "none";

    private static string FormatSourceSpan(
        MigrationSourceSpan? span) =>
        span is null
            ? "none"
            : string.Create(
                CultureInfo.InvariantCulture,
                $"start:{NullableIntOrNone(span.Start)},length:{NullableIntOrNone(span.Length)},line:{NullableIntOrNone(span.Line)},column:{NullableIntOrNone(span.Column)}");

    private static string CliToken<T>(T? value)
        where T : struct, Enum =>
        value is null
            ? "none"
            : CliToken(value.Value);

    private static string CliToken<T>(T value)
        where T : struct, Enum
    {
        string name = value.ToString();
        var result = new StringBuilder(name.Length + 4);
        for (int index = 0; index < name.Length; index++)
        {
            char character = name[index];
            if (index > 0 && char.IsUpper(character))
                result.Append('-');
            result.Append(char.ToLowerInvariant(character));
        }

        return result.ToString();
    }

    private static async ValueTask<int> RunDefaultPreviewAsync(
        string planValue,
        string catalogValue,
        string format,
        TextWriter output,
        TextWriter error,
        CancellationToken ct)
    {
        string planPath = Path.GetFullPath(planValue);
        string catalogPath = Path.GetFullPath(catalogValue);
        MigrationCatalog catalog = MigrationArtifactSerializer.DeserializeCatalog(
            await File.ReadAllTextAsync(catalogPath, ct));
        if (catalog.Source.Kind == MigrationSourceKind.Sqlite &&
            !IsSupportedSqliteV1Catalog(catalog))
        {
            return await OptionErrorAsync(
                SqliteCatalogRouteOnlyMessage,
                error);
        }
        if (catalog.Source.Kind == MigrationSourceKind.LiteDb &&
            !IsSupportedLiteDbV1Catalog(catalog))
        {
            return await OptionErrorAsync(
                LiteDbCatalogRouteOnlyMessage,
                error);
        }
        MigrationPlan plan = MigrationArtifactSerializer.DeserializePlan(
            await File.ReadAllTextAsync(planPath, ct),
            catalog);
        MigrationPlanReadiness readiness =
            MigrationPlanReadinessValidator.Evaluate(plan, catalog);
        PreviewCounts counts = PreviewCounts.Create(plan);
        string status = PreviewStatus(plan, readiness);

        if (string.Equals(format, "json", StringComparison.OrdinalIgnoreCase))
            await WriteJsonPreviewAsync(output, plan, readiness, counts, status);
        else
            await WriteTextPreviewAsync(output, plan, readiness, counts, status);

        return HasReviewFindings(plan, readiness)
            ? InspectorCommandRunner.ExitWarn
            : InspectorCommandRunner.ExitOk;
    }

    private static bool IsExplicitPreviewOptionToken(
        string argument) =>
        string.Equals(
            argument,
            "--ddl",
            StringComparison.OrdinalIgnoreCase) ||
        string.Equals(
            argument,
            "--scratch",
            StringComparison.OrdinalIgnoreCase) ||
        argument.StartsWith(
            "--ddl=",
            StringComparison.OrdinalIgnoreCase) ||
        argument.StartsWith(
            "--scratch=",
            StringComparison.OrdinalIgnoreCase);

    private static async ValueTask<int> RunExplicitPreviewAsync(
        string planValue,
        string catalogValue,
        string format,
        bool includeDdl,
        TextWriter output,
        TextWriter error,
        MigrationCommandDependencies dependencies,
        CancellationToken ct)
    {
        string planPath = Path.GetFullPath(planValue);
        string catalogPath = Path.GetFullPath(catalogValue);
        MigrationCatalog catalog =
            MigrationArtifactSerializer.DeserializeCatalog(
                await ReadBoundedMigrationContractArtifactAsync(
                    catalogPath,
                    ct));
        if (catalog.Source.Kind == MigrationSourceKind.Sqlite &&
            !IsSupportedSqliteV1Catalog(catalog))
        {
            return await OptionErrorAsync(
                SqliteCatalogRouteOnlyMessage,
                error);
        }
        if (catalog.Source.Kind == MigrationSourceKind.LiteDb &&
            !IsSupportedLiteDbV1Catalog(catalog))
        {
            return await OptionErrorAsync(
                LiteDbCatalogRouteOnlyMessage,
                error);
        }
        MigrationPlan plan = MigrationArtifactSerializer.DeserializePlan(
            await ReadBoundedMigrationContractArtifactAsync(
                planPath,
                ct),
            catalog);
        MigrationPlanReadiness readiness =
            MigrationPlanReadinessValidator.Evaluate(plan, catalog);
        int planExitCode = HasReviewFindings(plan, readiness)
            ? InspectorCommandRunner.ExitWarn
            : InspectorCommandRunner.ExitOk;
        CSharpDbDdlPreview ddlPreview =
            dependencies.BuildCSharpDbDdlPreview(
                plan,
                catalog,
                ct);
        ValidateExplicitPreviewBinding(plan, ddlPreview);

        if (includeDdl)
        {
            if (string.Equals(
                    format,
                    "json",
                    StringComparison.OrdinalIgnoreCase))
            {
                await output.WriteLineAsync(
                    JsonSerializer.Serialize(ddlPreview, JsonOptions));
            }
            else
            {
                await WriteTextDdlPreviewAsync(output, ddlPreview);
            }

            return planExitCode;
        }

        CSharpDbDdlScratchValidationReport scratchReport =
            await CSharpDbDdlScratchValidator.ValidateAsync(
                plan,
                catalog,
                ddlPreview,
                cancellationToken: ct);
        if (string.Equals(format, "json", StringComparison.OrdinalIgnoreCase))
        {
            await output.WriteLineAsync(
                JsonSerializer.Serialize(scratchReport, JsonOptions));
        }
        else
        {
            await WriteTextScratchValidationAsync(output, scratchReport);
        }

        return scratchReport.Status ==
            CSharpDbDdlScratchValidationStatus.Passed
                ? planExitCode
                : InspectorCommandRunner.ExitError;
    }

    private static void ValidateExplicitPreviewBinding(
        MigrationPlan plan,
        CSharpDbDdlPreview preview)
    {
        ArgumentNullException.ThrowIfNull(plan);
        ArgumentNullException.ThrowIfNull(preview);
        if (plan.GeneratedDdlDigest is not null &&
            !string.Equals(
                plan.GeneratedDdlDigest,
                preview.GeneratedDdlDigest,
                StringComparison.Ordinal))
        {
            throw new InvalidDataException(
                "The explicit CSharpDB preview does not match the sealed migration plan.");
        }
    }

    private static async ValueTask<string>
        ReadBoundedMigrationContractArtifactAsync(
            string path,
            CancellationToken ct)
    {
        ct.ThrowIfCancellationRequested();
        await using var stream = new FileStream(
            path,
            FileMode.Open,
            FileAccess.Read,
            FileShare.Read,
            bufferSize: 4096,
            FileOptions.Asynchronous | FileOptions.SequentialScan);
        if (stream.CanSeek &&
            stream.Length > MaxMigrationContractArtifactBytes)
        {
            throw new InvalidDataException(
                "The migration contract artifact exceeds its byte limit.");
        }
        using var payload = new MemoryStream();
        byte[] buffer = new byte[64 * 1024];
        long totalBytes = 0;
        while (true)
        {
            ct.ThrowIfCancellationRequested();
            int read = await stream.ReadAsync(
                buffer.AsMemory(),
                ct);
            if (read == 0)
                break;
            if (totalBytes >
                MaxMigrationContractArtifactBytes - read)
            {
                throw new InvalidDataException(
                    "The migration contract artifact exceeds its byte limit.");
            }

            payload.Write(buffer, 0, read);
            totalBytes += read;
        }

        ct.ThrowIfCancellationRequested();
        int payloadLength = checked((int)payload.Length);
        ReadOnlySpan<byte> bytes =
            payload.GetBuffer().AsSpan(0, payloadLength);
        if (bytes.Length >= 3 &&
            bytes[0] == 0xEF &&
            bytes[1] == 0xBB &&
            bytes[2] == 0xBF)
        {
            bytes = bytes[3..];
        }
        return Utf8NoBomStrict.GetString(bytes);
    }

    private static bool ExceedsStrictUtf8ByteLimit(
        string content,
        long byteLimit,
        CancellationToken ct)
    {
        ArgumentNullException.ThrowIfNull(content);
        if (byteLimit < 0)
            throw new ArgumentOutOfRangeException(nameof(byteLimit));

        long byteCount = 0;
        for (int index = 0; index < content.Length; index++)
        {
            if ((index & 0x3FFF) == 0)
                ct.ThrowIfCancellationRequested();

            char character = content[index];
            int encodedBytes;
            if (character <= '\u007F')
            {
                encodedBytes = 1;
            }
            else if (character <= '\u07FF')
            {
                encodedBytes = 2;
            }
            else if (char.IsHighSurrogate(character))
            {
                if (index + 1 >= content.Length ||
                    !char.IsLowSurrogate(content[index + 1]))
                {
                    throw new EncoderFallbackException(
                        "The migration contract artifact contains invalid UTF-16.");
                }

                index++;
                encodedBytes = 4;
            }
            else if (char.IsLowSurrogate(character))
            {
                throw new EncoderFallbackException(
                    "The migration contract artifact contains invalid UTF-16.");
            }
            else
            {
                encodedBytes = 3;
            }

            if (byteCount > byteLimit - encodedBytes)
                return true;
            byteCount += encodedBytes;
        }

        ct.ThrowIfCancellationRequested();
        return false;
    }

    private static async ValueTask<int> RunApplyAsync(
        string[] args,
        TextWriter output,
        TextWriter error,
        CancellationToken ct)
    {
        if (args.Length < 3 || args[2].StartsWith("--", StringComparison.Ordinal))
            return await OptionErrorAsync("Missing plan artifact path.", error);
        if (!TryParseOptions(
                args,
                3,
                ["--resume", "--allow-deterministic-rejects"],
                out Dictionary<string, string> options,
                out string? parseError))
        {
            return await OptionErrorAsync(parseError!, error);
        }
        if (!RequireOnly(
                options,
                [
                    "--catalog",
                    "--target",
                    "--out",
                    "--format",
                    "--resume",
                    "--source-package",
                    "--expected-manifest-digest",
                    "--workspace",
                    "--max-source-bytes",
                    "--allow-deterministic-rejects",
                    "--reject-artifact",
                ],
                out parseError))
        {
            return await OptionErrorAsync(parseError!, error);
        }
        if (!options.TryGetValue("--catalog", out string? catalogValue))
            return await OptionErrorAsync("Missing required option --catalog.", error);
        if (!options.TryGetValue("--target", out string? targetValue))
            return await OptionErrorAsync("Missing required option --target.", error);
        if (!options.TryGetValue("--out", out string? runOutputValue))
            return await OptionErrorAsync("Missing required option --out.", error);
        if (!ValidatePackageOptionShapes(options, out parseError))
            return await OptionErrorAsync(parseError!, error);

        string format = options.GetValueOrDefault("--format", "text");
        if (!string.Equals(format, "text", StringComparison.OrdinalIgnoreCase) &&
            !string.Equals(format, "json", StringComparison.OrdinalIgnoreCase))
        {
            return await OptionErrorAsync($"Unsupported apply format '{format}'.", error);
        }

        string planPath = Path.GetFullPath(args[2]);
        string catalogPath = Path.GetFullPath(catalogValue);
        string targetPath = Path.GetFullPath(targetValue);
        string runOutputPath = Path.GetFullPath(runOutputValue);
        var protectedPaths = new List<string>
        {
            planPath,
            catalogPath,
            targetPath,
            targetPath + ".wal",
            targetPath + ".migration.lock",
            runOutputPath,
        };
        if (options.TryGetValue("--source-package", out string? sourcePackageValue))
            protectedPaths.Add(Path.GetFullPath(sourcePackageValue));
        if (ContainsEquivalentPaths(protectedPaths))
        {
            return await OptionErrorAsync(
                "Plan, catalog, source package, staged target, target companions, and run report must use different files.",
                error);
        }
        MigrationCatalog catalog = MigrationArtifactSerializer.DeserializeCatalog(
            await File.ReadAllTextAsync(catalogPath, ct));
        MigrationPlan plan = MigrationArtifactSerializer.DeserializePlan(
            await File.ReadAllTextAsync(planPath, ct),
            catalog);
        if (!TryValidateRejectExecutionOptions(
                plan,
                catalog,
                options,
                out MigrationRejectArtifactDestinationBinding? rejectArtifactBinding,
                out parseError))
        {
            return await OptionErrorAsync(parseError!, error);
        }
        if (rejectArtifactBinding is not null)
        {
            protectedPaths.Add(rejectArtifactBinding.DestinationPath);
            protectedPaths.Add(rejectArtifactBinding.TemporaryPath);
        }
        bool resolvedPathsCollide =
            options.ContainsKey("--source-package") || rejectArtifactBinding is not null
                ? ContainsEquivalentResolvedPaths(protectedPaths)
                : ContainsEquivalentPaths(protectedPaths);
        if (resolvedPathsCollide)
        {
            return await OptionErrorAsync(
                rejectArtifactBinding is null
                    ? "Plan, catalog, source package, staged target, target companions, and run report must use different files."
                    : "Plan, catalog, source package, staged target, target companions, run report, reject artifact, and reject temporary file must use different paths.",
                error);
        }
        if (!ValidateWorkspacePath(options, protectedPaths, out parseError))
            return await OptionErrorAsync(parseError!, error);
        if (!ValidateSourceOptions(catalog, options, out parseError))
            return await OptionErrorAsync(parseError!, error);
        MigrationPlanReadinessValidator.ValidateForApply(plan, catalog);
        if (plan.Load.RejectMode == MigrationRejectMode.FailFast)
        {
            MigrationApplyPolicyValidator.ValidateForExecution(plan);
        }

        MigrationSourceLease? sourceLease = null;
        IMigrationDataSource? source = null;
        string sourceSnapshotIdentity = "unavailable";
        MigrationApplyResult? completedResult = null;
        MigrationRejectArtifactWriteResult? completedRejectArtifact = null;
        bool runReportPublished = false;
        try
        {
            sourceLease = await OpenMigrationSourceAsync(catalog, options, ct);
            source = sourceLease.Source;
            sourceSnapshotIdentity = source.SnapshotIdentity;
            if (plan.Load.RejectMode == MigrationRejectMode.DeterministicRejects)
            {
                MigrationRejectSourceCapabilityValidator.ValidateForExecution(plan, source);
            }

            await using CSharpDbStagedMigrationTarget target = options.ContainsKey("--resume")
                ? await CSharpDbStagedMigrationTarget.OpenResumeAsync(
                    targetPath,
                    plan,
                    catalog,
                    sourceSnapshotIdentity,
                    cancellationToken: ct)
                : await CSharpDbStagedMigrationTarget.CreateNewAsync(
                    targetPath,
                    plan,
                    catalog,
                    sourceSnapshotIdentity,
                    cancellationToken: ct);

            MigrationApplyResult result = await new MigrationApplyRunner().ApplyAsync(
                new MigrationApplyRequest
                {
                    Plan = plan,
                    Catalog = catalog,
                    Source = source,
                    Target = target,
                },
                ct);
            completedResult = result;
            MigrationRejectArtifactWriteResult? rejectArtifact = null;
            if (rejectArtifactBinding is not null)
            {
                try
                {
                    rejectArtifact = await new MigrationRejectArtifactWriter().WriteAsync(
                        new MigrationRejectArtifactWriteRequest
                        {
                            Plan = plan,
                            Catalog = catalog,
                            Target = target,
                            OutputPath = rejectArtifactBinding.DestinationPath,
                        },
                        ct);
                    ValidateRejectArtifactResult(result, rejectArtifact);
                    completedRejectArtifact = rejectArtifact;
                }
                catch (OperationCanceledException) when (ct.IsCancellationRequested)
                {
                    throw;
                }
                catch (Exception artifactError)
                {
                    await TryWriteAwaitingValidationPublicationFailureReportAsync(
                        runOutputPath,
                        plan,
                        sourceLease,
                        result,
                        rejectArtifact: null,
                        rejectArtifactStatus: "unconfirmed",
                        errorCode: "MIG-APPLY-REJECT-ARTIFACT-001").ConfigureAwait(false);
                    throw new MigrationCliSafeException(
                        "MIG-APPLY-REJECT-ARTIFACT-001",
                        "The required migration reject artifact could not be published.",
                        artifactError);
                }
            }

            long rejectedRows = checked(result.RejectedRowsWritten + result.RejectedRowsSkipped);
            var runReport = new
            {
                Format = "csharpdb-migration-run/v1",
                Status = "awaitingValidation",
                result.TargetIdentity,
                result.PlanDigest,
                result.CatalogDigest,
                plan.CapabilityDigest,
                SourceFingerprint = plan.Source.Fingerprint,
                result.SourceSnapshotIdentity,
                SourcePackageFormat = sourceLease.PackageMetadata?.Format,
                SourcePackageManifestDigest =
                    sourceLease.PackageMetadata?.ManifestDigest,
                SourcePackageIntentManifestDigest =
                    sourceLease.PackageMetadata
                        ?.IntentManifestDigest,
                result.RejectContractVersion,
                result.BatchesWritten,
                result.BatchesSkipped,
                result.RowsWritten,
                result.RowsSkipped,
                RejectedRows = rejectedRows,
                RejectedRowsWritten = rejectArtifact is null
                    ? (long?)null
                    : result.RejectedRowsWritten,
                RejectedRowsSkipped = rejectArtifact is null
                    ? (long?)null
                    : result.RejectedRowsSkipped,
                RejectArtifactFormat = rejectArtifact is null
                    ? null
                    : MigrationRejectLedgerCodec.ArtifactFormat,
                RejectArtifactDigest = rejectArtifact?.ArtifactDigest,
                RejectArtifactBytes = rejectArtifact?.ArtifactBytes,
                RejectArtifactReused = rejectArtifact?.ReusedExistingArtifact,
                TargetSnapshotIdentity = rejectArtifact?.TargetSnapshotIdentity,
                ExcludedObjects = plan.Objects.Count(item => !item.Included),
                result.PeakBufferedRows,
                result.PeakBufferedBytes,
            };
            string runJson = JsonSerializer.Serialize(runReport, JsonOptions);
            await WriteArtifactAsync(runOutputPath, runJson, ct);
            runReportPublished = true;

            if (string.Equals(format, "json", StringComparison.OrdinalIgnoreCase))
            {
                await output.WriteLineAsync(runJson);
            }
            else
            {
                string rejectedStatus = rejectArtifact is null
                    ? string.Empty
                    : $" | rejected={rejectedRows} | rejectArtifact=published";
                await output.WriteLineAsync(
                    $"Status: AWAITING VALIDATION | targetId={result.TargetIdentity} | batches={result.BatchesWritten} written/{result.BatchesSkipped} resumed | rows={result.RowsWritten} written/{result.RowsSkipped} resumed{rejectedStatus} | report={runOutputPath}");
            }

            return plan.Objects.Any(item => !item.Included) || rejectedRows > 0
                ? InspectorCommandRunner.ExitWarn
                : InspectorCommandRunner.ExitOk;
        }
        catch (OperationCanceledException) when (ct.IsCancellationRequested)
        {
            throw;
        }
        catch (Exception ex)
        {
            if (source is null)
            {
                if (plan.Load.RejectMode == MigrationRejectMode.DeterministicRejects &&
                    ex is not MigrationCliSafeException)
                {
                    throw new MigrationCliSafeException(
                        "MIG-APPLY-DETERMINISTIC-SOURCE-001",
                        "The deterministic-reject migration source could not be opened and verified.",
                        ex);
                }

                throw;
            }

            if (plan.Load.RejectMode == MigrationRejectMode.DeterministicRejects &&
                completedResult is not null &&
                ex is not MigrationCliSafeException &&
                !runReportPublished)
            {
                await TryWriteAwaitingValidationPublicationFailureReportAsync(
                    runOutputPath,
                    plan,
                    sourceLease!,
                    completedResult,
                    completedRejectArtifact,
                    rejectArtifactStatus: completedRejectArtifact is null
                        ? "unconfirmed"
                        : "published",
                    errorCode: "MIG-APPLY-RUN-REPORT-001").ConfigureAwait(false);
                throw new MigrationCliSafeException(
                    "MIG-APPLY-RUN-REPORT-001",
                    "The migration completed in staged form, but its run report could not be published.",
                    ex);
            }

            if (plan.Load.RejectMode == MigrationRejectMode.DeterministicRejects &&
                runReportPublished &&
                ex is not MigrationCliSafeException)
            {
                throw new MigrationCliSafeException(
                    "MIG-APPLY-OUTPUT-001",
                    "The migration completed in staged form and its run report was published, but command output failed.",
                    ex);
            }

            if (ex is not MigrationCliSafeException ||
                !string.Equals(
                    ((MigrationCliSafeException)ex).Code,
                    "MIG-APPLY-REJECT-ARTIFACT-001",
                    StringComparison.Ordinal))
            {
                await TryWriteFailureReportAsync(
                    runOutputPath,
                    plan,
                    sourceSnapshotIdentity,
                    ex).ConfigureAwait(false);
            }

            if (plan.Load.RejectMode == MigrationRejectMode.DeterministicRejects &&
                ex is not MigrationCliSafeException)
            {
                throw new MigrationCliSafeException(
                    "MIG-APPLY-DETERMINISTIC-001",
                    "The deterministic-reject migration apply operation failed.",
                    ex);
            }

            throw;
        }
        finally
        {
            if (sourceLease is not null)
            {
                try
                {
                    await sourceLease.DisposeAsync();
                }
                catch (Exception disposeError) when (
                    plan.Load.RejectMode == MigrationRejectMode.DeterministicRejects &&
                    disposeError is not
                        (OutOfMemoryException or StackOverflowException or AccessViolationException))
                {
                    throw new MigrationCliSafeException(
                        "MIG-APPLY-DETERMINISTIC-CLEANUP-001",
                        runReportPublished
                            ? "The migration completed in staged form and its run report was published, but the deterministic-reject source could not be closed safely."
                            : completedResult is not null
                                ? "The migration completed in staged form, but the deterministic-reject source could not be closed safely."
                                : "The deterministic-reject migration source could not be closed safely.",
                        disposeError);
                }
            }
        }
    }

    private static async ValueTask<int> RunValidateAsync(
        string[] args,
        TextWriter output,
        TextWriter error,
        CancellationToken ct)
    {
        if (args.Length < 3 || args[2].StartsWith("--", StringComparison.Ordinal))
            return await OptionErrorAsync("Missing plan artifact path.", error);
        if (!TryParseOptions(
                args,
                3,
                ["--allow-deterministic-rejects"],
                out Dictionary<string, string> options,
                out string? parseError))
            return await OptionErrorAsync(parseError!, error);
        if (!RequireOnly(
                options,
                [
                    "--catalog",
                    "--target",
                    "--out",
                    "--level",
                    "--spill-dir",
                    "--format",
                    "--source-package",
                    "--expected-manifest-digest",
                    "--workspace",
                    "--max-source-bytes",
                    "--allow-deterministic-rejects",
                    "--reject-artifact",
                ],
                out parseError))
        {
            return await OptionErrorAsync(parseError!, error);
        }
        if (!options.TryGetValue("--catalog", out string? catalogValue))
            return await OptionErrorAsync("Missing required option --catalog.", error);
        if (!options.TryGetValue("--target", out string? targetValue))
            return await OptionErrorAsync("Missing required option --target.", error);
        if (!options.TryGetValue("--out", out string? reportValue))
            return await OptionErrorAsync("Missing required option --out.", error);
        if (!ValidatePackageOptionShapes(options, out parseError))
            return await OptionErrorAsync(parseError!, error);

        string format = options.GetValueOrDefault("--format", "text");
        if (!string.Equals(format, "text", StringComparison.OrdinalIgnoreCase) &&
            !string.Equals(format, "json", StringComparison.OrdinalIgnoreCase))
        {
            return await OptionErrorAsync($"Unsupported validate format '{format}'.", error);
        }

        MigrationValidationLevel? requestedLevel = null;
        if (options.TryGetValue("--level", out string? levelValue))
        {
            requestedLevel = levelValue.ToLowerInvariant() switch
            {
                "schema" => MigrationValidationLevel.Schema,
                "count" => MigrationValidationLevel.Count,
                "checksum" => MigrationValidationLevel.Checksum,
                _ => null,
            };
            if (requestedLevel is null)
                return await OptionErrorAsync($"Unsupported validation level '{levelValue}'.", error);
        }

        string planPath = Path.GetFullPath(args[2]);
        string catalogPath = Path.GetFullPath(catalogValue);
        string targetPath = Path.GetFullPath(targetValue);
        string reportPath = Path.GetFullPath(reportValue);
        string spillRoot = options.TryGetValue("--spill-dir", out string? spillValue)
            ? Path.GetFullPath(spillValue)
            : Path.GetDirectoryName(reportPath)!;
        var protectedPaths = new List<string>
        {
            planPath,
            catalogPath,
            targetPath,
            targetPath + ".wal",
            targetPath + ".migration.lock",
            reportPath,
        };
        if (options.TryGetValue("--source-package", out string? sourcePackageValue))
            protectedPaths.Add(Path.GetFullPath(sourcePackageValue));
        if (ContainsEquivalentPaths(protectedPaths))
        {
            return await OptionErrorAsync(
                "Plan, catalog, source package, staged target, target companions, and validation report must use different files.",
                error);
        }
        if (!Directory.Exists(spillRoot))
            return await OptionErrorAsync($"Validation spill directory '{spillRoot}' does not exist.", error);

        MigrationCatalog catalog = MigrationArtifactSerializer.DeserializeCatalog(
            await File.ReadAllTextAsync(catalogPath, ct));
        MigrationPlan plan = MigrationArtifactSerializer.DeserializePlan(
            await File.ReadAllTextAsync(planPath, ct),
            catalog);
        if (!TryValidateRejectExecutionOptions(
                plan,
                catalog,
                options,
                out MigrationRejectArtifactDestinationBinding? rejectArtifactBinding,
                out parseError))
        {
            return await OptionErrorAsync(parseError!, error);
        }
        if (rejectArtifactBinding is not null)
        {
            protectedPaths.Add(rejectArtifactBinding.DestinationPath);
            protectedPaths.Add(rejectArtifactBinding.TemporaryPath);
        }
        bool resolvedPathsCollide =
            options.ContainsKey("--source-package") || rejectArtifactBinding is not null
                ? ContainsEquivalentResolvedPaths(protectedPaths)
                : ContainsEquivalentPaths(protectedPaths);
        if (resolvedPathsCollide ||
            rejectArtifactBinding is not null &&
            (ResolvedPathsAreEquivalent(spillRoot, rejectArtifactBinding.DestinationPath) ||
             ResolvedPathsAreEquivalent(spillRoot, rejectArtifactBinding.TemporaryPath)))
        {
            return await OptionErrorAsync(
                rejectArtifactBinding is null
                    ? "Plan, catalog, source package, staged target, target companions, and validation report must use different files."
                    : "Plan, catalog, source package, staged target, target companions, validation report, spill directory, reject artifact, and reject temporary file must use different paths.",
                error);
        }
        if (!ValidateWorkspacePath(options, protectedPaths, out parseError))
            return await OptionErrorAsync(parseError!, error);
        if (!ValidateSourceOptions(catalog, options, out parseError))
            return await OptionErrorAsync(parseError!, error);
        MigrationPlanReadinessValidator.ValidateForApply(plan, catalog);
        if (plan.Load.RejectMode == MigrationRejectMode.FailFast)
            MigrationValidationPolicyValidator.ValidateForExecution(plan);

        MigrationValidationLevel requiredLevel = plan.Validation.ValidateChecksums
            ? MigrationValidationLevel.Checksum
            : plan.Validation.ValidateCounts
                ? MigrationValidationLevel.Count
                : MigrationValidationLevel.Schema;
        MigrationValidationLevel level = requestedLevel ?? requiredLevel;

        MigrationSourceLease? sourceLease = null;
        MigrationValidationRunResult? completedValidation = null;
        try
        {
            sourceLease = await OpenMigrationSourceAsync(catalog, options, ct);
            IMigrationDataSource source = sourceLease.Source;
            string sourceSnapshotIdentity = source.SnapshotIdentity;
            if (plan.Load.RejectMode == MigrationRejectMode.DeterministicRejects)
                MigrationRejectSourceCapabilityValidator.ValidateForExecution(plan, source);

            await using var sourceSnapshot = new MigrationDataSourceValidationSnapshot(
                plan,
                catalog,
                source);
            await using CSharpDbStagedMigrationTarget target =
                await CSharpDbStagedMigrationTarget.OpenResumeAsync(
                    targetPath,
                    plan,
                    catalog,
                    sourceSnapshotIdentity,
                    cancellationToken: ct);

            MigrationRejectArtifactWriteResult? validatedRejectArtifact = null;
            Func<MigrationValidationPreActivationContext, CancellationToken, ValueTask>?
                beforeActivation = null;
            if (rejectArtifactBinding is not null)
            {
                beforeActivation = async (context, callbackCancellation) =>
                {
                    MigrationRejectArtifactWriteResult artifact =
                        await new MigrationRejectArtifactWriter().WriteAsync(
                            new MigrationRejectArtifactWriteRequest
                            {
                                Plan = plan,
                                Catalog = catalog,
                                Target = target,
                                OutputPath = rejectArtifactBinding.DestinationPath,
                            },
                            callbackCancellation);
                    ValidateRejectArtifactResult(context, artifact);
                    validatedRejectArtifact = artifact;
                };
            }

            MigrationValidationRunResult result = await new MigrationValidationRunner().ValidateAsync(
                new MigrationValidationRunRequest
                {
                    Plan = plan,
                    Catalog = catalog,
                    SourceSnapshot = sourceSnapshot,
                    Target = target,
                    Level = level,
                    ReportOutputPath = reportPath,
                    ChecksumOptions = new PartitionedChecksumValidatorOptions
                    {
                        SpillRootDirectory = spillRoot,
                    },
                    BeforeActivationAsync = beforeActivation,
                },
                ct);
            completedValidation = result;

            if (string.Equals(format, "json", StringComparison.OrdinalIgnoreCase))
            {
                await output.WriteLineAsync(
                    await File.ReadAllTextAsync(
                        reportPath,
                        plan.Load.RejectMode == MigrationRejectMode.DeterministicRejects
                            ? CancellationToken.None
                            : ct));
            }
            else
            {
                await output.WriteAsync(MigrationValidationTextFormatter.Format(result.Report));
                if (validatedRejectArtifact is not null)
                {
                    await output.WriteLineAsync(
                        $"Reject artifact: verified | rejected={validatedRejectArtifact.RejectedRowCount} | digest={validatedRejectArtifact.ArtifactDigest} | reused={validatedRejectArtifact.ReusedExistingArtifact.ToString().ToLowerInvariant()}");
                }
                await output.WriteLineAsync(
                    $"Activation: {(result.Activated ? "activated" : "withheld")}");
                await output.WriteLineAsync($"JSON report: {reportPath}");
            }

            return result.Report.Outcome switch
            {
                MigrationValidationStatus.Passed =>
                    plan.Objects.Any(item => !item.Included) ||
                    validatedRejectArtifact?.RejectedRowCount > 0
                        ? InspectorCommandRunner.ExitWarn
                        : InspectorCommandRunner.ExitOk,
                MigrationValidationStatus.Inconclusive or MigrationValidationStatus.Skipped =>
                    InspectorCommandRunner.ExitWarn,
                _ => InspectorCommandRunner.ExitError,
            };
        }
        catch (OperationCanceledException cancellation) when (ct.IsCancellationRequested)
        {
            if (plan.Load.RejectMode == MigrationRejectMode.DeterministicRejects &&
                completedValidation?.Activated == true)
            {
                throw new MigrationCliSafeException(
                    "MIG-VALIDATE-OUTPUT-AFTER-ACTIVATION-001",
                    "Validation completed and the staged target was activated, but command output was canceled.",
                    cancellation);
            }

            throw;
        }
        catch (Exception validationError) when (
            plan.Load.RejectMode == MigrationRejectMode.DeterministicRejects &&
            validationError is not
                (OutOfMemoryException or StackOverflowException or AccessViolationException))
        {
            if (validationError is MigrationCliSafeException)
                throw;
            if (completedValidation?.Activated == true)
            {
                throw new MigrationCliSafeException(
                    "MIG-VALIDATE-OUTPUT-AFTER-ACTIVATION-001",
                    "Validation completed and the staged target was activated, but command output failed.",
                    validationError);
            }
            throw new MigrationCliSafeException(
                "MIG-VALIDATE-DETERMINISTIC-001",
                "The deterministic-reject migration validation operation failed; activation was withheld.",
                validationError);
        }
        finally
        {
            if (sourceLease is not null)
            {
                try
                {
                    await sourceLease.DisposeAsync();
                }
                catch (Exception disposeError) when (
                    plan.Load.RejectMode == MigrationRejectMode.DeterministicRejects &&
                    disposeError is not
                        (OutOfMemoryException or StackOverflowException or AccessViolationException))
                {
                    throw new MigrationCliSafeException(
                        "MIG-VALIDATE-DETERMINISTIC-CLEANUP-001",
                        completedValidation?.Activated == true
                            ? "Validation completed and the staged target was activated, but the deterministic-reject source could not be closed safely."
                            : "The deterministic-reject migration source could not be closed safely; activation was withheld.",
                        disposeError);
                }
            }
        }
    }

    private static async ValueTask WriteTextPreviewAsync(
        TextWriter output,
        MigrationPlan plan,
        MigrationPlanReadiness readiness,
        PreviewCounts counts,
        string status)
    {
        await output.WriteLineAsync($"Status: {status}");
        await output.WriteLineAsync($"Target CSharpDB: {plan.TargetCSharpDbVersion}");
        await output.WriteLineAsync($"Profile: {plan.MappingProfile}");
        await output.WriteLineAsync(
            $"Objects: total={counts.TotalObjects} included={counts.IncludedObjects} excluded={counts.ExcludedObjects}");
        await output.WriteLineAsync(
            $"Mappings: exact={counts.Exact} losslessReencoded={counts.LosslessReencoded} lossy={counts.Lossy} unsupported={counts.Unsupported}");
        await output.WriteLineAsync(
            $"Diagnostics: information={counts.Information} warning={counts.Warning} error={counts.Error}");
        await output.WriteLineAsync(
            $"Pending approvals: diagnostics={readiness.PendingDiagnosticIds.Count} exclusions={readiness.PendingExclusionObjectIds.Count}");
        if (plan.Load.RejectMode == MigrationRejectMode.DeterministicRejects &&
            plan.Load.RejectPolicy is MigrationDeterministicRejectPolicy rejectPolicy)
        {
            await output.WriteLineAsync("Reject mode: deterministic (explicit runtime consent required)");
            await output.WriteLineAsync(
                $"Reject rules: {string.Join(',', rejectPolicy.AllowedRuleIds)}");
            await output.WriteLineAsync(
                $"Reject limits: rows={rejectPolicy.MaxRejectedRowsPerBatch}/batch,{rejectPolicy.MaxRejectedRowsPerRun}/run evidenceBytes={rejectPolicy.MaxRawValueBytes}/value,{rejectPolicy.MaxRawValueBytesPerBatch}/batch,{rejectPolicy.MaxRawValueBytesPerRun}/run artifactBytes={rejectPolicy.MaxArtifactBytes}");
            await output.WriteLineAsync(
                "Reject artifact: sensitive raw source evidence; choose a private absolute destination and manage retention explicitly.");
        }

        foreach (MigrationPlanObject item in plan.Objects.Where(item => !item.Included))
            await output.WriteLineAsync($"[excluded] {item.SourceObjectId}: {item.ExclusionReason}");
        foreach (MigrationDiagnostic diagnostic in plan.Diagnostics)
        {
            await output.WriteLineAsync(
                $"[{diagnostic.Severity.ToString().ToLowerInvariant()}] {diagnostic.DiagnosticId} {diagnostic.RuleId}: {diagnostic.Summary}");
        }
    }

    private static async ValueTask WriteJsonPreviewAsync(
        TextWriter output,
        MigrationPlan plan,
        MigrationPlanReadiness readiness,
        PreviewCounts counts,
        string status)
    {
        var preview = new
        {
            Format = "csharpdb-migration-preview/v1",
            Status = status.ToLowerInvariant().Replace(' ', '-'),
            plan.TargetCSharpDbVersion,
            plan.MappingProfile,
            RejectMode = plan.Load.RejectMode == MigrationRejectMode.DeterministicRejects
                ? "deterministic"
                : null,
            RejectPolicy = plan.Load.RejectMode == MigrationRejectMode.DeterministicRejects &&
                plan.Load.RejectPolicy is MigrationDeterministicRejectPolicy rejectPolicy
                    ? new
                    {
                        rejectPolicy.ContractVersion,
                        rejectPolicy.AllowedRuleIds,
                        rejectPolicy.MaxRejectedRowsPerBatch,
                        rejectPolicy.MaxRejectedRowsPerRun,
                        MaxRejectEvidenceValueBytes = rejectPolicy.MaxRawValueBytes,
                        MaxRejectEvidenceBytesPerBatch = rejectPolicy.MaxRawValueBytesPerBatch,
                        MaxRejectEvidenceBytesPerRun = rejectPolicy.MaxRawValueBytesPerRun,
                        MaxRejectArtifactBytes = rejectPolicy.MaxArtifactBytes,
                        Sensitive = true,
                    }
                    : null,
            Objects = new
            {
                Total = counts.TotalObjects,
                Included = counts.IncludedObjects,
                Excluded = counts.ExcludedObjects,
            },
            Mappings = new
            {
                counts.Exact,
                counts.LosslessReencoded,
                counts.Lossy,
                counts.Unsupported,
            },
            Diagnostics = new
            {
                counts.Information,
                counts.Warning,
                counts.Error,
                Items = plan.Diagnostics,
            },
            readiness.PendingDiagnosticIds,
            readiness.PendingExclusionObjectIds,
            readiness.BlockingDiagnosticIds,
            ExcludedObjects = plan.Objects
                .Where(item => !item.Included)
                .Select(item => new { item.SourceObjectId, item.ExclusionReason })
                .ToArray(),
        };
        await output.WriteLineAsync(JsonSerializer.Serialize(preview, JsonOptions));
    }

    private static async ValueTask WriteTextDdlPreviewAsync(
        TextWriter output,
        CSharpDbDdlPreview preview)
    {
        await output.WriteLineAsync($"Format: {preview.Format}");
        await output.WriteLineAsync(
            $"Target CSharpDB version: {preview.TargetCSharpDbVersion}");
        await output.WriteLineAsync(
            $"Catalog digest: {preview.CatalogDigest}");
        await output.WriteLineAsync(
            $"Plan contract digest: {preview.PlanContractDigest}");
        await output.WriteLineAsync(
            $"Generated DDL digest: {preview.GeneratedDdlDigest}");
        await output.WriteLineAsync(
            $"Readiness: {preview.Readiness.Status.ToString().ToLowerInvariant()}");

        foreach (CSharpDbDdlPreviewStage stage in preview.Stages)
        {
            await output.WriteLineAsync(
                $"Stage {stage.Ordinal}: {DdlStageLabel(stage.Stage)}");
            foreach (CSharpDbDdlPreviewAction action in stage.Actions)
            {
                if (action.Kind == CSharpDbDdlPreviewActionKind.Sql)
                {
                    await output.WriteLineAsync(
                        $"  Action {action.Ordinal}: sql");
                    await output.WriteLineAsync(action.Sql);
                }
                else
                {
                    await output.WriteLineAsync(
                        $"  Action {action.Ordinal}: ensure-json-document-collection {action.TargetName}");
                }
            }
        }
    }

    private static async ValueTask WriteTextScratchValidationAsync(
        TextWriter output,
        CSharpDbDdlScratchValidationReport report)
    {
        await output.WriteLineAsync($"Format: {report.Format}");
        await output.WriteLineAsync(
            $"Status: {report.Status.ToString().ToLowerInvariant()}");
        await output.WriteLineAsync(
            $"Highest evidence: {report.HighestEvidence?.ToString().ToLowerInvariant() ?? "none"}");
        await output.WriteLineAsync(
            $"Target CSharpDB version: {report.TargetCSharpDbVersion}");
        await output.WriteLineAsync(
            $"Catalog digest: {report.CatalogDigest}");
        await output.WriteLineAsync(
            $"Plan contract digest: {report.PlanContractDigest}");
        await output.WriteLineAsync(
            $"Generated DDL digest: {report.GeneratedDdlDigest}");
        if (report.AttachedPlanDigest is not null)
        {
            await output.WriteLineAsync(
                $"Attached plan digest: {report.AttachedPlanDigest}");
        }
        await output.WriteLineAsync(
            $"Readiness: {report.ReadinessStatus?.ToString().ToLowerInvariant() ?? "unverified"}");
        await output.WriteLineAsync($"Rule: {report.RuleId}");
        if (report.StageId is not null)
            await output.WriteLineAsync($"Stage: {report.StageId}");
        if (report.ActionId is not null)
            await output.WriteLineAsync($"Action: {report.ActionId}");
        await output.WriteLineAsync(
            $"Actions: parsed={report.ParsedActionCount} executed={report.ExecutedActionCount}");
        if (report.ExpectedSchemaDigest is not null)
        {
            await output.WriteLineAsync(
                $"Expected schema digest: {report.ExpectedSchemaDigest}");
        }
        if (report.ActualSchemaDigest is not null)
        {
            await output.WriteLineAsync(
                $"Actual schema digest: {report.ActualSchemaDigest}");
        }
        await output.WriteLineAsync(
            $"Differences: {report.Differences.Count}");
        foreach (CSharpDbDdlScratchValidationDifference difference
                 in report.Differences)
        {
            await output.WriteLineAsync(
                $"  [{difference.Ordinal}] kind={difference.Kind.ToString().ToLowerInvariant()} objectDigest={difference.ObjectIdentityDigest} expectedDigest={difference.ExpectedDefinitionDigest ?? "none"} actualDigest={difference.ActualDefinitionDigest ?? "none"}");
        }
    }

    private static string DdlStageLabel(MigrationSchemaStage stage) =>
        stage switch
        {
            MigrationSchemaStage.LoadEssential => "load-essential",
            MigrationSchemaStage.SecondaryIndexes => "secondary-indexes",
            MigrationSchemaStage.Constraints => "constraints",
            MigrationSchemaStage.Views => "views",
            MigrationSchemaStage.Triggers => "triggers",
            _ => throw new InvalidDataException(
                "The CSharpDB DDL preview contains an unknown schema stage."),
        };

    private static bool HasReviewFindings(MigrationPlan plan, MigrationPlanReadiness readiness) =>
        readiness.Status != MigrationPlanReadinessStatus.Ready ||
        plan.Objects.Any(item => !item.Included) ||
        plan.Diagnostics.Any(item => item.Severity != MigrationDiagnosticSeverity.Information);

    private static string PreviewStatus(MigrationPlan plan, MigrationPlanReadiness readiness)
    {
        if (readiness.Status == MigrationPlanReadinessStatus.Blocked)
            return "BLOCKED";
        return HasReviewFindings(plan, readiness) ? "REVIEW REQUIRED" : "READY";
    }

    private static bool TryResolveDelimiter(
        string value,
        out string configuredDelimiter,
        out IReadOnlyList<string> candidates)
    {
        configuredDelimiter = ",";
        candidates = new CsvInspectionOptions().DelimiterCandidates;
        if (string.Equals(value, "auto", StringComparison.OrdinalIgnoreCase))
            return true;

        string? resolved = value.ToLowerInvariant() switch
        {
            "comma" => ",",
            "semicolon" => ";",
            "tab" => "\t",
            "pipe" => "|",
            _ when value.Length == 1 => value,
            _ => null,
        };
        if (resolved is null ||
            resolved[0] is '\r' or '\n' or '\0' or '"' ||
            char.IsSurrogate(resolved[0]))
        {
            return false;
        }

        configuredDelimiter = resolved;
        candidates = [resolved];
        return true;
    }

    private static bool TryParsePositiveInt(string value, out int result) =>
        int.TryParse(
            value,
            NumberStyles.None,
            CultureInfo.InvariantCulture,
            out result) &&
        result > 0;

    private static bool TryParsePositiveLong(string value, out long result) =>
        long.TryParse(
            value,
            NumberStyles.None,
            CultureInfo.InvariantCulture,
            out result) &&
        result > 0;

    private static bool TryParseRetainedSnapshotIdentity(
        string value,
        out RetainedDatabaseSnapshotIdentity identity)
    {
        identity = null!;
        if (string.IsNullOrWhiteSpace(value) ||
            !value.StartsWith(
                CsvExportCheckpointContracts.RetainedSnapshotIdentityPrefix,
                StringComparison.Ordinal))
        {
            return false;
        }

        ReadOnlySpan<char> remainder = value.AsSpan(
            CsvExportCheckpointContracts.RetainedSnapshotIdentityPrefix.Length);
        int separator = remainder.IndexOf(':');
        if (separator <= 0)
            return false;

        ReadOnlySpan<char> byteLengthText = remainder[..separator];
        ReadOnlySpan<char> sha256 = remainder[(separator + 1)..];
        if (!long.TryParse(
                byteLengthText,
                NumberStyles.None,
                CultureInfo.InvariantCulture,
                out long byteLength) ||
            byteLength <= 0 ||
            !byteLengthText.SequenceEqual(
                byteLength.ToString(CultureInfo.InvariantCulture)))
        {
            return false;
        }
        const string sha256Prefix = "sha256:";
        if (!sha256.StartsWith(sha256Prefix, StringComparison.Ordinal) ||
            sha256.Length != sha256Prefix.Length + 64 ||
            sha256[sha256Prefix.Length..].ContainsAnyExcept(
                "0123456789abcdef"))
        {
            return false;
        }

        string canonicalSha256 = sha256.ToString();
        identity = new RetainedDatabaseSnapshotIdentity(
            byteLength,
            canonicalSha256,
            value);
        return true;
    }

    private static string FormatCsvExportProfile(CsvExportProfile profile) =>
        profile switch
        {
            CsvExportProfile.LosslessV1 => "lossless-v1",
            CsvExportProfile.SpreadsheetSafeLossyV1 =>
                "spreadsheet-safe-lossy-v1",
            _ => throw new ArgumentOutOfRangeException(nameof(profile)),
        };

    private static bool SnapshotUsesReservedExportNamespace(
        string snapshotPath,
        string destinationPath,
        string reservedPrefix)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(
            reservedPrefix);
        string? snapshotParent = Path.GetDirectoryName(snapshotPath);
        string? destinationParent = Path.GetDirectoryName(destinationPath);
        return snapshotParent is not null &&
            destinationParent is not null &&
            ResolvedPathsAreEquivalent(snapshotParent, destinationParent) &&
            Path.GetFileName(snapshotPath).StartsWith(
                reservedPrefix,
                StringComparison.OrdinalIgnoreCase);
    }

    private static bool HasWindowsDosAliasSegment(string path)
    {
        if (!OperatingSystem.IsWindows())
            return false;

        string root = Path.GetPathRoot(path) ?? string.Empty;
        return path[root.Length..]
            .Split(
                [Path.DirectorySeparatorChar, Path.AltDirectorySeparatorChar],
                StringSplitOptions.RemoveEmptyEntries)
            .Any(static segment => segment.Contains('~'));
    }

    private static bool TryParsePlanLoadPolicy(
        IReadOnlyDictionary<string, string> options,
        out ParsedPlanLoadPolicy parsed,
        out string? error)
    {
        ArgumentNullException.ThrowIfNull(options);

        var load = new MigrationLoadPolicy();
        parsed = new ParsedPlanLoadPolicy(load, UseAllRuleIds: false);
        string rejectModeValue = options.GetValueOrDefault("--reject-mode", "fail-fast");
        bool deterministic = string.Equals(
            rejectModeValue,
            "deterministic",
            StringComparison.OrdinalIgnoreCase);
        if (!deterministic &&
            !string.Equals(rejectModeValue, "fail-fast", StringComparison.OrdinalIgnoreCase))
        {
            error = $"Unsupported reject mode '{rejectModeValue}'.";
            return false;
        }

        string[] policyOptions =
        [
            "--reject-rules",
            "--max-rejected-rows-per-batch",
            "--max-rejected-rows-per-run",
            "--max-reject-evidence-value-bytes",
            "--max-reject-evidence-bytes-per-batch",
            "--max-reject-evidence-bytes-per-run",
            "--max-reject-artifact-bytes",
        ];
        if (!deterministic)
        {
            string? unexpected = policyOptions.FirstOrDefault(options.ContainsKey);
            if (unexpected is not null)
            {
                error = $"Option {unexpected} requires --reject-mode deterministic.";
                return false;
            }

            error = null;
            return true;
        }

        string? missing = policyOptions.FirstOrDefault(option => !options.ContainsKey(option));
        if (missing is not null)
        {
            error = $"Missing required option {missing} for deterministic reject mode.";
            return false;
        }

        string ruleValue = options["--reject-rules"];
        bool useAllRuleIds = string.Equals(
            ruleValue,
            "all",
            StringComparison.OrdinalIgnoreCase);
        IReadOnlyList<string> allowedRuleIds;
        if (useAllRuleIds)
        {
            allowedRuleIds = [DeferredDeterministicRejectRuleId];
        }
        else if (!TryParseIdList(ruleValue, out allowedRuleIds, out error))
        {
            return false;
        }

        if (!TryParsePositiveInt(
                options["--max-rejected-rows-per-batch"],
                out int maxRejectedRowsPerBatch) ||
            !TryParsePositiveLong(
                options["--max-rejected-rows-per-run"],
                out long maxRejectedRowsPerRun) ||
            !TryParsePositiveInt(
                options["--max-reject-evidence-value-bytes"],
                out int maxRawValueBytes) ||
            !TryParsePositiveLong(
                options["--max-reject-evidence-bytes-per-batch"],
                out long maxRawValueBytesPerBatch) ||
            !TryParsePositiveLong(
                options["--max-reject-evidence-bytes-per-run"],
                out long maxRawValueBytesPerRun) ||
            !TryParsePositiveLong(
                options["--max-reject-artifact-bytes"],
                out long maxArtifactBytes))
        {
            error = "Deterministic reject limits must be positive base-10 integers.";
            return false;
        }

        var rejectPolicy = new MigrationDeterministicRejectPolicy
        {
            ContractVersion = MigrationRejectContract.DeterministicRejectsV1,
            AllowedRuleIds = allowedRuleIds,
            MaxRejectedRowsPerBatch = maxRejectedRowsPerBatch,
            MaxRejectedRowsPerRun = maxRejectedRowsPerRun,
            MaxRawValueBytes = maxRawValueBytes,
            MaxRawValueBytesPerBatch = maxRawValueBytesPerBatch,
            MaxRawValueBytesPerRun = maxRawValueBytesPerRun,
            MaxArtifactBytes = maxArtifactBytes,
        };
        try
        {
            MigrationRejectReadPolicyValidator.Validate(
                MigrationRejectContract.DeterministicRejectsV1,
                rejectPolicy,
                load.BatchSize);
        }
        catch (InvalidDataException policyError)
        {
            error = $"Invalid deterministic reject policy: {policyError.Message}";
            return false;
        }

        load = load with
        {
            RejectMode = MigrationRejectMode.DeterministicRejects,
            RejectPolicy = rejectPolicy,
        };
        parsed = new ParsedPlanLoadPolicy(load, useAllRuleIds);
        error = null;
        return true;
    }

    private static bool TryBindPlanLoadPolicy(
        ParsedPlanLoadPolicy parsed,
        MigrationCatalog catalog,
        out MigrationLoadPolicy load,
        out string? error)
    {
        ArgumentNullException.ThrowIfNull(parsed);
        ArgumentNullException.ThrowIfNull(catalog);

        load = parsed.Load;
        if (load.RejectMode == MigrationRejectMode.FailFast)
        {
            error = null;
            return true;
        }

        MigrationDeterministicRejectPolicy rejectPolicy =
            load.RejectPolicy ??
            throw new InvalidOperationException(
                "Parsed deterministic reject policy state is inconsistent.");
        if (!TryGetDeterministicRejectRuleRegistry(
                catalog,
                out IReadOnlyList<string> supportedRuleIds,
                out string sourceDescription,
                out error))
        {
            return false;
        }

        IReadOnlyList<string> allowedRuleIds = parsed.UseAllRuleIds
            ? supportedRuleIds.ToArray()
            : rejectPolicy.AllowedRuleIds;
        string? unsupportedRule = allowedRuleIds.FirstOrDefault(
            ruleId => !supportedRuleIds.Contains(
                ruleId,
                StringComparer.Ordinal));
        if (unsupportedRule is not null)
        {
            error =
                $"Reject rule '{unsupportedRule}' is not supported by the {sourceDescription}.";
            return false;
        }

        rejectPolicy = rejectPolicy with
        {
            AllowedRuleIds = allowedRuleIds,
        };
        try
        {
            MigrationRejectReadPolicyValidator.Validate(
                MigrationRejectContract.DeterministicRejectsV1,
                rejectPolicy,
                load.BatchSize);
        }
        catch (InvalidDataException policyError)
        {
            error =
                $"Invalid deterministic reject policy: {policyError.Message}";
            return false;
        }

        load = load with
        {
            RejectPolicy = rejectPolicy,
        };
        error = null;
        return true;
    }

    private static bool TryGetDeterministicRejectRuleRegistry(
        MigrationCatalog catalog,
        out IReadOnlyList<string> supportedRuleIds,
        out string sourceDescription,
        out string? error)
    {
        switch (catalog.Source.Kind)
        {
            case MigrationSourceKind.Csv:
                supportedRuleIds = CsvDeterministicRejectRuleIds;
                sourceDescription = "retained CSV source";
                error = null;
                return true;

            case MigrationSourceKind.Json when IsUntypedJsonV1Catalog(catalog):
                supportedRuleIds = JsonDeterministicRejectRuleIds;
                sourceDescription =
                    "untyped retained JSON package v1 source";
                error = null;
                return true;

            case MigrationSourceKind.Json
                when ClassifyJsonCatalog(
                    catalog,
                    out _) == JsonCatalogRoute.TypedV2:
                supportedRuleIds = [];
                sourceDescription =
                    "explicitly typed retained JSON package v2 source";
                error =
                    "Deterministic rejects are not supported for explicitly typed retained JSON package v2 migrations.";
                return false;

            case MigrationSourceKind.Sqlite:
                supportedRuleIds = [];
                sourceDescription = "retained SQLite backup source";
                error =
                    "Deterministic rejects are not supported for retained SQLite backup migrations.";
                return false;

            case MigrationSourceKind.LiteDb:
                supportedRuleIds = [];
                sourceDescription = "retained LiteDB snapshot source";
                error =
                    "Deterministic rejects are not supported for retained LiteDB snapshot migrations.";
                return false;

            default:
                supportedRuleIds = [];
                sourceDescription = "unsupported migration source";
                error =
                    "Deterministic rejects are supported only for retained CSV or untyped retained JSON package v1 migrations.";
                return false;
        }
    }

    private static bool TryParseSourceByteLimit(string value, out long result) =>
        long.TryParse(
            value,
            NumberStyles.None,
            CultureInfo.InvariantCulture,
            out result) &&
        result >= 0 &&
        result < long.MaxValue;

    private static bool ValidatePackageOptionShapes(
        IReadOnlyDictionary<string, string> options,
        out string? error)
    {
        if (options.TryGetValue("--source-package", out string? packageValue) &&
            string.IsNullOrWhiteSpace(packageValue))
        {
            error = "The source package path cannot be blank.";
            return false;
        }
        if (options.TryGetValue("--expected-manifest-digest", out string? digest) &&
            !IsCanonicalSha256(digest))
        {
            error = "The expected manifest digest must be canonical lowercase sha256:<64-hex>.";
            return false;
        }
        if (options.TryGetValue("--workspace", out string? workspaceValue) &&
            string.IsNullOrWhiteSpace(workspaceValue))
        {
            error = "The retained-source workspace path cannot be blank.";
            return false;
        }
        if (options.TryGetValue("--max-source-bytes", out string? maxSourceBytesValue) &&
            !TryParseSourceByteLimit(maxSourceBytesValue, out _))
        {
            error =
                "The retained source byte limit must be a non-negative 64-bit integer below Int64.MaxValue.";
            return false;
        }

        error = null;
        return true;
    }

    private static bool TryValidateRejectExecutionOptions(
        MigrationPlan plan,
        MigrationCatalog catalog,
        IReadOnlyDictionary<string, string> options,
        out MigrationRejectArtifactDestinationBinding? artifactBinding,
        out string? error)
    {
        ArgumentNullException.ThrowIfNull(plan);
        ArgumentNullException.ThrowIfNull(catalog);
        ArgumentNullException.ThrowIfNull(options);

        artifactBinding = null;
        bool allowDeterministicRejects = options.ContainsKey("--allow-deterministic-rejects");
        bool hasArtifact = options.TryGetValue("--reject-artifact", out string? artifactPath);
        if (plan.Load.RejectMode == MigrationRejectMode.FailFast)
        {
            if (allowDeterministicRejects || hasArtifact)
            {
                error =
                    "Deterministic-reject execution options cannot be used with a fail-fast plan.";
                return false;
            }

            error = null;
            return true;
        }

        if (plan.Load.RejectMode != MigrationRejectMode.DeterministicRejects ||
            plan.Load.RejectPolicy is null)
        {
            error = "The migration plan contains an unsupported reject policy.";
            return false;
        }
        if (catalog.Source.Kind == MigrationSourceKind.Json &&
            ClassifyJsonCatalog(
                catalog,
                out _) == JsonCatalogRoute.TypedV2)
        {
            error =
                "Deterministic-reject CLI execution is not supported for explicitly typed retained JSON package v2 migrations.";
            return false;
        }
        if (catalog.Source.Kind != MigrationSourceKind.Csv &&
            (catalog.Source.Kind != MigrationSourceKind.Json ||
             !IsUntypedJsonV1Catalog(catalog)))
        {
            error =
                "Deterministic-reject CLI execution is supported only for retained CSV or untyped retained JSON package v1 migrations.";
            return false;
        }
        if (!allowDeterministicRejects)
        {
            error =
                "Missing required option --allow-deterministic-rejects for a deterministic-reject plan.";
            return false;
        }
        if (!hasArtifact)
        {
            error = "Missing required option --reject-artifact for a deterministic-reject plan.";
            return false;
        }

        try
        {
            artifactBinding = MigrationRejectArtifactDestinationValidator.ValidateForPublication(
                plan,
                artifactPath!);
        }
        catch (Exception pathError) when (pathError is
            ArgumentException or InvalidDataException or IOException or
            UnauthorizedAccessException or MigrationExecutionPolicyException)
        {
            error = $"Invalid reject artifact destination: {pathError.Message}";
            return false;
        }

        error = null;
        return true;
    }

    private static void ValidateRejectArtifactResult(
        MigrationApplyResult applyResult,
        MigrationRejectArtifactWriteResult artifactResult)
    {
        ArgumentNullException.ThrowIfNull(applyResult);
        ArgumentNullException.ThrowIfNull(artifactResult);

        long expectedRejectedRows = checked(
            applyResult.RejectedRowsWritten + applyResult.RejectedRowsSkipped);
        if (!string.Equals(
                artifactResult.PlanDigest,
                applyResult.PlanDigest,
                StringComparison.Ordinal) ||
            !string.Equals(
                artifactResult.TargetIdentity,
                applyResult.TargetIdentity,
                StringComparison.Ordinal) ||
            string.IsNullOrWhiteSpace(artifactResult.TargetSnapshotIdentity) ||
            !IsCanonicalSha256Hex(artifactResult.ArtifactDigest) ||
            artifactResult.ArtifactBytes <= 0 ||
            artifactResult.RejectedRowCount != expectedRejectedRows)
        {
            throw new InvalidDataException(
                "The reject artifact result does not match the completed migration apply result.");
        }
    }

    private static void ValidateRejectArtifactResult(
        MigrationValidationPreActivationContext context,
        MigrationRejectArtifactWriteResult artifactResult)
    {
        ArgumentNullException.ThrowIfNull(context);
        ArgumentNullException.ThrowIfNull(context.Report);
        ArgumentNullException.ThrowIfNull(artifactResult);

        MigrationValidationBinding binding = context.Report.Binding;
        if (!string.Equals(
                artifactResult.PlanDigest,
                binding.PlanDigest,
                StringComparison.Ordinal) ||
            !string.Equals(
                artifactResult.TargetIdentity,
                binding.TargetIdentity,
                StringComparison.Ordinal) ||
            !string.Equals(
                artifactResult.TargetSnapshotIdentity,
                binding.TargetSnapshotIdentity,
                StringComparison.Ordinal) ||
            !IsCanonicalSha256Hex(artifactResult.ArtifactDigest) ||
            artifactResult.ArtifactBytes <= 0 ||
            artifactResult.RejectedRowCount < 0)
        {
            throw new InvalidDataException(
                "The reject artifact result does not match the published validation report.");
        }
    }

    private static bool ValidateSourceOptions(
        MigrationCatalog catalog,
        IReadOnlyDictionary<string, string> options,
        out string? error)
    {
        bool hasPackage = options.ContainsKey("--source-package");
        bool hasDigest = options.ContainsKey("--expected-manifest-digest");
        bool hasRetainedEnvironment = options.ContainsKey("--workspace") ||
            options.ContainsKey("--max-source-bytes");

        if (catalog.Source.Kind == MigrationSourceKind.Json &&
            ClassifyJsonCatalog(
                catalog,
                out _) == JsonCatalogRoute.Unsupported)
        {
            error = JsonCatalogRouteOnlyMessage;
            return false;
        }

        if (catalog.Source.Kind == MigrationSourceKind.Sqlite &&
            !IsSupportedSqliteV1Catalog(catalog))
        {
            error = SqliteCatalogRouteOnlyMessage;
            return false;
        }
        if (catalog.Source.Kind == MigrationSourceKind.LiteDb &&
            !IsSupportedLiteDbV1Catalog(catalog))
        {
            error = LiteDbCatalogRouteOnlyMessage;
            return false;
        }
        if (catalog.Source.Kind == MigrationSourceKind.Access &&
            !IsRetainedAccessCatalog(catalog))
        {
            error =
                "This Microsoft Access catalog is not a retained Access package catalog and has no row replay route.";
            return false;
        }
        if (catalog.Source.Kind ==
                MigrationSourceKind.SqlServer &&
            !IsRetainedSqlServerCatalog(catalog))
        {
            error =
                "This SQL Server catalog is schema-only and has no retained row route. Inspect the source again with --package before apply or data validation.";
            return false;
        }
        if (catalog.Source.Kind ==
                MigrationSourceKind.MySql &&
            !IsRetainedMySqlCatalog(catalog))
        {
            error =
                "This MySQL catalog is schema-only and has no retained row route. Inspect the source again with --package before apply or data validation.";
            return false;
        }

        if (catalog.Source.Kind is
            MigrationSourceKind.Csv or
            MigrationSourceKind.Json or
            MigrationSourceKind.Sqlite or
            MigrationSourceKind.LiteDb or
            MigrationSourceKind.Access or
            MigrationSourceKind.SqlServer or
            MigrationSourceKind.MySql)
        {
            string sourceDescription = catalog.Source.Kind switch
            {
                MigrationSourceKind.Csv => "CSV",
                MigrationSourceKind.Json => "JSON",
                MigrationSourceKind.Sqlite => "SQLite",
                MigrationSourceKind.LiteDb => "LiteDB",
                MigrationSourceKind.Access =>
                    "Microsoft Access",
                MigrationSourceKind.SqlServer =>
                    "SQL Server",
                MigrationSourceKind.MySql => "MySQL",
                _ => "retained-source",
            };
            if (!hasPackage)
            {
                error =
                    $"Missing required option --source-package for a {sourceDescription} migration.";
                return false;
            }
            if (!hasDigest)
            {
                error =
                    $"Missing required option --expected-manifest-digest for a {sourceDescription} migration.";
                return false;
            }

            error = null;
            return true;
        }

        if (catalog.Source.Kind == MigrationSourceKind.Synthetic &&
            (hasPackage || hasDigest || hasRetainedEnvironment))
        {
            error =
                "Retained source-package options cannot be used with a synthetic migration.";
            return false;
        }

        error = null;
        return true;
    }

    private static bool ValidateWorkspacePath(
        IReadOnlyDictionary<string, string> options,
        IReadOnlyList<string> protectedFilePaths,
        out string? error)
    {
        if (!options.TryGetValue("--workspace", out string? workspaceValue))
        {
            error = null;
            return true;
        }

        string workspacePath = Path.GetFullPath(workspaceValue);
        if (!Directory.Exists(workspacePath))
        {
            error =
                "The retained-source workspace must be an existing caller-controlled directory.";
            return false;
        }

        FileAttributes attributes = File.GetAttributes(workspacePath);
        if ((attributes & (FileAttributes.ReparsePoint | FileAttributes.Device)) != 0)
        {
            error =
                "The retained-source workspace cannot be a link, reparse point, or device.";
            return false;
        }

        foreach (string protectedFilePath in protectedFilePaths)
        {
            if (ResolvedPathsAreEquivalent(workspacePath, protectedFilePath) ||
                IsPathWithin(workspacePath, protectedFilePath))
            {
                error =
                    "The retained-source workspace cannot be the same as or nested beneath an artifact file path.";
                return false;
            }
        }

        error = null;
        return true;
    }

    private static string ResolvePathForComparison(string path)
    {
        string fullPath = Path.GetFullPath(path);
        string root = Path.GetPathRoot(fullPath) ?? throw new IOException(
            "A retained-source migration path does not have a filesystem root.");
        string current = root;
        string[] components = fullPath[root.Length..].Split(
            [Path.DirectorySeparatorChar, Path.AltDirectorySeparatorChar],
            StringSplitOptions.RemoveEmptyEntries);
        foreach (string component in components)
        {
            string candidate = Path.Combine(current, component);
            try
            {
                FileAttributes attributes = File.GetAttributes(candidate);
                if ((attributes & FileAttributes.Device) != 0)
                {
                    throw new IOException(
                        "Retained-source migration artifact paths cannot resolve through devices.");
                }

                if ((attributes & FileAttributes.ReparsePoint) != 0)
                {
                    FileSystemInfo link = (attributes & FileAttributes.Directory) != 0
                        ? new DirectoryInfo(candidate)
                        : new FileInfo(candidate);
                    FileSystemInfo target = link.ResolveLinkTarget(returnFinalTarget: true)
                        ?? throw new IOException(
                            "A retained-source migration path contains an unsupported reparse point.");
                    current = Path.GetFullPath(target.FullName);
                    continue;
                }
            }
            catch (Exception exception) when (
                exception is FileNotFoundException or DirectoryNotFoundException)
            {
            }

            current = candidate;
        }

        return Path.TrimEndingDirectorySeparator(Path.GetFullPath(current));
    }

    private static bool IsPathWithin(string path, string parentPath)
    {
        string normalizedPath = ResolvePathForComparison(path);
        string normalizedParent = ResolvePathForComparison(parentPath);
        string prefix = normalizedParent + Path.DirectorySeparatorChar;
        return normalizedPath.StartsWith(prefix, PortableArtifactPathComparison);
    }

    private static async ValueTask<MigrationSourceLease> OpenMigrationSourceAsync(
        MigrationCatalog catalog,
        IReadOnlyDictionary<string, string> options,
        CancellationToken ct)
    {
        switch (catalog.Source.Kind)
        {
            case MigrationSourceKind.Synthetic:
                if (!string.Equals(
                        catalog.Source.Identity,
                        SyntheticMigrationSourceInspector.FixtureIdentity,
                        StringComparison.Ordinal))
                {
                    throw new NotSupportedException(
                        "The synthetic migration source is not registered in this CLI build.");
                }

                var synthetic = new SyntheticMigrationDataSource(catalog);
                ValidateOpenedSource(catalog, synthetic);
                return new MigrationSourceLease(
                    synthetic,
                    synthetic,
                    packageMetadata: null);

            case MigrationSourceKind.Csv:
                long maxSourceBytes = new CsvSnapshotPackageOpenOptions().MaxSourceBytes;
                if (options.TryGetValue("--max-source-bytes", out string? maxSourceBytesValue))
                    _ = TryParseSourceByteLimit(maxSourceBytesValue, out maxSourceBytes);

                CsvSnapshotPackageSession? session = null;
                try
                {
                    session = await CsvSnapshotPackage.OpenAsync(
                        Path.GetFullPath(options["--source-package"]),
                        new CsvSnapshotPackageOpenOptions
                        {
                            WorkspacePath = options.GetValueOrDefault("--workspace"),
                            MaxSourceBytes = maxSourceBytes,
                            ExpectedManifestDigest = options["--expected-manifest-digest"],
                        },
                        ct);
                    ValidateOpenedCsvSource(catalog, session);
                    return new MigrationSourceLease(
                        session.DataSource,
                        session,
                        new MigrationSourcePackageMetadata(
                            CsvSnapshotPackage.Format,
                            session.Manifest.ManifestDigest));
                }
                catch (Exception operationFailure) when (session is not null)
                {
                    try
                    {
                        await session.DisposeAsync();
                    }
                    catch (Exception cleanupFailure)
                    {
                        throw new AggregateException(operationFailure, cleanupFailure);
                    }

                    ExceptionDispatchInfo.Capture(operationFailure).Throw();
                    throw;
                }

            case MigrationSourceKind.Json:
                JsonCatalogRoute jsonRoute =
                    ClassifyJsonCatalog(
                        catalog,
                        out string?
                            catalogIntentManifestDigest);
                if (jsonRoute == JsonCatalogRoute.Unsupported)
                {
                    throw new MigrationCliSafeException(
                        JsonCatalogRouteOnlyCode,
                        JsonCatalogRouteOnlyMessage,
                        new NotSupportedException(
                            JsonCatalogRouteOnlyMessage));
                }

                long jsonMaxSourceBytes =
                    new JsonSnapshotPackageOpenOptions().MaxSourceBytes;
                if (options.TryGetValue(
                        "--max-source-bytes",
                        out string? jsonMaxSourceBytesValue))
                {
                    _ = TryParseSourceByteLimit(
                        jsonMaxSourceBytesValue,
                        out jsonMaxSourceBytes);
                }

                var jsonOpenOptions =
                    new JsonSnapshotPackageOpenOptions
                    {
                        WorkspacePath =
                            options.GetValueOrDefault(
                                "--workspace"),
                        MaxSourceBytes =
                            jsonMaxSourceBytes,
                        ExpectedManifestDigest =
                            options[
                                "--expected-manifest-digest"],
                    };
                string jsonPackagePath = Path.GetFullPath(
                    options["--source-package"]);
                if (jsonRoute == JsonCatalogRoute.UntypedV1)
                {
                    JsonSnapshotPackageSession? jsonSession = null;
                    try
                    {
                        jsonSession =
                            await JsonSnapshotPackage.OpenAsync(
                                jsonPackagePath,
                                jsonOpenOptions,
                                ct);
                        ValidateOpenedJsonSource(
                            catalog,
                            jsonSession);
                        return new MigrationSourceLease(
                            jsonSession.DataSource,
                            jsonSession,
                            new MigrationSourcePackageMetadata(
                                JsonSnapshotPackage.Format,
                                jsonSession.Manifest
                                    .ManifestDigest));
                    }
                    catch (Exception operationFailure) when (
                        jsonSession is not null)
                    {
                        try
                        {
                            await jsonSession.DisposeAsync();
                        }
                        catch (Exception cleanupFailure)
                        {
                            throw new AggregateException(
                                operationFailure,
                                cleanupFailure);
                        }

                        ExceptionDispatchInfo.Capture(
                            operationFailure).Throw();
                        throw;
                    }
                }

                JsonTypedSnapshotPackageSession?
                    typedJsonSession = null;
                try
                {
                    typedJsonSession =
                        await JsonTypedSnapshotPackage.OpenAsync(
                            jsonPackagePath,
                            jsonOpenOptions,
                            ct);
                    ValidateOpenedTypedJsonSource(
                        catalog,
                        typedJsonSession,
                        catalogIntentManifestDigest!);
                    return new MigrationSourceLease(
                        typedJsonSession.DataSource,
                        typedJsonSession,
                        new MigrationSourcePackageMetadata(
                            JsonTypedSnapshotPackage.Format,
                            typedJsonSession.Manifest
                                .ManifestDigest,
                            typedJsonSession.Manifest
                                .IntentManifestDigest));
                }
                catch (Exception operationFailure) when (
                    typedJsonSession is not null)
                {
                    try
                    {
                        await typedJsonSession.DisposeAsync();
                    }
                    catch (Exception cleanupFailure)
                    {
                        throw new AggregateException(
                            operationFailure,
                            cleanupFailure);
                    }

                    ExceptionDispatchInfo.Capture(
                        operationFailure).Throw();
                    throw;
                }

            case MigrationSourceKind.Sqlite:
                long sqliteMaxSourceBytes =
                    SqliteSnapshotPackageOpenOptions.DefaultMaxSourceBytes;
                if (options.TryGetValue(
                        "--max-source-bytes",
                        out string? sqliteMaxSourceBytesValue))
                {
                    _ = TryParseSourceByteLimit(
                        sqliteMaxSourceBytesValue,
                        out sqliteMaxSourceBytes);
                }

                SqliteSnapshotPackageSession? sqliteSession = null;
                try
                {
                    sqliteSession =
                        await SqliteSnapshotPackageSession.OpenAsync(
                            Path.GetFullPath(
                                options["--source-package"]),
                            catalog,
                            new SqliteSnapshotPackageOpenOptions
                            {
                                WorkspacePath =
                                    options.GetValueOrDefault(
                                        "--workspace"),
                                MaxSourceBytes =
                                    sqliteMaxSourceBytes,
                                ExpectedContentDigest =
                                    options[
                                        "--expected-manifest-digest"],
                            },
                            ct);
                    ValidateOpenedSource(
                        catalog,
                        sqliteSession.DataSource);
                    return new MigrationSourceLease(
                        sqliteSession.DataSource,
                        sqliteSession,
                        new MigrationSourcePackageMetadata(
                            SqliteSnapshotPackageSession.Format,
                            sqliteSession.ContentDigest));
                }
                catch (Exception operationFailure) when (
                    sqliteSession is not null)
                {
                    try
                    {
                        await sqliteSession.DisposeAsync();
                    }
                    catch (Exception cleanupFailure)
                    {
                        throw new AggregateException(
                            operationFailure,
                            cleanupFailure);
                    }

                    ExceptionDispatchInfo.Capture(
                        operationFailure).Throw();
                    throw;
                }

            case MigrationSourceKind.LiteDb:
                long liteDbMaxSourceBytes =
                    LiteDbSnapshotPackageOpenOptions
                        .DefaultMaxSourceBytes;
                if (options.TryGetValue(
                        "--max-source-bytes",
                        out string? liteDbMaxSourceBytesValue))
                {
                    _ = TryParseSourceByteLimit(
                        liteDbMaxSourceBytesValue,
                        out liteDbMaxSourceBytes);
                }

                LiteDbSnapshotPackageSession?
                    liteDbSession = null;
                try
                {
                    liteDbSession =
                        await LiteDbSnapshotPackageSession
                            .OpenAsync(
                                Path.GetFullPath(
                                    options[
                                        "--source-package"]),
                                catalog,
                                new LiteDbSnapshotPackageOpenOptions
                                {
                                    WorkspacePath =
                                        options.GetValueOrDefault(
                                            "--workspace"),
                                    MaxSourceBytes =
                                        liteDbMaxSourceBytes,
                                    ExpectedContentDigest =
                                        options[
                                            "--expected-manifest-digest"],
                                },
                                ct);
                    ValidateOpenedSource(
                        catalog,
                        liteDbSession.DataSource);
                    return new MigrationSourceLease(
                        liteDbSession.DataSource,
                        liteDbSession,
                        new MigrationSourcePackageMetadata(
                            LiteDbSnapshotPackageSession
                                .Format,
                            liteDbSession.ContentDigest));
                }
                catch (Exception operationFailure) when (
                    liteDbSession is not null)
                {
                    try
                    {
                        await liteDbSession
                            .DisposeAsync();
                    }
                    catch (Exception cleanupFailure)
                    {
                        throw new AggregateException(
                            operationFailure,
                            cleanupFailure);
                    }

                    ExceptionDispatchInfo.Capture(
                        operationFailure).Throw();
                    throw;
                }

            case MigrationSourceKind.Access:
                if (!IsRetainedAccessCatalog(catalog))
                {
                    throw new NotSupportedException(
                        "This Microsoft Access catalog has no retained row route.");
                }

                long accessMaxSourceBytes =
                    new RetainedMigrationPackageOpenOptions
                    {
                        ExpectedPackageDigest =
                            options[
                                "--expected-manifest-digest"],
                    }.MaxPackageBytes;
                if (options.TryGetValue(
                        "--max-source-bytes",
                        out string? accessMaxSourceBytesValue))
                {
                    _ = TryParseSourceByteLimit(
                        accessMaxSourceBytesValue,
                        out accessMaxSourceBytes);
                }

                RetainedMigrationPackageSession?
                    accessSession = null;
                try
                {
                    accessSession =
                        await RetainedMigrationPackageSession
                            .OpenAsync(
                                Path.GetFullPath(
                                    options[
                                        "--source-package"]),
                                new RetainedMigrationPackageOpenOptions
                                {
                                    ExpectedPackageDigest =
                                        options[
                                            "--expected-manifest-digest"],
                                    WorkspacePath =
                                        options.GetValueOrDefault(
                                            "--workspace"),
                                    MaxPackageBytes =
                                        accessMaxSourceBytes,
                                },
                                ct);
                    ValidateOpenedAccessSource(
                        catalog,
                        accessSession);
                    return new MigrationSourceLease(
                        accessSession.DataSource,
                        accessSession,
                        new MigrationSourcePackageMetadata(
                            RetainedMigrationPackageContract
                                .Format,
                            accessSession.PackageDigest));
                }
                catch (Exception operationFailure) when (
                    accessSession is not null)
                {
                    try
                    {
                        await accessSession
                            .DisposeAsync();
                    }
                    catch (Exception cleanupFailure)
                    {
                        throw new AggregateException(
                            operationFailure,
                            cleanupFailure);
                    }

                    ExceptionDispatchInfo.Capture(
                        operationFailure).Throw();
                    throw;
                }

            case MigrationSourceKind.SqlServer:
                if (!IsRetainedSqlServerCatalog(catalog))
                {
                    throw new NotSupportedException(
                        "This SQL Server catalog is schema-only and has no retained row route.");
                }

                long sqlServerMaxSourceBytes =
                    new RetainedMigrationPackageOpenOptions
                    {
                        ExpectedPackageDigest =
                            options[
                                "--expected-manifest-digest"],
                    }.MaxPackageBytes;
                if (options.TryGetValue(
                        "--max-source-bytes",
                        out string?
                            sqlServerMaxSourceBytesValue))
                {
                    _ = TryParseSourceByteLimit(
                        sqlServerMaxSourceBytesValue,
                        out sqlServerMaxSourceBytes);
                }

                RetainedMigrationPackageSession?
                    sqlServerSession = null;
                try
                {
                    sqlServerSession =
                        await RetainedMigrationPackageSession
                            .OpenAsync(
                                Path.GetFullPath(
                                    options[
                                        "--source-package"]),
                                new RetainedMigrationPackageOpenOptions
                                {
                                    ExpectedPackageDigest =
                                        options[
                                            "--expected-manifest-digest"],
                                    WorkspacePath =
                                        options.GetValueOrDefault(
                                            "--workspace"),
                                    MaxPackageBytes =
                                        sqlServerMaxSourceBytes,
                                },
                                ct);
                    ValidateOpenedSqlServerSource(
                        catalog,
                        sqlServerSession);
                    return new MigrationSourceLease(
                        sqlServerSession.DataSource,
                        sqlServerSession,
                        new MigrationSourcePackageMetadata(
                            RetainedMigrationPackageContract
                                .Format,
                            sqlServerSession
                                .PackageDigest));
                }
                catch (Exception operationFailure) when (
                    sqlServerSession is not null)
                {
                    try
                    {
                        await sqlServerSession
                            .DisposeAsync();
                    }
                    catch (Exception cleanupFailure)
                    {
                        throw new AggregateException(
                            operationFailure,
                            cleanupFailure);
                    }

                    ExceptionDispatchInfo.Capture(
                        operationFailure).Throw();
                    throw;
                }

            case MigrationSourceKind.MySql:
                if (!IsRetainedMySqlCatalog(catalog))
                {
                    throw new NotSupportedException(
                        "This MySQL catalog is schema-only and has no retained row route.");
                }

                long mySqlMaxSourceBytes =
                    new RetainedMigrationPackageOpenOptions
                    {
                        ExpectedPackageDigest =
                            options[
                                "--expected-manifest-digest"],
                    }.MaxPackageBytes;
                if (options.TryGetValue(
                        "--max-source-bytes",
                        out string?
                            mySqlMaxSourceBytesValue))
                {
                    _ = TryParseSourceByteLimit(
                        mySqlMaxSourceBytesValue,
                        out mySqlMaxSourceBytes);
                }

                RetainedMigrationPackageSession?
                    mySqlSession = null;
                try
                {
                    mySqlSession =
                        await RetainedMigrationPackageSession
                            .OpenAsync(
                                Path.GetFullPath(
                                    options[
                                        "--source-package"]),
                                new RetainedMigrationPackageOpenOptions
                                {
                                    ExpectedPackageDigest =
                                        options[
                                            "--expected-manifest-digest"],
                                    WorkspacePath =
                                        options.GetValueOrDefault(
                                            "--workspace"),
                                    MaxPackageBytes =
                                        mySqlMaxSourceBytes,
                                },
                                ct);
                    ValidateOpenedMySqlSource(
                        catalog,
                        mySqlSession);
                    return new MigrationSourceLease(
                        mySqlSession.DataSource,
                        mySqlSession,
                        new MigrationSourcePackageMetadata(
                            RetainedMigrationPackageContract
                                .Format,
                            mySqlSession
                                .PackageDigest));
                }
                catch (Exception operationFailure) when (
                    mySqlSession is not null)
                {
                    try
                    {
                        await mySqlSession
                            .DisposeAsync();
                    }
                    catch (Exception cleanupFailure)
                    {
                        throw new AggregateException(
                            operationFailure,
                            cleanupFailure);
                    }

                    ExceptionDispatchInfo.Capture(
                        operationFailure).Throw();
                    throw;
                }

            default:
                throw new NotSupportedException(
                    $"Migration source '{catalog.Source.Kind}' is not registered in this CLI build.");
        }
    }

    private static bool IsUntypedJsonV1Catalog(
        MigrationCatalog catalog) =>
        ClassifyJsonCatalog(
            catalog,
            out _) == JsonCatalogRoute.UntypedV1;

    private static bool IsSupportedSqliteV1Catalog(
        MigrationCatalog catalog)
    {
        ArgumentNullException.ThrowIfNull(catalog);
        if (catalog.Source.Kind != MigrationSourceKind.Sqlite)
            return false;

        MigrationCatalogObject[] mainNamespaces = catalog.Objects
            .Where(item =>
                item.Kind == MigrationObjectKind.Namespace &&
                string.Equals(
                    item.SourceName,
                    "main",
                    StringComparison.Ordinal))
            .ToArray();
        if (mainNamespaces.Length != 1)
            return false;

        MigrationCatalogFacet[] contracts = catalog.Objects
            .Where(item => item.Kind == MigrationObjectKind.Namespace)
            .SelectMany(item => item.Facets)
            .Where(facet => string.Equals(
                facet.Name,
                SqliteCatalogContractFacet,
                StringComparison.Ordinal))
            .ToArray();
        return contracts.Length == 1 &&
            mainNamespaces[0].Facets.Contains(contracts[0]) &&
            string.Equals(
                contracts[0].Value,
                SqliteCatalogContractV1,
                StringComparison.Ordinal);
    }

    private static bool IsSupportedLiteDbV1Catalog(
        MigrationCatalog catalog)
    {
        ArgumentNullException.ThrowIfNull(catalog);
        if (catalog.Source.Kind != MigrationSourceKind.LiteDb)
            return false;

        MigrationCatalogObject[] mainNamespaces = catalog.Objects
            .Where(item =>
                item.Kind == MigrationObjectKind.Namespace &&
                string.Equals(
                    item.SourceName,
                    "main",
                    StringComparison.Ordinal))
            .ToArray();
        if (mainNamespaces.Length != 1)
            return false;

        MigrationCatalogFacet[] contracts = catalog.Objects
            .Where(item => item.Kind == MigrationObjectKind.Namespace)
            .SelectMany(item => item.Facets)
            .Where(facet => string.Equals(
                facet.Name,
                LiteDbCatalogContractFacet,
                StringComparison.Ordinal))
            .ToArray();
        return contracts.Length == 1 &&
            mainNamespaces[0].Facets.Contains(contracts[0]) &&
            string.Equals(
                contracts[0].Value,
                LiteDbMigrationSourceInspector.CatalogContract,
                StringComparison.Ordinal);
    }

    private static JsonCatalogRoute ClassifyJsonCatalog(
        MigrationCatalog catalog,
        out string? intentManifestDigest)
    {
        ArgumentNullException.ThrowIfNull(catalog);
        intentManifestDigest = null;
        if (catalog.Source.Kind != MigrationSourceKind.Json)
            return JsonCatalogRoute.Unsupported;

        MigrationCatalogObject[] tables = catalog.Objects
            .Where(item => item.Kind == MigrationObjectKind.Table)
            .ToArray();
        if (tables.Length != 1 ||
            !string.Equals(
                tables[0].ObjectId,
                JsonMigrationObjectIds.Table,
                StringComparison.Ordinal))
        {
            return JsonCatalogRoute.Unsupported;
        }

        IReadOnlyList<MigrationCatalogFacet> facets = tables[0].Facets;
        bool isUntypedV1 = HasSingleFacet(
                facets,
                "jsonSchemaAlgorithm",
                JsonTableSchemaInferenceResult.AlgorithmId) &&
            HasSingleFacet(
                facets,
                "jsonScalarPolicy",
                JsonTableSchemaInferenceResult.ScalarPolicyId) &&
            !catalog.Objects
                .SelectMany(item => item.Facets)
                .Any(
                    facet => facet.Name.StartsWith(
                        "jsonTyped",
                        StringComparison.Ordinal));
        if (isUntypedV1)
            return JsonCatalogRoute.UntypedV1;

        if (!HasSingleFacet(
                facets,
                "jsonSchemaAlgorithm",
                JsonTypedTableSchemaInferenceResult.AlgorithmId) ||
            !HasSingleFacet(
                facets,
                "jsonScalarPolicy",
                JsonTypedTableSchemaInferenceResult.ScalarPolicyId) ||
            !HasSingleFacet(
                facets,
                "jsonTypedIntentFormat",
                JsonTypedIntentSidecar.Format) ||
            !TryGetSingleFacetValue(
                facets,
                "jsonTypedIntentManifestDigest",
                out string typedIntentDigest) ||
            !IsCanonicalSha256(typedIntentDigest))
        {
            return JsonCatalogRoute.Unsupported;
        }

        intentManifestDigest = typedIntentDigest;
        return JsonCatalogRoute.TypedV2;
    }

    private static bool HasSingleFacet(
        IReadOnlyList<MigrationCatalogFacet> facets,
        string name,
        string expectedValue)
    {
        MigrationCatalogFacet[] matches = facets
            .Where(
                facet => string.Equals(
                    facet.Name,
                    name,
                    StringComparison.Ordinal))
            .ToArray();
        return matches.Length == 1 &&
            string.Equals(
                matches[0].Value,
                expectedValue,
                StringComparison.Ordinal);
    }

    private static bool TryGetSingleFacetValue(
        IReadOnlyList<MigrationCatalogFacet> facets,
        string name,
        out string value)
    {
        MigrationCatalogFacet[] matches = facets
            .Where(
                facet => string.Equals(
                    facet.Name,
                    name,
                    StringComparison.Ordinal))
            .ToArray();
        if (matches.Length == 1 &&
            matches[0].Value is string singleValue)
        {
            value = singleValue;
            return true;
        }

        value = string.Empty;
        return false;
    }

    private static void ValidateOpenedCsvSource(
        MigrationCatalog catalog,
        CsvSnapshotPackageSession session)
    {
        string catalogDigest = MigrationArtifactSerializer.ComputeCatalogDigest(catalog);
        string retainedCatalogDigest = MigrationArtifactSerializer.ComputeCatalogDigest(
            session.Catalog);
        if (!string.Equals(catalogDigest, retainedCatalogDigest, StringComparison.Ordinal) ||
            !string.Equals(catalogDigest, session.Manifest.CatalogDigest, StringComparison.Ordinal) ||
            !string.Equals(
                catalog.TargetCSharpDbVersion,
                session.Manifest.TargetCSharpDbVersion,
                StringComparison.Ordinal))
        {
            throw new InvalidDataException(
                "The retained CSV package catalog does not match the supplied catalog artifact.");
        }

        ValidateOpenedSource(catalog, session.DataSource);
    }

    private static void ValidateOpenedJsonSource(
        MigrationCatalog catalog,
        JsonSnapshotPackageSession session)
    {
        string catalogDigest =
            MigrationArtifactSerializer.ComputeCatalogDigest(catalog);
        string retainedCatalogDigest =
            MigrationArtifactSerializer.ComputeCatalogDigest(
                session.Catalog);
        if (!string.Equals(
                catalogDigest,
                retainedCatalogDigest,
                StringComparison.Ordinal) ||
            !string.Equals(
                catalogDigest,
                session.Manifest.CatalogDigest,
                StringComparison.Ordinal) ||
            !string.Equals(
                catalog.TargetCSharpDbVersion,
                session.Manifest.TargetCSharpDbVersion,
                StringComparison.Ordinal) ||
            session.Manifest.Source != catalog.Source)
        {
            throw new InvalidDataException(
                "The retained JSON package catalog does not match the supplied catalog artifact.");
        }

        ValidateOpenedSource(catalog, session.DataSource);
    }

    private static void ValidateOpenedTypedJsonSource(
        MigrationCatalog catalog,
        JsonTypedSnapshotPackageSession session,
        string catalogIntentManifestDigest)
    {
        string catalogDigest =
            MigrationArtifactSerializer.ComputeCatalogDigest(
                catalog);
        string retainedCatalogDigest =
            MigrationArtifactSerializer.ComputeCatalogDigest(
                session.Catalog);
        if (!string.Equals(
                catalogDigest,
                retainedCatalogDigest,
                StringComparison.Ordinal) ||
            !string.Equals(
                catalogDigest,
                session.Manifest.CatalogDigest,
                StringComparison.Ordinal) ||
            !string.Equals(
                catalog.TargetCSharpDbVersion,
                session.Manifest.TargetCSharpDbVersion,
                StringComparison.Ordinal) ||
            session.Manifest.Source != catalog.Source ||
            !string.Equals(
                catalogIntentManifestDigest,
                session.Manifest.IntentManifestDigest,
                StringComparison.Ordinal) ||
            !string.Equals(
                catalogIntentManifestDigest,
                session.IntentManifest.ManifestDigest,
                StringComparison.Ordinal) ||
            !string.Equals(
                catalogIntentManifestDigest,
                session.Schema.IntentManifest.ManifestDigest,
                StringComparison.Ordinal))
        {
            throw new InvalidDataException(
                "The retained typed JSON package catalog or intent does not match the supplied catalog artifact.");
        }

        ValidateOpenedSource(catalog, session.DataSource);
    }

    private static void ValidateAccessCaptureSession(
        RetainedMigrationPackageSession session,
        AccessCaptureReceipt receipt)
    {
        ArgumentNullException.ThrowIfNull(session);
        ArgumentNullException.ThrowIfNull(receipt);
        ValidateOpenedAccessSource(
            session.Catalog,
            session);

        long manifestRows = 0;
        foreach (RetainedMigrationPackageTableManifest table in
                 session.Manifest.Tables)
        {
            manifestRows = checked(
                manifestRows + table.RowCount);
        }
        if (!string.Equals(
                session.PackageDigest,
                receipt.PackageDigest,
                StringComparison.Ordinal) ||
            !string.Equals(
                session.Manifest.CatalogDigest,
                receipt.CatalogDigest,
                StringComparison.Ordinal) ||
            !string.Equals(
                session.Manifest.SnapshotIdentity,
                receipt.SnapshotIdentity,
                StringComparison.Ordinal) ||
            !string.Equals(
                session.DataSource.SnapshotIdentity,
                receipt.SnapshotIdentity,
                StringComparison.Ordinal) ||
            session.Manifest.Tables.Count !=
                receipt.TableCount ||
            manifestRows != receipt.RowCount)
        {
            throw new InvalidDataException(
                "The retained Microsoft Access package does not match the worker capture receipt.");
        }
    }

    private static void ValidateOpenedAccessSource(
        MigrationCatalog catalog,
        RetainedMigrationPackageSession session)
    {
        ArgumentNullException.ThrowIfNull(catalog);
        ArgumentNullException.ThrowIfNull(session);
        AccessRetainedManifestBindingValidator.Validate(
            catalog,
            session.Manifest);
        AccessRetainedManifestBindingValidator.Validate(
            session.Catalog,
            session.Manifest);
        string catalogDigest =
            MigrationArtifactSerializer.ComputeCatalogDigest(
                catalog);
        string retainedCatalogDigest =
            MigrationArtifactSerializer.ComputeCatalogDigest(
                session.Catalog);
        if (!IsRetainedAccessCatalog(catalog) ||
            !IsRetainedAccessCatalog(session.Catalog) ||
            !string.Equals(
                session.Manifest.Format,
                RetainedMigrationPackageContract.Format,
                StringComparison.Ordinal) ||
            session.Manifest.SourceKind !=
                MigrationSourceKind.Access ||
            !string.Equals(
                catalogDigest,
                retainedCatalogDigest,
                StringComparison.Ordinal) ||
            !string.Equals(
                catalogDigest,
                session.Manifest.CatalogDigest,
                StringComparison.Ordinal) ||
            !string.Equals(
                session.Manifest.SourceIdentity,
                catalog.Source.Identity,
                StringComparison.Ordinal) ||
            !string.Equals(
                session.Manifest.SourceFingerprint,
                catalog.Source.Fingerprint,
                StringComparison.Ordinal) ||
            !string.Equals(
                session.Manifest.SnapshotIdentity,
                session.DataSource.SnapshotIdentity,
                StringComparison.Ordinal))
        {
            throw new InvalidDataException(
                "The retained Microsoft Access package catalog does not match the supplied catalog artifact.");
        }

        ValidateOpenedSource(
            catalog,
            session.DataSource);
    }

    private static bool IsRetainedAccessCatalog(
        MigrationCatalog catalog)
    {
        if (catalog.Source.Kind != MigrationSourceKind.Access)
            return false;
        MigrationCatalogObject[] databases = catalog.Objects
            .Where(static item =>
                item.Kind == MigrationObjectKind.Database)
            .ToArray();
        if (databases.Length != 1)
            return false;

        MigrationCatalogObject database = databases[0];
        if (!TryGetSingleFacetValue(
                database.Facets,
                AccessCatalogFacet,
                out string catalogContract) ||
            !TryGetSingleFacetValue(
                database.Facets,
                AccessRetainedDataFacet,
                out string dataContract) ||
            !TryGetSingleFacetValue(
                database.Facets,
                AccessRetainedContentDigestFacet,
                out string contentDigest) ||
            !TryGetSingleFacetValue(
                database.Facets,
                AccessRetainedSnapshotIdentityFacet,
                out string snapshotIdentity))
        {
            return false;
        }

        MigrationDiagnostic[] qualification =
            catalog.Diagnostics.Where(item =>
                    string.Equals(
                        item.RuleId,
                        AccessLiveQualificationRule,
                        StringComparison.Ordinal))
                .ToArray();
        return string.Equals(
                catalogContract,
                AccessCatalogContract,
                StringComparison.Ordinal) &&
            string.Equals(
                dataContract,
                AccessRetainedDataContract,
                StringComparison.Ordinal) &&
            IsCanonicalSha256(contentDigest) &&
            IsCanonicalAccessSnapshotIdentity(
                snapshotIdentity) &&
            qualification.Length == 1 &&
            qualification[0].Severity ==
                MigrationDiagnosticSeverity.Error &&
            qualification[0].Status ==
                MigrationCompatibilityStatus.Unknown &&
            qualification[0].Evidence ==
                MigrationEvidenceLevel.Parsed &&
            !qualification[0].CanOverride &&
            string.Equals(
                qualification[0].ObjectId,
                database.ObjectId,
                StringComparison.Ordinal);
    }

    private static bool IsCanonicalAccessSnapshotIdentity(
        string value)
    {
        if (!value.StartsWith(
                AccessRetainedSnapshotIdentityPrefix,
                StringComparison.Ordinal))
        {
            return false;
        }
        ReadOnlySpan<char> digest = value.AsSpan(
            AccessRetainedSnapshotIdentityPrefix.Length);
        if (digest.Length != 64)
            return false;
        foreach (char character in digest)
        {
            if (character is not (>= '0' and <= '9') and
                not (>= 'a' and <= 'f'))
            {
                return false;
            }
        }
        return true;
    }

    private static void ValidateSqlServerCaptureSession(
        RetainedMigrationPackageSession session,
        SqlServerCaptureReceipt receipt,
        string targetCSharpDbVersion)
    {
        ArgumentNullException.ThrowIfNull(session);
        ArgumentNullException.ThrowIfNull(receipt);
        MigrationCatalog catalog = session.Catalog;
        string catalogDigest =
            MigrationArtifactSerializer.ComputeCatalogDigest(
                catalog);
        long rowCount = 0;
        foreach (RetainedMigrationPackageTableManifest table
                 in session.Manifest.Tables)
        {
            rowCount = checked(rowCount + table.RowCount);
        }

        if (catalog.Source.Kind != MigrationSourceKind.SqlServer ||
            !IsRetainedSqlServerCatalog(catalog) ||
            !string.Equals(
                catalog.TargetCSharpDbVersion,
                targetCSharpDbVersion,
                StringComparison.Ordinal) ||
            !string.Equals(
                session.Manifest.Format,
                RetainedMigrationPackageContract.Format,
                StringComparison.Ordinal) ||
            session.Manifest.SourceKind !=
                MigrationSourceKind.SqlServer ||
            !string.Equals(
                session.PackageDigest,
                receipt.PackageDigest,
                StringComparison.Ordinal) ||
            !string.Equals(
                session.Manifest.CatalogDigest,
                receipt.CatalogDigest,
                StringComparison.Ordinal) ||
            !string.Equals(
                catalogDigest,
                receipt.CatalogDigest,
                StringComparison.Ordinal) ||
            !string.Equals(
                session.Manifest.SnapshotIdentity,
                receipt.SnapshotIdentity,
                StringComparison.Ordinal) ||
            !string.Equals(
                session.DataSource.SnapshotIdentity,
                receipt.SnapshotIdentity,
                StringComparison.Ordinal) ||
            !string.Equals(
                session.Manifest.SourceIdentity,
                catalog.Source.Identity,
                StringComparison.Ordinal) ||
            !string.Equals(
                session.Manifest.SourceFingerprint,
                catalog.Source.Fingerprint,
                StringComparison.Ordinal) ||
            session.Manifest.Tables.Count !=
                receipt.TableCount ||
            rowCount != receipt.RowCount)
        {
            throw new InvalidDataException(
                "The retained SQL Server package does not match the worker capture receipt.");
        }

        ValidateOpenedSource(catalog, session.DataSource);
    }

    private static void ValidateOpenedSqlServerSource(
        MigrationCatalog catalog,
        RetainedMigrationPackageSession session)
    {
        string catalogDigest =
            MigrationArtifactSerializer.ComputeCatalogDigest(
                catalog);
        string retainedCatalogDigest =
            MigrationArtifactSerializer.ComputeCatalogDigest(
                session.Catalog);
        if (!IsRetainedSqlServerCatalog(catalog) ||
            !IsRetainedSqlServerCatalog(session.Catalog) ||
            !string.Equals(
                session.Manifest.Format,
                RetainedMigrationPackageContract.Format,
                StringComparison.Ordinal) ||
            session.Manifest.SourceKind !=
                MigrationSourceKind.SqlServer ||
            !string.Equals(
                catalogDigest,
                retainedCatalogDigest,
                StringComparison.Ordinal) ||
            !string.Equals(
                catalogDigest,
                session.Manifest.CatalogDigest,
                StringComparison.Ordinal) ||
            !string.Equals(
                session.Manifest.SourceIdentity,
                catalog.Source.Identity,
                StringComparison.Ordinal) ||
            !string.Equals(
                session.Manifest.SourceFingerprint,
                catalog.Source.Fingerprint,
                StringComparison.Ordinal) ||
            !string.Equals(
                session.Manifest.SnapshotIdentity,
                session.DataSource.SnapshotIdentity,
                StringComparison.Ordinal))
        {
            throw new InvalidDataException(
                "The retained SQL Server package catalog does not match the supplied catalog artifact.");
        }

        ValidateOpenedSource(catalog, session.DataSource);
    }

    private static bool IsRetainedSqlServerCatalog(
        MigrationCatalog catalog)
    {
        if (catalog.Source.Kind != MigrationSourceKind.SqlServer)
            return false;
        MigrationCatalogObject[] databases = catalog.Objects
            .Where(static item =>
                item.Kind == MigrationObjectKind.Database)
            .ToArray();
        if (databases.Length != 1)
            return false;

        string? catalogContract = databases[0].Facets
            .FirstOrDefault(facet =>
                string.Equals(
                    facet.Name,
                    SqlServerRetainedCatalogFacet,
                    StringComparison.Ordinal))
            ?.Value;
        string? dataContract = databases[0].Facets
            .FirstOrDefault(facet =>
                string.Equals(
                    facet.Name,
                    SqlServerRetainedDataFacet,
                    StringComparison.Ordinal))
            ?.Value;
        return string.Equals(
                catalogContract,
                SqlServerRetainedCatalogContract,
                StringComparison.Ordinal) &&
            string.Equals(
                dataContract,
                SqlServerRetainedDataContract,
                StringComparison.Ordinal);
    }

    private static void ValidateMySqlCaptureSession(
        RetainedMigrationPackageSession session,
        MySqlCaptureReceipt receipt,
        long capturedPackageBytes,
        string targetCSharpDbVersion)
    {
        ArgumentNullException.ThrowIfNull(session);
        ArgumentNullException.ThrowIfNull(receipt);
        MigrationCatalog catalog = session.Catalog;
        MySqlRetainedManifestBindingValidator.Validate(
            catalog,
            session.Manifest);
        string retainedSnapshotIdentity =
            GetRetainedMySqlSnapshotIdentity(catalog);
        string catalogDigest =
            MigrationArtifactSerializer.ComputeCatalogDigest(
                catalog);
        long rowCount = 0;
        foreach (RetainedMigrationPackageTableManifest table
                 in session.Manifest.Tables)
        {
            rowCount = checked(rowCount + table.RowCount);
        }

        if (catalog.Source.Kind != MigrationSourceKind.MySql ||
            !IsRetainedMySqlCatalog(catalog) ||
            !string.Equals(
                catalog.TargetCSharpDbVersion,
                targetCSharpDbVersion,
                StringComparison.Ordinal) ||
            !string.Equals(
                session.Manifest.Format,
                RetainedMigrationPackageContract.Format,
                StringComparison.Ordinal) ||
            session.Manifest.SourceKind !=
                MigrationSourceKind.MySql ||
            !string.Equals(
                receipt.Format,
                MySqlCaptureReceipt.CurrentFormat,
                StringComparison.Ordinal) ||
            !string.Equals(
                session.PackageDigest,
                receipt.PackageDigest,
                StringComparison.Ordinal) ||
            !string.Equals(
                session.Manifest.CatalogDigest,
                receipt.CatalogDigest,
                StringComparison.Ordinal) ||
            !string.Equals(
                catalogDigest,
                receipt.CatalogDigest,
                StringComparison.Ordinal) ||
            !string.Equals(
                session.Manifest.SnapshotIdentity,
                receipt.SnapshotIdentity,
                StringComparison.Ordinal) ||
            !string.Equals(
                session.Manifest.SnapshotIdentity,
                retainedSnapshotIdentity,
                StringComparison.Ordinal) ||
            !string.Equals(
                session.DataSource.SnapshotIdentity,
                receipt.SnapshotIdentity,
                StringComparison.Ordinal) ||
            !string.Equals(
                session.Manifest.SourceIdentity,
                catalog.Source.Identity,
                StringComparison.Ordinal) ||
            !string.Equals(
                session.Manifest.SourceFingerprint,
                catalog.Source.Fingerprint,
                StringComparison.Ordinal) ||
            capturedPackageBytes <= 0 ||
            receipt.PackageBytes != capturedPackageBytes ||
            session.Manifest.Tables.Count !=
                receipt.TableCount ||
            rowCount != receipt.RowCount)
        {
            throw new InvalidDataException(
                "The retained MySQL package does not match the worker capture receipt.");
        }

        ValidateOpenedSource(catalog, session.DataSource);
    }

    private static void ValidateOpenedMySqlSource(
        MigrationCatalog catalog,
        RetainedMigrationPackageSession session)
    {
        MySqlRetainedManifestBindingValidator.Validate(
            session.Catalog,
            session.Manifest);
        MySqlRetainedManifestBindingValidator.Validate(
            catalog,
            session.Manifest);
        string retainedSnapshotIdentity =
            GetRetainedMySqlSnapshotIdentity(catalog);
        string catalogDigest =
            MigrationArtifactSerializer.ComputeCatalogDigest(
                catalog);
        string retainedCatalogDigest =
            MigrationArtifactSerializer.ComputeCatalogDigest(
                session.Catalog);
        if (!IsRetainedMySqlCatalog(catalog) ||
            !IsRetainedMySqlCatalog(session.Catalog) ||
            !string.Equals(
                session.Manifest.Format,
                RetainedMigrationPackageContract.Format,
                StringComparison.Ordinal) ||
            session.Manifest.SourceKind !=
                MigrationSourceKind.MySql ||
            !string.Equals(
                catalogDigest,
                retainedCatalogDigest,
                StringComparison.Ordinal) ||
            !string.Equals(
                catalogDigest,
                session.Manifest.CatalogDigest,
                StringComparison.Ordinal) ||
            !string.Equals(
                session.Manifest.SourceIdentity,
                catalog.Source.Identity,
                StringComparison.Ordinal) ||
            !string.Equals(
                session.Manifest.SourceFingerprint,
                catalog.Source.Fingerprint,
                StringComparison.Ordinal) ||
            !string.Equals(
                session.Manifest.SnapshotIdentity,
                session.DataSource.SnapshotIdentity,
                StringComparison.Ordinal) ||
            !string.Equals(
                session.Manifest.SnapshotIdentity,
                retainedSnapshotIdentity,
                StringComparison.Ordinal))
        {
            throw new InvalidDataException(
                "The retained MySQL package catalog does not match the supplied catalog artifact.");
        }

        ValidateOpenedSource(catalog, session.DataSource);
    }

    private static bool IsRetainedMySqlCatalog(
        MigrationCatalog catalog)
    {
        if (catalog.Source.Kind != MigrationSourceKind.MySql)
            return false;
        MigrationCatalogObject[] databases = catalog.Objects
            .Where(static item =>
                item.Kind == MigrationObjectKind.Database)
            .ToArray();
        if (databases.Length != 1)
            return false;

        MigrationCatalogObject database = databases[0];
        if (!TryGetSingleFacetValue(
                database.Facets,
                MySqlRetainedCatalogFacet,
                out string catalogContract) ||
            !TryGetSingleFacetValue(
                database.Facets,
                MySqlAnalyzerCatalogFacet,
                out string analyzerContract) ||
            !TryGetSingleFacetValue(
                database.Facets,
                MySqlRetainedDataFacet,
                out string dataContract) ||
            !TryGetSingleFacetValue(
                database.Facets,
                MySqlRetainedContentDigestFacet,
                out string contentDigest) ||
            !TryGetSingleFacetValue(
                database.Facets,
                MySqlRetainedSnapshotIdentityFacet,
                out string snapshotIdentity) ||
            !TryGetSingleFacetValue(
                database.Facets,
                MySqlRetainedMetadataScopeFacet,
                out string metadataScope) ||
            !TryGetSingleFacetValue(
                database.Facets,
                MySqlRetainedDirectSelectFacet,
                out string directSelectProven))
        {
            return false;
        }

        return
            string.Equals(
                catalogContract,
                MySqlRetainedCatalogContract,
                StringComparison.Ordinal) &&
            string.Equals(
                analyzerContract,
                MySqlAnalyzerCatalogContract,
                StringComparison.Ordinal) &&
            string.Equals(
                dataContract,
                MySqlRetainedDataContract,
                StringComparison.Ordinal) &&
            IsCanonicalSha256(contentDigest) &&
            string.Equals(
                catalog.Source.Fingerprint,
                contentDigest,
                StringComparison.Ordinal) &&
            string.Equals(
                snapshotIdentity,
                MySqlRetainedSnapshotIdentityPrefix +
                contentDigest,
                StringComparison.Ordinal) &&
            string.Equals(
                metadataScope,
                MySqlRetainedMetadataScope,
                StringComparison.Ordinal) &&
            string.Equals(
                directSelectProven,
                "true",
                StringComparison.Ordinal) &&
            HasRequiredRetainedMySqlDiagnostic(
                catalog,
                database.ObjectId,
                MySqlRetainedScopeRule) &&
            HasRequiredRetainedMySqlDiagnostic(
                catalog,
                database.ObjectId,
                MySqlRetainedQualificationRule);
    }

    private static string GetRetainedMySqlSnapshotIdentity(
        MigrationCatalog catalog)
    {
        MigrationCatalogObject database = catalog.Objects
            .Single(static item =>
                item.Kind == MigrationObjectKind.Database);
        if (!TryGetSingleFacetValue(
                database.Facets,
                MySqlRetainedSnapshotIdentityFacet,
                out string snapshotIdentity))
        {
            throw new InvalidDataException(
                "The retained MySQL catalog snapshot binding is invalid.");
        }

        return snapshotIdentity;
    }

    private static bool HasRequiredRetainedMySqlDiagnostic(
        MigrationCatalog catalog,
        string databaseObjectId,
        string ruleId)
    {
        MigrationDiagnostic[] matches = catalog.Diagnostics
            .Where(item =>
                string.Equals(
                    item.RuleId,
                    ruleId,
                    StringComparison.Ordinal))
            .ToArray();
        return matches.Length == 1 &&
            matches[0].Severity ==
                MigrationDiagnosticSeverity.Warning &&
            matches[0].Status ==
                MigrationCompatibilityStatus.Conditional &&
            matches[0].Evidence ==
                MigrationEvidenceLevel.Bound &&
            !matches[0].CanOverride &&
            string.Equals(
                matches[0].ObjectId,
                databaseObjectId,
                StringComparison.Ordinal);
    }

    private static void ValidateOpenedSource(
        MigrationCatalog catalog,
        IMigrationDataSource source)
    {
        if (source.Source != catalog.Source)
        {
            throw new InvalidDataException(
                "The migration data source identity does not match the supplied catalog artifact.");
        }
        if (string.IsNullOrWhiteSpace(source.SnapshotIdentity))
            throw new InvalidDataException("The migration data source snapshot identity is missing.");
        if (source is IMigrationCatalogBoundDataSource catalogBoundSource &&
            !string.Equals(
                catalogBoundSource.CatalogDigest,
                MigrationArtifactSerializer.ComputeCatalogDigest(catalog),
                StringComparison.Ordinal))
        {
            throw new InvalidDataException(
                "The migration data source policy does not match the supplied catalog artifact.");
        }
    }

    private static bool IsCanonicalSha256(string value)
    {
        if (value.Length != 71 || !value.StartsWith("sha256:", StringComparison.Ordinal))
            return false;
        foreach (char character in value.AsSpan(7))
        {
            if (character is not (>= '0' and <= '9') and not (>= 'a' and <= 'f'))
                return false;
        }

        return true;
    }

    private static bool IsCanonicalSha256Hex(string value)
    {
        if (value.Length != 64)
            return false;
        foreach (char character in value)
        {
            if (character is not (>= '0' and <= '9') and not (>= 'a' and <= 'f'))
                return false;
        }

        return true;
    }

    private static bool TryParseOptions(
        IReadOnlyList<string> args,
        int startIndex,
        out Dictionary<string, string> options,
        out string? error) => TryParseOptions(
            args,
            startIndex,
            [],
            out options,
            out error);

    private static bool TryParseOptions(
        IReadOnlyList<string> args,
        int startIndex,
        IReadOnlyList<string> valuelessOptions,
        out Dictionary<string, string> options,
        out string? error)
    {
        options = new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase);
        error = null;
        for (int i = startIndex; i < args.Count; i++)
        {
            string option = args[i];
            if (!option.StartsWith("--", StringComparison.Ordinal))
            {
                error = $"Unexpected positional argument '{option}'.";
                return false;
            }
            if (!options.TryAdd(option, string.Empty))
            {
                error = $"Duplicate option '{option}'.";
                return false;
            }
            if (valuelessOptions.Contains(
                    option,
                    StringComparer.OrdinalIgnoreCase))
            {
                options[option] = "true";
                continue;
            }
            if (i + 1 >= args.Count || args[i + 1].StartsWith("--", StringComparison.Ordinal))
            {
                error = $"Missing value for {option}.";
                return false;
            }

            options[option] = args[++i];
        }

        return true;
    }

    private static bool TryParseIdList(
        string value,
        out IReadOnlyList<string> ids,
        out string? error)
    {
        string[] parsed = value
            .Split(',', StringSplitOptions.TrimEntries)
            .Where(item => item.Length > 0)
            .ToArray();
        if (parsed.Length == 0)
        {
            ids = [];
            error = "Accepted id lists must contain at least one non-empty id.";
            return false;
        }
        if (parsed.Distinct(StringComparer.Ordinal).Count() != parsed.Length)
        {
            ids = [];
            error = "Accepted id lists cannot contain duplicates.";
            return false;
        }

        ids = parsed;
        error = null;
        return true;
    }

    private static bool RequireOnly(
        IReadOnlyDictionary<string, string> options,
        IReadOnlyList<string> allowed,
        out string? error)
    {
        string? unknown = options.Keys.FirstOrDefault(
            key => !allowed.Contains(key, StringComparer.OrdinalIgnoreCase));
        error = unknown is null ? null : $"Unknown option: {unknown}.";
        return unknown is null;
    }

    private static async ValueTask WriteArtifactAsync(
        string path,
        string content,
        CancellationToken ct)
    {
        ct.ThrowIfCancellationRequested();
        string? directory = Path.GetDirectoryName(path);
        if (string.IsNullOrEmpty(directory))
            throw new InvalidOperationException($"Artifact path '{path}' has no parent directory.");

        Directory.CreateDirectory(directory);
        string temporaryPath = Path.Combine(
            directory,
            $".csharpdb-migration-{Guid.NewGuid():N}.tmp");
        bool committed = false;
        try
        {
            await using (var stream = new FileStream(
                temporaryPath,
                FileMode.CreateNew,
                FileAccess.Write,
                FileShare.None,
                bufferSize: 4096,
                FileOptions.Asynchronous | FileOptions.SequentialScan))
            await using (var writer = new StreamWriter(stream, Utf8NoBom))
            {
                await writer.WriteAsync(content.AsMemory(), ct);
                await writer.FlushAsync(ct);
            }

            ct.ThrowIfCancellationRequested();
            File.Move(temporaryPath, path, overwrite: true);
            committed = true;
        }
        finally
        {
            if (!committed)
                TryDeleteTemporaryArtifact(temporaryPath);
        }
    }

    private static async ValueTask WriteNewArtifactAsync(
        string path,
        string content,
        CancellationToken ct)
    {
        ct.ThrowIfCancellationRequested();
        string? directory = Path.GetDirectoryName(path);
        if (string.IsNullOrEmpty(directory))
        {
            throw new InvalidOperationException(
                "The artifact destination has no parent directory.");
        }

        Directory.CreateDirectory(directory);
        string temporaryPath = Path.Combine(
            directory,
            $".csharpdb-migration-{Guid.NewGuid():N}.tmp");
        bool committed = false;
        try
        {
            await using (var stream = new FileStream(
                temporaryPath,
                FileMode.CreateNew,
                FileAccess.Write,
                FileShare.None,
                bufferSize: 4096,
                FileOptions.Asynchronous | FileOptions.SequentialScan))
            await using (var writer = new StreamWriter(
                stream,
                Utf8NoBom))
            {
                await writer.WriteAsync(content.AsMemory(), ct);
                await writer.FlushAsync(ct);
            }

            ct.ThrowIfCancellationRequested();
            File.Move(temporaryPath, path, overwrite: false);
            committed = true;
        }
        finally
        {
            if (!committed)
                TryDeleteTemporaryArtifact(temporaryPath);
        }
    }

    private static async ValueTask TryWriteFailureReportAsync(
        string path,
        MigrationPlan plan,
        string sourceSnapshotIdentity,
        Exception error)
    {
        string errorCode = error switch
        {
            MigrationCliSafeException safe => safe.Code,
            MigrationRowRejectedException rejected => rejected.Code,
            MigrationExecutionPolicyException policy => policy.Code,
            MigrationValueException valueError => valueError.Code,
            InvalidDataException => "MIG-APPLY-CONTRACT-001",
            IOException => "MIG-APPLY-TARGET-IO-001",
            NotSupportedException => "MIG-APPLY-UNSUPPORTED-001",
            _ => "MIG-APPLY-OPERATION-001",
        };
        var firstRejectedRow = error is MigrationRowRejectedException rejectedRow
            ? new
            {
                rejectedRow.SourceObjectId,
                rejectedRow.BatchOrdinal,
                rejectedRow.SourceRowOrdinal,
                rejectedRow.ColumnObjectId,
            }
            : null;
        bool deterministicRejects =
            plan.Load.RejectMode == MigrationRejectMode.DeterministicRejects;
        var report = new
        {
            Format = "csharpdb-migration-run/v1",
            Status = "failed",
            PlanDigest = MigrationArtifactSerializer.ComputePlanDigest(plan),
            plan.CatalogDigest,
            plan.CapabilityDigest,
            SourceFingerprint = plan.Source.Fingerprint,
            SourceSnapshotIdentity = sourceSnapshotIdentity,
            RejectContractVersion = deterministicRejects
                ? plan.Load.RejectPolicy?.ContractVersion
                : MigrationRejectContract.DeterministicFailFastV1,
            RejectedRows = deterministicRejects
                ? (long?)null
                : firstRejectedRow is null ? 0L : 1L,
            FirstRejectedRow = deterministicRejects ? null : firstRejectedRow,
            ErrorCode = errorCode,
        };
        try
        {
            await WriteArtifactAsync(
                path,
                JsonSerializer.Serialize(report, JsonOptions),
                CancellationToken.None).ConfigureAwait(false);
        }
        catch (Exception reportError) when (reportError is IOException or UnauthorizedAccessException)
        {
        }
    }

    private static async ValueTask TryWriteAwaitingValidationPublicationFailureReportAsync(
        string path,
        MigrationPlan plan,
        MigrationSourceLease sourceLease,
        MigrationApplyResult result,
        MigrationRejectArtifactWriteResult? rejectArtifact,
        string rejectArtifactStatus,
        string errorCode)
    {
        long rejectedRows = checked(result.RejectedRowsWritten + result.RejectedRowsSkipped);
        var report = new
        {
            Format = "csharpdb-migration-run/v1",
            Status = "awaitingValidation",
            RejectArtifactStatus = rejectArtifactStatus,
            ErrorCode = errorCode,
            result.TargetIdentity,
            result.PlanDigest,
            result.CatalogDigest,
            plan.CapabilityDigest,
            SourceFingerprint = plan.Source.Fingerprint,
            result.SourceSnapshotIdentity,
            SourcePackageFormat = sourceLease.PackageMetadata?.Format,
            SourcePackageManifestDigest =
                sourceLease.PackageMetadata?.ManifestDigest,
            SourcePackageIntentManifestDigest =
                sourceLease.PackageMetadata
                    ?.IntentManifestDigest,
            result.RejectContractVersion,
            result.BatchesWritten,
            result.BatchesSkipped,
            result.RowsWritten,
            result.RowsSkipped,
            RejectedRows = rejectedRows,
            result.RejectedRowsWritten,
            result.RejectedRowsSkipped,
            RejectArtifactFormat = rejectArtifact is null
                ? null
                : MigrationRejectLedgerCodec.ArtifactFormat,
            RejectArtifactDigest = rejectArtifact?.ArtifactDigest,
            RejectArtifactBytes = rejectArtifact?.ArtifactBytes,
            RejectArtifactReused = rejectArtifact?.ReusedExistingArtifact,
            TargetSnapshotIdentity = rejectArtifact?.TargetSnapshotIdentity,
            ExcludedObjects = plan.Objects.Count(item => !item.Included),
            result.PeakBufferedRows,
            result.PeakBufferedBytes,
        };
        try
        {
            await WriteArtifactAsync(
                path,
                JsonSerializer.Serialize(report, JsonOptions),
                CancellationToken.None).ConfigureAwait(false);
        }
        catch (Exception reportError) when (reportError is
            IOException or UnauthorizedAccessException)
        {
        }
    }

    private static bool PathsAreEquivalent(string left, string right) =>
        string.Equals(
            Path.TrimEndingDirectorySeparator(left),
            Path.TrimEndingDirectorySeparator(right),
            PathComparison);

    private static bool ContainsEquivalentPaths(IReadOnlyList<string> paths)
    {
        for (int left = 0; left < paths.Count; left++)
        {
            for (int right = left + 1; right < paths.Count; right++)
            {
                if (PathsAreEquivalent(paths[left], paths[right]))
                    return true;
            }
        }

        return false;
    }

    private static bool ResolvedPathsAreEquivalent(string left, string right) =>
        string.Equals(
            ResolvePathForComparison(left),
            ResolvePathForComparison(right),
            PortableArtifactPathComparison);

    private static bool ContainsEquivalentResolvedPaths(IReadOnlyList<string> paths)
    {
        string[] resolved = paths.Select(ResolvePathForComparison).ToArray();
        for (int left = 0; left < resolved.Length; left++)
        {
            for (int right = left + 1; right < resolved.Length; right++)
            {
                if (string.Equals(
                        resolved[left],
                        resolved[right],
                        PortableArtifactPathComparison))
                {
                    return true;
                }
            }
        }

        return false;
    }

    private static void TryDeleteTemporaryArtifact(string path)
    {
        try
        {
            File.Delete(path);
        }
        catch (IOException)
        {
        }
        catch (UnauthorizedAccessException)
        {
        }
    }

    private static async ValueTask<int> UnsupportedVerbAsync(string verb, TextWriter error)
    {
        await error.WriteLineAsync($"Unsupported migrate command '{verb}'.");
        return await UsageAsync(error);
    }

    private static async ValueTask<int> OptionErrorAsync(string message, TextWriter error)
    {
        await error.WriteLineAsync(message);
        return await UsageAsync(error);
    }

    private static async ValueTask<int> UsageAsync(TextWriter error)
    {
        await error.WriteLineAsync(Usage);
        return InspectorCommandRunner.ExitUsage;
    }

    private static string StatusLabel(int exitCode) =>
        exitCode == InspectorCommandRunner.ExitOk ? "OK" : "REVIEW REQUIRED";

    private static JsonSerializerOptions CreateJsonOptions()
    {
        var options = new JsonSerializerOptions(JsonSerializerDefaults.Web)
        {
            WriteIndented = true,
            DefaultIgnoreCondition = JsonIgnoreCondition.WhenWritingNull,
        };
        options.Converters.Add(new JsonStringEnumConverter(JsonNamingPolicy.CamelCase, allowIntegerValues: false));
        return options;
    }

    internal sealed class SqlServerCaptureWorkspace : IDisposable
    {
        private const uint UnixPrivateDirectoryMode = 0x1C0; // 0700
        private const uint UnixFileTypeMask = 0xF000;
        private const uint UnixDirectoryType = 0x4000;
        private const uint UnixModeMask = 0x0FFF;
        private const int UnixAlreadyExists = 17;
        private const int UnixPermissionDenied = 13;
        private const int UnixAtSymlinkNoFollow = 0x0100;
        private const uint LinuxStatxBasicStats = 0x07FF;
        private const uint LinuxStatxRequired =
            0x0001 | // STATX_TYPE
            0x0002 | // STATX_MODE
            0x0008;  // STATX_UID
        private readonly string rootPath;
        private RetainedCaptureDirectoryLease? directoryLease;
        private int disposed;

        private SqlServerCaptureWorkspace(
            string rootPath,
            string capturePath,
            string verificationWorkspacePath)
        {
            this.rootPath = rootPath;
            CapturePath = capturePath;
            VerificationWorkspacePath =
                verificationWorkspacePath;
        }

        internal string CapturePath { get; }

        internal string VerificationWorkspacePath { get; }

        internal static SqlServerCaptureWorkspace Create(
            string parentPath,
            RetainedCaptureDirectoryLease parentLease) =>
            Create(
                parentPath,
                SqlServerWorkerClient
                    .CaptureWorkspacePrefix,
                SqlServerWorkerClient
                    .CaptureOutputFileName,
                parentLease);

        internal static SqlServerCaptureWorkspace Create(
            string parentPath,
            string workspacePrefix,
            string captureOutputFileName,
            RetainedCaptureDirectoryLease parentLease)
        {
            ArgumentException.ThrowIfNullOrWhiteSpace(
                parentPath);
            ArgumentException.ThrowIfNullOrWhiteSpace(
                workspacePrefix);
            ArgumentException.ThrowIfNullOrWhiteSpace(
                captureOutputFileName);
            ArgumentNullException.ThrowIfNull(parentLease);
            string fullParent = Path.GetFullPath(parentPath);
            for (int attempt = 0; attempt < 8; attempt++)
            {
                parentLease.AssertUnchanged();
                string root = Path.Combine(
                    fullParent,
                    workspacePrefix +
                    Guid.NewGuid().ToString("N"));
                if (Directory.Exists(root) ||
                    File.Exists(root))
                {
                    continue;
                }

                SqlServerCaptureWorkspace? partial =
                    null;
                try
                {
                    CreatePrivateDirectoryExclusive(
                        root);
                    var created =
                        new DirectoryInfo(root);
                    partial =
                        new SqlServerCaptureWorkspace(
                            root,
                            Path.Combine(
                            root,
                                captureOutputFileName),
                            Path.Combine(
                                root,
                                "verified"));
                    if (!PathsAreEquivalent(
                            created.FullName,
                            root))
                    {
                        throw new IOException(
                            "The retained capture workspace resolved unexpectedly.");
                    }
                    FileAttributes rootAttributes =
                        File.GetAttributes(root);
                    if ((rootAttributes &
                        (FileAttributes.ReparsePoint |
                         FileAttributes.Device)) != 0)
                    {
                        throw new IOException(
                            "The retained capture workspace resolved to an unsafe filesystem object.");
                    }
                    string verification =
                        partial.VerificationWorkspacePath;
                    CreatePrivateDirectoryExclusive(
                        verification);
                    FileAttributes verificationAttributes =
                        File.GetAttributes(verification);
                    if ((verificationAttributes &
                        (FileAttributes.ReparsePoint |
                         FileAttributes.Device)) != 0)
                    {
                        throw new IOException(
                            "The retained verification workspace resolved to an unsafe filesystem object.");
                    }
                    parentLease.AssertUnchanged();
                    partial.directoryLease =
                        RetainedCaptureDirectoryLease.Open(root);
                    partial.directoryLease.AssertUnchanged();
                    parentLease.AssertUnchanged();
                    return partial;
                }
                catch (Exception creationFailure) when (
                    creationFailure is IOException or
                        UnauthorizedAccessException or
                        InvalidDataException)
                {
                    if (partial is not null)
                    {
                        try
                        {
                            partial.Dispose();
                        }
                        catch (
                            RetainedCaptureWorkspaceCleanupException
                                cleanupFailure)
                        {
                            throw new RetainedCaptureWorkspaceCleanupException(
                                creationFailure,
                                cleanupFailure);
                        }
                    }

                    if (attempt >= 7)
                        throw;
                }
            }

            throw new IOException(
                "A private retained capture workspace could not be created.");
        }

        internal void AssertUnchanged()
        {
            if (Volatile.Read(ref disposed) != 0)
            {
                throw new ObjectDisposedException(
                    nameof(SqlServerCaptureWorkspace));
            }

            RetainedCaptureDirectoryLease lease =
                directoryLease ??
                throw new IOException(
                    "The retained capture workspace identity lease is unavailable.");
            lease.AssertUnchanged();
            RequireKnownWorkspaceEntries();
        }

        public void Dispose()
        {
            if (Interlocked.Exchange(ref disposed, 1) != 0)
                return;
            RetainedCaptureDirectoryLease? lease =
                directoryLease;
            directoryLease = null;
            try
            {
                if (lease is null)
                    throw new IOException(
                        "The retained capture workspace identity lease is unavailable during cleanup.");

                lease.AssertUnchanged();
                RequireKnownWorkspaceEntries();

                if (File.Exists(CapturePath))
                {
                    RequireRegularWorkspaceFile(CapturePath);
                    File.Delete(CapturePath);
                }

                if (!Directory.Exists(
                        VerificationWorkspacePath))
                {
                    throw new IOException(
                        "The retained verification workspace disappeared before cleanup.");
                }
                RequireRealWorkspaceDirectory(
                    VerificationWorkspacePath);
                Directory.Delete(
                    VerificationWorkspacePath,
                    recursive: false);

                lease.AssertUnchanged();
                lease.Dispose();
                lease = null;
                Directory.Delete(
                    rootPath,
                    recursive: false);
            }
            catch (Exception exception) when (
                exception is IOException or
                    UnauthorizedAccessException or
                    InvalidDataException)
            {
                throw new RetainedCaptureWorkspaceCleanupException(
                    exception);
            }
            finally
            {
                lease?.Dispose();
            }
        }

        private void RequireKnownWorkspaceEntries()
        {
            string[] entries =
                Directory.GetFileSystemEntries(rootPath);
            foreach (string entry in entries)
            {
                if (PathsAreEquivalent(entry, CapturePath))
                {
                    RequireRegularWorkspaceFile(entry);
                    continue;
                }
                if (PathsAreEquivalent(
                        entry,
                        VerificationWorkspacePath))
                {
                    RequireRealWorkspaceDirectory(entry);
                    continue;
                }

                throw new IOException(
                    "The retained capture workspace contains an unexpected entry; it was preserved for manual review.");
            }
        }

        private static void RequireRegularWorkspaceFile(
            string path)
        {
            FileAttributes attributes =
                File.GetAttributes(path);
            if ((attributes &
                (FileAttributes.Directory |
                 FileAttributes.ReparsePoint |
                 FileAttributes.Device)) != 0)
            {
                throw new IOException(
                    "A retained capture workspace file changed into an unsafe filesystem object.");
            }
        }

        private static void RequireRealWorkspaceDirectory(
            string path)
        {
            FileAttributes attributes =
                File.GetAttributes(path);
            if ((attributes & FileAttributes.Directory) == 0 ||
                (attributes &
                    (FileAttributes.ReparsePoint |
                     FileAttributes.Device)) != 0)
            {
                throw new IOException(
                    "A retained capture workspace directory changed into an unsafe filesystem object.");
            }
        }

        internal static void
            CreatePrivateDirectoryExclusive(
            string path)
        {
            if (OperatingSystem.IsWindows())
            {
                byte[] descriptor =
                    CreatePrivateWindowsSecurityDescriptor();
                GCHandle descriptorHandle =
                    GCHandle.Alloc(
                        descriptor,
                        GCHandleType.Pinned);
                int error;
                try
                {
                    var securityAttributes =
                        new SecurityAttributes
                        {
                            Length = checked(
                                (uint)Marshal.SizeOf<
                                    SecurityAttributes>()),
                            SecurityDescriptor =
                                descriptorHandle
                                    .AddrOfPinnedObject(),
                        };
                    if (CreateDirectoryW(
                            path,
                            ref securityAttributes))
                    {
                        return;
                    }
                    error =
                        Marshal.GetLastPInvokeError();
                }
                finally
                {
                    descriptorHandle.Free();
                    CryptographicOperations
                        .ZeroMemory(descriptor);
                }

                Exception nativeFailure =
                    new Win32Exception(error);
                if (error == 5)
                {
                    throw new UnauthorizedAccessException(
                        "Access to the retained capture workspace parent was denied.",
                        nativeFailure);
                }
                throw new IOException(
                    "The private retained capture workspace directory could not be created.",
                    nativeFailure);
            }

            if (UnixMakeDirectory(
                    path,
                    UnixPrivateDirectoryMode) != 0)
            {
                int unixError =
                    Marshal.GetLastPInvokeError();
                Exception nativeFailure =
                    new Win32Exception(unixError);
                if (unixError == UnixAlreadyExists)
                {
                    throw new IOException(
                        "The retained capture workspace candidate already exists.",
                        nativeFailure);
                }
                if (unixError is 1 or
                    UnixPermissionDenied)
                {
                    throw new UnauthorizedAccessException(
                        "Access to the retained capture workspace parent was denied.",
                        nativeFailure);
                }
                throw new IOException(
                    "The private retained capture workspace directory could not be created.",
                    nativeFailure);
            }

            UnixFileMode privateMode =
                UnixFileMode.UserRead |
                UnixFileMode.UserWrite |
                UnixFileMode.UserExecute;
            try
            {
                File.SetUnixFileMode(path, privateMode);
                VerifyPrivateUnixDirectory(path);
            }
            catch (Exception creationFailure)
            {
                try
                {
                    Directory.Delete(
                        path,
                        recursive: false);
                }
                catch (DirectoryNotFoundException)
                {
                }
                catch (Exception cleanupFailure) when (
                    cleanupFailure is IOException or
                        UnauthorizedAccessException)
                {
                    throw new IOException(
                        "The private retained capture workspace could not be verified or cleaned safely.",
                        new AggregateException(
                            creationFailure,
                            cleanupFailure));
                }

                ExceptionDispatchInfo.Capture(
                    creationFailure).Throw();
                throw;
            }
        }

        [UnsupportedOSPlatform("windows")]
        private static void VerifyPrivateUnixDirectory(
            string path)
        {
            FileAttributes attributes =
                File.GetAttributes(path);
            if ((attributes & FileAttributes.Directory) == 0 ||
                (attributes &
                    (FileAttributes.ReparsePoint |
                     FileAttributes.Device)) != 0)
            {
                throw new IOException(
                    "The retained capture workspace is not a real directory.");
            }

            UnixFileMode mode =
                File.GetUnixFileMode(path);
            UnixFileMode expectedMode =
                UnixFileMode.UserRead |
                UnixFileMode.UserWrite |
                UnixFileMode.UserExecute;
            if (mode != expectedMode)
            {
                throw new IOException(
                    "The retained capture workspace does not have owner-only Unix permissions.");
            }

            UnixDirectoryMetadata? metadata =
                ReadUnixDirectoryMetadata(path);
            if (metadata is null)
                return;
            if ((metadata.Value.Mode &
                    UnixFileTypeMask) !=
                    UnixDirectoryType ||
                (metadata.Value.Mode &
                    UnixModeMask) !=
                    UnixPrivateDirectoryMode ||
                metadata.Value.OwnerUserId !=
                    UnixGetEffectiveUserId())
            {
                throw new IOException(
                    "The retained capture workspace Unix owner or mode is unsafe.");
            }
        }

        private static UnixDirectoryMetadata?
            ReadUnixDirectoryMetadata(string path)
        {
            if (OperatingSystem.IsLinux())
            {
                try
                {
                    if (LinuxStatx(
                            -100,
                            path,
                            UnixAtSymlinkNoFollow,
                            LinuxStatxBasicStats,
                            out LinuxStatxBuffer metadata) != 0)
                    {
                        int error =
                            Marshal.GetLastPInvokeError();
                        if (error == 38)
                            return null;
                        throw new IOException(
                            "The retained capture workspace Unix identity could not be read.",
                            new Win32Exception(error));
                    }
                    if ((metadata.Mask &
                            LinuxStatxRequired) !=
                            LinuxStatxRequired)
                    {
                        throw new IOException(
                            "The retained capture workspace Unix identity is incomplete.");
                    }
                    return new UnixDirectoryMetadata(
                        metadata.Mode,
                        metadata.UserId);
                }
                catch (EntryPointNotFoundException)
                {
                    return null;
                }
            }

            if (OperatingSystem.IsMacOS())
            {
                if (DarwinFileStatus(
                        path,
                        out DarwinStatBuffer metadata) != 0)
                {
                    throw new IOException(
                        "The retained capture workspace Unix identity could not be read.",
                        new Win32Exception(
                            Marshal.GetLastPInvokeError()));
                }
                return new UnixDirectoryMetadata(
                    metadata.Mode,
                    metadata.UserId);
            }

            return null;
        }

        [SupportedOSPlatform("windows")]
        private static byte[]
            CreatePrivateWindowsSecurityDescriptor()
        {
            using WindowsIdentity identity =
                WindowsIdentity.GetCurrent(
                    TokenAccessLevels.Query);
            SecurityIdentifier owner =
                identity.User ??
                throw new IOException(
                    "The current Windows identity does not have a security identifier.");
            var security =
                new DirectorySecurity();
            security.SetOwner(owner);
            security.SetAccessRuleProtection(
                isProtected: true,
                preserveInheritance: false);
            security.AddAccessRule(
                new FileSystemAccessRule(
                    owner,
                    FileSystemRights.FullControl,
                    InheritanceFlags.ContainerInherit |
                    InheritanceFlags.ObjectInherit,
                    PropagationFlags.None,
                    AccessControlType.Allow));
            return security
                .GetSecurityDescriptorBinaryForm();
        }

        [DllImport(
            "kernel32.dll",
            EntryPoint = "CreateDirectoryW",
            CharSet = CharSet.Unicode,
            ExactSpelling = true,
            SetLastError = true)]
        [return: MarshalAs(UnmanagedType.Bool)]
        private static extern bool CreateDirectoryW(
            string path,
            ref SecurityAttributes
                securityAttributes);

        [DllImport(
            "libc",
            EntryPoint = "mkdir",
            SetLastError = true)]
        private static extern int UnixMakeDirectory(
            [MarshalAs(UnmanagedType.LPUTF8Str)]
            string path,
            uint mode);

        [DllImport(
            "libc",
            EntryPoint = "geteuid",
            SetLastError = false)]
        private static extern uint
            UnixGetEffectiveUserId();

        [DllImport(
            "libc",
            EntryPoint = "statx",
            SetLastError = true)]
        private static extern int LinuxStatx(
            int directoryDescriptor,
            [MarshalAs(UnmanagedType.LPUTF8Str)]
            string path,
            int flags,
            uint mask,
            out LinuxStatxBuffer metadata);

        [DllImport(
            "libc",
            EntryPoint = "lstat",
            SetLastError = true)]
        private static extern int DarwinFileStatus(
            [MarshalAs(UnmanagedType.LPUTF8Str)]
            string path,
            out DarwinStatBuffer metadata);

        [StructLayout(LayoutKind.Sequential)]
        private struct SecurityAttributes
        {
            internal uint Length;

            internal IntPtr SecurityDescriptor;

            internal int InheritHandle;
        }

        private readonly record struct
            UnixDirectoryMetadata(
            uint Mode,
            uint OwnerUserId);

        [StructLayout(
            LayoutKind.Explicit,
            Size = 256)]
        private struct LinuxStatxBuffer
        {
            [FieldOffset(0)]
            internal uint Mask;

            [FieldOffset(20)]
            internal uint UserId;

            [FieldOffset(28)]
            internal ushort Mode;
        }

        [StructLayout(
            LayoutKind.Explicit,
            Size = 144)]
        private struct DarwinStatBuffer
        {
            [FieldOffset(4)]
            internal ushort Mode;

            [FieldOffset(16)]
            internal uint UserId;
        }
    }

    private sealed class RetainedCaptureWorkspaceCleanupException
        : IOException
    {
        internal RetainedCaptureWorkspaceCleanupException(
            Exception cleanupFailure)
            : base(
                "The private retained capture workspace could not be cleaned safely.",
                cleanupFailure)
        {
        }

        internal RetainedCaptureWorkspaceCleanupException(
            Exception operationFailure,
            Exception cleanupFailure)
            : base(
                "The retained capture operation and private workspace cleanup both failed.",
                new AggregateException(
                    operationFailure,
                    cleanupFailure))
        {
        }
    }

    private sealed class MigrationCliSafeException : Exception
    {
        internal MigrationCliSafeException(string code, string message, Exception innerException)
            : base(message, innerException)
        {
            ArgumentException.ThrowIfNullOrWhiteSpace(code);
            Code = code;
        }

        internal string Code { get; }
    }

    private sealed class DdlScriptLimitException : Exception
    {
    }

    private sealed class MigrationSourceLease : IAsyncDisposable
    {
        private readonly IAsyncDisposable owner;

        internal MigrationSourceLease(
            IMigrationDataSource source,
            IAsyncDisposable owner,
            MigrationSourcePackageMetadata? packageMetadata)
        {
            Source = source;
            this.owner = owner;
            PackageMetadata = packageMetadata;
        }

        internal IMigrationDataSource Source { get; }

        internal MigrationSourcePackageMetadata? PackageMetadata { get; }

        public ValueTask DisposeAsync() => owner.DisposeAsync();
    }

    private sealed record MigrationSourcePackageMetadata(
        string Format,
        string ManifestDigest,
        string? IntentManifestDigest = null);

    private sealed record ParsedPlanLoadPolicy(
        MigrationLoadPolicy Load,
        bool UseAllRuleIds);

    private enum JsonCatalogRoute
    {
        Unsupported,
        UntypedV1,
        TypedV2,
    }

    private sealed record PreviewCounts(
        int TotalObjects,
        int IncludedObjects,
        int ExcludedObjects,
        int Exact,
        int LosslessReencoded,
        int Lossy,
        int Unsupported,
        int Information,
        int Warning,
        int Error)
    {
        public static PreviewCounts Create(MigrationPlan plan)
        {
            MigrationTypeMapping[] mappings = plan.Objects.SelectMany(item => item.TypeMappings).ToArray();
            return new PreviewCounts(
                plan.Objects.Count,
                plan.Objects.Count(item => item.Included),
                plan.Objects.Count(item => !item.Included),
                mappings.Count(item => item.Classification == MigrationMappingClassification.Exact),
                mappings.Count(item => item.Classification == MigrationMappingClassification.LosslessReencoded),
                mappings.Count(item => item.Classification == MigrationMappingClassification.Lossy),
                mappings.Count(item => item.Classification == MigrationMappingClassification.Unsupported),
                plan.Diagnostics.Count(item => item.Severity == MigrationDiagnosticSeverity.Information),
                plan.Diagnostics.Count(item => item.Severity == MigrationDiagnosticSeverity.Warning),
                plan.Diagnostics.Count(item => item.Severity == MigrationDiagnosticSeverity.Error));
        }
    }
}
