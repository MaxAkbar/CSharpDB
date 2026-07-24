using System.Globalization;
using System.Runtime.ExceptionServices;
using System.Text;
using System.Text.Json;
using System.Text.Json.Serialization;
using CSharpDB.Engine;
using CSharpDB.Migration;
using CSharpDB.Migration.CSharpDb;
using CSharpDB.Migration.Files.Csv;
using CSharpDB.Migration.Files.Json;
using CSharpDB.Migration.Validation;

namespace CSharpDB.Cli;

internal static class MigrationCommandRunner
{
    internal const string Usage =
        "Usage: csharpdb migrate inspect --source synthetic --out <catalog.json>\n" +
        "       csharpdb migrate inspect --source csv --input <source.csv> --package <source.csdbcsv> --out <catalog.json> [--delimiter auto|comma|semicolon|tab|pipe|<character>] [--no-header] [--table <name>] [--sample-rows <count>] [--null-token <text>] [--source-id <label>] [--workspace <directory>] [--max-source-bytes <count>]\n" +
        "       csharpdb migrate inspect --source json --input <source.json|source.ndjson> --package <source.csdbjson> --out <catalog.json> [--framing root-array|ndjson] [--table <name>] [--sample-rows <count>] [--source-id <label>] [--workspace <directory>] [--max-source-bytes <count>] [--typed-intent <source.csdbjson-intent.json> --expected-intent-manifest-digest <sha256:...>]\n" +
        "       csharpdb migrate plan <catalog.json> --out <plan.json> [--profile preserve|queryable] [--accept-exclusions all|<id,...>] [--accept-diagnostics <id,...>] [--reject-mode fail-fast|deterministic --reject-rules all|<id,...> --max-rejected-rows-per-batch <count> --max-rejected-rows-per-run <count> --max-reject-evidence-value-bytes <count> --max-reject-evidence-bytes-per-batch <count> --max-reject-evidence-bytes-per-run <count> --max-reject-artifact-bytes <count>]\n" +
        "       csharpdb migrate preview <plan.json> --catalog <catalog.json> [--format text|json]\n" +
        "       csharpdb migrate apply <plan.json> --catalog <catalog.json> [--source-package <source.csdbcsv|source.csdbjson> --expected-manifest-digest <sha256:...> --workspace <directory> --max-source-bytes <count>] --target <staged.csdb> --out <run.json> [--resume] [--allow-deterministic-rejects --reject-artifact <absolute-normalized-rejects.jsonl>] [--format text|json]\n" +
        "       csharpdb migrate validate <plan.json> --catalog <catalog.json> [--source-package <source.csdbcsv|source.csdbjson> --expected-manifest-digest <sha256:...> --workspace <directory> --max-source-bytes <count>] --target <staged.csdb> --out <validation.json> [--level schema|count|checksum] [--spill-dir <directory>] [--allow-deterministic-rejects --reject-artifact <absolute-normalized-rejects.jsonl>] [--format text|json]\n" +
        "       csharpdb migrate export <retained-snapshot.db> --format csv --table <physical-table> --out <table.csv> --manifest <table.manifest.json> --expected-snapshot-identity <csharpdb-retained-snapshot/v1:<bytes>:sha256:<64-lowercase-hex>> [--profile lossless-v1|spreadsheet-safe-lossy-v1] [--max-data-bytes <count>] [--max-decoded-blob-bytes <count>] [--checkpoint-row-interval <count>] [--json]\n" +
        "       csharpdb migrate export <retained-snapshot.db> --format json|ndjson --table <physical-table> --out <table.json|table.ndjson> --manifest <table.manifest.json> --expected-snapshot-identity <csharpdb-retained-snapshot/v1:<bytes>:sha256:<64-lowercase-hex>> [--profile lossless-v1] [--max-data-bytes <count>] [--max-decoded-blob-bytes <count>] [--checkpoint-row-interval <count>] [--json]";

    private static readonly JsonSerializerOptions JsonOptions = CreateJsonOptions();
    private static readonly UTF8Encoding Utf8NoBom = new(encoderShouldEmitUTF8Identifier: false);
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

    public static bool IsKnownCommand(string? arg) =>
        string.Equals(arg, "migrate", StringComparison.OrdinalIgnoreCase);

    public static async ValueTask<int> RunAsync(
        string[] args,
        TextWriter output,
        TextWriter error,
        CancellationToken ct = default)
    {
        if (args.Length < 2 || !IsKnownCommand(args[0]))
            return await UsageAsync(error);

        try
        {
            return args[1].ToLowerInvariant() switch
            {
                "inspect" => await RunInspectAsync(args, output, error, ct),
                "plan" => await RunPlanAsync(args, output, error, ct),
                "preview" => await RunPreviewAsync(args, output, error, ct),
                "apply" => await RunApplyAsync(args, output, error, ct),
                "validate" => await RunValidateAsync(args, output, error, ct),
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
        CancellationToken ct)
    {
        if (!TryParseOptions(
                args,
                2,
                ["--no-header"],
                out Dictionary<string, string> options,
                out string? parseError))
        {
            return await OptionErrorAsync(parseError!, error);
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

        return await OptionErrorAsync($"Unsupported migration source '{source}'.", error);
    }

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
        MigrationCatalog catalog = MigrationArtifactSerializer.DeserializeCatalog(
            await File.ReadAllTextAsync(catalogPath, ct));
        if (catalog.Source.Kind == MigrationSourceKind.Json &&
            ClassifyJsonCatalog(
                catalog,
                out _) == JsonCatalogRoute.Unsupported)
        {
            return await OptionErrorAsync(
                JsonCatalogRouteOnlyMessage,
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
        await WriteArtifactAsync(
            outputPath,
            MigrationArtifactSerializer.SerializePlan(plan, catalog),
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
        CancellationToken ct)
    {
        if (args.Length < 3 || args[2].StartsWith("--", StringComparison.Ordinal))
            return await OptionErrorAsync("Missing plan artifact path.", error);
        if (!TryParseOptions(args, 3, out Dictionary<string, string> options, out string? parseError))
            return await OptionErrorAsync(parseError!, error);
        if (!RequireOnly(options, ["--catalog", "--format"], out parseError))
            return await OptionErrorAsync(parseError!, error);
        if (!options.TryGetValue("--catalog", out string? catalogValue))
            return await OptionErrorAsync("Missing required option --catalog.", error);

        string format = options.GetValueOrDefault("--format", "text");
        if (!string.Equals(format, "text", StringComparison.OrdinalIgnoreCase) &&
            !string.Equals(format, "json", StringComparison.OrdinalIgnoreCase))
        {
            return await OptionErrorAsync($"Unsupported preview format '{format}'.", error);
        }

        string planPath = Path.GetFullPath(args[2]);
        string catalogPath = Path.GetFullPath(catalogValue);
        MigrationCatalog catalog = MigrationArtifactSerializer.DeserializeCatalog(
            await File.ReadAllTextAsync(catalogPath, ct));
        MigrationPlan plan = MigrationArtifactSerializer.DeserializePlan(
            await File.ReadAllTextAsync(planPath, ct),
            catalog);
        MigrationPlanReadiness readiness = MigrationPlanReadinessValidator.Evaluate(plan, catalog);
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

        if (catalog.Source.Kind is MigrationSourceKind.Csv or MigrationSourceKind.Json)
        {
            if (!hasPackage)
            {
                error = catalog.Source.Kind == MigrationSourceKind.Csv
                    ? "Missing required option --source-package for a CSV migration."
                    : "Missing required option --source-package for a JSON migration.";
                return false;
            }
            if (!hasDigest)
            {
                error = catalog.Source.Kind == MigrationSourceKind.Csv
                    ? "Missing required option --expected-manifest-digest for a CSV migration."
                    : "Missing required option --expected-manifest-digest for a JSON migration.";
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
            if (valuelessOptions.Contains(option))
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
