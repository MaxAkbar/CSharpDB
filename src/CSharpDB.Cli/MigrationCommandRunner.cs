using System.Globalization;
using System.Runtime.ExceptionServices;
using System.Text;
using System.Text.Json;
using System.Text.Json.Serialization;
using CSharpDB.Migration;
using CSharpDB.Migration.CSharpDb;
using CSharpDB.Migration.Files.Csv;
using CSharpDB.Migration.Validation;

namespace CSharpDB.Cli;

internal static class MigrationCommandRunner
{
    internal const string Usage =
        "Usage: csharpdb migrate inspect --source synthetic --out <catalog.json>\n" +
        "       csharpdb migrate inspect --source csv --input <source.csv> --package <source.csdbcsv> --out <catalog.json> [--delimiter auto|comma|semicolon|tab|pipe|<character>] [--no-header] [--table <name>] [--sample-rows <count>] [--null-token <text>] [--source-id <label>] [--workspace <directory>] [--max-source-bytes <count>]\n" +
        "       csharpdb migrate plan <catalog.json> --out <plan.json> [--profile preserve|queryable] [--accept-exclusions all|<id,...>] [--accept-diagnostics <id,...>]\n" +
        "       csharpdb migrate preview <plan.json> --catalog <catalog.json> [--format text|json]\n" +
        "       csharpdb migrate apply <plan.json> --catalog <catalog.json> [--source-package <source.csdbcsv> --expected-manifest-digest <sha256:...> --workspace <directory> --max-source-bytes <count>] --target <staged.csdb> --out <run.json> [--resume] [--format text|json]\n" +
        "       csharpdb migrate validate <plan.json> --catalog <catalog.json> [--source-package <source.csdbcsv> --expected-manifest-digest <sha256:...> --workspace <directory> --max-source-bytes <count>] --target <staged.csdb> --out <validation.json> [--level schema|count|checksum] [--spill-dir <directory>] [--format text|json]";

    private static readonly JsonSerializerOptions JsonOptions = CreateJsonOptions();
    private static readonly UTF8Encoding Utf8NoBom = new(encoderShouldEmitUTF8Identifier: false);
    private static readonly StringComparison PathComparison =
        OperatingSystem.IsWindows() || OperatingSystem.IsMacOS()
        ? StringComparison.OrdinalIgnoreCase
        : StringComparison.Ordinal;
    private const StringComparison PortableArtifactPathComparison =
        StringComparison.OrdinalIgnoreCase;

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
                CsvSnapshotPackageException packageError =>
                    $"{packageError.RuleId}: {packageError.Message}",
                CsvSourceSnapshotException snapshotError =>
                    $"{snapshotError.RuleId}: {snapshotError.Message}",
                _ => ex.Message,
            };
            await error.WriteLineAsync($"Error: {message}");
            return InspectorCommandRunner.ExitError;
        }
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
                ["--out", "--profile", "--accept-exclusions", "--accept-diagnostics"],
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

        MigrationCatalog catalog = MigrationArtifactSerializer.DeserializeCatalog(
            await File.ReadAllTextAsync(catalogPath, ct));
        MigrationPlan plan = new MigrationPlanner().CreatePlan(
            catalog,
            new MigrationPlanningOptions
            {
                MappingProfile = profile,
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
        await output.WriteLineAsync(
            $"Status: {StatusLabel(exitCode)} | plan={outputPath} | included={plan.Objects.Count(item => item.Included)} | excluded={plan.Objects.Count(item => !item.Included)} | diagnostics={plan.Diagnostics.Count}");
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
                ["--resume"],
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
        bool protectedPathsCollide = options.ContainsKey("--source-package")
            ? ContainsEquivalentResolvedPaths(protectedPaths)
            : ContainsEquivalentPaths(protectedPaths);
        if (protectedPathsCollide)
        {
            return await OptionErrorAsync(
                "Plan, catalog, source package, staged target, target companions, and run report must use different files.",
                error);
        }
        if (!ValidateWorkspacePath(options, protectedPaths, out parseError))
            return await OptionErrorAsync(parseError!, error);

        MigrationCatalog catalog = MigrationArtifactSerializer.DeserializeCatalog(
            await File.ReadAllTextAsync(catalogPath, ct));
        MigrationPlan plan = MigrationArtifactSerializer.DeserializePlan(
            await File.ReadAllTextAsync(planPath, ct),
            catalog);
        if (!ValidateSourceOptions(catalog, options, out parseError))
            return await OptionErrorAsync(parseError!, error);
        MigrationPlanReadinessValidator.ValidateForApply(plan, catalog);
        try
        {
            MigrationApplyPolicyValidator.ValidateForExecution(plan);
        }
        catch (MigrationExecutionPolicyException policyFailure) when (
            catalog.Source.Kind == MigrationSourceKind.Synthetic &&
            string.Equals(
                catalog.Source.Identity,
                SyntheticMigrationSourceInspector.FixtureIdentity,
                StringComparison.Ordinal))
        {
            await TryWriteFailureReportAsync(
                runOutputPath,
                plan,
                SyntheticMigrationDataSource.FixtureSnapshotIdentity,
                policyFailure);
            throw;
        }

        await using MigrationSourceLease sourceLease = await OpenMigrationSourceAsync(
            catalog,
            options,
            ct);
        IMigrationDataSource source = sourceLease.Source;
        try
        {
            await using CSharpDbStagedMigrationTarget target = options.ContainsKey("--resume")
                ? await CSharpDbStagedMigrationTarget.OpenResumeAsync(
                    targetPath,
                    plan,
                    catalog,
                    source.SnapshotIdentity,
                    cancellationToken: ct)
                : await CSharpDbStagedMigrationTarget.CreateNewAsync(
                    targetPath,
                    plan,
                    catalog,
                    source.SnapshotIdentity,
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
                SourcePackageFormat = sourceLease.PackageManifest is null
                    ? null
                    : CsvSnapshotPackage.Format,
                SourcePackageManifestDigest = sourceLease.PackageManifest?.ManifestDigest,
                result.RejectContractVersion,
                result.BatchesWritten,
                result.BatchesSkipped,
                result.RowsWritten,
                result.RowsSkipped,
                RejectedRows = 0,
                ExcludedObjects = plan.Objects.Count(item => !item.Included),
                result.PeakBufferedRows,
                result.PeakBufferedBytes,
            };
            string runJson = JsonSerializer.Serialize(runReport, JsonOptions);
            await WriteArtifactAsync(runOutputPath, runJson, ct);

            if (string.Equals(format, "json", StringComparison.OrdinalIgnoreCase))
            {
                await output.WriteLineAsync(runJson);
            }
            else
            {
                await output.WriteLineAsync(
                    $"Status: AWAITING VALIDATION | targetId={result.TargetIdentity} | batches={result.BatchesWritten} written/{result.BatchesSkipped} resumed | rows={result.RowsWritten} written/{result.RowsSkipped} resumed | report={runOutputPath}");
            }

            return plan.Objects.Any(item => !item.Included)
                ? InspectorCommandRunner.ExitWarn
                : InspectorCommandRunner.ExitOk;
        }
        catch (OperationCanceledException) when (ct.IsCancellationRequested)
        {
            throw;
        }
        catch (Exception ex)
        {
            await TryWriteFailureReportAsync(
                runOutputPath,
                plan,
                source.SnapshotIdentity,
                ex).ConfigureAwait(false);
            throw;
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
        if (!TryParseOptions(args, 3, out Dictionary<string, string> options, out string? parseError))
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
        bool protectedPathsCollide = options.ContainsKey("--source-package")
            ? ContainsEquivalentResolvedPaths(protectedPaths)
            : ContainsEquivalentPaths(protectedPaths);
        if (protectedPathsCollide)
        {
            return await OptionErrorAsync(
                "Plan, catalog, source package, staged target, target companions, and validation report must use different files.",
                error);
        }
        if (!ValidateWorkspacePath(options, protectedPaths, out parseError))
            return await OptionErrorAsync(parseError!, error);
        if (!Directory.Exists(spillRoot))
            return await OptionErrorAsync($"Validation spill directory '{spillRoot}' does not exist.", error);

        MigrationCatalog catalog = MigrationArtifactSerializer.DeserializeCatalog(
            await File.ReadAllTextAsync(catalogPath, ct));
        MigrationPlan plan = MigrationArtifactSerializer.DeserializePlan(
            await File.ReadAllTextAsync(planPath, ct),
            catalog);
        if (!ValidateSourceOptions(catalog, options, out parseError))
            return await OptionErrorAsync(parseError!, error);
        MigrationPlanReadinessValidator.ValidateForApply(plan, catalog);
        MigrationValidationPolicyValidator.ValidateForExecution(plan);

        MigrationValidationLevel requiredLevel = plan.Validation.ValidateChecksums
            ? MigrationValidationLevel.Checksum
            : plan.Validation.ValidateCounts
                ? MigrationValidationLevel.Count
                : MigrationValidationLevel.Schema;
        MigrationValidationLevel level = requestedLevel ?? requiredLevel;

        await using MigrationSourceLease sourceLease = await OpenMigrationSourceAsync(
            catalog,
            options,
            ct);
        IMigrationDataSource source = sourceLease.Source;
        await using var sourceSnapshot = new MigrationDataSourceValidationSnapshot(plan, catalog, source);
        await using CSharpDbStagedMigrationTarget target = await CSharpDbStagedMigrationTarget.OpenResumeAsync(
            targetPath,
            plan,
            catalog,
            source.SnapshotIdentity,
            cancellationToken: ct);
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
            },
            ct);

        if (string.Equals(format, "json", StringComparison.OrdinalIgnoreCase))
        {
            await output.WriteLineAsync(await File.ReadAllTextAsync(reportPath, ct));
        }
        else
        {
            await output.WriteAsync(MigrationValidationTextFormatter.Format(result.Report));
            await output.WriteLineAsync($"Activation: {(result.Activated ? "activated" : "withheld")}");
            await output.WriteLineAsync($"JSON report: {reportPath}");
        }

        return result.Report.Outcome switch
        {
            MigrationValidationStatus.Passed => plan.Objects.Any(item => !item.Included)
                ? InspectorCommandRunner.ExitWarn
                : InspectorCommandRunner.ExitOk,
            MigrationValidationStatus.Inconclusive or MigrationValidationStatus.Skipped =>
                InspectorCommandRunner.ExitWarn,
            _ => InspectorCommandRunner.ExitError,
        };
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
            error = "The CSV workspace path cannot be blank.";
            return false;
        }
        if (options.TryGetValue("--max-source-bytes", out string? maxSourceBytesValue) &&
            !TryParseSourceByteLimit(maxSourceBytesValue, out _))
        {
            error =
                "The CSV source byte limit must be a non-negative 64-bit integer below Int64.MaxValue.";
            return false;
        }

        error = null;
        return true;
    }

    private static bool ValidateSourceOptions(
        MigrationCatalog catalog,
        IReadOnlyDictionary<string, string> options,
        out string? error)
    {
        bool hasPackage = options.ContainsKey("--source-package");
        bool hasDigest = options.ContainsKey("--expected-manifest-digest");
        bool hasCsvEnvironment = options.ContainsKey("--workspace") ||
            options.ContainsKey("--max-source-bytes");

        if (catalog.Source.Kind == MigrationSourceKind.Csv)
        {
            if (!hasPackage)
            {
                error = "Missing required option --source-package for a CSV migration.";
                return false;
            }
            if (!hasDigest)
            {
                error = "Missing required option --expected-manifest-digest for a CSV migration.";
                return false;
            }

            error = null;
            return true;
        }

        if (catalog.Source.Kind == MigrationSourceKind.Synthetic &&
            (hasPackage || hasDigest || hasCsvEnvironment))
        {
            error = "CSV source-package options cannot be used with a synthetic migration.";
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
            error = "The CSV workspace must be an existing caller-controlled directory.";
            return false;
        }

        FileAttributes attributes = File.GetAttributes(workspacePath);
        if ((attributes & (FileAttributes.ReparsePoint | FileAttributes.Device)) != 0)
        {
            error = "The CSV workspace cannot be a link, reparse point, or device.";
            return false;
        }

        foreach (string protectedFilePath in protectedFilePaths)
        {
            if (ResolvedPathsAreEquivalent(workspacePath, protectedFilePath) ||
                IsPathWithin(workspacePath, protectedFilePath))
            {
                error =
                    "The CSV workspace cannot be the same as or nested beneath an artifact file path.";
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
            "A CSV migration path does not have a filesystem root.");
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
                        "CSV migration artifact paths cannot resolve through devices.");
                }

                if ((attributes & FileAttributes.ReparsePoint) != 0)
                {
                    FileSystemInfo link = (attributes & FileAttributes.Directory) != 0
                        ? new DirectoryInfo(candidate)
                        : new FileInfo(candidate);
                    FileSystemInfo target = link.ResolveLinkTarget(returnFinalTarget: true)
                        ?? throw new IOException(
                            "A CSV migration path contains an unsupported reparse point.");
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
                return new MigrationSourceLease(synthetic, synthetic, packageManifest: null);

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
                        session.Manifest);
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

            default:
                throw new NotSupportedException(
                    $"Migration source '{catalog.Source.Kind}' is not registered in this CLI build.");
        }
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
        var report = new
        {
            Format = "csharpdb-migration-run/v1",
            Status = "failed",
            PlanDigest = MigrationArtifactSerializer.ComputePlanDigest(plan),
            plan.CatalogDigest,
            plan.CapabilityDigest,
            SourceFingerprint = plan.Source.Fingerprint,
            SourceSnapshotIdentity = sourceSnapshotIdentity,
            RejectContractVersion = MigrationRejectContract.DeterministicFailFastV1,
            RejectedRows = firstRejectedRow is null ? 0 : 1,
            FirstRejectedRow = firstRejectedRow,
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

    private sealed class MigrationSourceLease : IAsyncDisposable
    {
        private readonly IAsyncDisposable owner;

        internal MigrationSourceLease(
            IMigrationDataSource source,
            IAsyncDisposable owner,
            CsvSnapshotPackageManifest? packageManifest)
        {
            Source = source;
            this.owner = owner;
            PackageManifest = packageManifest;
        }

        internal IMigrationDataSource Source { get; }

        internal CsvSnapshotPackageManifest? PackageManifest { get; }

        public ValueTask DisposeAsync() => owner.DisposeAsync();
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
