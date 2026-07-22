using System.Text;
using System.Text.Json;
using System.Text.Json.Serialization;
using CSharpDB.Migration;
using CSharpDB.Migration.CSharpDb;
using CSharpDB.Migration.Validation;

namespace CSharpDB.Cli;

internal static class MigrationCommandRunner
{
    internal const string Usage =
        "Usage: csharpdb migrate inspect --source synthetic --out <catalog.json>\n" +
        "       csharpdb migrate plan <catalog.json> --out <plan.json> [--profile preserve|queryable] [--accept-exclusions all|<id,...>] [--accept-diagnostics <id,...>]\n" +
        "       csharpdb migrate preview <plan.json> --catalog <catalog.json> [--format text|json]\n" +
        "       csharpdb migrate apply <plan.json> --catalog <catalog.json> --target <staged.csdb> --out <run.json> [--resume] [--format text|json]\n" +
        "       csharpdb migrate validate <plan.json> --catalog <catalog.json> --target <staged.csdb> --out <validation.json> [--level schema|count|checksum] [--spill-dir <directory>] [--format text|json]";

    private static readonly JsonSerializerOptions JsonOptions = CreateJsonOptions();
    private static readonly UTF8Encoding Utf8NoBom = new(encoderShouldEmitUTF8Identifier: false);
    private static readonly StringComparison PathComparison = OperatingSystem.IsWindows()
        ? StringComparison.OrdinalIgnoreCase
        : StringComparison.Ordinal;

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
            await error.WriteLineAsync($"Error: {ex.Message}");
            return InspectorCommandRunner.ExitError;
        }
    }

    private static async ValueTask<int> RunInspectAsync(
        string[] args,
        TextWriter output,
        TextWriter error,
        CancellationToken ct)
    {
        if (!TryParseOptions(args, 2, out Dictionary<string, string> options, out string? parseError))
            return await OptionErrorAsync(parseError!, error);
        if (!RequireOnly(options, ["--source", "--out"], out parseError))
            return await OptionErrorAsync(parseError!, error);
        if (!options.TryGetValue("--source", out string? source))
            return await OptionErrorAsync("Missing required option --source.", error);
        if (!string.Equals(source, "synthetic", StringComparison.OrdinalIgnoreCase))
            return await OptionErrorAsync($"Unsupported migration source '{source}'.", error);
        if (!options.TryGetValue("--out", out string? outputValue))
            return await OptionErrorAsync("Missing required option --out.", error);

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
        if (!RequireOnly(options, ["--catalog", "--target", "--out", "--format", "--resume"], out parseError))
            return await OptionErrorAsync(parseError!, error);
        if (!options.TryGetValue("--catalog", out string? catalogValue))
            return await OptionErrorAsync("Missing required option --catalog.", error);
        if (!options.TryGetValue("--target", out string? targetValue))
            return await OptionErrorAsync("Missing required option --target.", error);
        if (!options.TryGetValue("--out", out string? runOutputValue))
            return await OptionErrorAsync("Missing required option --out.", error);

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
        string[] protectedPaths =
        [
            planPath,
            catalogPath,
            targetPath,
            targetPath + ".wal",
            targetPath + ".migration.lock",
            runOutputPath,
        ];
        if (ContainsEquivalentPaths(protectedPaths))
        {
            return await OptionErrorAsync(
                "Plan, catalog, staged target, target companions, and run report must use different files.",
                error);
        }

        MigrationCatalog catalog = MigrationArtifactSerializer.DeserializeCatalog(
            await File.ReadAllTextAsync(catalogPath, ct));
        MigrationPlan plan = MigrationArtifactSerializer.DeserializePlan(
            await File.ReadAllTextAsync(planPath, ct),
            catalog);
        MigrationPlanReadinessValidator.ValidateForApply(plan, catalog);
        if (catalog.Source.Kind != MigrationSourceKind.Synthetic ||
            !string.Equals(
                catalog.Source.Identity,
                SyntheticMigrationSourceInspector.FixtureIdentity,
                StringComparison.Ordinal))
        {
            throw new NotSupportedException(
                $"Migration apply source '{catalog.Source.Kind}' is not registered in this CLI build.");
        }

        await using var source = new SyntheticMigrationDataSource(catalog);
        try
        {
            MigrationApplyPolicyValidator.ValidateForExecution(plan);
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
                ["--catalog", "--target", "--out", "--level", "--spill-dir", "--format"],
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
        string[] protectedPaths =
        [
            planPath,
            catalogPath,
            targetPath,
            targetPath + ".wal",
            targetPath + ".migration.lock",
            reportPath,
        ];
        if (ContainsEquivalentPaths(protectedPaths))
        {
            return await OptionErrorAsync(
                "Plan, catalog, staged target, target companions, and validation report must use different files.",
                error);
        }
        if (!Directory.Exists(spillRoot))
            return await OptionErrorAsync($"Validation spill directory '{spillRoot}' does not exist.", error);

        MigrationCatalog catalog = MigrationArtifactSerializer.DeserializeCatalog(
            await File.ReadAllTextAsync(catalogPath, ct));
        MigrationPlan plan = MigrationArtifactSerializer.DeserializePlan(
            await File.ReadAllTextAsync(planPath, ct),
            catalog);
        MigrationPlanReadinessValidator.ValidateForApply(plan, catalog);
        if (catalog.Source.Kind != MigrationSourceKind.Synthetic ||
            !string.Equals(
                catalog.Source.Identity,
                SyntheticMigrationSourceInspector.FixtureIdentity,
                StringComparison.Ordinal))
        {
            throw new NotSupportedException(
                $"Migration validation source '{catalog.Source.Kind}' is not registered in this CLI build.");
        }

        MigrationValidationLevel requiredLevel = plan.Validation.ValidateChecksums
            ? MigrationValidationLevel.Checksum
            : plan.Validation.ValidateCounts
                ? MigrationValidationLevel.Count
                : MigrationValidationLevel.Schema;
        MigrationValidationLevel level = requestedLevel ?? requiredLevel;

        await using var source = new SyntheticMigrationDataSource(catalog);
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
