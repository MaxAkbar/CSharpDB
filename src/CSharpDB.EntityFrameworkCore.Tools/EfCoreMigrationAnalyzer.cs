using System.Buffers.Binary;
using System.Reflection;
using System.Reflection.Metadata;
using System.Reflection.PortableExecutable;
using System.Runtime.Loader;
using System.Security.Cryptography;
using System.Text;
using CSharpDB.EntityFrameworkCore;
using CSharpDB.Migration;
using CSharpDB.Migration.CSharpDb;
using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Infrastructure;
using Microsoft.EntityFrameworkCore.Metadata;
using Microsoft.EntityFrameworkCore.Migrations;
using Microsoft.EntityFrameworkCore.Migrations.Operations;

namespace CSharpDB.EntityFrameworkCore.Tools;

internal enum EfCoreAnalysisFailureKind
{
    InvalidRequest,
    AssemblyUnavailable,
    AssemblyDigestMismatch,
    AnalysisLimit,
    ContextUnavailable,
    AnalysisFailed,
}

internal sealed class EfCoreAnalysisException : Exception
{
    internal EfCoreAnalysisException(EfCoreAnalysisFailureKind kind)
        : base("EF Core migration analysis could not be completed.")
    {
        Kind = kind;
    }

    internal EfCoreAnalysisFailureKind Kind { get; }
}

/// <summary>
/// Performs bounded analysis of compiled EF Core migrations.
/// </summary>
/// <remarks>
/// This in-process API executes compiled application code. Loading the
/// assembly, creating its design-time context, initializing models, and
/// reading migration operations can run module initializers, factories,
/// application host setup, constructors, and migration methods. Generation
/// analysis never opens the configured database. Explicit scratch analysis
/// executes only against tool-owned private-memory databases. Call either API
/// only for trusted applications and prefer the isolated worker entry point.
/// </remarks>
public static class EfCoreMigrationAnalyzer
{
    public const int MaxAssemblyBytes = 128 * 1024 * 1024;
    public const int MaxTypeDefinitions = 100_000;
    public const int MaxMigrations = 1_024;
    public const int MaxOperations = 10_000;
    public const int MaxCommands = 20_000;
    public const int MaxGeneratedSqlUtf8Bytes = 32 * 1024 * 1024;
    public const int MaxAnnotationsPerOperation = 4_096;
    public const int MaxDiagnostics =
        MaxOperations + MaxMigrations + 1;

    private const int MaxContextNameLength = 512;
    private const int MaxMigrationIdLength = 256;
    private static readonly UTF8Encoding StrictUtf8 =
        new(
            encoderShouldEmitUTF8Identifier: false,
            throwOnInvalidBytes: true);

    /// <summary>
    /// Loads and analyzes one compiled application assembly.
    /// </summary>
    /// <remarks>
    /// This method executes code from the selected assembly. It does not open
    /// or migrate the configured database, but application code can perform
    /// arbitrary side effects of its own.
    /// </remarks>
    public static async ValueTask<EfCoreMigrationAnalysisReport> AnalyzeAsync(
        EfCoreMigrationAnalysisRequest request,
        CancellationToken cancellationToken = default)
    {
        LoadedAnalysis analysis = await AnalyzeRequestAsync(
            request,
            scratchRequested: false,
            cancellationToken);
        return analysis.Generation;
    }

    /// <summary>
    /// Loads and analyzes one compiled migration chain, then executes eligible
    /// migrations only against tool-owned private-memory CSharpDB databases.
    /// </summary>
    /// <remarks>
    /// This method executes trusted application code while discovering the
    /// compiled context and migrations. It never obtains or opens the selected
    /// context's configured connection.
    /// </remarks>
    public static async ValueTask<EfCoreMigrationScratchAnalysisReport>
        AnalyzeScratchAsync(
            EfCoreMigrationAnalysisRequest request,
            CancellationToken cancellationToken = default)
    {
        LoadedAnalysis analysis = await AnalyzeRequestAsync(
            request,
            scratchRequested: true,
            cancellationToken);
        return analysis.Scratch ??
            throw new EfCoreAnalysisException(
                EfCoreAnalysisFailureKind.AnalysisFailed);
    }

    private static async ValueTask<LoadedAnalysis> AnalyzeRequestAsync(
        EfCoreMigrationAnalysisRequest request,
        bool scratchRequested,
        CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(request);
        ValidateRequest(request);
        cancellationToken.ThrowIfCancellationRequested();

        byte[] assemblyBytes = await ReadMainAssemblyAsync(
            request.AssemblyPath,
            cancellationToken);
        string assemblyDigest = ComputeLowerHexDigest(assemblyBytes);
        if (!CryptographicOperations.FixedTimeEquals(
                StrictUtf8.GetBytes(assemblyDigest),
                StrictUtf8.GetBytes(request.AssemblyDigest)))
        {
            throw new EfCoreAnalysisException(
                EfCoreAnalysisFailureKind.AssemblyDigestMismatch);
        }

        PreflightAssembly(assemblyBytes);
        string mainPath = Path.GetFullPath(request.AssemblyPath);
        Assembly designAssembly = LoadDesignAssembly();
        var loadContext = new EfCoreAnalyzerLoadContext(
            mainPath,
            designAssembly);
        try
        {
            using var stream = new MemoryStream(
                assemblyBytes,
                writable: false);
            Assembly assembly = loadContext.LoadFromStream(stream);
            return await AnalyzeLoadedAssemblyAsync(
                assembly,
                designAssembly,
                request.Context,
                assemblyDigest,
                scratchRequested,
                cancellationToken);
        }
        catch (OperationCanceledException)
            when (cancellationToken.IsCancellationRequested)
        {
            throw;
        }
        catch (EfCoreAnalysisException)
        {
            throw;
        }
        catch (Exception exception)
            when (IsRecoverable(exception))
        {
            throw new EfCoreAnalysisException(
                EfCoreAnalysisFailureKind.AnalysisFailed);
        }
        finally
        {
            loadContext.Unload();
        }
    }

    private static async ValueTask<LoadedAnalysis>
        AnalyzeLoadedAssemblyAsync(
            Assembly assembly,
            Assembly designAssembly,
            string? contextSelector,
            string assemblyDigest,
            bool scratchRequested,
            CancellationToken cancellationToken)
    {
        cancellationToken.ThrowIfCancellationRequested();
        Type contextType = SelectContext(
            assembly,
            contextSelector);
        string contextName = contextType.FullName!;

        await using DbContext context = CreateContext(
            contextType,
            assembly,
            designAssembly);
        cancellationToken.ThrowIfCancellationRequested();

        _ = context.Model;
        string? providerName = context.Database.ProviderName;
        if (!string.Equals(
                providerName,
                EfCoreMigrationAnalysisReport.CSharpDbProvider,
                StringComparison.Ordinal))
        {
            throw new EfCoreAnalysisException(
                EfCoreAnalysisFailureKind.AnalysisFailed);
        }

        EnsureCanonicalProviderConfiguration(context);
        IMigrationsAssembly migrationsAssembly =
            context.GetService<IMigrationsAssembly>();
        await using DbContext providerServicesContext =
            CreateProviderServicesContext();
        IMigrationsSqlGenerator sqlGenerator =
            providerServicesContext
                .GetService<IMigrationsSqlGenerator>();
        IModelRuntimeInitializer modelInitializer =
            providerServicesContext
                .GetService<IModelRuntimeInitializer>();
        IMigrationsModelDiffer modelDiffer =
            providerServicesContext
                .GetService<IMigrationsModelDiffer>();
        EnsureCanonicalSqlGenerator(sqlGenerator);

        IReadOnlyDictionary<string, TypeInfo>
            configuredMigrations =
                migrationsAssembly.Migrations;
        if (configuredMigrations.Count > MaxMigrations)
        {
            throw new EfCoreAnalysisException(
                EfCoreAnalysisFailureKind.AnalysisLimit);
        }
        KeyValuePair<string, TypeInfo>[] migrationTypes =
            configuredMigrations
                .OrderBy(
                    static item => item.Key,
                    StringComparer.Ordinal)
                .ToArray();

        var diagnostics =
            new List<EfCoreMigrationAnalysisDiagnostic>();
        var migrations =
            new List<EfCoreMigrationAnalysisMigration>(
                migrationTypes.Length);
        var scratchInputs =
            new List<EfCoreScratchMigrationInput>(
                migrationTypes.Length);
        bool scratchInputComplete = true;
        using var chainDigest =
            new FramedDigestAccumulator(
                EfCoreMigrationAnalysisDigestDomains.Chain);

        int totalOperations = 0;
        int totalDestructive = 0;
        var totals = new AnalysisTotals();
        MigrationCompatibilityStatus chainStatus =
            MigrationCompatibilityStatus.Conditional;
        string chainRule =
            EfCoreMigrationAnalysisRules.GenerationBound;
        IModel? previousTargetModel = null;

        for (int migrationOrdinal = 0;
             migrationOrdinal < migrationTypes.Length;
             migrationOrdinal++)
        {
            cancellationToken.ThrowIfCancellationRequested();
            KeyValuePair<string, TypeInfo> migrationEntry =
                migrationTypes[migrationOrdinal];
            ValidateMigrationId(migrationEntry.Key);

            Microsoft.EntityFrameworkCore.Migrations.Migration
                migration = migrationsAssembly.CreateMigration(
                migrationEntry.Value,
                EfCoreMigrationAnalysisReport.CSharpDbProvider);
            IReadOnlyList<MigrationOperation> upOperations =
                migration.UpOperations;
            IReadOnlyList<MigrationOperation> downOperations =
                migration.DownOperations;
            IModel? declaredTargetModel =
                migration.TargetModel;
            IModel? targetModel = declaredTargetModel is null
                ? null
                : modelInitializer.Initialize(
                    declaredTargetModel,
                    designTime: true,
                    validationLogger: null);
            if (targetModel is null)
            {
                scratchInputComplete = false;
            }
            else
            {
                scratchInputs.Add(
                    new EfCoreScratchMigrationInput
                    {
                        Ordinal = migrationOrdinal,
                        MigrationId = migrationEntry.Key,
                        UpOperations = upOperations.ToArray(),
                        DownOperations = downOperations.ToArray(),
                        TargetModel = targetModel,
                    });
            }

            int migrationOperationCount = checked(
                upOperations.Count + downOperations.Count);
            if (migrationOperationCount >
                    MaxOperations - totalOperations)
            {
                throw new EfCoreAnalysisException(
                    EfCoreAnalysisFailureKind.AnalysisLimit);
            }

            var findings =
                new List<EfCoreMigrationOperationFinding>(
                    migrationOperationCount);
            using var migrationDigest =
                new FramedDigestAccumulator(
                    EfCoreMigrationAnalysisDigestDomains
                        .Migration);
            var migrationState = new MigrationAnalysisState();

            await AnalyzeDirectionAsync(
                upOperations,
                EfCoreMigrationDirection.Up,
                targetModel,
                migrationOrdinal,
                sqlGenerator,
                findings,
                diagnostics,
                migrationDigest,
                chainDigest,
                totals,
                migrationState,
                cancellationToken);
            await AnalyzeDirectionAsync(
                downOperations,
                EfCoreMigrationDirection.Down,
                previousTargetModel,
                migrationOrdinal,
                sqlGenerator,
                findings,
                diagnostics,
                migrationDigest,
                chainDigest,
                totals,
                migrationState,
                cancellationToken);

            if (migrationOperationCount == 0)
            {
                migrationState.RuleId =
                    EfCoreMigrationAnalysisRules.EmptyMigration;
                AddDiagnostic(
                    diagnostics,
                    migrationState.RuleId,
                    MigrationDiagnosticSeverity.Warning,
                    MigrationCompatibilityStatus.Conditional,
                    migrationOrdinal,
                    operationOrdinal: null);
            }
            else if (downOperations.Count == 0 &&
                     migrationState.Status ==
                        MigrationCompatibilityStatus.Conditional)
            {
                migrationState.RuleId =
                    EfCoreMigrationAnalysisRules.EmptyDownMigration;
                AddDiagnostic(
                    diagnostics,
                    migrationState.RuleId,
                    MigrationDiagnosticSeverity.Warning,
                    MigrationCompatibilityStatus.Conditional,
                    migrationOrdinal,
                    operationOrdinal: null);
            }

            migrations.Add(new EfCoreMigrationAnalysisMigration
            {
                Ordinal = migrationOrdinal,
                MigrationId = migrationEntry.Key,
                Status = migrationState.Status,
                HighestEvidence = MigrationEvidenceLevel.Bound,
                RuleId = migrationState.RuleId,
                UpOperationCount = upOperations.Count,
                DownOperationCount = downOperations.Count,
                OperationCount = migrationOperationCount,
                DestructiveOperationCount =
                    migrationState.DestructiveCount,
                CommandCount = migrationState.CommandCount,
                GeneratedSqlDigest =
                    migrationState.CommandCount == 0
                    ? null
                    : migrationDigest.Finish(),
                Operations = findings,
            });

            totalOperations += migrationOperationCount;
            totalDestructive +=
                migrationState.DestructiveCount;
            UpdateAggregate(
                migrationState.Status,
                migrationState.RuleId,
                ref chainStatus,
                ref chainRule);
            previousTargetModel = targetModel;
        }

        if (chainStatus ==
            MigrationCompatibilityStatus.Conditional)
        {
            chainRule =
                EfCoreMigrationAnalysisRules.GenerationBound;
            AddDiagnostic(
                diagnostics,
                chainRule,
                MigrationDiagnosticSeverity.Warning,
                chainStatus,
                migrationOrdinal: null,
                operationOrdinal: null);
        }

        CSharpDbCapabilityCatalog capabilities =
            CSharpDbCapabilityCatalogLoader.LoadEmbedded();
        var generation = new EfCoreMigrationAnalysisReport
        {
            TargetCSharpDbVersion =
                capabilities.TargetCSharpDbVersion,
            CapabilityDigest = capabilities.Digest,
            AssemblyDigest = assemblyDigest,
            QualifiedEfCoreVersion = ProductInfo.GetVersion(),
            Context = contextName,
            Status = chainStatus,
            HighestEvidence = MigrationEvidenceLevel.Bound,
            RuleId = chainRule,
            MigrationCount = migrations.Count,
            OperationCount = totalOperations,
            DestructiveOperationCount = totalDestructive,
            CommandCount = totals.CommandCount,
            GeneratedSqlDigest = totals.CommandCount == 0
                ? null
                : chainDigest.Finish(),
            Migrations = migrations,
            Diagnostics = diagnostics,
        };
        if (!scratchRequested)
            return new LoadedAnalysis(generation, Scratch: null);

        if (!CanExecuteScratch(
                generation,
                scratchInputs,
                scratchInputComplete))
        {
            return new LoadedAnalysis(
                generation,
                CreateBlockedScratchReport(generation));
        }

        EfCoreScratchChainValidationResult validation =
            await EfCoreScratchChainValidator.ValidateAsync(
                scratchInputs,
                sqlGenerator,
                modelDiffer,
                cancellationToken);
        return new LoadedAnalysis(
            generation,
            CreateScratchReport(generation, validation));
    }

    private static void EnsureCanonicalSqlGenerator(
        IMigrationsSqlGenerator sqlGenerator)
    {
        Type sqlGeneratorType = sqlGenerator.GetType();
        if (!string.Equals(
                sqlGeneratorType.FullName,
                "CSharpDB.EntityFrameworkCore.Migrations.Internal.CSharpDbMigrationsSqlGenerator",
                StringComparison.Ordinal) ||
            sqlGeneratorType.Assembly !=
                typeof(
                    CSharpDbDbContextOptionsBuilderExtensions)
                    .Assembly)
        {
            throw new EfCoreAnalysisException(
                EfCoreAnalysisFailureKind.AnalysisFailed);
        }
    }

    private static void EnsureCanonicalProviderConfiguration(
        DbContext context)
    {
        IDbContextOptions options =
            context.GetService<IDbContextOptions>();
        CoreOptionsExtension? coreOptions =
            options.FindExtension<CoreOptionsExtension>();
        if (options.Extensions.Any(static extension =>
                !IsCanonicalOptionsExtension(extension)) ||
            coreOptions?.InternalServiceProvider is not null ||
            coreOptions?.ReplacedServices is { Count: > 0 })
        {
            throw new EfCoreAnalysisException(
                EfCoreAnalysisFailureKind.AnalysisFailed);
        }

        EnsureCanonicalSqlGenerator(
            context.GetService<IMigrationsSqlGenerator>());
    }

    private static bool IsCanonicalOptionsExtension(
        IDbContextOptionsExtension extension)
    {
        Type extensionType = extension.GetType();
        return extensionType == typeof(CoreOptionsExtension) ||
            string.Equals(
                extensionType.FullName,
                "CSharpDB.EntityFrameworkCore.Infrastructure.Internal.CSharpDbOptionsExtension",
                StringComparison.Ordinal) &&
            extensionType.Assembly ==
                typeof(
                    CSharpDbDbContextOptionsBuilderExtensions)
                    .Assembly;
    }

    private static async ValueTask AnalyzeDirectionAsync(
        IReadOnlyList<MigrationOperation> operations,
        EfCoreMigrationDirection direction,
        IModel? model,
        int migrationOrdinal,
        IMigrationsSqlGenerator sqlGenerator,
        List<EfCoreMigrationOperationFinding> findings,
        List<EfCoreMigrationAnalysisDiagnostic> diagnostics,
        FramedDigestAccumulator migrationDigest,
        FramedDigestAccumulator chainDigest,
        AnalysisTotals totals,
        MigrationAnalysisState migrationState,
        CancellationToken cancellationToken)
    {
        for (int directionOrdinal = 0;
             directionOrdinal < operations.Count;
             directionOrdinal++)
        {
            cancellationToken.ThrowIfCancellationRequested();
            MigrationOperation operation =
                operations[directionOrdinal];
            int operationOrdinal = findings.Count;
            OperationResult result =
                await AnalyzeOperationAsync(
                    operation,
                    model,
                    migrationOrdinal,
                    direction,
                    directionOrdinal,
                    sqlGenerator,
                    totals.CommandCount,
                    totals.SqlUtf8Bytes,
                    cancellationToken);

            totals.CommandCount = checked(
                totals.CommandCount + result.CommandCount);
            totals.SqlUtf8Bytes = checked(
                totals.SqlUtf8Bytes + result.SqlUtf8Bytes);
            migrationState.CommandCount = checked(
                migrationState.CommandCount +
                result.CommandCount);
            if (operation.IsDestructiveChange)
                migrationState.DestructiveCount++;

            for (int commandOrdinal = 0;
                 commandOrdinal < result.CommandDigests.Count;
                 commandOrdinal++)
            {
                AppendCommandIdentity(
                    migrationDigest,
                    migrationOrdinal,
                    direction,
                    directionOrdinal,
                    commandOrdinal,
                    result.CommandDigests[commandOrdinal]);
                AppendCommandIdentity(
                    chainDigest,
                    migrationOrdinal,
                    direction,
                    directionOrdinal,
                    commandOrdinal,
                    result.CommandDigests[commandOrdinal]);
            }

            int annotationCount =
                operation.GetAnnotations()
                    .Take(
                        MaxAnnotationsPerOperation + 1)
                    .Count();
            if (annotationCount >
                MaxAnnotationsPerOperation)
            {
                throw new EfCoreAnalysisException(
                    EfCoreAnalysisFailureKind.AnalysisLimit);
            }

            var finding =
                new EfCoreMigrationOperationFinding
                {
                    Ordinal = operationOrdinal,
                    Direction = direction,
                    DirectionOrdinal = directionOrdinal,
                    Kind = result.Kind,
                    Status = result.Status,
                    Evidence = MigrationEvidenceLevel.Bound,
                    RuleId = result.RuleId,
                    IsDestructive =
                        operation.IsDestructiveChange,
                    AnnotationCount = annotationCount,
                    CommandCount = result.CommandCount,
                    GeneratedSqlUtf8Bytes =
                        result.SqlUtf8Bytes,
                    GeneratedSqlDigest =
                        result.GeneratedSqlDigest,
                };
            findings.Add(finding);
            UpdateAggregate(
                finding.Status,
                finding.RuleId,
                ref migrationState.Status,
                ref migrationState.RuleId);

            if (finding.Status !=
                MigrationCompatibilityStatus.Conditional)
            {
                AddDiagnostic(
                    diagnostics,
                    finding.RuleId,
                    MigrationDiagnosticSeverity.Error,
                    finding.Status,
                    migrationOrdinal,
                    operationOrdinal);
            }
        }
    }

    private static async ValueTask<OperationResult>
        AnalyzeOperationAsync(
            MigrationOperation operation,
            IModel? model,
            int migrationOrdinal,
            EfCoreMigrationDirection direction,
            int directionOrdinal,
            IMigrationsSqlGenerator sqlGenerator,
            int existingCommandCount,
            int existingSqlBytes,
            CancellationToken cancellationToken)
    {
        Type operationType = operation.GetType();
        EfCoreMigrationOperationKind kind =
            GetExactOperationKind(operationType);

        if (IsSchemaKind(kind))
        {
            return OperationResult.Rejected(
                kind,
                MigrationCompatibilityStatus.Unsupported,
                EfCoreMigrationAnalysisRules.SchemaUnsupported);
        }
        if (IsSequenceKind(kind))
        {
            return OperationResult.Rejected(
                kind,
                MigrationCompatibilityStatus.Unsupported,
                EfCoreMigrationAnalysisRules.SequenceUnsupported);
        }
        if (IsDataKind(kind))
        {
            return OperationResult.Rejected(
                kind,
                MigrationCompatibilityStatus.Unknown,
                EfCoreMigrationAnalysisRules.DataUnknown);
        }
        if (kind is EfCoreMigrationOperationKind.AlterDatabase or
            EfCoreMigrationOperationKind.AlterTable or
            EfCoreMigrationOperationKind.Unknown)
        {
            return OperationResult.Rejected(
                kind,
                MigrationCompatibilityStatus.Unknown,
                EfCoreMigrationAnalysisRules.OperationUnknown);
        }

        string rule =
            EfCoreMigrationAnalysisRules.GenerationBound;
        if (kind == EfCoreMigrationOperationKind.RawSql)
        {
            if (operationType != typeof(SqlOperation))
            {
                return OperationResult.Rejected(
                    EfCoreMigrationOperationKind.Unknown,
                    MigrationCompatibilityStatus.Unknown,
                    EfCoreMigrationAnalysisRules.OperationUnknown);
            }

            string rawSql = ((SqlOperation)operation).Sql;
            int rawSqlBytes;
            try
            {
                rawSqlBytes = StrictUtf8.GetByteCount(rawSql);
            }
            catch (EncoderFallbackException)
            {
                return OperationResult.Rejected(
                    kind,
                    MigrationCompatibilityStatus.Unknown,
                    EfCoreMigrationAnalysisRules.RawSqlUnknown);
            }
            if (rawSqlBytes > MaxGeneratedSqlUtf8Bytes)
            {
                throw new EfCoreAnalysisException(
                    EfCoreAnalysisFailureKind.AnalysisLimit);
            }

            CSharpDbDdlCompatibilityReport ddlReport;
            try
            {
                ddlReport =
                    await CSharpDbDdlCompatibilityAnalyzer
                        .AnalyzeAsync(
                            rawSql,
                            cancellationToken:
                                cancellationToken);
            }
            catch (OperationCanceledException)
                when (cancellationToken
                    .IsCancellationRequested)
            {
                throw;
            }
            catch (Exception exception)
                when (IsRecoverable(exception))
            {
                return OperationResult.Rejected(
                    kind,
                    MigrationCompatibilityStatus.Unknown,
                    EfCoreMigrationAnalysisRules.RawSqlUnknown);
            }

            if (ddlReport.Status !=
                MigrationCompatibilityStatus.Compatible)
            {
                MigrationCompatibilityStatus status =
                    ddlReport.Status is
                        MigrationCompatibilityStatus.Unsupported or
                        MigrationCompatibilityStatus
                            .CompatibleWithRewrite
                    ? MigrationCompatibilityStatus.Unsupported
                    : MigrationCompatibilityStatus.Unknown;
                return OperationResult.Rejected(
                    kind,
                    status,
                    status ==
                        MigrationCompatibilityStatus.Unsupported
                        ? EfCoreMigrationAnalysisRules
                            .RawSqlUnsupported
                        : EfCoreMigrationAnalysisRules
                            .RawSqlUnknown);
            }
            rule = EfCoreMigrationAnalysisRules.RawSqlBound;
        }

        IReadOnlyList<MigrationCommand> commands;
        try
        {
            commands = sqlGenerator.Generate(
                [operation],
                model,
                MigrationsSqlGenerationOptions.Default);
        }
        catch (OperationCanceledException)
            when (cancellationToken.IsCancellationRequested)
        {
            throw;
        }
        catch (NotSupportedException)
        {
            return OperationResult.Rejected(
                kind,
                MigrationCompatibilityStatus.Unsupported,
                EfCoreMigrationAnalysisRules
                    .GenerationUnsupported);
        }
        catch (Exception exception)
            when (IsRecoverable(exception))
        {
            return OperationResult.Rejected(
                kind,
                MigrationCompatibilityStatus.Unknown,
                EfCoreMigrationAnalysisRules.GenerationFailed);
        }

        if (commands.Count > MaxCommands - existingCommandCount)
        {
            throw new EfCoreAnalysisException(
                EfCoreAnalysisFailureKind.AnalysisLimit);
        }

        int sqlBytes = 0;
        var commandDigests =
            new List<byte[]>(commands.Count);
        bool transactionSuppressed = false;
        foreach (MigrationCommand command in commands)
        {
            cancellationToken.ThrowIfCancellationRequested();
            int commandByteCount;
            byte[] bytes;
            try
            {
                commandByteCount =
                    StrictUtf8.GetByteCount(
                        command.CommandText);
            }
            catch (EncoderFallbackException)
            {
                return OperationResult.Rejected(
                    kind,
                    MigrationCompatibilityStatus.Unknown,
                    EfCoreMigrationAnalysisRules.GenerationFailed);
            }
            if (commandByteCount <= 0)
            {
                return OperationResult.Rejected(
                    kind,
                    MigrationCompatibilityStatus.Unknown,
                    EfCoreMigrationAnalysisRules.GenerationFailed);
            }
            if (commandByteCount >
                MaxGeneratedSqlUtf8Bytes -
                existingSqlBytes -
                sqlBytes)
            {
                throw new EfCoreAnalysisException(
                    EfCoreAnalysisFailureKind.AnalysisLimit);
            }
            bytes = new byte[commandByteCount];
            try
            {
                int encoded = StrictUtf8.GetBytes(
                    command.CommandText.AsSpan(),
                    bytes);
                if (encoded != commandByteCount)
                {
                    return OperationResult.Rejected(
                        kind,
                        MigrationCompatibilityStatus.Unknown,
                        EfCoreMigrationAnalysisRules
                            .GenerationFailed);
                }
            }
            catch (EncoderFallbackException)
            {
                return OperationResult.Rejected(
                    kind,
                    MigrationCompatibilityStatus.Unknown,
                    EfCoreMigrationAnalysisRules.GenerationFailed);
            }
            sqlBytes = checked(
                sqlBytes + commandByteCount);
            byte[] commandDigest;
            try
            {
                commandDigest = SHA256.HashData(bytes);
            }
            finally
            {
                CryptographicOperations.ZeroMemory(bytes);
            }
            commandDigests.Add(commandDigest);
            transactionSuppressed |=
                command.TransactionSuppressed;
        }

        string? generatedSqlDigest =
            commandDigests.Count == 0
                ? null
                : BuildOperationDigest(
                    migrationOrdinal,
                    direction,
                    directionOrdinal,
                    commandDigests);
        return new OperationResult(
            kind,
            transactionSuppressed
                ? MigrationCompatibilityStatus.Unsupported
                : MigrationCompatibilityStatus.Conditional,
            transactionSuppressed
                ? EfCoreMigrationAnalysisRules
                    .TransactionSuppressed
                : rule,
            commands.Count,
            sqlBytes,
            generatedSqlDigest,
            commandDigests);
    }

    private static EfCoreMigrationOperationKind
        GetExactOperationKind(Type type)
    {
        if (type == typeof(CreateTableOperation))
            return EfCoreMigrationOperationKind.CreateTable;
        if (type == typeof(DropTableOperation))
            return EfCoreMigrationOperationKind.DropTable;
        if (type == typeof(RenameTableOperation))
            return EfCoreMigrationOperationKind.RenameTable;
        if (type == typeof(AddColumnOperation))
            return EfCoreMigrationOperationKind.AddColumn;
        if (type == typeof(AlterColumnOperation))
            return EfCoreMigrationOperationKind.AlterColumn;
        if (type == typeof(DropColumnOperation))
            return EfCoreMigrationOperationKind.DropColumn;
        if (type == typeof(RenameColumnOperation))
            return EfCoreMigrationOperationKind.RenameColumn;
        if (type == typeof(CreateIndexOperation))
            return EfCoreMigrationOperationKind.CreateIndex;
        if (type == typeof(DropIndexOperation))
            return EfCoreMigrationOperationKind.DropIndex;
        if (type == typeof(RenameIndexOperation))
            return EfCoreMigrationOperationKind.RenameIndex;
        if (type == typeof(AddPrimaryKeyOperation))
            return EfCoreMigrationOperationKind.AddPrimaryKey;
        if (type == typeof(DropPrimaryKeyOperation))
            return EfCoreMigrationOperationKind.DropPrimaryKey;
        if (type == typeof(AddUniqueConstraintOperation))
            return EfCoreMigrationOperationKind.AddUniqueConstraint;
        if (type == typeof(DropUniqueConstraintOperation))
            return EfCoreMigrationOperationKind.DropUniqueConstraint;
        if (type == typeof(AddForeignKeyOperation))
            return EfCoreMigrationOperationKind.AddForeignKey;
        if (type == typeof(DropForeignKeyOperation))
            return EfCoreMigrationOperationKind.DropForeignKey;
        if (type == typeof(AddCheckConstraintOperation))
            return EfCoreMigrationOperationKind.AddCheckConstraint;
        if (type == typeof(DropCheckConstraintOperation))
            return EfCoreMigrationOperationKind.DropCheckConstraint;
        if (type == typeof(SqlOperation))
            return EfCoreMigrationOperationKind.RawSql;
        if (type == typeof(EnsureSchemaOperation))
            return EfCoreMigrationOperationKind.EnsureSchema;
        if (type == typeof(DropSchemaOperation))
            return EfCoreMigrationOperationKind.DropSchema;
        if (type == typeof(CreateSequenceOperation))
            return EfCoreMigrationOperationKind.CreateSequence;
        if (type == typeof(AlterSequenceOperation))
            return EfCoreMigrationOperationKind.AlterSequence;
        if (type == typeof(RenameSequenceOperation))
            return EfCoreMigrationOperationKind.RenameSequence;
        if (type == typeof(DropSequenceOperation))
            return EfCoreMigrationOperationKind.DropSequence;
        if (type == typeof(RestartSequenceOperation))
            return EfCoreMigrationOperationKind.RestartSequence;
        if (type == typeof(InsertDataOperation))
            return EfCoreMigrationOperationKind.InsertData;
        if (type == typeof(UpdateDataOperation))
            return EfCoreMigrationOperationKind.UpdateData;
        if (type == typeof(DeleteDataOperation))
            return EfCoreMigrationOperationKind.DeleteData;
        if (type == typeof(AlterDatabaseOperation))
            return EfCoreMigrationOperationKind.AlterDatabase;
        if (type == typeof(AlterTableOperation))
            return EfCoreMigrationOperationKind.AlterTable;
        return EfCoreMigrationOperationKind.Unknown;
    }

    private static bool IsSchemaKind(
        EfCoreMigrationOperationKind kind) =>
        kind is EfCoreMigrationOperationKind.EnsureSchema or
            EfCoreMigrationOperationKind.DropSchema;

    private static bool IsSequenceKind(
        EfCoreMigrationOperationKind kind) =>
        kind is EfCoreMigrationOperationKind.CreateSequence or
            EfCoreMigrationOperationKind.AlterSequence or
            EfCoreMigrationOperationKind.RenameSequence or
            EfCoreMigrationOperationKind.DropSequence or
            EfCoreMigrationOperationKind.RestartSequence;

    private static bool IsDataKind(
        EfCoreMigrationOperationKind kind) =>
        kind is EfCoreMigrationOperationKind.InsertData or
            EfCoreMigrationOperationKind.UpdateData or
            EfCoreMigrationOperationKind.DeleteData;

    private static Type SelectContext(
        Assembly assembly,
        string? selector)
    {
        Type[] contexts;
        try
        {
            contexts = assembly.DefinedTypes
                .Where(static type =>
                    !type.IsAbstract &&
                    !type.ContainsGenericParameters &&
                    typeof(DbContext).IsAssignableFrom(
                        type.AsType()))
                .Select(static type => type.AsType())
                .ToArray();
        }
        catch (Exception exception)
            when (IsRecoverable(exception))
        {
            throw new EfCoreAnalysisException(
                EfCoreAnalysisFailureKind.ContextUnavailable);
        }

        Type[] matches;
        if (selector is null)
        {
            matches = contexts;
        }
        else
        {
            Type[] fullMatches = contexts
                .Where(type => string.Equals(
                    type.FullName,
                    selector,
                    StringComparison.Ordinal))
                .ToArray();
            matches = fullMatches.Length != 0
                ? fullMatches
                : contexts
                    .Where(type => string.Equals(
                        type.Name,
                        selector,
                        StringComparison.Ordinal))
                    .ToArray();
        }

        if (matches.Length != 1 ||
            !IsSafeQualifiedName(matches[0].FullName))
        {
            throw new EfCoreAnalysisException(
                EfCoreAnalysisFailureKind.ContextUnavailable);
        }
        return matches[0];
    }

    private static DbContext CreateContext(
        Type contextType,
        Assembly startupAssembly,
        Assembly designAssembly)
    {
        try
        {
            Type activatorType = designAssembly.GetType(
                "Microsoft.EntityFrameworkCore.Design.DbContextActivator",
                throwOnError: true)!;
            Type handlerType = designAssembly.GetType(
                "Microsoft.EntityFrameworkCore.Design.OperationReportHandler",
                throwOnError: true)!;
            Action<string> silent = static _ => { };
            object handler = Activator.CreateInstance(
                handlerType,
                silent,
                silent,
                silent,
                silent)!;
            MethodInfo method = activatorType
                .GetMethods(
                    BindingFlags.Public |
                    BindingFlags.Static)
                .Single(candidate =>
                {
                    ParameterInfo[] parameters =
                        candidate.GetParameters();
                    return candidate.Name == "CreateInstance" &&
                        parameters.Length == 4 &&
                        parameters[0].ParameterType ==
                            typeof(Type) &&
                        parameters[1].ParameterType ==
                            typeof(Assembly) &&
                        parameters[3].ParameterType ==
                            typeof(string[]);
                });
            object? created = method.Invoke(
                obj: null,
                [contextType, startupAssembly, handler,
                    Array.Empty<string>()]);
            if (created is not DbContext context ||
                context.GetType() != contextType)
            {
                throw new EfCoreAnalysisException(
                    EfCoreAnalysisFailureKind.ContextUnavailable);
            }
            return context;
        }
        catch (EfCoreAnalysisException)
        {
            throw;
        }
        catch (Exception exception)
            when (IsRecoverable(exception))
        {
            throw new EfCoreAnalysisException(
                EfCoreAnalysisFailureKind.ContextUnavailable);
        }
    }

    private static DbContext CreateProviderServicesContext()
    {
        try
        {
            DbContextOptions options =
                new DbContextOptionsBuilder()
                    .UseCSharpDb(
                        "Data Source=:memory:;Pooling=false")
                    .Options;
            return new DbContext(options);
        }
        catch (Exception exception)
            when (IsRecoverable(exception))
        {
            throw new EfCoreAnalysisException(
                EfCoreAnalysisFailureKind.AnalysisFailed);
        }
    }

    private static Assembly LoadDesignAssembly()
    {
        try
        {
            return Assembly.Load(
                new AssemblyName(
                    "Microsoft.EntityFrameworkCore.Design"));
        }
        catch (Exception exception)
            when (IsRecoverable(exception))
        {
            throw new EfCoreAnalysisException(
                EfCoreAnalysisFailureKind.AnalysisFailed);
        }
    }

    private static void ValidateRequest(
        EfCoreMigrationAnalysisRequest request)
    {
        if (!string.Equals(
                request.Format,
                EfCoreMigrationAnalysisRequest.CurrentFormat,
                StringComparison.Ordinal) ||
            string.IsNullOrWhiteSpace(request.AssemblyPath) ||
            !Path.IsPathFullyQualified(request.AssemblyPath) ||
            !IsLowerHexSha256(request.AssemblyDigest) ||
            request.Context is not null &&
            !IsSafeContextSelector(request.Context))
        {
            throw new EfCoreAnalysisException(
                EfCoreAnalysisFailureKind.InvalidRequest);
        }
    }

    private static async ValueTask<byte[]> ReadMainAssemblyAsync(
        string path,
        CancellationToken cancellationToken)
    {
        try
        {
            await using var stream = new FileStream(
                path,
                FileMode.Open,
                FileAccess.Read,
                FileShare.Read,
                bufferSize: 64 * 1024,
                FileOptions.Asynchronous |
                FileOptions.SequentialScan);
            if (stream.Length <= 0 ||
                stream.Length > MaxAssemblyBytes)
            {
                throw new EfCoreAnalysisException(
                    EfCoreAnalysisFailureKind.AnalysisLimit);
            }

            var bytes = new byte[checked((int)stream.Length)];
            int readTotal = 0;
            while (readTotal < bytes.Length)
            {
                int read = await stream.ReadAsync(
                    bytes.AsMemory(readTotal),
                    cancellationToken);
                if (read == 0)
                {
                    throw new EfCoreAnalysisException(
                        EfCoreAnalysisFailureKind
                            .AssemblyUnavailable);
                }
                readTotal += read;
            }
            if (await stream.ReadAsync(
                    new byte[1],
                    cancellationToken) != 0)
            {
                throw new EfCoreAnalysisException(
                    EfCoreAnalysisFailureKind
                        .AssemblyUnavailable);
            }
            return bytes;
        }
        catch (OperationCanceledException)
            when (cancellationToken.IsCancellationRequested)
        {
            throw;
        }
        catch (EfCoreAnalysisException)
        {
            throw;
        }
        catch (Exception exception)
            when (IsRecoverable(exception))
        {
            throw new EfCoreAnalysisException(
                EfCoreAnalysisFailureKind.AssemblyUnavailable);
        }
    }

    private static void PreflightAssembly(byte[] bytes)
    {
        try
        {
            using var stream = new MemoryStream(
                bytes,
                writable: false);
            using var peReader = new PEReader(stream);
            if (!peReader.HasMetadata ||
                peReader.GetMetadataReader()
                    .TypeDefinitions.Count >
                MaxTypeDefinitions)
            {
                throw new EfCoreAnalysisException(
                    EfCoreAnalysisFailureKind.AnalysisLimit);
            }
        }
        catch (EfCoreAnalysisException)
        {
            throw;
        }
        catch (Exception exception)
            when (IsRecoverable(exception))
        {
            throw new EfCoreAnalysisException(
                EfCoreAnalysisFailureKind.AssemblyUnavailable);
        }
    }

    private static void ValidateMigrationId(string migrationId)
    {
        if (string.IsNullOrEmpty(migrationId) ||
            migrationId.Length > MaxMigrationIdLength ||
            !migrationId.All(IsSafeIdentifierCharacter))
        {
            throw new EfCoreAnalysisException(
                EfCoreAnalysisFailureKind.AnalysisFailed);
        }
    }

    private static bool IsSafeContextSelector(string value)
    {
        if (value.Length is <= 0 or >
            MaxContextNameLength)
        {
            return false;
        }

        bool atSegmentStart = true;
        foreach (char character in value)
        {
            if (character is '.' or '+')
            {
                if (atSegmentStart)
                    return false;
                atSegmentStart = true;
                continue;
            }

            if (atSegmentStart)
            {
                if (character is not (>= 'A' and <= 'Z') and
                    not (>= 'a' and <= 'z') and
                    not '_')
                {
                    return false;
                }
                atSegmentStart = false;
                continue;
            }

            if (character is not (>= 'A' and <= 'Z') and
                not (>= 'a' and <= 'z') and
                not (>= '0' and <= '9') and
                not '_')
            {
                return false;
            }
        }
        return !atSegmentStart;
    }

    private static bool IsSafeQualifiedName(string? value) =>
        value is not null &&
        IsSafeContextSelector(value);

    private static bool IsSafeIdentifierCharacter(char value) =>
        value is >= 'A' and <= 'Z' or
            >= 'a' and <= 'z' or
            >= '0' and <= '9' or
            '_' or '-' or '.';

    private static bool IsLowerHexSha256(string value) =>
        value.Length == 64 &&
        value.All(static character =>
            character is >= '0' and <= '9' or
                >= 'a' and <= 'f');

    internal static bool IsRecoverable(Exception exception)
    {
        ArgumentNullException.ThrowIfNull(exception);
        if (exception is OutOfMemoryException or
            StackOverflowException or
            AccessViolationException)
        {
            return false;
        }

        if (exception is AggregateException aggregate)
        {
            return aggregate.InnerExceptions.All(
                IsRecoverable);
        }

        if (exception is TargetInvocationException or
            TypeInitializationException)
        {
            return exception.InnerException is null ||
                IsRecoverable(exception.InnerException);
        }

        return true;
    }

    private static string ComputeLowerHexDigest(
        ReadOnlySpan<byte> bytes) =>
        Convert.ToHexString(SHA256.HashData(bytes))
            .ToLowerInvariant();

    private static string BuildOperationDigest(
        int migrationOrdinal,
        EfCoreMigrationDirection direction,
        int directionOrdinal,
        IReadOnlyList<byte[]> commandDigests)
    {
        using var digest =
            new FramedDigestAccumulator(
                EfCoreMigrationAnalysisDigestDomains
                    .Operation);
        for (int commandOrdinal = 0;
             commandOrdinal < commandDigests.Count;
             commandOrdinal++)
        {
            AppendCommandIdentity(
                digest,
                migrationOrdinal,
                direction,
                directionOrdinal,
                commandOrdinal,
                commandDigests[commandOrdinal]);
        }
        return digest.Finish();
    }

    private static void AppendCommandIdentity(
        FramedDigestAccumulator digest,
        int migrationOrdinal,
        EfCoreMigrationDirection direction,
        int directionOrdinal,
        int commandOrdinal,
        byte[] commandDigest)
    {
        digest.AppendInt32(migrationOrdinal);
        digest.AppendString(
            direction == EfCoreMigrationDirection.Up
                ? "up"
                : "down");
        digest.AppendInt32(directionOrdinal);
        digest.AppendInt32(commandOrdinal);
        digest.AppendBytes(commandDigest);
    }

    private static void UpdateAggregate(
        MigrationCompatibilityStatus candidateStatus,
        string candidateRule,
        ref MigrationCompatibilityStatus aggregateStatus,
        ref string aggregateRule)
    {
        if (StatusRank(candidateStatus) >
            StatusRank(aggregateStatus))
        {
            aggregateStatus = candidateStatus;
            aggregateRule = candidateRule;
        }
    }

    private static int StatusRank(
        MigrationCompatibilityStatus status) =>
        status switch
        {
            MigrationCompatibilityStatus.Conditional => 0,
            MigrationCompatibilityStatus.Unsupported => 1,
            MigrationCompatibilityStatus.Unknown => 2,
            _ => throw new EfCoreAnalysisException(
                EfCoreAnalysisFailureKind.AnalysisFailed),
        };

    private static void AddDiagnostic(
        List<EfCoreMigrationAnalysisDiagnostic> diagnostics,
        string ruleId,
        MigrationDiagnosticSeverity severity,
        MigrationCompatibilityStatus status,
        int? migrationOrdinal,
        int? operationOrdinal)
    {
        if (diagnostics.Count >= MaxDiagnostics)
        {
            throw new EfCoreAnalysisException(
                EfCoreAnalysisFailureKind.AnalysisLimit);
        }
        (string summary, string? remediation) =
            GetDiagnosticProse(ruleId);
        int ordinal = diagnostics.Count;
        diagnostics.Add(
            new EfCoreMigrationAnalysisDiagnostic
            {
                Ordinal = ordinal,
                DiagnosticId =
                    FormattableString.Invariant(
                        $"ef.diagnostic.{ordinal:D6}"),
                RuleId = ruleId,
                Severity = severity,
                Status = status,
                Evidence = MigrationEvidenceLevel.Bound,
                MigrationOrdinal = migrationOrdinal,
                OperationOrdinal = operationOrdinal,
                Summary = summary,
                Remediation = remediation,
            });
    }

    private static (string Summary, string? Remediation)
        GetDiagnosticProse(string ruleId) =>
        ruleId switch
        {
            EfCoreMigrationAnalysisRules.GenerationBound =>
                (
                    "Migration SQL generation succeeded, but the chain was not executed.",
                    "Validate every migration prefix in an isolated scratch database before production use."),
            EfCoreMigrationAnalysisRules.SchemaUnsupported =>
                (
                    "The migration contains a schema operation.",
                    "Remove schema usage before targeting CSharpDB."),
            EfCoreMigrationAnalysisRules.SequenceUnsupported =>
                (
                    "The migration contains a sequence operation.",
                    "Replace sequence-backed value generation before targeting CSharpDB."),
            EfCoreMigrationAnalysisRules.DataUnknown =>
                (
                    "The migration contains a data operation that was not proven.",
                    "Review and migrate the affected data separately."),
            EfCoreMigrationAnalysisRules.OperationUnknown =>
                (
                    "The migration contains an operation type that was not recognized.",
                    "Replace the operation with the bounded CSharpDB schema subset."),
            EfCoreMigrationAnalysisRules.RawSqlUnsupported =>
                (
                    "Raw SQL contains DDL that is unsupported by CSharpDB.",
                    "Replace the raw SQL with supported migration operations."),
            EfCoreMigrationAnalysisRules.RawSqlUnknown =>
                (
                    "Raw SQL could not be proven by the bounded DDL analyzer.",
                    "Replace the raw SQL with supported migration operations."),
            EfCoreMigrationAnalysisRules.GenerationUnsupported =>
                (
                    "The CSharpDB SQL generator rejected the migration operation.",
                    "Rewrite the operation using the supported CSharpDB migration subset."),
            EfCoreMigrationAnalysisRules.GenerationFailed =>
                (
                    "Migration SQL generation could not be completed.",
                    "Review the compiled migration and provider configuration."),
            EfCoreMigrationAnalysisRules.TransactionSuppressed =>
                (
                    "The generated command suppresses the migration transaction.",
                    "Rewrite the operation so every generated command remains transactional."),
            EfCoreMigrationAnalysisRules.AnalysisLimit =>
                (
                    "The migration analysis exceeded a fixed safety limit.",
                    "Reduce the compiled migration input before retrying."),
            EfCoreMigrationAnalysisRules.EmptyMigration =>
                (
                    "The compiled migration contains no Up or Down operations.",
                    "Review whether the empty migration should remain in the chain."),
            EfCoreMigrationAnalysisRules.EmptyDownMigration =>
                (
                    "The compiled migration contains no Down operations.",
                    "Add a bounded rollback path or document the irreversible migration."),
            _ => throw new EfCoreAnalysisException(
                EfCoreAnalysisFailureKind.AnalysisFailed),
        };

    private static bool CanExecuteScratch(
        EfCoreMigrationAnalysisReport generation,
        IReadOnlyList<EfCoreScratchMigrationInput> inputs,
        bool inputComplete) =>
        inputComplete &&
        generation.Status ==
            MigrationCompatibilityStatus.Conditional &&
        generation.MigrationCount is > 0 and <=
            EfCoreScratchChainValidator.MaxMigrations &&
        inputs.Count == generation.MigrationCount &&
        generation.Migrations.All(static migration =>
            migration.UpOperationCount > 0 &&
            migration.DownOperationCount > 0 &&
            migration.CommandCount > 0 &&
            migration.Operations.All(static operation =>
                operation.Status ==
                    MigrationCompatibilityStatus.Conditional &&
                operation.Kind !=
                    EfCoreMigrationOperationKind.RawSql));

    private static EfCoreMigrationScratchAnalysisReport
        CreateScratchReport(
            EfCoreMigrationAnalysisReport generation,
            EfCoreScratchChainValidationResult validation)
    {
        bool passed = validation.Outcome ==
            EfCoreMigrationScratchAnalysisOutcome.Passed;
        MigrationEvidenceLevel evidence = passed ||
            validation.Proof.ExecutedCommandCount > 0
            ? MigrationEvidenceLevel.ScratchExecuted
            : MigrationEvidenceLevel.Bound;
        return new EfCoreMigrationScratchAnalysisReport
        {
            Outcome = validation.Outcome,
            Status = passed
                ? MigrationCompatibilityStatus.Compatible
                : MigrationCompatibilityStatus.Unknown,
            HighestEvidence = evidence,
            RuleId = validation.RuleId,
            GenerationPreflight = generation,
            ScratchChain = validation.Proof,
            Diagnostics = [],
        };
    }

    private static EfCoreMigrationScratchAnalysisReport
        CreateBlockedScratchReport(
            EfCoreMigrationAnalysisReport generation) =>
        new()
        {
            Outcome =
                EfCoreMigrationScratchAnalysisOutcome.Blocked,
            Status = generation.Status,
            HighestEvidence = MigrationEvidenceLevel.Bound,
            RuleId = EfCoreMigrationScratchAnalysisRules
                .GenerationPreflightBlocked,
            GenerationPreflight = generation,
            ScratchChain = new EfCoreMigrationScratchChainProof
            {
                Outcome =
                    EfCoreMigrationScratchAnalysisOutcome.Blocked,
                PrefixCount = generation.MigrationCount,
                ResourcesDisposed = true,
            },
            Diagnostics = [],
        };

    private sealed record LoadedAnalysis(
        EfCoreMigrationAnalysisReport Generation,
        EfCoreMigrationScratchAnalysisReport? Scratch);

    private sealed record OperationResult(
        EfCoreMigrationOperationKind Kind,
        MigrationCompatibilityStatus Status,
        string RuleId,
        int CommandCount,
        int SqlUtf8Bytes,
        string? GeneratedSqlDigest,
        IReadOnlyList<byte[]> CommandDigests)
    {
        internal static OperationResult Rejected(
            EfCoreMigrationOperationKind kind,
            MigrationCompatibilityStatus status,
            string ruleId) =>
            new(
                kind,
                status,
                ruleId,
                CommandCount: 0,
                SqlUtf8Bytes: 0,
                GeneratedSqlDigest: null,
                CommandDigests: []);
    }

    private sealed class AnalysisTotals
    {
        internal int CommandCount;

        internal int SqlUtf8Bytes;
    }

    private sealed class MigrationAnalysisState
    {
        internal MigrationCompatibilityStatus Status =
            MigrationCompatibilityStatus.Conditional;

        internal string RuleId =
            EfCoreMigrationAnalysisRules.GenerationBound;

        internal int CommandCount;

        internal int DestructiveCount;
    }

    private sealed class FramedDigestAccumulator : IDisposable
    {
        private readonly IncrementalHash _hash =
            IncrementalHash.CreateHash(
                HashAlgorithmName.SHA256);
        private bool _finished;

        internal FramedDigestAccumulator(string domain)
        {
            AppendString(domain);
        }

        internal void AppendInt32(int value)
        {
            Span<byte> encoded = stackalloc byte[4];
            BinaryPrimitives.WriteInt32BigEndian(
                encoded,
                value);
            AppendBytes(encoded);
        }

        internal void AppendString(string value) =>
            AppendBytes(StrictUtf8.GetBytes(value));

        internal void AppendBytes(ReadOnlySpan<byte> value)
        {
            if (_finished)
                throw new InvalidOperationException();
            Span<byte> length = stackalloc byte[4];
            BinaryPrimitives.WriteInt32BigEndian(
                length,
                value.Length);
            _hash.AppendData(length);
            _hash.AppendData(value);
        }

        internal string Finish()
        {
            if (_finished)
                throw new InvalidOperationException();
            _finished = true;
            return Convert.ToHexString(
                    _hash.GetHashAndReset())
                .ToLowerInvariant();
        }

        public void Dispose() => _hash.Dispose();
    }

    private sealed class EfCoreAnalyzerLoadContext
        : AssemblyLoadContext
    {
        private readonly AssemblyDependencyResolver _resolver;
        private readonly string _root;
        private readonly IReadOnlyDictionary<string, Assembly>
            _sharedAssemblies;

        internal EfCoreAnalyzerLoadContext(
            string mainAssemblyPath,
            Assembly designAssembly)
            : base(
                "csharpdb-ef-analysis",
                isCollectible: true)
        {
            _resolver =
                new AssemblyDependencyResolver(
                    mainAssemblyPath);
            _root = Path.GetDirectoryName(
                mainAssemblyPath)!;

            var shared = new Dictionary<string, Assembly>(
                StringComparer.OrdinalIgnoreCase);
            foreach (Assembly candidate in
                AssemblyLoadContext.Default.Assemblies
                    .Append(designAssembly))
            {
                string? name =
                    candidate.GetName().Name;
                if (name is not null &&
                    IsHostSharedAssembly(name))
                {
                    shared.TryAdd(name, candidate);
                }
            }
            _sharedAssemblies = shared;
        }

        protected override Assembly? Load(
            AssemblyName assemblyName)
        {
            if (assemblyName.Name is not null &&
                IsHostSharedAssembly(assemblyName.Name))
            {
                if (_sharedAssemblies.TryGetValue(
                        assemblyName.Name,
                        out Assembly? shared))
                {
                    if (!HasExactAssemblyIdentity(
                            assemblyName,
                            shared.GetName()))
                    {
                        throw new EfCoreAnalysisException(
                            EfCoreAnalysisFailureKind
                                .AssemblyUnavailable);
                    }
                    return shared;
                }

                try
                {
                    return LoadDefaultAssemblyExact(
                        assemblyName);
                }
                catch (Exception exception)
                    when (IsRecoverable(exception) &&
                        (exception is FileNotFoundException or
                            FileLoadException or
                            BadImageFormatException))
                {
                    // Optional host-framework components which are not part
                    // of this tool may still be valid target-local
                    // dependencies. Resolve them adjacent to the analyzed
                    // application below.
                }
            }

            string? path =
                _resolver.ResolveAssemblyToPath(
                    assemblyName);
            if (path is null)
                return null;
            if (!IsAdjacent(path))
            {
                try
                {
                    return LoadDefaultAssemblyExact(
                        assemblyName);
                }
                catch (Exception exception)
                    when (IsRecoverable(exception) &&
                        (exception is FileNotFoundException or
                            FileLoadException or
                            BadImageFormatException))
                {
                    throw new EfCoreAnalysisException(
                        EfCoreAnalysisFailureKind
                            .AssemblyUnavailable);
                }
            }
            var file = new FileInfo(path);
            if (!file.Exists ||
                file.Length <= 0 ||
                file.Length > MaxAssemblyBytes)
            {
                throw new EfCoreAnalysisException(
                    EfCoreAnalysisFailureKind.AnalysisLimit);
            }
            if (!HasExactAssemblyIdentity(
                    assemblyName,
                    AssemblyName.GetAssemblyName(
                        file.FullName)))
            {
                throw new EfCoreAnalysisException(
                    EfCoreAnalysisFailureKind
                        .AssemblyUnavailable);
            }
            return LoadFromAssemblyPath(
                Path.GetFullPath(path));
        }

        protected override nint LoadUnmanagedDll(
            string unmanagedDllName)
        {
            string? path =
                _resolver.ResolveUnmanagedDllToPath(
                    unmanagedDllName);
            if (path is null)
                return nint.Zero;
            EnsureAdjacent(path);
            var file = new FileInfo(path);
            if (!file.Exists ||
                file.Length <= 0 ||
                file.Length > MaxAssemblyBytes)
            {
                throw new EfCoreAnalysisException(
                    EfCoreAnalysisFailureKind.AnalysisLimit);
            }
            return LoadUnmanagedDllFromPath(
                Path.GetFullPath(path));
        }

        private void EnsureAdjacent(string path)
        {
            if (!IsAdjacent(path))
            {
                throw new EfCoreAnalysisException(
                    EfCoreAnalysisFailureKind
                        .AssemblyUnavailable);
            }
        }

        private bool IsAdjacent(string path)
        {
            string fullPath = Path.GetFullPath(path);
            string relative = Path.GetRelativePath(
                _root,
                fullPath);
            return !Path.IsPathFullyQualified(relative) &&
                relative != ".." &&
                !relative.StartsWith(
                    ".." +
                    Path.DirectorySeparatorChar,
                    StringComparison.Ordinal) &&
                !relative.StartsWith(
                    ".." +
                    Path.AltDirectorySeparatorChar,
                    StringComparison.Ordinal);
        }

        private static Assembly LoadDefaultAssemblyExact(
            AssemblyName requested)
        {
            Assembly loaded = AssemblyLoadContext.Default
                .LoadFromAssemblyName(requested);
            AssemblyName actual = loaded.GetName();
            if (!HasExactAssemblyIdentity(
                    requested,
                    actual))
            {
                throw new FileLoadException();
            }
            return loaded;
        }

        private static bool HasExactAssemblyIdentity(
            AssemblyName requested,
            AssemblyName actual) =>
            string.Equals(
                requested.Name,
                actual.Name,
                StringComparison.OrdinalIgnoreCase) &&
            (requested.Version is null ||
                requested.Version == actual.Version) &&
            string.Equals(
                requested.CultureName ?? string.Empty,
                actual.CultureName ?? string.Empty,
                StringComparison.OrdinalIgnoreCase) &&
            (requested.GetPublicKeyToken() ??
                []).AsSpan().SequenceEqual(
                    actual.GetPublicKeyToken() ??
                    []);

        private static bool IsHostSharedAssembly(
            string name) =>
            name is
                "Microsoft.EntityFrameworkCore" or
                "Microsoft.EntityFrameworkCore.Abstractions" or
                "Microsoft.EntityFrameworkCore.Design" or
                "Microsoft.EntityFrameworkCore.Relational" or
                "Microsoft.Extensions.Caching.Abstractions" or
                "Microsoft.Extensions.Caching.Memory" or
                "Microsoft.Extensions.Configuration.Abstractions" or
                "Microsoft.Extensions.DependencyInjection" or
                "Microsoft.Extensions.DependencyInjection.Abstractions" or
                "Microsoft.Extensions.DependencyModel" or
                "Microsoft.Extensions.Logging" or
                "Microsoft.Extensions.Logging.Abstractions" or
                "Microsoft.Extensions.Options" or
                "Microsoft.Extensions.Primitives" or
                "CSharpDB.Client" or
                "CSharpDB.Data" or
                "CSharpDB.Engine" or
                "CSharpDB.EntityFrameworkCore" or
                "CSharpDB.Execution" or
                "CSharpDB.ImportExport" or
                "CSharpDB.Migration" or
                "CSharpDB.Migration.CSharpDb.Ddl" or
                "CSharpDB.Pipelines" or
                "CSharpDB.Primitives" or
                "CSharpDB.Sql" or
                "CSharpDB.Storage" or
                "CSharpDB.Storage.Diagnostics";
    }
}
