using System.Collections.Immutable;
using System.Globalization;
using System.Text.Json;
using System.Text.Json.Serialization;
using CSharpDB.Client;
using CSharpDB.CodeModules.Infrastructure;
using CSharpDB.CodeModules.Runtime;
using CSharpDB.CodeModules.Trust;
using CSharpDB.Primitives;
using Microsoft.CodeAnalysis;
using Microsoft.CodeAnalysis.CSharp;

namespace CSharpDB.CodeModules;

public sealed class CSharpDbCodeModuleClient(
    ICSharpDbClient dbClient,
    ICodeModuleTrustStore trustStore)
{
    public CSharpDbCodeModuleClient(ICSharpDbClient dbClient)
        : this(dbClient, new FileCodeModuleTrustStore())
    {
    }

    public const string CodeModulesTableName = "__code_modules";
    public const string CodeModuleBuildsTableName = "__code_module_builds";
    public const string WorkspaceDirectoryName = ".csharpdb-code";
    public const string ManifestFileName = "csharpdb.codeproj.json";

    private static readonly JsonSerializerOptions s_jsonOptions = new()
    {
        PropertyNamingPolicy = JsonNamingPolicy.CamelCase,
        PropertyNameCaseInsensitive = true,
        DefaultIgnoreCondition = JsonIgnoreCondition.WhenWritingNull,
        WriteIndented = false,
        Converters = { new JsonStringEnumConverter(JsonNamingPolicy.CamelCase) },
    };

    private static readonly JsonSerializerOptions s_manifestJsonOptions = new(s_jsonOptions)
    {
        WriteIndented = true,
    };

    private readonly SemaphoreSlim _initLock = new(1, 1);
    private bool _initialized;

    public async Task<IReadOnlyList<CodeModuleSummary>> ListAsync(CancellationToken ct = default)
    {
        await EnsureInitializedAsync(ct);
        string sql = $"""
            SELECT module_id, name, module_kind, owner_kind, owner_id, type_name, source_hash, created_utc, updated_utc
            FROM {CodeModulesTableName}
            ORDER BY module_kind, name, module_id;
            """;

        return CodeModuleSql.ReadRows(await dbClient.ExecuteSqlAsync(sql, ct))
            .Select(ReadSummary)
            .ToArray();
    }

    public async Task<CodeModuleDefinition?> GetAsync(string moduleId, CancellationToken ct = default)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(moduleId);
        await EnsureInitializedAsync(ct);
        string sql = $"""
            SELECT module_id, name, module_kind, language, owner_kind, owner_id, type_name, source, source_hash, metadata_json, created_utc, updated_utc
            FROM {CodeModulesTableName}
            WHERE module_id = {CodeModuleSql.FormatLiteral(moduleId)}
            LIMIT 1;
            """;

        Dictionary<string, object?>? row = CodeModuleSql.ReadRows(await dbClient.ExecuteSqlAsync(sql, ct)).FirstOrDefault();
        return row is null ? null : ReadDefinition(row);
    }

    public async Task<CodeModuleDefinition> UpsertAsync(CodeModuleDefinition module, CancellationToken ct = default)
    {
        ArgumentNullException.ThrowIfNull(module);
        await EnsureInitializedAsync(ct);

        CodeModuleDefinition normalized = NormalizeForStorage(module);
        string now = DateTimeOffset.UtcNow.ToString("O", CultureInfo.InvariantCulture);
        CodeModuleDefinition? existing = await GetAsync(normalized.ModuleId, ct);
        string metadataJson = JsonSerializer.Serialize(normalized.SafeMetadata, s_jsonOptions);

        string sql = existing is null
            ? $"""
                INSERT INTO {CodeModulesTableName} (
                    module_id,
                    name,
                    module_kind,
                    language,
                    owner_kind,
                    owner_id,
                    type_name,
                    source,
                    source_hash,
                    metadata_json,
                    created_utc,
                    updated_utc
                )
                VALUES (
                    {CodeModuleSql.FormatLiteral(normalized.ModuleId)},
                    {CodeModuleSql.FormatLiteral(normalized.Name)},
                    {CodeModuleSql.FormatLiteral(normalized.Kind.ToString())},
                    {CodeModuleSql.FormatLiteral(normalized.Language)},
                    {CodeModuleSql.FormatLiteral(normalized.OwnerKind)},
                    {CodeModuleSql.FormatLiteral(normalized.OwnerId)},
                    {CodeModuleSql.FormatLiteral(normalized.TypeName)},
                    {CodeModuleSql.FormatLiteral(normalized.Source)},
                    {CodeModuleSql.FormatLiteral(normalized.SourceHash)},
                    {CodeModuleSql.FormatLiteral(metadataJson)},
                    {CodeModuleSql.FormatLiteral(now)},
                    {CodeModuleSql.FormatLiteral(now)}
                );
                """
            : $"""
                UPDATE {CodeModulesTableName}
                SET name = {CodeModuleSql.FormatLiteral(normalized.Name)},
                    module_kind = {CodeModuleSql.FormatLiteral(normalized.Kind.ToString())},
                    language = {CodeModuleSql.FormatLiteral(normalized.Language)},
                    owner_kind = {CodeModuleSql.FormatLiteral(normalized.OwnerKind)},
                    owner_id = {CodeModuleSql.FormatLiteral(normalized.OwnerId)},
                    type_name = {CodeModuleSql.FormatLiteral(normalized.TypeName)},
                    source = {CodeModuleSql.FormatLiteral(normalized.Source)},
                    source_hash = {CodeModuleSql.FormatLiteral(normalized.SourceHash)},
                    metadata_json = {CodeModuleSql.FormatLiteral(metadataJson)},
                    updated_utc = {CodeModuleSql.FormatLiteral(now)}
                WHERE module_id = {CodeModuleSql.FormatLiteral(normalized.ModuleId)};
                """;

        CodeModuleSql.ThrowIfError(await dbClient.ExecuteSqlAsync(sql, ct));
        return await GetAsync(normalized.ModuleId, ct)
            ?? throw new InvalidOperationException($"Code module '{normalized.ModuleId}' could not be loaded after save.");
    }

    public async Task<bool> DeleteAsync(string moduleId, CancellationToken ct = default)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(moduleId);
        await EnsureInitializedAsync(ct);
        string sql = $"""
            DELETE FROM {CodeModulesTableName}
            WHERE module_id = {CodeModuleSql.FormatLiteral(moduleId)};
            """;

        var result = await dbClient.ExecuteSqlAsync(sql, ct);
        CodeModuleSql.ThrowIfError(result);
        return result.RowsAffected > 0;
    }

    public async Task<CodeModuleExportResult> ExportAsync(string workspaceDirectory, CancellationToken ct = default)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(workspaceDirectory);
        await EnsureInitializedAsync(ct);
        string root = PrepareWorkspaceRoot(workspaceDirectory);
        IReadOnlyList<CodeModuleDefinition> modules = await ListDefinitionsAsync(ct);
        string moduleSetHash = CodeModuleHashing.ComputeModuleSetHash(modules);

        var entries = new List<CodeModuleWorkspaceManifestEntry>(modules.Count);
        var usedPaths = new HashSet<string>(StringComparer.OrdinalIgnoreCase);
        foreach (CodeModuleDefinition module in modules)
        {
            string relativePath = BuildWorkspacePath(module, usedPaths);
            string absolutePath = Path.Combine(root, relativePath);
            Directory.CreateDirectory(Path.GetDirectoryName(absolutePath)!);
            await File.WriteAllTextAsync(absolutePath, module.Source, ct);
            entries.Add(new CodeModuleWorkspaceManifestEntry(
                module.ModuleId,
                module.Name,
                module.Kind,
                module.OwnerKind,
                module.OwnerId,
                module.TypeName,
                relativePath.Replace('\\', '/'),
                module.SourceHash ?? CodeModuleHashing.ComputeSourceHash(module.Source),
                module.SafeMetadata));
        }

        var manifest = new CodeModuleWorkspaceManifest(1, moduleSetHash, entries);
        string manifestPath = Path.Combine(root, ManifestFileName);
        await File.WriteAllTextAsync(manifestPath, JsonSerializer.Serialize(manifest, s_manifestJsonOptions), ct);
        return new CodeModuleExportResult(root, manifestPath, modules.Count, moduleSetHash);
    }

    public async Task<CodeModuleImportResult> ImportAsync(string workspaceDirectory, CancellationToken ct = default)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(workspaceDirectory);
        await EnsureInitializedAsync(ct);
        string root = PrepareWorkspaceRoot(workspaceDirectory);
        string manifestPath = Path.Combine(root, ManifestFileName);
        if (!File.Exists(manifestPath))
            throw new FileNotFoundException("The CSharpDB code module manifest was not found.", manifestPath);

        CodeModuleWorkspaceManifest manifest = JsonSerializer.Deserialize<CodeModuleWorkspaceManifest>(
            await File.ReadAllTextAsync(manifestPath, ct),
            s_manifestJsonOptions)
            ?? throw new InvalidOperationException("The CSharpDB code module manifest could not be read.");

        var changes = new List<CodeModuleImportChange>();
        foreach (CodeModuleWorkspaceManifestEntry entry in manifest.Modules)
        {
            string relativePath = NormalizeRelativePath(entry.Path);
            string absolutePath = Path.Combine(root, relativePath);
            if (!File.Exists(absolutePath))
            {
                changes.Add(new CodeModuleImportChange(entry.ModuleId, entry.Path, CodeModuleImportChangeKind.Skipped, "Source file is missing."));
                continue;
            }

            string source = await File.ReadAllTextAsync(absolutePath, ct);
            string fileHash = CodeModuleHashing.ComputeSourceHash(source);
            CodeModuleDefinition? existing = await GetAsync(entry.ModuleId, ct);
            if (existing is not null &&
                !string.Equals(existing.SourceHash, entry.SourceHash, StringComparison.OrdinalIgnoreCase) &&
                !string.Equals(fileHash, entry.SourceHash, StringComparison.OrdinalIgnoreCase))
            {
                changes.Add(new CodeModuleImportChange(
                    entry.ModuleId,
                    entry.Path,
                    CodeModuleImportChangeKind.Conflict,
                    "Both the database module and exported file changed since export."));
                continue;
            }

            if (existing is not null && string.Equals(existing.SourceHash, fileHash, StringComparison.OrdinalIgnoreCase))
            {
                changes.Add(new CodeModuleImportChange(entry.ModuleId, entry.Path, CodeModuleImportChangeKind.Unchanged));
                continue;
            }

            if (existing is not null && !string.Equals(existing.SourceHash, entry.SourceHash, StringComparison.OrdinalIgnoreCase))
            {
                changes.Add(new CodeModuleImportChange(entry.ModuleId, entry.Path, CodeModuleImportChangeKind.Skipped, "Database module changed and exported file is unchanged."));
                continue;
            }

            var updated = new CodeModuleDefinition(
                entry.ModuleId,
                entry.Name,
                entry.Kind,
                source,
                entry.OwnerKind,
                entry.OwnerId,
                entry.TypeName,
                entry.Metadata,
                SourceHash: fileHash);
            await UpsertAsync(updated, ct);
            changes.Add(new CodeModuleImportChange(
                entry.ModuleId,
                entry.Path,
                existing is null ? CodeModuleImportChangeKind.Added : CodeModuleImportChangeKind.Updated));
        }

        return new CodeModuleImportResult(root, changes);
    }

    public async Task<CodeModuleBuildResult> BuildAsync(CancellationToken ct = default)
    {
        await EnsureInitializedAsync(ct);
        IReadOnlyList<CodeModuleDefinition> modules = await ListDefinitionsAsync(ct);
        string moduleSetHash = CodeModuleHashing.ComputeModuleSetHash(modules);
        DateTimeOffset builtUtc = DateTimeOffset.UtcNow;

        Dictionary<SyntaxTree, CodeModuleDefinition> treeModuleMap = new();
        List<SyntaxTree> syntaxTrees = [];
        foreach (CodeModuleDefinition module in modules)
        {
            string path = BuildDiagnosticPath(module);
            SyntaxTree tree = CSharpSyntaxTree.ParseText(
                CodeModuleHashing.NormalizeSource(module.Source),
                new CSharpParseOptions(LanguageVersion.Preview),
                path,
                cancellationToken: ct);
            treeModuleMap[tree] = module;
            syntaxTrees.Add(tree);
        }

        CSharpCompilation compilation = CSharpCompilation.Create(
            $"CSharpDB.CodeModules.Database.{moduleSetHash}",
            syntaxTrees,
            BuildReferences(),
            new CSharpCompilationOptions(OutputKind.DynamicallyLinkedLibrary));

        using var assemblyStream = new MemoryStream();
        Microsoft.CodeAnalysis.Emit.EmitResult emit = compilation.Emit(assemblyStream, cancellationToken: ct);
        IReadOnlyList<CodeModuleDiagnostic> diagnostics = emit.Diagnostics
            .Select(diagnostic => ConvertDiagnostic(diagnostic, treeModuleMap))
            .ToArray();
        var result = new CodeModuleBuildResult(
            moduleSetHash,
            emit.Success ? CodeModuleBuildStatus.Succeeded : CodeModuleBuildStatus.Failed,
            diagnostics,
            builtUtc,
            emit.Success ? assemblyStream.ToArray() : null);
        await StoreBuildResultAsync(result with { AssemblyBytes = null }, ct);
        return result;
    }

    public async Task TrustAsync(CancellationToken ct = default)
    {
        CodeModuleBuildResult build = await BuildAsync(ct);
        if (!build.Succeeded)
            throw new InvalidOperationException("C# code modules must build successfully before they can be trusted.");

        string databasePath = await GetDatabasePathAsync(ct);
        await trustStore.TrustAsync(databasePath, build.ModuleSetHash, ct);
    }

    public async Task<CodeModuleTrustState> GetTrustStateAsync(string? moduleSetHash = null, CancellationToken ct = default)
    {
        string resolvedHash = string.IsNullOrWhiteSpace(moduleSetHash)
            ? CodeModuleHashing.ComputeModuleSetHash(await ListDefinitionsAsync(ct))
            : moduleSetHash;
        string databasePath = await GetDatabasePathAsync(ct);
        return await trustStore.GetTrustStateAsync(databasePath, resolvedHash, ct);
    }

    public async Task<string> GetCurrentModuleSetHashAsync(CancellationToken ct = default)
        => CodeModuleHashing.ComputeModuleSetHash(await ListDefinitionsAsync(ct));

    private async Task<IReadOnlyList<CodeModuleDefinition>> ListDefinitionsAsync(CancellationToken ct)
    {
        await EnsureInitializedAsync(ct);
        string sql = $"""
            SELECT module_id, name, module_kind, language, owner_kind, owner_id, type_name, source, source_hash, metadata_json, created_utc, updated_utc
            FROM {CodeModulesTableName}
            ORDER BY module_id;
            """;

        return CodeModuleSql.ReadRows(await dbClient.ExecuteSqlAsync(sql, ct))
            .Select(ReadDefinition)
            .ToArray();
    }

    private async Task EnsureInitializedAsync(CancellationToken ct)
    {
        if (_initialized)
            return;

        await _initLock.WaitAsync(ct);
        try
        {
            if (_initialized)
                return;

            string sql = $"""
                CREATE TABLE IF NOT EXISTS {CodeModulesTableName} (
                    module_id TEXT PRIMARY KEY,
                    name TEXT NOT NULL,
                    module_kind TEXT NOT NULL,
                    language TEXT NOT NULL,
                    owner_kind TEXT,
                    owner_id TEXT,
                    type_name TEXT,
                    source TEXT NOT NULL,
                    source_hash TEXT NOT NULL,
                    metadata_json TEXT,
                    created_utc TEXT NOT NULL,
                    updated_utc TEXT NOT NULL
                );
                CREATE INDEX IF NOT EXISTS idx___code_modules_owner
                ON {CodeModulesTableName} (owner_kind, owner_id);
                CREATE TABLE IF NOT EXISTS {CodeModuleBuildsTableName} (
                    build_id TEXT PRIMARY KEY,
                    module_set_hash TEXT NOT NULL,
                    status TEXT NOT NULL,
                    diagnostics_json TEXT NOT NULL,
                    created_utc TEXT NOT NULL
                );
                CREATE INDEX IF NOT EXISTS idx___code_module_builds_module_set_hash
                ON {CodeModuleBuildsTableName} (module_set_hash);
                """;

            CodeModuleSql.ThrowIfError(await dbClient.ExecuteSqlAsync(sql, ct));
            _initialized = true;
        }
        finally
        {
            _initLock.Release();
        }
    }

    private async Task StoreBuildResultAsync(CodeModuleBuildResult result, CancellationToken ct)
    {
        string diagnosticsJson = JsonSerializer.Serialize(result.Diagnostics, s_jsonOptions);
        string sql = $"""
            INSERT INTO {CodeModuleBuildsTableName} (
                build_id,
                module_set_hash,
                status,
                diagnostics_json,
                created_utc
            )
            VALUES (
                {CodeModuleSql.FormatLiteral(Guid.NewGuid().ToString("N"))},
                {CodeModuleSql.FormatLiteral(result.ModuleSetHash)},
                {CodeModuleSql.FormatLiteral(result.Status.ToString())},
                {CodeModuleSql.FormatLiteral(diagnosticsJson)},
                {CodeModuleSql.FormatLiteral(result.BuiltUtc.ToString("O", CultureInfo.InvariantCulture))}
            );
            """;

        CodeModuleSql.ThrowIfError(await dbClient.ExecuteSqlAsync(sql, ct));
    }

    private async Task<string> GetDatabasePathAsync(CancellationToken ct)
    {
        string dataSource = dbClient.DataSource;
        if (!string.IsNullOrWhiteSpace(dataSource))
            return dataSource;

        return (await dbClient.GetInfoAsync(ct)).DataSource;
    }

    private static CodeModuleDefinition NormalizeForStorage(CodeModuleDefinition module)
    {
        if (string.IsNullOrWhiteSpace(module.ModuleId))
            throw new ArgumentException("Code modules require a module id.", nameof(module));
        if (string.IsNullOrWhiteSpace(module.Name))
            throw new ArgumentException("Code modules require a name.", nameof(module));
        if (string.IsNullOrWhiteSpace(module.Language))
            throw new ArgumentException("Code modules require a language.", nameof(module));

        string source = CodeModuleHashing.NormalizeSource(module.Source);
        string sourceHash = CodeModuleHashing.ComputeSourceHash(source);
        return module with
        {
            ModuleId = module.ModuleId.Trim(),
            Name = module.Name.Trim(),
            Language = module.Language.Trim(),
            OwnerKind = string.IsNullOrWhiteSpace(module.OwnerKind) ? null : module.OwnerKind.Trim(),
            OwnerId = string.IsNullOrWhiteSpace(module.OwnerId) ? null : module.OwnerId.Trim(),
            TypeName = string.IsNullOrWhiteSpace(module.TypeName) ? null : module.TypeName.Trim(),
            Source = source,
            SourceHash = sourceHash,
        };
    }

    private static CodeModuleSummary ReadSummary(Dictionary<string, object?> row)
        => new(
            ReadRequired(row, "module_id"),
            ReadRequired(row, "name"),
            ReadKind(row),
            ReadOptional(row, "owner_kind"),
            ReadOptional(row, "owner_id"),
            ReadOptional(row, "type_name"),
            ReadRequired(row, "source_hash"),
            ReadUtc(row, "created_utc"),
            ReadUtc(row, "updated_utc"));

    private static CodeModuleDefinition ReadDefinition(Dictionary<string, object?> row)
    {
        IReadOnlyDictionary<string, string>? metadata = null;
        string? metadataJson = ReadOptional(row, "metadata_json");
        if (!string.IsNullOrWhiteSpace(metadataJson))
        {
            metadata = JsonSerializer.Deserialize<Dictionary<string, string>>(metadataJson, s_jsonOptions)
                ?? new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase);
        }

        return new CodeModuleDefinition(
            ReadRequired(row, "module_id"),
            ReadRequired(row, "name"),
            ReadKind(row),
            ReadRequired(row, "source"),
            ReadOptional(row, "owner_kind"),
            ReadOptional(row, "owner_id"),
            ReadOptional(row, "type_name"),
            metadata,
            ReadOptional(row, "language") ?? "csharp",
            ReadRequired(row, "source_hash"),
            ReadUtc(row, "created_utc"),
            ReadUtc(row, "updated_utc"));
    }

    private static CodeModuleKind ReadKind(Dictionary<string, object?> row)
        => Enum.TryParse(ReadRequired(row, "module_kind"), ignoreCase: true, out CodeModuleKind kind)
            ? kind
            : throw new InvalidOperationException($"Unknown code module kind '{ReadRequired(row, "module_kind")}'.");

    private static string ReadRequired(Dictionary<string, object?> row, string column)
        => row.TryGetValue(column, out object? value) && value is not null
            ? Convert.ToString(value, CultureInfo.InvariantCulture) ?? string.Empty
            : throw new InvalidOperationException($"Code module row is missing '{column}'.");

    private static string? ReadOptional(Dictionary<string, object?> row, string column)
        => row.TryGetValue(column, out object? value) && value is not null
            ? Convert.ToString(value, CultureInfo.InvariantCulture)
            : null;

    private static DateTimeOffset ReadUtc(Dictionary<string, object?> row, string column)
        => DateTimeOffset.TryParse(ReadRequired(row, column), CultureInfo.InvariantCulture, DateTimeStyles.AssumeUniversal, out DateTimeOffset value)
            ? value
            : DateTimeOffset.MinValue;

    private static CodeModuleDiagnostic ConvertDiagnostic(
        Diagnostic diagnostic,
        IReadOnlyDictionary<SyntaxTree, CodeModuleDefinition> treeModuleMap)
    {
        FileLinePositionSpan span = diagnostic.Location.GetLineSpan();
        string? path = span.Path;
        CodeModuleDefinition? module = null;
        if (diagnostic.Location.SourceTree is not null)
            treeModuleMap.TryGetValue(diagnostic.Location.SourceTree, out module);

        return new CodeModuleDiagnostic(
            module?.ModuleId,
            string.IsNullOrWhiteSpace(path) ? null : path,
            span.StartLinePosition.Line + 1,
            span.StartLinePosition.Character + 1,
            diagnostic.Severity switch
            {
                DiagnosticSeverity.Hidden => CodeModuleDiagnosticSeverity.Hidden,
                DiagnosticSeverity.Info => CodeModuleDiagnosticSeverity.Info,
                DiagnosticSeverity.Warning => CodeModuleDiagnosticSeverity.Warning,
                DiagnosticSeverity.Error => CodeModuleDiagnosticSeverity.Error,
                _ => CodeModuleDiagnosticSeverity.Info,
            },
            diagnostic.Id,
            diagnostic.GetMessage(CultureInfo.InvariantCulture));
    }

    private static IReadOnlyList<MetadataReference> BuildReferences()
    {
        var references = new Dictionary<string, MetadataReference>(StringComparer.OrdinalIgnoreCase);
        if (AppContext.GetData("TRUSTED_PLATFORM_ASSEMBLIES") is string tpa)
        {
            foreach (string path in tpa.Split(Path.PathSeparator))
                AddReference(path);
        }

        AddReference(typeof(FormCodeModule).Assembly.Location);
        AddReference(typeof(DbCommandResult).Assembly.Location);
        AddReference(typeof(object).Assembly.Location);

        return references.Values.ToImmutableArray();

        void AddReference(string? path)
        {
            if (string.IsNullOrWhiteSpace(path) || !File.Exists(path) || references.ContainsKey(path))
                return;

            references[path] = MetadataReference.CreateFromFile(path);
        }
    }

    private static string PrepareWorkspaceRoot(string workspaceDirectory)
    {
        string root = Path.GetFullPath(workspaceDirectory);
        if (!string.Equals(Path.GetFileName(root), WorkspaceDirectoryName, StringComparison.OrdinalIgnoreCase))
            root = Path.Combine(root, WorkspaceDirectoryName);

        Directory.CreateDirectory(root);
        return root;
    }

    private static string BuildWorkspacePath(CodeModuleDefinition module, HashSet<string> usedPaths)
    {
        string folder = module.Kind switch
        {
            CodeModuleKind.Form => "forms",
            CodeModuleKind.Class => "classes",
            _ => "modules",
        };
        string baseName = ToSafeFileName(module.Name);
        string relativePath = Path.Combine(folder, $"{baseName}.cs");
        int suffix = 2;
        while (!usedPaths.Add(relativePath))
        {
            relativePath = Path.Combine(folder, $"{baseName}-{suffix}.cs");
            suffix++;
        }

        return relativePath;
    }

    private static string BuildDiagnosticPath(CodeModuleDefinition module)
        => module.Kind switch
        {
            CodeModuleKind.Form => $"forms/{ToSafeFileName(module.Name)}.cs",
            CodeModuleKind.Class => $"classes/{ToSafeFileName(module.Name)}.cs",
            _ => $"modules/{ToSafeFileName(module.Name)}.cs",
        };

    private static string NormalizeRelativePath(string path)
    {
        string normalized = path.Replace('/', Path.DirectorySeparatorChar).Replace('\\', Path.DirectorySeparatorChar);
        if (Path.IsPathRooted(normalized))
            throw new InvalidOperationException("Code module workspace paths must be relative.");

        return normalized;
    }

    private static string ToSafeFileName(string value)
    {
        string safe = new(value.Select(ch => char.IsLetterOrDigit(ch) || ch is '_' or '-' ? ch : '_').ToArray());
        return string.IsNullOrWhiteSpace(safe) ? "module" : safe;
    }

    private sealed record CodeModuleWorkspaceManifest(
        int Version,
        string ModuleSetHash,
        IReadOnlyList<CodeModuleWorkspaceManifestEntry> Modules);

    private sealed record CodeModuleWorkspaceManifestEntry(
        string ModuleId,
        string Name,
        CodeModuleKind Kind,
        string? OwnerKind,
        string? OwnerId,
        string? TypeName,
        string Path,
        string SourceHash,
        IReadOnlyDictionary<string, string>? Metadata);
}
