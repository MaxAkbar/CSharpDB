using System.Buffers.Binary;
using System.Security.Cryptography;
using System.Text;
using CSharpDB.Migration.Canonicalization;

namespace CSharpDB.Migration.DualRun;

/// <summary>
/// Executes one declared read-only query against source and target concurrently,
/// then compares deterministic, bounded canonical evidence. A pass is possible
/// only when both endpoints succeed and their schema and rows are equal.
/// </summary>
public sealed class DualRunValidator
{
    private const int MaxQueryBytes = 1024 * 1024;
    private const int MaxParameters = 1_024;
    private const int HardMaxRows = 1_000_000;
    private const int HardMaxColumns = 1_024;
    private const int HardMaxCellBytes = 64 * 1024 * 1024;
    private const long HardMaxTotalBytes = 1024L * 1024 * 1024;
    private const int HardMaxMismatchDetails = 1_000;
    private static readonly TimeSpan HardMaxTimeout = TimeSpan.FromMinutes(10);

    public async ValueTask<DualRunReport> ValidateAsync(
        DualRunQueryCase queryCase,
        IDualRunQueryExecutor sourceExecutor,
        IDualRunQueryExecutor targetExecutor,
        CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(queryCase);
        ArgumentNullException.ThrowIfNull(sourceExecutor);
        ArgumentNullException.ThrowIfNull(targetExecutor);

        ValidateQueryCase(queryCase);
        ValidateExecutor(sourceExecutor, nameof(sourceExecutor));
        ValidateExecutor(targetExecutor, nameof(targetExecutor));
        if (!string.Equals(
                queryCase.SourceSnapshotIdentity,
                sourceExecutor.SnapshotIdentity,
                StringComparison.Ordinal))
        {
            throw new ArgumentException(
                "The source executor snapshot identity does not match the query case.",
                nameof(sourceExecutor));
        }
        if (!string.Equals(
                queryCase.TargetSnapshotIdentity,
                targetExecutor.SnapshotIdentity,
                StringComparison.Ordinal))
        {
            throw new ArgumentException(
                "The target executor snapshot identity does not match the query case.",
                nameof(targetExecutor));
        }
        cancellationToken.ThrowIfCancellationRequested();

        DualRunQueryCase frozenCase = Freeze(queryCase);
        string invocationDigest = ComputeInvocationDigest(
            frozenCase,
            sourceExecutor,
            targetExecutor);

        Task<EndpointCapture> sourceTask = CaptureEndpointAsync(
            frozenCase.SourceSql,
            frozenCase,
            sourceExecutor,
            cancellationToken);
        Task<EndpointCapture> targetTask = CaptureEndpointAsync(
            frozenCase.TargetSql,
            frozenCase,
            targetExecutor,
            cancellationToken);

        await Task.WhenAll(sourceTask, targetTask).ConfigureAwait(false);
        EndpointCapture source = await sourceTask.ConfigureAwait(false);
        EndpointCapture target = await targetTask.ConfigureAwait(false);

        var differences = new List<DualRunDifference>();
        DualRunValidationStatus status = Compare(
            frozenCase,
            source,
            target,
            differences);

        return new DualRunReport
        {
            CaseId = frozenCase.CaseId,
            CanonicalizationId = frozenCase.CanonicalizationId,
            CanonicalizationContractHash = frozenCase.CanonicalizationContractHash,
            InvocationDigest = invocationDigest,
            Ordering = frozenCase.Ordering,
            Status = status,
            Limits = ToReportLimits(frozenCase.Limits),
            Source = source.Evidence,
            Target = target.Evidence,
            Differences = differences,
        };
    }

    private static async Task<EndpointCapture> CaptureEndpointAsync(
        string sql,
        DualRunQueryCase queryCase,
        IDualRunQueryExecutor executor,
        CancellationToken cancellationToken)
    {
        using var timeout = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
        timeout.CancelAfter(queryCase.Limits.TimeoutPerEndpoint);

        try
        {
            var request = new DualRunExecutionRequest
            {
                CaseId = queryCase.CaseId,
                SnapshotIdentity = executor.SnapshotIdentity,
                CanonicalizationId = queryCase.CanonicalizationId,
                CanonicalizationContractHash = queryCase.CanonicalizationContractHash,
                Sql = sql,
                Parameters = CloneParameters(queryCase.Parameters),
                Columns = queryCase.Columns.ToArray(),
                Limits = queryCase.Limits,
            };

            await using IDualRunQueryExecution execution =
                await executor.ExecuteReadOnlyAsync(request, timeout.Token).ConfigureAwait(false);
            IReadOnlyList<DualRunResultColumn> rawColumns =
                execution.Columns ?? throw new DualRunExecutionException(
                    DualRunErrorKind.InvalidResult,
                    "DUALRUN_SCHEMA_MISSING");

            if (rawColumns.Count > queryCase.Limits.MaxColumns)
            {
                throw new DualRunExecutionException(
                    DualRunErrorKind.LimitExceeded,
                    "DUALRUN_COLUMN_LIMIT_EXCEEDED");
            }

            CanonicalColumn[] columns = CanonicalizeColumns(rawColumns, queryCase.Columns);
            string schemaDigest = ComputeSchemaDigest(columns);
            var rowHashes = new List<byte[]>(Math.Min(queryCase.Limits.MaxRows, 4_096));
            long totalCanonicalBytes = 0;

            await foreach (IReadOnlyList<object?> row in execution
                               .ReadRowsAsync(timeout.Token)
                               .WithCancellation(timeout.Token)
                               .ConfigureAwait(false))
            {
                if (rowHashes.Count >= queryCase.Limits.MaxRows)
                {
                    throw new DualRunExecutionException(
                        DualRunErrorKind.LimitExceeded,
                        "DUALRUN_ROW_LIMIT_EXCEEDED");
                }

                if (row is null || row.Count != columns.Length)
                {
                    throw new DualRunExecutionException(
                        DualRunErrorKind.InvalidResult,
                        "DUALRUN_ROW_WIDTH_MISMATCH");
                }

                var canonical = new CanonicalValue[columns.Length];
                for (int index = 0; index < columns.Length; index++)
                {
                    object? value = row[index];
                    int rawSize = DualRunCanonicalizer.GetRawSize(value);
                    if (rawSize > queryCase.Limits.MaxCellBytes)
                    {
                        throw new DualRunExecutionException(
                            DualRunErrorKind.LimitExceeded,
                            "DUALRUN_CELL_LIMIT_EXCEEDED");
                    }

                    canonical[index] = DualRunCanonicalizer.Canonicalize(value, columns[index].Type);
                    int canonicalCellSize = EncodeProviderRow([canonical[index]]).Length;
                    if (canonicalCellSize > queryCase.Limits.MaxCellBytes)
                    {
                        throw new DualRunExecutionException(
                            DualRunErrorKind.LimitExceeded,
                            "DUALRUN_CELL_LIMIT_EXCEEDED");
                    }
                }

                byte[] encoded = EncodeProviderRow(canonical);
                totalCanonicalBytes = checked(totalCanonicalBytes + encoded.Length);
                if (totalCanonicalBytes > queryCase.Limits.MaxTotalCanonicalBytesPerEndpoint)
                {
                    throw new DualRunExecutionException(
                        DualRunErrorKind.LimitExceeded,
                        "DUALRUN_TOTAL_BYTE_LIMIT_EXCEEDED");
                }

                rowHashes.Add(SHA256.HashData(encoded));
            }

            string resultDigest = ComputeResultDigest(
                schemaDigest,
                rowHashes,
                queryCase.Ordering);
            return new EndpointCapture(
                new DualRunEndpointEvidence
                {
                    ProviderId = executor.ProviderId,
                    SnapshotIdentity = executor.SnapshotIdentity,
                    ReadOnlyEnforcement = executor.ReadOnlyEnforcement,
                    ReadOnlyValidatorId = executor.ReadOnlyValidatorId,
                    Status = DualRunEndpointStatus.Succeeded,
                    ColumnCount = columns.Length,
                    RowCount = rowHashes.Count,
                    SchemaDigest = schemaDigest,
                    ResultDigest = resultDigest,
                },
                columns,
                rowHashes);
        }
        catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
        {
            throw;
        }
        catch (OperationCanceledException)
        {
            return Failed(
                executor,
                DualRunErrorKind.TimedOut,
                "DUALRUN_ENDPOINT_TIMEOUT");
        }
        catch (DualRunExecutionException ex)
        {
            return Failed(executor, ex.Kind, NormalizeErrorCode(ex.Code));
        }
        catch (Exception ex)
        {
            return Failed(
                executor,
                DualRunErrorKind.ProviderError,
                NormalizeProviderExceptionCode(ex.GetType()));
        }
    }

    private static EndpointCapture Failed(
        IDualRunQueryExecutor executor,
        DualRunErrorKind kind,
        string code) =>
        new(
            new DualRunEndpointEvidence
            {
                ProviderId = executor.ProviderId,
                SnapshotIdentity = executor.SnapshotIdentity,
                ReadOnlyEnforcement = executor.ReadOnlyEnforcement,
                ReadOnlyValidatorId = executor.ReadOnlyValidatorId,
                Status = DualRunEndpointStatus.Failed,
                Error = new DualRunEndpointError
                {
                    Kind = kind,
                    Code = code,
                },
            },
            [],
            []);

    private static DualRunValidationStatus Compare(
        DualRunQueryCase queryCase,
        EndpointCapture source,
        EndpointCapture target,
        List<DualRunDifference> differences)
    {
        if (source.Evidence.Status == DualRunEndpointStatus.Failed)
        {
            differences.Add(new DualRunDifference
            {
                Code = DualRunDifferenceCodes.EndpointFailed,
                Endpoint = "source",
            });
        }

        if (target.Evidence.Status == DualRunEndpointStatus.Failed)
        {
            differences.Add(new DualRunDifference
            {
                Code = DualRunDifferenceCodes.EndpointFailed,
                Endpoint = "target",
            });
        }

        if (differences.Count > 0)
            return DualRunValidationStatus.Inconclusive;

        bool schemaMatches;
        if (queryCase.Columns.Count > 0)
        {
            CanonicalColumn[] expected = queryCase.Columns
                .Select(static column => new CanonicalColumn(
                    DualRunCanonicalizer.NormalizeIdentifier(column.Name),
                    column.Type))
                .ToArray();
            string expectedDigest = ComputeSchemaDigest(expected);
            schemaMatches = true;
            if (!string.Equals(
                    source.Evidence.SchemaDigest,
                    expectedDigest,
                    StringComparison.Ordinal))
            {
                schemaMatches = false;
                differences.Add(new DualRunDifference
                {
                    Code = DualRunDifferenceCodes.SchemaMismatch,
                    Endpoint = "source",
                });
            }
            if (!string.Equals(
                    target.Evidence.SchemaDigest,
                    expectedDigest,
                    StringComparison.Ordinal))
            {
                schemaMatches = false;
                differences.Add(new DualRunDifference
                {
                    Code = DualRunDifferenceCodes.SchemaMismatch,
                    Endpoint = "target",
                });
            }
        }
        else
        {
            schemaMatches = string.Equals(
                source.Evidence.SchemaDigest,
                target.Evidence.SchemaDigest,
                StringComparison.Ordinal);
            if (!schemaMatches)
            {
                differences.Add(new DualRunDifference
                {
                    Code = DualRunDifferenceCodes.SchemaMismatch,
                });
            }
        }

        if (source.Evidence.RowCount != target.Evidence.RowCount)
        {
            differences.Add(new DualRunDifference
            {
                Code = DualRunDifferenceCodes.RowCountMismatch,
                SourceCount = source.Evidence.RowCount,
                TargetCount = target.Evidence.RowCount,
            });
        }

        if (!schemaMatches)
            return DualRunValidationStatus.Different;

        if (queryCase.Ordering == DualRunOrdering.Ordered)
            CompareOrdered(queryCase.Limits, source.RowHashes, target.RowHashes, differences);
        else
            CompareUnordered(queryCase.Limits, source.RowHashes, target.RowHashes, differences);

        bool resultMatches = string.Equals(
            source.Evidence.ResultDigest,
            target.Evidence.ResultDigest,
            StringComparison.Ordinal);
        return differences.Count == 0 && resultMatches
            ? DualRunValidationStatus.Passed
            : DualRunValidationStatus.Different;
    }

    private static void CompareOrdered(
        DualRunLimits limits,
        IReadOnlyList<byte[]> source,
        IReadOnlyList<byte[]> target,
        List<DualRunDifference> differences)
    {
        int compared = Math.Min(source.Count, target.Count);
        for (int index = 0; index < compared; index++)
        {
            if (source[index].AsSpan().SequenceEqual(target[index]))
                continue;
            if (CountRowDifferences(differences) >= limits.MaxMismatchDetails)
                return;

            differences.Add(new DualRunDifference
            {
                Code = DualRunDifferenceCodes.OrderedRowMismatch,
                RowOrdinal = index,
                RowDigest = Hex(source[index]),
            });
        }
    }

    private static void CompareUnordered(
        DualRunLimits limits,
        IReadOnlyList<byte[]> source,
        IReadOnlyList<byte[]> target,
        List<DualRunDifference> differences)
    {
        Dictionary<string, long> sourceCounts = CountHashes(source);
        Dictionary<string, long> targetCounts = CountHashes(target);
        string[] allHashes = sourceCounts.Keys
            .Concat(targetCounts.Keys)
            .Distinct(StringComparer.Ordinal)
            .Order(StringComparer.Ordinal)
            .ToArray();

        foreach (string hash in allHashes)
        {
            sourceCounts.TryGetValue(hash, out long sourceCount);
            targetCounts.TryGetValue(hash, out long targetCount);
            if (sourceCount == targetCount)
                continue;
            if (CountRowDifferences(differences) >= limits.MaxMismatchDetails)
                return;

            differences.Add(new DualRunDifference
            {
                Code = DualRunDifferenceCodes.UnorderedRowMultiplicityMismatch,
                RowDigest = hash,
                SourceCount = sourceCount,
                TargetCount = targetCount,
            });
        }
    }

    private static int CountRowDifferences(IReadOnlyList<DualRunDifference> differences) =>
        differences.Count(static difference =>
            difference.Code is DualRunDifferenceCodes.OrderedRowMismatch or
                DualRunDifferenceCodes.UnorderedRowMultiplicityMismatch);

    private static Dictionary<string, long> CountHashes(IReadOnlyList<byte[]> hashes)
    {
        var counts = new Dictionary<string, long>(StringComparer.Ordinal);
        foreach (byte[] hash in hashes)
        {
            string text = Hex(hash);
            counts[text] = counts.TryGetValue(text, out long count) ? checked(count + 1) : 1;
        }
        return counts;
    }

    private static CanonicalColumn[] CanonicalizeColumns(
        IReadOnlyList<DualRunResultColumn> raw,
        IReadOnlyList<DualRunColumnContract> declared)
    {
        var result = new CanonicalColumn[raw.Count];
        bool applyDeclaredTypes = declared.Count == raw.Count;
        for (int index = 0; index < raw.Count; index++)
        {
            DualRunResultColumn column = raw[index] ??
                throw new DualRunExecutionException(
                    DualRunErrorKind.InvalidResult,
                    "DUALRUN_COLUMN_MISSING");
            result[index] = new CanonicalColumn(
                DualRunCanonicalizer.NormalizeIdentifier(column.Name),
                applyDeclaredTypes ? declared[index].Type : column.InferredType);
        }
        return result;
    }

    private static string ComputeSchemaDigest(IReadOnlyList<CanonicalColumn> columns)
    {
        using IncrementalHash hash = IncrementalHash.CreateHash(HashAlgorithmName.SHA256);
        Append(hash, "csharpdb-dual-run-schema/v1");
        Append(hash, columns.Count);
        foreach (CanonicalColumn column in columns)
        {
            Append(hash, column.Name);
            Append(hash, (byte)column.Type);
        }
        return Hex(hash.GetHashAndReset());
    }

    private static string ComputeResultDigest(
        string schemaDigest,
        IReadOnlyList<byte[]> rowHashes,
        DualRunOrdering ordering)
    {
        IEnumerable<byte[]> ordered = ordering == DualRunOrdering.Ordered
            ? rowHashes
            : rowHashes.Order(ByteArrayComparer.Instance);
        using IncrementalHash hash = IncrementalHash.CreateHash(HashAlgorithmName.SHA256);
        Append(hash, "csharpdb-dual-run-result/v1");
        Append(hash, schemaDigest);
        Append(hash, (byte)ordering);
        Append(hash, rowHashes.Count);
        foreach (byte[] rowHash in ordered)
            Append(hash, rowHash);
        return Hex(hash.GetHashAndReset());
    }

    private static string ComputeInvocationDigest(
        DualRunQueryCase queryCase,
        IDualRunQueryExecutor sourceExecutor,
        IDualRunQueryExecutor targetExecutor)
    {
        using IncrementalHash hash = IncrementalHash.CreateHash(HashAlgorithmName.SHA256);
        Append(hash, "csharpdb-dual-run-invocation/v1");
        Append(hash, queryCase.CaseId);
        Append(hash, queryCase.CanonicalizationId);
        Append(hash, queryCase.CanonicalizationContractHash);
        Append(hash, queryCase.SourceSnapshotIdentity);
        Append(hash, queryCase.TargetSnapshotIdentity);
        Append(hash, sourceExecutor.ProviderId);
        Append(hash, sourceExecutor.ReadOnlyValidatorId);
        Append(hash, (byte)sourceExecutor.ReadOnlyEnforcement);
        Append(hash, targetExecutor.ProviderId);
        Append(hash, targetExecutor.ReadOnlyValidatorId);
        Append(hash, (byte)targetExecutor.ReadOnlyEnforcement);
        Append(hash, queryCase.SourceSql);
        Append(hash, queryCase.TargetSql);
        Append(hash, (byte)queryCase.Ordering);
        Append(hash, queryCase.Parameters.Count);
        foreach (DualRunParameter parameter in queryCase.Parameters)
        {
            Append(hash, parameter.Name);
            Append(hash, (byte)parameter.Type);
            CanonicalValue value = DualRunCanonicalizer.Canonicalize(parameter.Value, parameter.Type);
            Append(hash, CanonicalRowCodec.EncodeRow([value]));
        }
        Append(hash, queryCase.Columns.Count);
        foreach (DualRunColumnContract column in queryCase.Columns)
        {
            Append(hash, DualRunCanonicalizer.NormalizeIdentifier(column.Name));
            Append(hash, (byte)column.Type);
        }
        Append(hash, queryCase.Limits.MaxRows);
        Append(hash, queryCase.Limits.MaxColumns);
        Append(hash, queryCase.Limits.MaxCellBytes);
        Append(hash, queryCase.Limits.MaxTotalCanonicalBytesPerEndpoint);
        Append(hash, queryCase.Limits.MaxMismatchDetails);
        Append(
            hash,
            checked((long)queryCase.Limits.TimeoutPerEndpoint.TotalMilliseconds));
        return Hex(hash.GetHashAndReset());
    }

    private static byte[] EncodeProviderRow(IReadOnlyList<CanonicalValue> row)
    {
        try
        {
            return CanonicalRowCodec.EncodeRow(row);
        }
        catch (DualRunExecutionException)
        {
            throw;
        }
        catch (Exception ex) when (
            ex is ArgumentException or ArithmeticException or InvalidDataException)
        {
            throw new DualRunExecutionException(
                DualRunErrorKind.InvalidResult,
                "DUALRUN_VALUE_CANONICALIZATION_FAILED",
                ex);
        }
    }

    private static void ValidateQueryCase(DualRunQueryCase queryCase)
    {
        ValidateStableId(queryCase.CaseId, nameof(queryCase.CaseId));
        ValidateSnapshotIdentity(
            queryCase.SourceSnapshotIdentity,
            nameof(queryCase.SourceSnapshotIdentity));
        ValidateSnapshotIdentity(
            queryCase.TargetSnapshotIdentity,
            nameof(queryCase.TargetSnapshotIdentity));
        ValidateCanonicalization(
            queryCase.CanonicalizationId,
            queryCase.CanonicalizationContractHash);
        ValidateSql(queryCase.SourceSql, nameof(queryCase.SourceSql));
        ValidateSql(queryCase.TargetSql, nameof(queryCase.TargetSql));
        ArgumentNullException.ThrowIfNull(queryCase.Parameters);
        ArgumentNullException.ThrowIfNull(queryCase.Columns);
        ArgumentNullException.ThrowIfNull(queryCase.Limits);
        ValidateLimits(queryCase.Limits);

        if (queryCase.Parameters.Count > MaxParameters)
            throw new ArgumentOutOfRangeException(nameof(queryCase), $"At most {MaxParameters} parameters are allowed.");

        var parameterNames = new HashSet<string>(StringComparer.OrdinalIgnoreCase);
        foreach (DualRunParameter parameter in queryCase.Parameters)
        {
            ArgumentNullException.ThrowIfNull(parameter);
            ValidateParameterName(parameter.Name);
            if (!parameterNames.Add(parameter.Name))
                throw new ArgumentException($"Duplicate dual-run parameter '{parameter.Name}'.", nameof(queryCase));

            CanonicalValue value = DualRunCanonicalizer.Canonicalize(parameter.Value, parameter.Type);
            if (CanonicalRowCodec.EncodeRow([value]).Length > queryCase.Limits.MaxCellBytes)
                throw new ArgumentOutOfRangeException(nameof(queryCase), "A parameter exceeds the cell byte limit.");
        }

        if (queryCase.Columns.Count > queryCase.Limits.MaxColumns)
            throw new ArgumentOutOfRangeException(nameof(queryCase), "The declared schema exceeds the column limit.");

        var columnNames = new HashSet<string>(StringComparer.Ordinal);
        foreach (DualRunColumnContract column in queryCase.Columns)
        {
            ArgumentNullException.ThrowIfNull(column);
            string name = DualRunCanonicalizer.NormalizeIdentifier(column.Name);
            if (!columnNames.Add(name))
                throw new ArgumentException($"Duplicate dual-run result column '{name}'.", nameof(queryCase));
            _ = CanonicalValue.Null(column.Type);
        }
    }

    private static void ValidateLimits(DualRunLimits limits)
    {
        if (limits.MaxRows < 1 || limits.MaxRows > HardMaxRows)
            throw new ArgumentOutOfRangeException(nameof(limits.MaxRows));
        if (limits.MaxColumns < 1 || limits.MaxColumns > HardMaxColumns)
            throw new ArgumentOutOfRangeException(nameof(limits.MaxColumns));
        if (limits.MaxCellBytes < 1 || limits.MaxCellBytes > HardMaxCellBytes)
            throw new ArgumentOutOfRangeException(nameof(limits.MaxCellBytes));
        if (limits.MaxTotalCanonicalBytesPerEndpoint < limits.MaxCellBytes ||
            limits.MaxTotalCanonicalBytesPerEndpoint > HardMaxTotalBytes)
        {
            throw new ArgumentOutOfRangeException(nameof(limits.MaxTotalCanonicalBytesPerEndpoint));
        }
        if (limits.MaxMismatchDetails <= 0 || limits.MaxMismatchDetails > HardMaxMismatchDetails)
            throw new ArgumentOutOfRangeException(nameof(limits.MaxMismatchDetails));
        if (limits.TimeoutPerEndpoint <= TimeSpan.Zero || limits.TimeoutPerEndpoint > HardMaxTimeout)
            throw new ArgumentOutOfRangeException(nameof(limits.TimeoutPerEndpoint));
    }

    private static void ValidateExecutor(IDualRunQueryExecutor executor, string parameterName)
    {
        ValidateStableId(executor.ProviderId, parameterName);
        ValidateSnapshotIdentity(executor.SnapshotIdentity, parameterName);
        ValidateStableId(executor.ReadOnlyValidatorId, parameterName);
        if (!Enum.IsDefined(executor.ReadOnlyEnforcement))
            throw new ArgumentException("The executor must declare a read-only enforcement mode.", parameterName);
    }

    private static void ValidateStableId(string value, string parameterName)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(value, parameterName);
        if (value.Length > 128 ||
            value.Any(static character => char.IsControl(character)))
        {
            throw new ArgumentException("Stable identifiers must be 1-128 visible characters.", parameterName);
        }
    }

    private static void ValidateSnapshotIdentity(string value, string parameterName)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(value, parameterName);
        if (value.Length > 512 ||
            value.Any(static character =>
                char.IsControl(character) || char.IsWhiteSpace(character)))
        {
            throw new ArgumentException(
                "Snapshot identities must be 1-512 non-whitespace visible characters.",
                parameterName);
        }
    }

    private static void ValidateCanonicalization(string id, string contractHash)
    {
        if (!string.Equals(
                id,
                DualRunReportFormats.CanonicalizationId,
                StringComparison.Ordinal) ||
            !string.Equals(
                contractHash,
                DualRunReportFormats.CanonicalizationContractHash,
                StringComparison.Ordinal))
        {
            throw new ArgumentException(
                "The query case canonicalization contract does not match this binary.");
        }
    }

    private static void ValidateParameterName(string value)
    {
        ValidateStableId(value, nameof(value));
        int start = value[0] is '@' or ':' or '$' or '?' ? 1 : 0;
        if (start == value.Length ||
            !(char.IsAsciiLetter(value[start]) || value[start] == '_') ||
            value.AsSpan(start + 1).ContainsAnyExcept(
                "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789_"))
        {
            throw new ArgumentException($"Parameter name '{value}' is not a bounded SQL parameter identifier.");
        }
    }

    private static void ValidateSql(string sql, string parameterName)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(sql, parameterName);
        int byteCount;
        try
        {
            byteCount = DualRunCanonicalizer.Utf8(sql).Length;
        }
        catch (EncoderFallbackException ex)
        {
            throw new ArgumentException("Query text must be valid UTF-8.", parameterName, ex);
        }
        if (byteCount > MaxQueryBytes)
            throw new ArgumentOutOfRangeException(parameterName, $"Query text exceeds {MaxQueryBytes} bytes.");
    }

    private static DualRunReportLimits ToReportLimits(DualRunLimits limits) =>
        new()
        {
            MaxRows = limits.MaxRows,
            MaxColumns = limits.MaxColumns,
            MaxCellBytes = limits.MaxCellBytes,
            MaxTotalCanonicalBytesPerEndpoint = limits.MaxTotalCanonicalBytesPerEndpoint,
            MaxMismatchDetails = limits.MaxMismatchDetails,
            TimeoutPerEndpointMilliseconds = checked((long)limits.TimeoutPerEndpoint.TotalMilliseconds),
        };

    private static DualRunQueryCase Freeze(DualRunQueryCase queryCase) =>
        queryCase with
        {
            Parameters = CloneParameters(queryCase.Parameters),
            Columns = queryCase.Columns
                .Select(static column => column with { })
                .ToArray(),
            Limits = queryCase.Limits with { },
        };

    private static IReadOnlyList<DualRunParameter> CloneParameters(
        IReadOnlyList<DualRunParameter> parameters) =>
        parameters
            .Select(static parameter => parameter with
            {
                Value = parameter.Value switch
                {
                    byte[] bytes => (byte[])bytes.Clone(),
                    Memory<byte> memory => memory.ToArray(),
                    ReadOnlyMemory<byte> memory => memory.ToArray(),
                    _ => parameter.Value,
                },
            })
            .ToArray();

    private static string NormalizeErrorCode(string code)
    {
        if (string.IsNullOrWhiteSpace(code) || code.Length > 128)
            return "DUALRUN_PROVIDER_ERROR";

        var normalized = new StringBuilder(code.Length);
        foreach (char character in code)
        {
            normalized.Append(
                char.IsAsciiLetterOrDigit(character) || character is '_' or '-' or '.'
                    ? character
                    : '_');
        }
        return normalized.ToString();
    }

    private static string NormalizeProviderExceptionCode(Type type) =>
        NormalizeErrorCode(type.FullName ?? type.Name);

    private static string Hex(ReadOnlySpan<byte> bytes) =>
        Convert.ToHexString(bytes).ToLowerInvariant();

    private static void Append(IncrementalHash hash, string value) =>
        Append(hash, DualRunCanonicalizer.Utf8(value));

    private static void Append(IncrementalHash hash, int value)
    {
        Span<byte> bytes = stackalloc byte[sizeof(int)];
        BinaryPrimitives.WriteInt32BigEndian(bytes, value);
        hash.AppendData(bytes);
    }

    private static void Append(IncrementalHash hash, long value)
    {
        Span<byte> bytes = stackalloc byte[sizeof(long)];
        BinaryPrimitives.WriteInt64BigEndian(bytes, value);
        hash.AppendData(bytes);
    }

    private static void Append(IncrementalHash hash, byte value)
    {
        Span<byte> bytes = stackalloc byte[1];
        bytes[0] = value;
        hash.AppendData(bytes);
    }

    private static void Append(IncrementalHash hash, ReadOnlySpan<byte> value)
    {
        Span<byte> length = stackalloc byte[sizeof(int)];
        BinaryPrimitives.WriteInt32BigEndian(length, value.Length);
        hash.AppendData(length);
        hash.AppendData(value);
    }

    private sealed record CanonicalColumn(string Name, CanonicalType Type);

    private sealed record EndpointCapture(
        DualRunEndpointEvidence Evidence,
        IReadOnlyList<CanonicalColumn> Columns,
        IReadOnlyList<byte[]> RowHashes);

    private sealed class ByteArrayComparer : IComparer<byte[]>
    {
        internal static readonly ByteArrayComparer Instance = new();

        public int Compare(byte[]? left, byte[]? right)
        {
            if (ReferenceEquals(left, right))
                return 0;
            if (left is null)
                return -1;
            if (right is null)
                return 1;
            return left.AsSpan().SequenceCompareTo(right);
        }
    }
}
