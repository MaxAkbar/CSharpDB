using System.Buffers.Binary;
using System.Collections;
using System.Collections.Concurrent;
using System.Reflection;
using System.Security.Cryptography;
using System.Text;
using CSharpDB.Migration.CSharpDb;
using Microsoft.SqlServer.TransactSql.ScriptDom;

namespace CSharpDB.Migration.SqlServer;

/// <summary>
/// Reducible production bounds for one standalone T-SQL DDL proof. Callers may
/// lower but cannot raise the qualified ceilings.
/// </summary>
public sealed record SqlServerTsqlDdlCompatibilityOptions
{
    public const int HardMaxScriptCharacters = 4 * 1024 * 1024;
    public const int HardMaxScriptUtf8Bytes = 16 * 1024 * 1024;
    public const int HardMaxStatementCount = 4096;
    public const int HardMaxStatementCharacters = 1024 * 1024;
    public const int HardMaxTokenCount = 250_000;
    public const int HardMaxNestingDepth = 128;
    public const int HardMaxAstNodeCount = 250_000;
    public const int HardMaxParseErrorCount = 64;
    public const int HardMaxCatalogObjectCount = 100_000;

    public static SqlServerTsqlDdlCompatibilityOptions Default { get; } =
        new();

    public int MaxScriptCharacters { get; init; } =
        HardMaxScriptCharacters;
    public int MaxScriptUtf8Bytes { get; init; } =
        HardMaxScriptUtf8Bytes;
    public int MaxStatementCount { get; init; } =
        HardMaxStatementCount;
    public int MaxStatementCharacters { get; init; } =
        HardMaxStatementCharacters;
    public int MaxTokenCount { get; init; } = HardMaxTokenCount;
    public int MaxNestingDepth { get; init; } = HardMaxNestingDepth;
    public int MaxAstNodeCount { get; init; } = HardMaxAstNodeCount;
    public int MaxParseErrorCount { get; init; } =
        HardMaxParseErrorCount;
    public int MaxCatalogObjectCount { get; init; } =
        HardMaxCatalogObjectCount;

    public CSharpDbDdlCompatibilityOptions TargetProofOptions { get; init; } =
        CSharpDbDdlCompatibilityOptions.Default;

    internal void Validate()
    {
        Bound(
            MaxScriptCharacters,
            HardMaxScriptCharacters,
            nameof(MaxScriptCharacters));
        Bound(
            MaxScriptUtf8Bytes,
            HardMaxScriptUtf8Bytes,
            nameof(MaxScriptUtf8Bytes));
        Bound(
            MaxStatementCount,
            HardMaxStatementCount,
            nameof(MaxStatementCount));
        Bound(
            MaxStatementCharacters,
            HardMaxStatementCharacters,
            nameof(MaxStatementCharacters));
        Bound(MaxTokenCount, HardMaxTokenCount, nameof(MaxTokenCount));
        Bound(
            MaxNestingDepth,
            HardMaxNestingDepth,
            nameof(MaxNestingDepth));
        Bound(
            MaxAstNodeCount,
            HardMaxAstNodeCount,
            nameof(MaxAstNodeCount));
        Bound(
            MaxParseErrorCount,
            HardMaxParseErrorCount,
            nameof(MaxParseErrorCount));
        Bound(
            MaxCatalogObjectCount,
            HardMaxCatalogObjectCount,
            nameof(MaxCatalogObjectCount));
        ArgumentNullException.ThrowIfNull(TargetProofOptions);
    }

    private static void Bound(int value, int hardMaximum, string name)
    {
        if (value <= 0 || value > hardMaximum)
        {
            throw new ArgumentOutOfRangeException(
                name,
                $"The limit must be between 1 and {hardMaximum}.");
        }
    }
}

/// <summary>
/// Proves a fixed T-SQL 160, QUOTED_IDENTIFIER ON, standalone additive DDL
/// subset against the current CSharpDB target capability catalog.
/// </summary>
public static class SqlServerTsqlDdlCompatibilityAnalyzer
{
    public const string SourceGrammar = "tsql160";
    public const string InputDigestDomain = "tsql-ddl-input/v1";

    public const string ParseRuleId = "tsql.ddl.script.parse";
    public const string LimitRuleId = "tsql.ddl.script.limit";
    public const string EmptyRuleId = "tsql.ddl.script.empty";
    public const string InternalRuleId = "tsql.ddl.proof.unavailable";
    public const string UnsupportedStatementRuleId =
        "tsql.ddl.statement.unsupported";
    public const string UnsupportedFeatureRuleId =
        "tsql.ddl.feature.unsupported";
    public const string DuplicateObjectRuleId =
        "tsql.ddl.object.duplicate";
    public const string InvalidReferenceRuleId =
        "tsql.ddl.reference.invalid";
    public const string TextCollationRuleId =
        "tsql.ddl.collation.unresolved";

    private static readonly UTF8Encoding StrictUtf8 =
        new(false, true);

    public static async ValueTask<CSharpDbDdlCompatibilityReport>
        AnalyzeAsync(
            string script,
            SqlServerTsqlDdlCompatibilityOptions? options = null,
            CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(script);
        options ??= SqlServerTsqlDdlCompatibilityOptions.Default;
        options.Validate();
        cancellationToken.ThrowIfCancellationRequested();

        CSharpDbCapabilityCatalog capabilities =
            CSharpDbCapabilityCatalogLoader.LoadEmbedded();
        if (script.Length > options.MaxScriptCharacters)
        {
            return LimitFailure(
                capabilities,
                LimitDigest(script.Length),
                "The T-SQL script exceeded a production character limit.");
        }
        if (!HasValidUtf16(script, cancellationToken))
        {
            return Failure(
                capabilities,
                InvalidUtf16Digest(script, cancellationToken),
                MigrationCompatibilityStatus.Unknown,
                evidence: null,
                InternalRuleId,
                "The T-SQL input is not valid UTF-16.");
        }

        int utf8Bytes;
        try
        {
            utf8Bytes = StrictUtf8.GetByteCount(script);
        }
        catch (EncoderFallbackException)
        {
            return Failure(
                capabilities,
                InvalidUtf16Digest(script, cancellationToken),
                MigrationCompatibilityStatus.Unknown,
                evidence: null,
                InternalRuleId,
                "The T-SQL input is not valid UTF-16.");
        }
        if (utf8Bytes > options.MaxScriptUtf8Bytes)
        {
            return LimitFailure(
                capabilities,
                LimitDigest(utf8Bytes),
                "The T-SQL script exceeded a production UTF-8 byte limit.");
        }

        string scriptDigest = Digest(script, utf8Bytes);
        if (ExceedsPreflightLexicalUnitLimit(
                script,
                options.MaxTokenCount,
                cancellationToken))
        {
            return LimitFailure(
                capabilities,
                scriptDigest,
                "The T-SQL script exceeded the preflight lexical-unit limit.");
        }

        var parser = new TSql160Parser(
            initialQuotedIdentifiers: true,
            SqlEngineType.Standalone);
        IList<TSqlParserToken> tokens;
        IList<ParseError>? lexerErrors;
        int parenthesisDepth;
        try
        {
            using var reader = new CancellationTextReader(
                script,
                cancellationToken);
            tokens = parser.GetTokenStream(reader, out lexerErrors);
            parenthesisDepth = ParenthesisDepth(
                tokens,
                cancellationToken);
        }
        catch (OperationCanceledException)
            when (cancellationToken.IsCancellationRequested)
        {
            throw;
        }
        catch (Exception error) when (
            CSharpDbDdlCompatibilityAnalyzer.IsRecoverable(error))
        {
            return Failure(
                capabilities,
                scriptDigest,
                MigrationCompatibilityStatus.Unknown,
                evidence: null,
                InternalRuleId,
                "The bounded T-SQL token analysis could not be completed.");
        }

        if (tokens.Count > options.MaxTokenCount ||
            parenthesisDepth > options.MaxNestingDepth)
        {
            return LimitFailure(
                capabilities,
                scriptDigest,
                "The T-SQL script exceeded a production token or nesting limit.");
        }
        lexerErrors ??= [];
        if (lexerErrors.Count > options.MaxParseErrorCount)
        {
            return LimitFailure(
                capabilities,
                scriptDigest,
                "The T-SQL lexer exceeded the production error limit.");
        }
        if (lexerErrors.Count > 0)
        {
            return ParseFailure(
                capabilities,
                scriptDigest,
                lexerErrors[0]);
        }

        TSqlFragment fragment;
        IList<ParseError>? parseErrors;
        try
        {
            cancellationToken.ThrowIfCancellationRequested();
            fragment = parser.Parse(tokens, out parseErrors);
        }
        catch (OperationCanceledException)
            when (cancellationToken.IsCancellationRequested)
        {
            throw;
        }
        catch (Exception error) when (
            CSharpDbDdlCompatibilityAnalyzer.IsRecoverable(error))
        {
            return Failure(
                capabilities,
                scriptDigest,
                MigrationCompatibilityStatus.Unknown,
                evidence: null,
                InternalRuleId,
                "The bounded T-SQL parser could not produce safe evidence.");
        }

        parseErrors ??= [];
        if (parseErrors.Count > options.MaxParseErrorCount)
        {
            return LimitFailure(
                capabilities,
                scriptDigest,
                "The T-SQL parser exceeded the production error limit.");
        }
        if (parseErrors.Count > 0)
        {
            return ParseFailure(
                capabilities,
                scriptDigest,
                parseErrors[0]);
        }
        if (fragment is not TSqlScript parsed)
        {
            return Failure(
                capabilities,
                scriptDigest,
                MigrationCompatibilityStatus.Unknown,
                evidence: null,
                InternalRuleId,
                "The T-SQL parser returned an unexpected root.");
        }

        FragmentCounts counts;
        try
        {
            counts = CountFragments(
                parsed,
                options.MaxAstNodeCount,
                options.MaxNestingDepth,
                cancellationToken);
        }
        catch (OperationCanceledException)
            when (cancellationToken.IsCancellationRequested)
        {
            throw;
        }
        catch (Exception error) when (
            CSharpDbDdlCompatibilityAnalyzer.IsRecoverable(error))
        {
            return Failure(
                capabilities,
                scriptDigest,
                MigrationCompatibilityStatus.Unknown,
                evidence: null,
                InternalRuleId,
                "The bounded T-SQL syntax analysis could not be completed.");
        }
        if (counts.NodeCount > options.MaxAstNodeCount ||
            counts.NestingDepth > options.MaxNestingDepth)
        {
            return LimitFailure(
                capabilities,
                scriptDigest,
                "The T-SQL AST exceeded a production node or nesting limit.");
        }

        TSqlStatement[] sourceStatements = parsed.Batches
            .SelectMany(static batch => batch.Statements)
            .ToArray();
        if (sourceStatements.Length > options.MaxStatementCount ||
            sourceStatements.Any(statement =>
                statement.FragmentLength >
                options.MaxStatementCharacters))
        {
            return LimitFailure(
                capabilities,
                scriptDigest,
                "The T-SQL script exceeded a production statement limit.");
        }
        if (sourceStatements.Length == 0)
        {
            return Failure(
                capabilities,
                scriptDigest,
                MigrationCompatibilityStatus.Unsupported,
                MigrationEvidenceLevel.Parsed,
                EmptyRuleId,
                "The T-SQL script contains no schema statements.");
        }
        if (sourceStatements.Any(statement =>
                !ValidSpan(statement, script.Length)))
        {
            return Failure(
                capabilities,
                scriptDigest,
                MigrationCompatibilityStatus.Unknown,
                MigrationEvidenceLevel.Parsed,
                InternalRuleId,
                "The T-SQL parser returned an invalid source span.");
        }

        TsqlDdlLoweringResult lowering;
        try
        {
            lowering = TsqlDdlLowerer.Lower(
                sourceStatements,
                scriptDigest,
                capabilities,
                options.MaxCatalogObjectCount,
                cancellationToken);
        }
        catch (OperationCanceledException)
            when (cancellationToken.IsCancellationRequested)
        {
            throw;
        }
        catch (Exception error) when (
            CSharpDbDdlCompatibilityAnalyzer.IsRecoverable(error))
        {
            CSharpDbDdlCompatibilityStatement[] unproven =
                sourceStatements.Select((statement, index) =>
                    Statement(
                        statement,
                        index,
                        "unproven",
                        MigrationCompatibilityStatus.Unknown,
                        InternalRuleId)).ToArray();
            return CSharpDbDdlCompatibilityAnalyzer.Failure(
                capabilities,
                scriptDigest,
                MigrationCompatibilityStatus.Unknown,
                MigrationEvidenceLevel.Parsed,
                InternalRuleId,
                sourceStatements.Length,
                unproven,
                [
                    Diagnostic(
                        0,
                        InternalRuleId,
                        MigrationCompatibilityStatus.Unknown,
                        MigrationEvidenceLevel.Parsed,
                        statementIndex: null,
                        span: null,
                        "The parsed T-SQL could not be lowered safely."),
                ],
                dialect: "tsql",
                sourceGrammar: SourceGrammar);
        }

        if (lowering.Catalog is null)
        {
            return CSharpDbDdlCompatibilityAnalyzer.Failure(
                capabilities,
                scriptDigest,
                lowering.LimitExceeded
                    ? MigrationCompatibilityStatus.Unknown
                    : MigrationCompatibilityStatus.Unsupported,
                MigrationEvidenceLevel.Parsed,
                lowering.RuleId,
                sourceStatements.Length,
                lowering.Statements,
                lowering.Diagnostics,
                dialect: "tsql",
                sourceGrammar: SourceGrammar);
        }

        return await CSharpDbDdlCompatibilityAnalyzer.ProveLoweredAsync(
            capabilities,
            lowering.Catalog,
            dialect: "tsql",
            sourceGrammar: SourceGrammar,
            scriptDigest,
            sourceStatements.Length,
            requiresRewrite: true,
            retainedStatusAfterScratch: lowering.HasUnresolvedTextCollation
                ? MigrationCompatibilityStatus.Conditional
                : null,
            retainedRuleAfterScratch: lowering.HasUnresolvedTextCollation
                ? TextCollationRuleId
                : null,
            lowering.Statements,
            lowering.Diagnostics,
            options.TargetProofOptions,
            cancellationToken).ConfigureAwait(false);
    }

    internal static CSharpDbDdlCompatibilityStatement Statement(
        TSqlStatement statement,
        int index,
        string kind,
        MigrationCompatibilityStatus status =
            MigrationCompatibilityStatus.Conditional,
        string ruleId = CSharpDbDdlCompatibilityAnalyzer.CapabilityRuleId) =>
        new()
        {
            Index = index,
            Kind = kind,
            Span = Span(statement),
            Status = status,
            Evidence = MigrationEvidenceLevel.Parsed,
            RuleId = ruleId,
        };

    internal static CSharpDbDdlCompatibilityDiagnostic Diagnostic(
        int ordinal,
        string ruleId,
        MigrationCompatibilityStatus status,
        MigrationEvidenceLevel? evidence,
        int? statementIndex,
        MigrationSourceSpan? span,
        string summary) =>
        new()
        {
            Ordinal = ordinal,
            DiagnosticId = string.Concat(
                "tsql-ddl/",
                ordinal.ToString(
                    "D6",
                    System.Globalization.CultureInfo.InvariantCulture),
                "/",
                ruleId),
            RuleId = ruleId,
            Severity = status == MigrationCompatibilityStatus.Conditional
                ? MigrationDiagnosticSeverity.Warning
                : MigrationDiagnosticSeverity.Error,
            Status = status,
            Evidence = evidence,
            StatementIndex = statementIndex,
            SourceSpan = span,
            Summary = summary,
        };

    internal static MigrationSourceSpan Span(TSqlFragment fragment) =>
        new()
        {
            SourceId = "input",
            Start = fragment.StartOffset,
            Length = fragment.FragmentLength,
            Line = fragment.StartLine,
            Column = fragment.StartColumn,
        };

    private static CSharpDbDdlCompatibilityReport LimitFailure(
        CSharpDbCapabilityCatalog capabilities,
        string digest,
        string summary) =>
        Failure(
            capabilities,
            digest,
            MigrationCompatibilityStatus.Unknown,
            evidence: null,
            LimitRuleId,
            summary);

    private static CSharpDbDdlCompatibilityReport ParseFailure(
        CSharpDbCapabilityCatalog capabilities,
        string digest,
        ParseError error) =>
        CSharpDbDdlCompatibilityAnalyzer.Failure(
            capabilities,
            digest,
            MigrationCompatibilityStatus.Unsupported,
            highestEvidence: null,
            ParseRuleId,
            statementCount: 0,
            statements: [],
            diagnostics:
            [
                Diagnostic(
                    0,
                    ParseRuleId,
                    MigrationCompatibilityStatus.Unsupported,
                    evidence: null,
                    statementIndex: null,
                    new MigrationSourceSpan
                    {
                        SourceId = "input",
                        Start = error.Offset,
                        Line = error.Line,
                        Column = error.Column,
                    },
                    "The T-SQL script could not be parsed completely."),
            ],
            dialect: "tsql",
            sourceGrammar: SourceGrammar);

    private static CSharpDbDdlCompatibilityReport Failure(
        CSharpDbCapabilityCatalog capabilities,
        string digest,
        MigrationCompatibilityStatus status,
        MigrationEvidenceLevel? evidence,
        string ruleId,
        string summary) =>
        CSharpDbDdlCompatibilityAnalyzer.Failure(
            capabilities,
            digest,
            status,
            evidence,
            ruleId,
            statementCount: 0,
            statements: [],
            diagnostics:
            [
                Diagnostic(
                    0,
                    ruleId,
                    status,
                    evidence,
                    statementIndex: null,
                    span: null,
                    summary),
            ],
            dialect: "tsql",
            sourceGrammar: SourceGrammar);

    private static bool HasValidUtf16(
        string value,
        CancellationToken cancellationToken)
    {
        for (int index = 0; index < value.Length; index++)
        {
            if ((index & 2047) == 0)
                cancellationToken.ThrowIfCancellationRequested();
            char character = value[index];
            if (!char.IsSurrogate(character))
                continue;
            if (!char.IsHighSurrogate(character) ||
                index + 1 >= value.Length ||
                !char.IsLowSurrogate(value[++index]))
            {
                return false;
            }
        }
        return true;
    }

    private static string Digest(string script, int byteCount)
    {
        byte[] source = new byte[byteCount];
        byte[] domain = Encoding.UTF8.GetBytes(InputDigestDomain);
        try
        {
            StrictUtf8.GetBytes(script, source);
            using IncrementalHash hash =
                IncrementalHash.CreateHash(HashAlgorithmName.SHA256);
            hash.AppendData(domain);
            hash.AppendData([0]);
            hash.AppendData(source);
            return Convert.ToHexString(hash.GetHashAndReset())
                .ToLowerInvariant();
        }
        finally
        {
            CryptographicOperations.ZeroMemory(source);
            CryptographicOperations.ZeroMemory(domain);
        }
    }

    private static string LimitDigest(int count) =>
        CSharpDbDdlCompatibilityAnalyzer.Digest(
            "tsql-ddl-limit/v1",
            count.ToString(
                System.Globalization.CultureInfo.InvariantCulture),
            CancellationToken.None);

    private static string InvalidUtf16Digest(
        string value,
        CancellationToken cancellationToken)
    {
        using IncrementalHash hash =
            IncrementalHash.CreateHash(HashAlgorithmName.SHA256);
        byte[] domain = Encoding.UTF8.GetBytes(
            "tsql-ddl-invalid-utf16/v1");
        try
        {
            hash.AppendData(domain);
            hash.AppendData([0]);
            Span<byte> buffer = stackalloc byte[4096];
            const int charactersPerChunk = 2048;
            for (int start = 0;
                 start < value.Length;
                 start += charactersPerChunk)
            {
                cancellationToken.ThrowIfCancellationRequested();
                int count = Math.Min(
                    charactersPerChunk,
                    value.Length - start);
                for (int index = 0; index < count; index++)
                {
                    BinaryPrimitives.WriteUInt16LittleEndian(
                        buffer.Slice(index * 2, 2),
                        value[start + index]);
                }

                hash.AppendData(buffer[..(count * 2)]);
                CryptographicOperations.ZeroMemory(
                    buffer[..(count * 2)]);
            }

            return Convert.ToHexString(hash.GetHashAndReset())
                .ToLowerInvariant();
        }
        finally
        {
            CryptographicOperations.ZeroMemory(domain);
        }
    }

    private static bool ExceedsPreflightLexicalUnitLimit(
        string script,
        int maximumUnits,
        CancellationToken cancellationToken)
    {
        // ScriptDom always materializes an end-of-file token. This scan counts
        // only units that are certainly distinct tokens and deliberately
        // under-counts ambiguous malformed or numeric input.
        int units = 1;
        int index = 0;
        while (index < script.Length)
        {
            if ((index & 2047) == 0)
                cancellationToken.ThrowIfCancellationRequested();

            char character = script[index];
            if (char.IsWhiteSpace(character))
            {
                if (character is '\r' or '\n')
                {
                    index++;
                    if (character == '\r' &&
                        index < script.Length &&
                        script[index] == '\n')
                    {
                        index++;
                    }
                }
                else
                {
                    index++;
                    while (index < script.Length &&
                           char.IsWhiteSpace(script[index]) &&
                           script[index] is not ('\r' or '\n'))
                    {
                        if ((index & 2047) == 0)
                        {
                            cancellationToken
                                .ThrowIfCancellationRequested();
                        }
                        index++;
                    }
                }
                if (++units > maximumUnits)
                    return true;
                continue;
            }

            if (character == '-' &&
                index + 1 < script.Length &&
                script[index + 1] == '-')
            {
                index += 2;
                while (index < script.Length &&
                       script[index] is not ('\r' or '\n'))
                {
                    if ((index & 2047) == 0)
                        cancellationToken.ThrowIfCancellationRequested();
                    index++;
                }
                if (++units > maximumUnits)
                    return true;
                continue;
            }

            if (character == '/' &&
                index + 1 < script.Length &&
                script[index + 1] == '*')
            {
                index += 2;
                int depth = 1;
                while (index < script.Length && depth > 0)
                {
                    if ((index & 2047) == 0)
                        cancellationToken.ThrowIfCancellationRequested();
                    if (index + 1 < script.Length &&
                        script[index] == '/' &&
                        script[index + 1] == '*')
                    {
                        depth++;
                        index += 2;
                    }
                    else if (index + 1 < script.Length &&
                             script[index] == '*' &&
                             script[index + 1] == '/')
                    {
                        depth--;
                        index += 2;
                    }
                    else
                    {
                        index++;
                    }
                }
                if (++units > maximumUnits)
                    return true;
                continue;
            }

            if ((character is 'N' or 'n') &&
                index + 1 < script.Length &&
                script[index + 1] == '\'')
            {
                index++;
                character = '\'';
            }

            if (character is '\'' or '"' or '[')
            {
                char terminator = character == '[' ? ']' : character;
                index++;
                while (index < script.Length)
                {
                    if ((index & 2047) == 0)
                        cancellationToken.ThrowIfCancellationRequested();
                    if (script[index] != terminator)
                    {
                        index++;
                        continue;
                    }
                    index++;
                    if (index < script.Length &&
                        script[index] == terminator)
                    {
                        index++;
                        continue;
                    }
                    break;
                }
            }
            else if (char.IsDigit(character) ||
                     character == '.' &&
                     index + 1 < script.Length &&
                     char.IsDigit(script[index + 1]))
            {
                ConsumeNumericUnit(script, ref index);
            }
            else if (character == '$' &&
                     index + 1 < script.Length &&
                     char.IsDigit(script[index + 1]))
            {
                index++;
                ConsumeNumericUnit(script, ref index);
            }
            else if (IsBroadWordCharacter(character) ||
                     IsSurrogatePairAt(script, index))
            {
                do
                {
                    if ((index & 2047) == 0)
                        cancellationToken.ThrowIfCancellationRequested();
                    index += IsSurrogatePairAt(script, index)
                        ? 2
                        : 1;
                }
                while (index < script.Length &&
                       (IsBroadWordCharacter(script[index]) ||
                        IsSurrogatePairAt(script, index)));
            }
            else
            {
                bool pair =
                    index + 1 < script.Length &&
                    IsTwoCharacterLexicalUnit(
                        character,
                        script[index + 1]);
                index += pair ? 2 : 1;
            }

            if (++units > maximumUnits)
                return true;
        }

        return false;
    }

    private static bool IsBroadWordCharacter(char character) =>
        char.IsLetterOrDigit(character) ||
        character is '_' or '@' or '#' or '$' ||
        char.GetUnicodeCategory(character) is
            System.Globalization.UnicodeCategory.NonSpacingMark or
            System.Globalization.UnicodeCategory.SpacingCombiningMark or
            System.Globalization.UnicodeCategory.EnclosingMark or
            System.Globalization.UnicodeCategory.ConnectorPunctuation or
            System.Globalization.UnicodeCategory.Format;

    private static bool IsSurrogatePairAt(
        string value,
        int index) =>
        index + 1 < value.Length &&
        char.IsHighSurrogate(value[index]) &&
        char.IsLowSurrogate(value[index + 1]);

    private static void ConsumeNumericUnit(
        string script,
        ref int index)
    {
        if (script[index] == '.')
        {
            index++;
        }
        else
        {
            if (script[index] == '0' &&
                index + 1 < script.Length &&
                script[index + 1] is 'x' or 'X')
            {
                index += 2;
                while (index < script.Length &&
                       Uri.IsHexDigit(script[index]))
                {
                    index++;
                }
                return;
            }

            while (index < script.Length &&
                   char.IsDigit(script[index]))
            {
                index++;
            }
            if (index < script.Length && script[index] == '.')
                index++;
        }

        while (index < script.Length &&
               char.IsDigit(script[index]))
        {
            index++;
        }
        if (index >= script.Length ||
            script[index] is not ('e' or 'E'))
        {
            return;
        }

        index++;
        if (index < script.Length &&
            script[index] is '+' or '-')
        {
            index++;
        }
        while (index < script.Length &&
               char.IsDigit(script[index]))
        {
            index++;
        }
    }

    private static bool IsTwoCharacterLexicalUnit(
        char first,
        char second) =>
        first switch
        {
            '+' or '-' or '*' or '/' or '%' or '&' or '^' =>
                second == '=',
            '|' => second is '=' or '|',
            '<' => second == '<',
            '>' => second == '>',
            ':' => second == ':',
            _ => false,
        };

    private static int ParenthesisDepth(
        IEnumerable<TSqlParserToken> tokens,
        CancellationToken cancellationToken)
    {
        int current = 0;
        int maximum = 0;
        foreach (TSqlParserToken token in tokens)
        {
            cancellationToken.ThrowIfCancellationRequested();
            if (token.TokenType == TSqlTokenType.LeftParenthesis)
                maximum = Math.Max(maximum, ++current);
            else if (token.TokenType == TSqlTokenType.RightParenthesis)
                current = Math.Max(0, current - 1);
        }
        return maximum;
    }

    private static bool ValidSpan(TSqlFragment fragment, int length) =>
        fragment.StartOffset >= 0 &&
        fragment.FragmentLength >= 0 &&
        fragment.StartOffset <= length - fragment.FragmentLength &&
        fragment.StartLine > 0 &&
        fragment.StartColumn > 0;

    private static readonly ConcurrentDictionary<Type, PropertyInfo[]>
        ChildProperties = new();

    private static FragmentCounts CountFragments(
        TSqlFragment root,
        int maximumNodes,
        int maximumDepth,
        CancellationToken cancellationToken)
    {
        var seen = new HashSet<TSqlFragment>(
            ReferenceEqualityComparer.Instance);
        var pending = new Stack<(TSqlFragment Fragment, int Depth)>();
        pending.Push((root, 1));
        int nodes = 0;
        int depth = 0;
        while (pending.TryPop(
                   out (TSqlFragment Fragment, int Depth) item))
        {
            cancellationToken.ThrowIfCancellationRequested();
            if (!seen.Add(item.Fragment))
                continue;
            nodes++;
            depth = Math.Max(depth, item.Depth);
            if (nodes > maximumNodes || depth > maximumDepth)
                break;
            foreach (TSqlFragment child in Children(item.Fragment))
                pending.Push((child, checked(item.Depth + 1)));
        }
        return new(nodes, depth);
    }

    private static IEnumerable<TSqlFragment> Children(
        TSqlFragment fragment)
    {
        PropertyInfo[] properties = ChildProperties.GetOrAdd(
            fragment.GetType(),
            static type => type
                .GetProperties(BindingFlags.Public | BindingFlags.Instance)
                .Where(static property =>
                    property.CanRead &&
                    property.GetIndexParameters().Length == 0 &&
                    property.Name !=
                    nameof(TSqlFragment.ScriptTokenStream) &&
                    (typeof(TSqlFragment).IsAssignableFrom(
                         property.PropertyType) ||
                     typeof(IEnumerable).IsAssignableFrom(
                         property.PropertyType)))
                .ToArray());
        foreach (PropertyInfo property in properties)
        {
            object? value = property.GetValue(fragment);
            if (value is TSqlFragment child)
            {
                yield return child;
                continue;
            }
            if (value is not IEnumerable values || value is string)
                continue;
            foreach (object? candidate in values)
            {
                if (candidate is TSqlFragment listed)
                    yield return listed;
            }
        }
    }

    private readonly record struct FragmentCounts(
        int NodeCount,
        int NestingDepth);

    private sealed class CancellationTextReader : TextReader
    {
        private readonly StringReader inner;
        private readonly CancellationToken cancellationToken;

        internal CancellationTextReader(
            string value,
            CancellationToken cancellationToken)
        {
            inner = new StringReader(value);
            this.cancellationToken = cancellationToken;
        }

        public override int Peek()
        {
            cancellationToken.ThrowIfCancellationRequested();
            return inner.Peek();
        }

        public override int Read()
        {
            cancellationToken.ThrowIfCancellationRequested();
            return inner.Read();
        }

        public override int Read(char[] buffer, int index, int count)
        {
            cancellationToken.ThrowIfCancellationRequested();
            return inner.Read(buffer, index, count);
        }

        public override int Read(Span<char> buffer)
        {
            cancellationToken.ThrowIfCancellationRequested();
            return inner.Read(buffer);
        }

        protected override void Dispose(bool disposing)
        {
            if (disposing)
                inner.Dispose();
            base.Dispose(disposing);
        }
    }
}

internal sealed record TsqlDdlLoweringResult(
    MigrationCatalog? Catalog,
    string RuleId,
    bool HasUnresolvedTextCollation,
    IReadOnlyList<CSharpDbDdlCompatibilityStatement> Statements,
    IReadOnlyList<CSharpDbDdlCompatibilityDiagnostic> Diagnostics,
    bool LimitExceeded = false);
