using System.Text;
using CSharpDB.Migration.Files.Json;

namespace CSharpDB.Migration.Files.Tests;

public sealed class JsonExportCheckpointFramingTests
{
    [Theory]
    [InlineData(
        JsonExportFraming.RootArray,
        JsonExportCheckpointPhase.Writing,
        0,
        1,
        1,
        2)]
    [InlineData(
        JsonExportFraming.RootArray,
        JsonExportCheckpointPhase.DataComplete,
        0,
        3,
        3,
        0)]
    [InlineData(
        JsonExportFraming.Ndjson,
        JsonExportCheckpointPhase.Writing,
        0,
        0,
        0,
        0)]
    [InlineData(
        JsonExportFraming.Ndjson,
        JsonExportCheckpointPhase.DataComplete,
        0,
        0,
        0,
        0)]
    [InlineData(
        JsonExportFraming.RootArray,
        JsonExportCheckpointPhase.Writing,
        2,
        16,
        130,
        2)]
    [InlineData(
        JsonExportFraming.RootArray,
        JsonExportCheckpointPhase.DataComplete,
        2,
        18,
        132,
        0)]
    [InlineData(
        JsonExportFraming.Ndjson,
        JsonExportCheckpointPhase.Writing,
        2,
        16,
        130,
        0)]
    [InlineData(
        JsonExportFraming.Ndjson,
        JsonExportCheckpointPhase.DataComplete,
        2,
        16,
        130,
        0)]
    public void ExactGeometryIsStableAcrossFramingAndPhase(
        JsonExportFraming framing,
        JsonExportCheckpointPhase phase,
        long rowCount,
        long expectedMinimum,
        long expectedMaximum,
        int expectedCompletionTail)
    {
        long? lastRowId =
            rowCount == 0
                ? null
                : long.MinValue;
        JsonExportCheckpointBinding binding =
            CreateBinding(
                framing,
                maximumValueBytes: 64,
                maximumDataBytes: 1_000);
        JsonExportCheckpointProgress progress =
            CreateProgress(
                rowCount,
                lastRowId,
                expectedMinimum);

        JsonExportCheckpointPrefixGeometry geometry =
            JsonExportCheckpointFraming.ValidateGeometry(
                binding,
                phase,
                progress);

        Assert.Equal(
            7,
            geometry.MinimumObjectByteLength);
        Assert.Equal(
            expectedMinimum,
            geometry.MinimumDataPrefixByteLength);
        Assert.Equal(
            expectedMaximum,
            geometry.MaximumDataPrefixByteLength);
        Assert.Equal(
            expectedCompletionTail,
            geometry.CompletionTailByteLength);
    }

    [Theory]
    [InlineData(
        JsonExportFraming.RootArray,
        JsonExportCheckpointPhase.Writing,
        16,
        18)]
    [InlineData(
        JsonExportFraming.RootArray,
        JsonExportCheckpointPhase.DataComplete,
        18,
        20)]
    [InlineData(
        JsonExportFraming.Ndjson,
        JsonExportCheckpointPhase.Writing,
        16,
        20)]
    [InlineData(
        JsonExportFraming.Ndjson,
        JsonExportCheckpointPhase.DataComplete,
        16,
        20)]
    public void DataByteCeilingCapsMaximumGeometry(
        JsonExportFraming framing,
        JsonExportCheckpointPhase phase,
        long minimum,
        long maximum)
    {
        JsonExportCheckpointBinding binding =
            CreateBinding(
                framing,
                maximumValueBytes: 64,
                maximumDataBytes: 20);
        JsonExportCheckpointProgress progress =
            CreateProgress(
                2,
                9,
                maximum);

        JsonExportCheckpointPrefixGeometry geometry =
            JsonExportCheckpointFraming.ValidateGeometry(
                binding,
                phase,
                progress);

        Assert.Equal(
            minimum,
            geometry.MinimumDataPrefixByteLength);
        Assert.Equal(
            maximum,
            geometry.MaximumDataPrefixByteLength);
    }

    [Fact]
    public void RootArrayCloseAlwaysAddsExactlyTwoBytes()
    {
        JsonExportCheckpointBinding binding =
            CreateBinding(
                JsonExportFraming.RootArray);

        foreach (long rowCount in
                 new[] { 0L, 1L, 7L })
        {
            long? lastRowId =
                rowCount == 0
                    ? null
                    : rowCount;
            long writingLength =
                rowCount == 0
                    ? 1L
                    : checked(
                        rowCount * 8L);
            JsonExportCheckpointPrefixGeometry writing =
                JsonExportCheckpointFraming.ValidateGeometry(
                    binding,
                    JsonExportCheckpointPhase.Writing,
                    CreateProgress(
                        rowCount,
                        lastRowId,
                        writingLength));
            JsonExportCheckpointPrefixGeometry complete =
                JsonExportCheckpointFraming.ValidateGeometry(
                    binding,
                    JsonExportCheckpointPhase.DataComplete,
                    CreateProgress(
                        rowCount,
                        lastRowId,
                        writingLength + 2L));

            Assert.Equal(
                2,
                writing.CompletionTailByteLength);
            Assert.Equal(
                writing.MinimumDataPrefixByteLength + 2L,
                complete.MinimumDataPrefixByteLength);
            Assert.Equal(
                writing.MaximumDataPrefixByteLength + 2L,
                complete.MaximumDataPrefixByteLength);
        }
    }

    [Fact]
    public void SchemaMinimumUsesExactEscapedPropertyAndValueBytes()
    {
        JsonExportColumnManifest[] columns =
        [
            CreateColumn(
                0,
                "\"",
                JsonExportDatabaseType.Integer),
            CreateColumn(
                1,
                "\u0001",
                JsonExportDatabaseType.Text),
            CreateColumn(
                2,
                "\U0001f642",
                JsonExportDatabaseType.Blob),
        ];
        JsonExportCheckpointBinding binding =
            CreateBinding(
                JsonExportFraming.Ndjson,
                columns: columns);

        JsonExportCheckpointPrefixGeometry geometry =
            JsonExportCheckpointFraming.ValidateGeometry(
                binding,
                JsonExportCheckpointPhase.Writing,
                CreateProgress(
                    1,
                    1,
                    31));

        Assert.Equal(
            30,
            geometry.MinimumObjectByteLength);
        Assert.Equal(
            31,
            geometry.MinimumDataPrefixByteLength);
    }

    [Fact]
    public void GeometryAcceptsInclusiveMinimumAndMaximumOnly()
    {
        JsonExportCheckpointBinding binding =
            CreateBinding(
                JsonExportFraming.RootArray,
                maximumValueBytes: 64,
                maximumDataBytes: 1_000);

        _ = JsonExportCheckpointFraming.ValidateGeometry(
            binding,
            JsonExportCheckpointPhase.Writing,
            CreateProgress(
                2,
                2,
                16));
        _ = JsonExportCheckpointFraming.ValidateGeometry(
            binding,
            JsonExportCheckpointPhase.Writing,
            CreateProgress(
                2,
                2,
                130));

        Assert.Throws<InvalidDataException>(
            () => JsonExportCheckpointFraming.ValidateGeometry(
                binding,
                JsonExportCheckpointPhase.Writing,
                CreateProgress(
                    2,
                    2,
                    15)));
        Assert.Throws<InvalidDataException>(
            () => JsonExportCheckpointFraming.ValidateGeometry(
                binding,
                JsonExportCheckpointPhase.Writing,
                CreateProgress(
                    2,
                    2,
                    131)));
    }

    [Fact]
    public void InvalidCountsLengthsAndRowIdPresenceAreRejected()
    {
        JsonExportCheckpointBinding binding =
            CreateBinding(
                JsonExportFraming.Ndjson);
        JsonExportCheckpointProgress[] invalid =
        [
            CreateProgress(
                -1,
                null,
                0),
            CreateProgress(
                0,
                null,
                -1),
            CreateProgress(
                0,
                7,
                0),
            CreateProgress(
                1,
                null,
                8),
        ];

        foreach (JsonExportCheckpointProgress progress in
                 invalid)
        {
            Assert.Throws<InvalidDataException>(
                () => JsonExportCheckpointFraming.ValidateGeometry(
                    binding,
                    JsonExportCheckpointPhase.Writing,
                    progress));
        }
    }

    [Fact]
    public void InvalidLimitsFramingAndPhaseAreRejected()
    {
        JsonExportCheckpointProgress emptyNdjson =
            CreateProgress(
                0,
                null,
                0);
        JsonExportCheckpointBinding binding =
            CreateBinding(
                JsonExportFraming.Ndjson);
        JsonExportCheckpointBinding[] invalidBindings =
        [
            CreateBinding(
                JsonExportFraming.Ndjson,
                maximumDataBytes: 0),
            CreateBinding(
                JsonExportFraming.Ndjson,
                maximumDataBytes: -1),
            CreateBinding(
                JsonExportFraming.Ndjson,
                maximumValueBytes: 0),
            CreateBinding(
                JsonExportFraming.Ndjson,
                maximumValueBytes: -1),
            CreateBinding(
                JsonExportFraming.Ndjson,
                maximumValueBytes: 6),
            CreateBinding(
                (JsonExportFraming)999),
        ];

        foreach (JsonExportCheckpointBinding invalid in
                 invalidBindings)
        {
            Assert.Throws<InvalidDataException>(
                () => JsonExportCheckpointFraming.ValidateGeometry(
                    invalid,
                    JsonExportCheckpointPhase.Writing,
                    emptyNdjson));
        }

        Assert.Throws<InvalidDataException>(
            () => JsonExportCheckpointFraming.ValidateGeometry(
                binding,
                (JsonExportCheckpointPhase)999,
                emptyNdjson));
    }

    [Theory]
    [InlineData(
        JsonExportFraming.RootArray,
        JsonExportCheckpointPhase.Writing,
        2)]
    [InlineData(
        JsonExportFraming.RootArray,
        JsonExportCheckpointPhase.DataComplete,
        2)]
    [InlineData(
        JsonExportFraming.Ndjson,
        JsonExportCheckpointPhase.Writing,
        7)]
    [InlineData(
        JsonExportFraming.Ndjson,
        JsonExportCheckpointPhase.DataComplete,
        7)]
    public void InsufficientDataCeilingIsRejected(
        JsonExportFraming framing,
        JsonExportCheckpointPhase phase,
        long maximumDataBytes)
    {
        JsonExportCheckpointBinding binding =
            CreateBinding(
                framing,
                maximumDataBytes:
                    maximumDataBytes);
        long prefixLength =
            framing ==
                JsonExportFraming.RootArray
                ? phase ==
                    JsonExportCheckpointPhase.Writing
                    ? 8
                    : 10
                : 8;

        Assert.Throws<InvalidDataException>(
            () => JsonExportCheckpointFraming.ValidateGeometry(
                binding,
                phase,
                CreateProgress(
                    1,
                    1,
                    prefixLength)));
    }

    [Theory]
    [InlineData(JsonExportFraming.RootArray)]
    [InlineData(JsonExportFraming.Ndjson)]
    public void RowGeometryOverflowIsRejected(
        JsonExportFraming framing)
    {
        JsonExportCheckpointBinding binding =
            CreateBinding(
                framing,
                maximumDataBytes:
                    long.MaxValue);

        Assert.Throws<InvalidDataException>(
            () => JsonExportCheckpointFraming.ValidateGeometry(
                binding,
                JsonExportCheckpointPhase.Writing,
                CreateProgress(
                    long.MaxValue,
                    long.MaxValue,
                    long.MaxValue)));
    }

    [Fact]
    public void InvalidSchemaCannotProduceGeometry()
    {
        JsonExportColumnManifest valid =
            CreateColumn(
                0,
                "i",
                JsonExportDatabaseType.Integer);
        JsonExportColumnManifest[][] invalid =
        [
            [],
            [valid with { PropertyName = "\ud800" }],
            [valid with { PropertyName = "\udc00" }],
            [
                valid with
                {
                    DatabaseType =
                        (JsonExportDatabaseType)999,
                },
            ],
        ];

        foreach (JsonExportColumnManifest[] columns in
                 invalid)
        {
            JsonExportCheckpointBinding binding =
                CreateBinding(
                    JsonExportFraming.Ndjson,
                    columns: columns);
            Assert.Throws<InvalidDataException>(
                () => JsonExportCheckpointFraming.ValidateGeometry(
                    binding,
                    JsonExportCheckpointPhase.Writing,
                    CreateProgress(
                        0,
                        null,
                        0)));
        }
    }

    [Theory]
    [InlineData(
        JsonExportFraming.RootArray,
        JsonExportCheckpointPhase.Writing,
        0,
        "[",
        (byte)'[')]
    [InlineData(
        JsonExportFraming.RootArray,
        JsonExportCheckpointPhase.DataComplete,
        0,
        "[]\n",
        (byte)'[')]
    [InlineData(
        JsonExportFraming.Ndjson,
        JsonExportCheckpointPhase.Writing,
        0,
        "",
        null)]
    [InlineData(
        JsonExportFraming.Ndjson,
        JsonExportCheckpointPhase.DataComplete,
        0,
        "",
        null)]
    [InlineData(
        JsonExportFraming.RootArray,
        JsonExportCheckpointPhase.Writing,
        1,
        "}",
        (byte)'[')]
    [InlineData(
        JsonExportFraming.RootArray,
        JsonExportCheckpointPhase.DataComplete,
        1,
        "}]\n",
        (byte)'[')]
    [InlineData(
        JsonExportFraming.Ndjson,
        JsonExportCheckpointPhase.Writing,
        1,
        "}\n",
        (byte)'{')]
    [InlineData(
        JsonExportFraming.Ndjson,
        JsonExportCheckpointPhase.DataComplete,
        1,
        "}\n",
        (byte)'{')]
    public void ExactObservedBoundariesAreAccepted(
        JsonExportFraming framing,
        JsonExportCheckpointPhase phase,
        long rowCount,
        string trailingBoundary,
        byte? firstByte)
    {
        long prefixLength =
            (framing, phase, rowCount) switch
            {
                (
                    JsonExportFraming.RootArray,
                    JsonExportCheckpointPhase.Writing,
                    0) => 1,
                (
                    JsonExportFraming.RootArray,
                    JsonExportCheckpointPhase.DataComplete,
                    0) => 3,
                (
                    JsonExportFraming.RootArray,
                    JsonExportCheckpointPhase.Writing,
                    _) => 8,
                (
                    JsonExportFraming.RootArray,
                    JsonExportCheckpointPhase.DataComplete,
                    _) => 10,
                _ when rowCount == 0 => 0,
                _ => 8,
            };

        _ = JsonExportCheckpointFraming.ValidateObservedBoundary(
            CreateBinding(framing),
            phase,
            CreateProgress(
                rowCount,
                rowCount == 0
                    ? null
                    : 1,
                prefixLength),
            firstByte,
            Encoding.UTF8.GetBytes(
                trailingBoundary));
    }

    [Fact]
    public void PartialOrClosedWritingBoundariesAreRejected()
    {
        JsonExportCheckpointBinding root =
            CreateBinding(
                JsonExportFraming.RootArray);
        JsonExportCheckpointProgress rootProgress =
            CreateProgress(
                1,
                1,
                8);
        byte[][] invalidRootTails =
        [
            "{"u8.ToArray(),
            ","u8.ToArray(),
            "}]"u8.ToArray(),
            "}]\n"u8.ToArray(),
        ];

        foreach (byte[] tail in
                 invalidRootTails)
        {
            Assert.Throws<InvalidDataException>(
                () => JsonExportCheckpointFraming.ValidateObservedBoundary(
                    root,
                    JsonExportCheckpointPhase.Writing,
                    rootProgress,
                    (byte)'[',
                    tail));
        }

        Assert.Throws<InvalidDataException>(
            () => JsonExportCheckpointFraming.ValidateObservedBoundary(
                root,
                JsonExportCheckpointPhase.Writing,
                rootProgress,
                (byte)'{',
                "}"u8));

        JsonExportCheckpointBinding ndjson =
            CreateBinding(
                JsonExportFraming.Ndjson);
        JsonExportCheckpointProgress ndjsonProgress =
            CreateProgress(
                1,
                1,
                8);
        Assert.Throws<InvalidDataException>(
            () => JsonExportCheckpointFraming.ValidateObservedBoundary(
                ndjson,
                JsonExportCheckpointPhase.Writing,
                ndjsonProgress,
                (byte)'{',
                "}"u8));
        Assert.Throws<InvalidDataException>(
            () => JsonExportCheckpointFraming.ValidateObservedBoundary(
                ndjson,
                JsonExportCheckpointPhase.Writing,
                ndjsonProgress,
                (byte)'[',
                "}\n"u8));
    }

    [Fact]
    public void EmptyBoundariesMustMatchExactFramingBytes()
    {
        JsonExportCheckpointBinding root =
            CreateBinding(
                JsonExportFraming.RootArray);
        Assert.Throws<InvalidDataException>(
            () => JsonExportCheckpointFraming.ValidateObservedBoundary(
                root,
                JsonExportCheckpointPhase.Writing,
                CreateProgress(
                    0,
                    null,
                    1),
                (byte)'[',
                ReadOnlySpan<byte>.Empty));
        Assert.Throws<InvalidDataException>(
            () => JsonExportCheckpointFraming.ValidateObservedBoundary(
                root,
                JsonExportCheckpointPhase.DataComplete,
                CreateProgress(
                    0,
                    null,
                    3),
                (byte)'[',
                "]\n"u8));

        JsonExportCheckpointBinding ndjson =
            CreateBinding(
                JsonExportFraming.Ndjson);
        Assert.Throws<InvalidDataException>(
            () => JsonExportCheckpointFraming.ValidateObservedBoundary(
                ndjson,
                JsonExportCheckpointPhase.Writing,
                CreateProgress(
                    0,
                    null,
                    0),
                (byte)'{',
                ReadOnlySpan<byte>.Empty));
        Assert.Throws<InvalidDataException>(
            () => JsonExportCheckpointFraming.ValidateObservedBoundary(
                ndjson,
                JsonExportCheckpointPhase.DataComplete,
                CreateProgress(
                    0,
                    null,
                    0),
                null,
                "\n"u8));
    }

    [Fact]
    public void EqualGenerationAllowsOnlyStructuralIdempotence()
    {
        JsonExportCheckpoint current =
            CreateWritingCheckpoint(
                JsonExportFraming.RootArray);
        JsonExportCheckpoint clone =
            DeepClone(current);

        JsonExportCheckpointFraming.ValidateTransition(
            current,
            clone);

        JsonExportCheckpoint changed =
            clone with
            {
                Progress =
                    clone.Progress with
                    {
                        DataPrefixDigest =
                            Hash('9'),
                    },
            };
        Assert.Throws<InvalidDataException>(
            () => JsonExportCheckpointFraming.ValidateTransition(
                current,
                changed));
    }

    [Fact]
    public void WritingGenerationCanAdvanceRowsAndPrefixEvidence()
    {
        JsonExportCheckpoint current =
            CreateWritingCheckpoint(
                JsonExportFraming.RootArray);
        JsonExportCheckpoint next =
            CreateWritingAdvance(current);

        JsonExportCheckpointFraming.ValidateTransition(
            current,
            next);
    }

    [Fact]
    public void WritingAdvanceRequiresEveryMonotonicField()
    {
        JsonExportCheckpoint current =
            CreateWritingCheckpoint(
                JsonExportFraming.RootArray);
        JsonExportCheckpoint valid =
            CreateWritingAdvance(current);
        JsonExportCheckpoint[] invalid =
        [
            valid with
            {
                Progress =
                    valid.Progress with
                    {
                        CompletedRowCount =
                            current.Progress
                                .CompletedRowCount,
                    },
            },
            valid with
            {
                Progress =
                    valid.Progress with
                    {
                        LastCompletedRowId =
                            current.Progress
                                .LastCompletedRowId,
                    },
            },
            valid with
            {
                Progress =
                    valid.Progress with
                    {
                        DataPrefixByteLength =
                            current.Progress
                                .DataPrefixByteLength,
                    },
            },
            valid with
            {
                Progress =
                    valid.Progress with
                    {
                        DataPrefixDigest =
                            current.Progress
                                .DataPrefixDigest,
                    },
            },
            valid with
            {
                Progress =
                    valid.Progress with
                    {
                        SourceLogicalRowHashPrefixDigest =
                            current.Progress
                                .SourceLogicalRowHashPrefixDigest,
                    },
            },
            valid with
            {
                Progress =
                    valid.Progress with
                    {
                        ExportedLogicalRowHashPrefixDigest =
                            current.Progress
                                .ExportedLogicalRowHashPrefixDigest,
                    },
            },
            valid with
            {
                Progress =
                    valid.Progress with
                    {
                        LogicalPrefixAggregation =
                            "changed",
                    },
            },
        ];

        foreach (JsonExportCheckpoint next in
                 invalid)
        {
            Assert.Throws<InvalidDataException>(
                () => JsonExportCheckpointFraming.ValidateTransition(
                    current,
                    next));
        }
    }

    [Theory]
    [InlineData(JsonExportFraming.RootArray)]
    [InlineData(JsonExportFraming.Ndjson)]
    public void CompletionWithoutRowsUsesOnlyFramingTailOrPhase(
        JsonExportFraming framing)
    {
        JsonExportCheckpoint current =
            CreateWritingCheckpoint(framing);
        JsonExportCheckpoint complete =
            CreateCompletion(
                current,
                addRows: false);

        JsonExportCheckpointFraming.ValidateTransition(
            current,
            complete);
    }

    [Theory]
    [InlineData(JsonExportFraming.RootArray)]
    [InlineData(JsonExportFraming.Ndjson)]
    public void CompletionCanIncludeTerminalRows(
        JsonExportFraming framing)
    {
        JsonExportCheckpoint current =
            CreateWritingCheckpoint(framing);
        JsonExportCheckpoint complete =
            CreateCompletion(
                current,
                addRows: true);

        JsonExportCheckpointFraming.ValidateTransition(
            current,
            complete);
    }

    [Theory]
    [InlineData(JsonExportFraming.RootArray)]
    [InlineData(JsonExportFraming.Ndjson)]
    public void CompletionRejectsInvalidNoRowPhysicalChange(
        JsonExportFraming framing)
    {
        JsonExportCheckpoint current =
            CreateWritingCheckpoint(framing);
        JsonExportCheckpoint valid =
            CreateCompletion(
                current,
                addRows: false);
        JsonExportCheckpoint wrongLength =
            valid with
            {
                Progress =
                    valid.Progress with
                    {
                        DataPrefixByteLength =
                            framing ==
                                JsonExportFraming.RootArray
                                ? current.Progress
                                    .DataPrefixByteLength +
                                    3L
                                : current.Progress
                                    .DataPrefixByteLength +
                                    1L,
                    },
            };
        JsonExportCheckpoint wrongDigest =
            valid with
            {
                Progress =
                    valid.Progress with
                    {
                        DataPrefixDigest =
                            framing ==
                                JsonExportFraming.RootArray
                                ? current.Progress
                                    .DataPrefixDigest
                                : Hash('9'),
                    },
            };

        Assert.Throws<InvalidDataException>(
            () => JsonExportCheckpointFraming.ValidateTransition(
                current,
                wrongLength));
        Assert.Throws<InvalidDataException>(
            () => JsonExportCheckpointFraming.ValidateTransition(
                current,
                wrongDigest));
    }

    [Theory]
    [InlineData(JsonExportFraming.RootArray)]
    [InlineData(JsonExportFraming.Ndjson)]
    public void CompletionWithRowsRequiresPhysicalAndLogicalAdvance(
        JsonExportFraming framing)
    {
        JsonExportCheckpoint current =
            CreateWritingCheckpoint(framing);
        JsonExportCheckpoint valid =
            CreateCompletion(
                current,
                addRows: true);
        JsonExportCheckpoint[] invalid =
        [
            valid with
            {
                Progress =
                    valid.Progress with
                    {
                        LastCompletedRowId =
                            current.Progress
                                .LastCompletedRowId,
                    },
            },
            valid with
            {
                Progress =
                    valid.Progress with
                    {
                        DataPrefixByteLength =
                            framing ==
                                JsonExportFraming.RootArray
                                ? current.Progress
                                    .DataPrefixByteLength +
                                    2L
                                : current.Progress
                                    .DataPrefixByteLength,
                    },
            },
            valid with
            {
                Progress =
                    valid.Progress with
                    {
                        DataPrefixDigest =
                            current.Progress
                                .DataPrefixDigest,
                    },
            },
            valid with
            {
                Progress =
                    valid.Progress with
                    {
                        SourceLogicalRowHashPrefixDigest =
                            current.Progress
                                .SourceLogicalRowHashPrefixDigest,
                    },
            },
            valid with
            {
                Progress =
                    valid.Progress with
                    {
                        ExportedLogicalRowHashPrefixDigest =
                            current.Progress
                                .ExportedLogicalRowHashPrefixDigest,
                    },
            },
        ];

        foreach (JsonExportCheckpoint next in
                 invalid)
        {
            Assert.Throws<InvalidDataException>(
                () => JsonExportCheckpointFraming.ValidateTransition(
                    current,
                    next));
        }
    }

    [Fact]
    public void GenerationBindingAndTerminalRulesAreEnforced()
    {
        JsonExportCheckpoint current =
            CreateWritingCheckpoint(
                JsonExportFraming.RootArray);
        JsonExportCheckpoint valid =
            CreateWritingAdvance(current);
        JsonExportCheckpoint[] invalid =
        [
            valid with
            {
                Generation =
                    current.Generation - 1L,
            },
            valid with
            {
                Generation =
                    current.Generation + 2L,
            },
            valid with
            {
                Binding =
                    valid.Binding with
                    {
                        SourceSnapshotIdentity =
                            "changed",
                    },
            },
            valid with
            {
                BindingDigest = Hash('9'),
            },
        ];

        foreach (JsonExportCheckpoint next in
                 invalid)
        {
            Assert.Throws<InvalidDataException>(
                () => JsonExportCheckpointFraming.ValidateTransition(
                    current,
                    next));
        }

        JsonExportCheckpoint complete =
            CreateCompletion(
                current,
                addRows: false);
        JsonExportCheckpoint later =
            complete with
            {
                Generation =
                    complete.Generation + 1L,
            };
        Assert.Throws<InvalidDataException>(
            () => JsonExportCheckpointFraming.ValidateTransition(
                complete,
                later));

        JsonExportCheckpoint maxGeneration =
            current with
            {
                Generation = long.MaxValue,
            };
        Assert.Throws<InvalidDataException>(
            () => JsonExportCheckpointFraming.ValidateTransition(
                maxGeneration,
                valid with
                {
                    Generation = 0,
                }));
    }

    [Fact]
    public void SignedRowIdsAdvanceByTheirActualOrdering()
    {
        JsonExportCheckpoint current =
            CreateWritingCheckpoint(
                JsonExportFraming.Ndjson) with
            {
                Progress =
                    CreateProgress(
                        1,
                        long.MinValue,
                        8),
            };
        JsonExportCheckpoint next =
            CreateWritingAdvance(current) with
            {
                Progress =
                    CreateWritingAdvance(current)
                        .Progress with
                    {
                        LastCompletedRowId =
                            long.MinValue + 1L,
                    },
            };

        JsonExportCheckpointFraming.ValidateTransition(
            current,
            next);

        Assert.Throws<InvalidDataException>(
            () => JsonExportCheckpointFraming.ValidateTransition(
                current,
                next with
                {
                    Progress =
                        next.Progress with
                        {
                            LastCompletedRowId =
                                long.MinValue,
                        },
                }));
    }

    private static JsonExportCheckpoint
        CreateWritingCheckpoint(
        JsonExportFraming framing)
    {
        long prefixLength = 8;
        return new JsonExportCheckpoint
        {
            Generation = 4,
            Phase =
                JsonExportCheckpointPhase.Writing,
            Binding = CreateBinding(framing),
            BindingDigest = Hash('0'),
            Progress =
                CreateProgress(
                    1,
                    10,
                    prefixLength),
            Completion = null,
        };
    }

    private static JsonExportCheckpoint
        CreateWritingAdvance(
        JsonExportCheckpoint current) =>
        current with
        {
            Generation =
                current.Generation + 1L,
            Progress =
                CreateProgress(
                    2,
                    20,
                    16,
                    dataDigest: 'd',
                    sourceDigest: 'e',
                    exportedDigest: 'f'),
        };

    private static JsonExportCheckpoint CreateCompletion(
        JsonExportCheckpoint current,
        bool addRows)
    {
        JsonExportFraming framing =
            current.Binding.Json.Framing;
        long rowCount =
            addRows
                ? current.Progress.CompletedRowCount +
                    1L
                : current.Progress.CompletedRowCount;
        long? lastRowId =
            addRows
                ? current.Progress.LastCompletedRowId +
                    10L
                : current.Progress.LastCompletedRowId;
        long byteLength =
            framing ==
                JsonExportFraming.RootArray
                ? addRows
                    ? 18L
                    : current.Progress
                        .DataPrefixByteLength +
                        2L
                : addRows
                    ? 16L
                    : current.Progress
                        .DataPrefixByteLength;
        char dataDigest =
            framing ==
                    JsonExportFraming.RootArray ||
                addRows
                ? 'd'
                : 'a';
        char sourceDigest =
            addRows
                ? 'e'
                : 'b';
        char exportedDigest =
            addRows
                ? 'f'
                : 'c';

        return current with
        {
            Generation =
                current.Generation + 1L,
            Phase =
                JsonExportCheckpointPhase.DataComplete,
            Progress =
                CreateProgress(
                    rowCount,
                    lastRowId,
                    byteLength,
                    dataDigest,
                    sourceDigest,
                    exportedDigest),
            Completion =
                new JsonExportCheckpointCompletion
                {
                    SourceLogicalDigest = Hash('7'),
                    ExportedLogicalDigest = Hash('8'),
                    ManifestDigest =
                        new string('9', 64),
                },
        };
    }

    private static JsonExportCheckpoint DeepClone(
        JsonExportCheckpoint checkpoint) =>
        checkpoint with
        {
            Binding =
                checkpoint.Binding with
                {
                    Source =
                        checkpoint.Binding.Source with
                        {
                            SnapshotDigest =
                                checkpoint.Binding
                                    .Source
                                    .SnapshotDigest with
                                {
                                },
                        },
                    Table =
                        checkpoint.Binding.Table with
                        {
                            SchemaDigest =
                                checkpoint.Binding
                                    .Table
                                    .SchemaDigest with
                                {
                                },
                            Columns =
                                checkpoint.Binding
                                    .Table
                                    .Columns
                                    .Select(
                                        static column =>
                                            column with
                                            {
                                            })
                                    .ToArray(),
                        },
                    Json =
                        checkpoint.Binding.Json with
                        {
                        },
                },
            BindingDigest =
                checkpoint.BindingDigest with
                {
                },
            Progress =
                checkpoint.Progress with
                {
                    DataPrefixDigest =
                        checkpoint.Progress
                            .DataPrefixDigest with
                        {
                        },
                    SourceLogicalRowHashPrefixDigest =
                        checkpoint.Progress
                            .SourceLogicalRowHashPrefixDigest with
                        {
                        },
                    ExportedLogicalRowHashPrefixDigest =
                        checkpoint.Progress
                            .ExportedLogicalRowHashPrefixDigest with
                        {
                        },
                },
        };

    private static JsonExportCheckpointBinding CreateBinding(
        JsonExportFraming framing,
        long maximumDataBytes = 1_000,
        int maximumValueBytes = 64,
        IReadOnlyList<JsonExportColumnManifest>? columns = null) =>
        new()
        {
            Profile = JsonExportProfile.LosslessV1,
            Source =
                new JsonExportSourceManifest
                {
                    Kind =
                        JsonExportContracts.SourceKind,
                    Version = "4.3.0",
                    SnapshotByteLength = 128,
                    SnapshotDigest = Hash('1'),
                },
            SourceSnapshotIdentity =
                JsonExportCheckpointContracts
                    .RetainedSnapshotIdentityPrefix +
                new string('2', 64),
            Table =
                new JsonExportTableManifest
                {
                    Name = "items",
                    SchemaContract =
                        JsonExportContracts.Schema,
                    SchemaDigest = Hash('3'),
                    RowOrder =
                        JsonExportContracts.RowOrder,
                    Columns =
                        columns ??
                        [
                            CreateColumn(
                                0,
                                "i",
                                JsonExportDatabaseType
                                    .Integer),
                        ],
                },
            Json =
                new JsonExportFormatManifest
                {
                    Encoding =
                        JsonExportContracts.Encoding,
                    HasByteOrderMark = false,
                    Culture =
                        JsonExportContracts.Culture,
                    Framing = framing,
                    Compact = true,
                    PropertyOrder =
                        JsonExportContracts
                            .PropertyOrder,
                    Newline =
                        JsonExportContracts.Newline,
                    HasFinalNewline = true,
                    NullEncoding =
                        JsonExportContracts
                            .NullEncoding,
                    TextEscape =
                        JsonExportContracts.TextEscape,
                    MaxDataBytes =
                        maximumDataBytes,
                    MaximumDecodedBlobBytes =
                        JsonExportContracts
                            .MaximumSupportedDecodedBlobBytes,
                    MaximumValueBytes =
                        maximumValueBytes,
                    MaximumStringBytes =
                        JsonInputContracts
                            .MaximumStringBytes,
                    MaximumPropertyNameBytes =
                        JsonInputContracts
                            .MaximumPropertyNameBytes,
                    MaximumPropertiesPerObject =
                        JsonInputContracts
                            .MaximumPropertiesPerObject,
                },
        };

    private static JsonExportCheckpointProgress CreateProgress(
        long rowCount,
        long? lastRowId,
        long byteLength,
        char dataDigest = 'a',
        char sourceDigest = 'b',
        char exportedDigest = 'c') =>
        new()
        {
            CompletedRowCount = rowCount,
            LastCompletedRowId = lastRowId,
            DataPrefixByteLength = byteLength,
            DataPrefixDigest =
                Hash(dataDigest),
            LogicalPrefixAggregation =
                JsonExportCheckpointContracts
                    .LogicalPrefixAggregation,
            SourceLogicalRowHashPrefixDigest =
                Hash(sourceDigest),
            ExportedLogicalRowHashPrefixDigest =
                Hash(exportedDigest),
        };

    private static JsonExportColumnManifest CreateColumn(
        int ordinal,
        string propertyName,
        JsonExportDatabaseType databaseType) =>
        new()
        {
            Ordinal = ordinal,
            SourceName = propertyName,
            PropertyName = propertyName,
            DatabaseType = databaseType,
            Nullable = true,
            ValueEncoding =
                databaseType switch
                {
                    JsonExportDatabaseType.Integer =>
                        JsonExportContracts
                            .IntegerValueEncoding,
                    JsonExportDatabaseType.Real =>
                        JsonExportContracts
                            .RealValueEncoding,
                    JsonExportDatabaseType.Text =>
                        JsonExportContracts
                            .TextValueEncoding,
                    JsonExportDatabaseType.Blob =>
                        JsonExportContracts
                            .BlobValueEncoding,
                    _ => "invalid",
                },
            MaximumDecodedBytes =
                databaseType ==
                    JsonExportDatabaseType.Blob
                    ? JsonExportContracts
                        .MaximumSupportedDecodedBlobBytes
                    : 0,
        };

    private static JsonExportHashManifest Hash(
        char character) =>
        new()
        {
            Algorithm =
                JsonExportHashManifest
                    .Sha256Algorithm,
            Value =
                new string(
                    character,
                    64),
        };
}
