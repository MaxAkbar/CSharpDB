using CSharpDB.Execution;
using CSharpDB.Primitives;
using CSharpDB.Sql;

namespace CSharpDB.Tests;

public sealed class WindowOperatorPhase5Tests
{
    [Fact]
    public async Task LeadingFollowingFrame_DoesNotAccumulateRowsBeforeFrameStart()
    {
        ColumnDefinition[] inputColumns =
        [
            new() { Name = "value", Type = DbType.Text, Nullable = true },
        ];
        var inputSchema = new TableSchema
        {
            TableName = "input",
            Columns = inputColumns,
        };
        var source = new FixedRowsOperator(
            inputColumns,
            [
                [DbValue.FromText("skipped-one")],
                [DbValue.FromText("skipped-two")],
                [DbValue.FromInteger(10)],
                [DbValue.FromInteger(20)],
            ]);
        var expression = new WindowFunctionExpression
        {
            Function = new FunctionCallExpression
            {
                FunctionName = "SUM",
                Arguments =
                [
                    new ColumnRefExpression { ColumnName = "value" },
                ],
            },
            Window = new WindowSpecification
            {
                Frame = new WindowFrame
                {
                    Start = new WindowFrameBound
                    {
                        Kind = WindowFrameBoundKind.Following,
                        Offset = 2,
                    },
                    End = new WindowFrameBound
                    {
                        Kind = WindowFrameBoundKind.Following,
                        Offset = 3,
                    },
                },
            },
        };
        ColumnDefinition[] outputColumns =
        [
            inputColumns[0],
            new() { Name = "future_sum", Type = DbType.Integer, Nullable = true },
        ];
        var window = new WindowOperator(
            source,
            inputSchema,
            [expression],
            outputColumns);

        try
        {
            await window.OpenAsync(TestContext.Current.CancellationToken);

            Assert.True(await window.MoveNextAsync(TestContext.Current.CancellationToken));
            Assert.Equal(30, window.Current[1].AsInteger);
            Assert.True(await window.MoveNextAsync(TestContext.Current.CancellationToken));
            Assert.Equal(20, window.Current[1].AsInteger);
            Assert.True(await window.MoveNextAsync(TestContext.Current.CancellationToken));
            Assert.True(window.Current[1].IsNull);
            Assert.True(await window.MoveNextAsync(TestContext.Current.CancellationToken));
            Assert.True(window.Current[1].IsNull);
            Assert.False(await window.MoveNextAsync(TestContext.Current.CancellationToken));
        }
        finally
        {
            await window.DisposeAsync();
        }
    }

    [Fact]
    public async Task RealCurrentRowFrame_PreservesSmallValuesAfterLargePredecessor()
    {
        ColumnDefinition[] inputColumns =
        [
            new() { Name = "value", Type = DbType.Real, Nullable = false },
        ];
        var source = new FixedRowsOperator(
            inputColumns,
            [
                [DbValue.FromReal(10_000_000_000_000_000d)],
                [DbValue.FromReal(1d)],
                [DbValue.FromReal(1d)],
            ]);
        WindowFrame frame = CreateFrame(
            WindowFrameBoundKind.CurrentRow,
            WindowFrameBoundKind.CurrentRow);
        WindowFunctionExpression[] functions =
        [
            CreateAggregate("SUM", frame),
            CreateAggregate("AVG", frame),
        ];
        var window = CreateWindowOperator(
            source,
            inputColumns,
            functions,
            DbType.Real,
            DbType.Real);

        try
        {
            List<DbValue[]> rows = await ReadAllAsync(window);

            Assert.Equal(10_000_000_000_000_000d, rows[0][1].AsReal);
            Assert.Equal(10_000_000_000_000_000d, rows[0][2].AsReal);
            Assert.Equal(1d, rows[1][1].AsReal);
            Assert.Equal(1d, rows[1][2].AsReal);
            Assert.Equal(1d, rows[2][1].AsReal);
            Assert.Equal(1d, rows[2][2].AsReal);
        }
        finally
        {
            await window.DisposeAsync();
        }
    }

    [Fact]
    public async Task RealTwoRowFrame_PreservesSmallValuesAfterLargeValueLeaves()
    {
        ColumnDefinition[] inputColumns =
        [
            new() { Name = "value", Type = DbType.Real, Nullable = false },
        ];
        var source = new FixedRowsOperator(
            inputColumns,
            [
                [DbValue.FromReal(10_000_000_000_000_000d)],
                [DbValue.FromReal(1d)],
                [DbValue.FromReal(1d)],
            ]);
        WindowFrame frame = CreateFrame(
            WindowFrameBoundKind.Preceding,
            WindowFrameBoundKind.CurrentRow,
            startOffset: 1);
        WindowFunctionExpression[] functions =
        [
            CreateAggregate("SUM", frame),
            CreateAggregate("AVG", frame),
        ];
        var window = CreateWindowOperator(
            source,
            inputColumns,
            functions,
            DbType.Real,
            DbType.Real);

        try
        {
            List<DbValue[]> rows = await ReadAllAsync(window);

            Assert.Equal(10_000_000_000_000_000d, rows[0][1].AsReal);
            Assert.Equal(10_000_000_000_000_000d, rows[0][2].AsReal);
            Assert.Equal(10_000_000_000_000_000d, rows[1][1].AsReal);
            Assert.Equal(5_000_000_000_000_000d, rows[1][2].AsReal);
            Assert.Equal(2d, rows[2][1].AsReal);
            Assert.Equal(1d, rows[2][2].AsReal);
        }
        finally
        {
            await window.DisposeAsync();
        }
    }

    [Fact]
    public async Task SlidingRealFrame_RecoversAfterInfinitiesLeave()
    {
        ColumnDefinition[] inputColumns =
        [
            new() { Name = "value", Type = DbType.Real, Nullable = false },
        ];
        var source = new FixedRowsOperator(
            inputColumns,
            [
                [DbValue.FromReal(double.PositiveInfinity)],
                [DbValue.FromReal(double.NegativeInfinity)],
                [DbValue.FromReal(4d)],
                [DbValue.FromReal(6d)],
            ]);
        WindowFrame frame = CreateFrame(
            WindowFrameBoundKind.Preceding,
            WindowFrameBoundKind.CurrentRow,
            startOffset: 1);
        WindowFunctionExpression[] functions =
        [
            CreateAggregate("SUM", frame),
            CreateAggregate("AVG", frame),
        ];
        var window = CreateWindowOperator(
            source,
            inputColumns,
            functions,
            DbType.Real,
            DbType.Real);

        try
        {
            List<DbValue[]> rows = await ReadAllAsync(window);

            Assert.True(double.IsPositiveInfinity(rows[0][1].AsReal));
            Assert.True(double.IsPositiveInfinity(rows[0][2].AsReal));
            Assert.True(double.IsNaN(rows[1][1].AsReal));
            Assert.True(double.IsNaN(rows[1][2].AsReal));
            Assert.True(double.IsNegativeInfinity(rows[2][1].AsReal));
            Assert.True(double.IsNegativeInfinity(rows[2][2].AsReal));
            Assert.Equal(10d, rows[3][1].AsReal);
            Assert.Equal(5d, rows[3][2].AsReal);
        }
        finally
        {
            await window.DisposeAsync();
        }
    }

    [Fact]
    public async Task SlidingRealFrame_RecoversAfterNaNLeaves()
    {
        ColumnDefinition[] inputColumns =
        [
            new() { Name = "value", Type = DbType.Real, Nullable = false },
        ];
        var source = new FixedRowsOperator(
            inputColumns,
            [
                [DbValue.FromReal(double.NaN)],
                [DbValue.FromReal(4d)],
                [DbValue.FromReal(6d)],
            ]);
        WindowFrame frame = CreateFrame(
            WindowFrameBoundKind.Preceding,
            WindowFrameBoundKind.CurrentRow,
            startOffset: 1);
        WindowFunctionExpression[] functions =
        [
            CreateAggregate("SUM", frame),
            CreateAggregate("AVG", frame),
        ];
        var window = CreateWindowOperator(
            source,
            inputColumns,
            functions,
            DbType.Real,
            DbType.Real);

        try
        {
            List<DbValue[]> rows = await ReadAllAsync(window);

            Assert.True(double.IsNaN(rows[0][1].AsReal));
            Assert.True(double.IsNaN(rows[0][2].AsReal));
            Assert.True(double.IsNaN(rows[1][1].AsReal));
            Assert.True(double.IsNaN(rows[1][2].AsReal));
            Assert.Equal(10d, rows[2][1].AsReal);
            Assert.Equal(5d, rows[2][2].AsReal);
        }
        finally
        {
            await window.DisposeAsync();
        }
    }

    [Fact]
    public async Task IntegerCurrentRowFrame_DoesNotReportTransientOverflow()
    {
        ColumnDefinition[] inputColumns =
        [
            new() { Name = "value", Type = DbType.Integer, Nullable = false },
        ];
        var source = new FixedRowsOperator(
            inputColumns,
            [
                [DbValue.FromInteger(long.MaxValue)],
                [DbValue.FromInteger(1)],
            ]);
        WindowFunctionExpression[] functions =
        [
            CreateAggregate(
                "SUM",
                CreateFrame(
                    WindowFrameBoundKind.CurrentRow,
                    WindowFrameBoundKind.CurrentRow)),
        ];
        var window = CreateWindowOperator(
            source,
            inputColumns,
            functions,
            DbType.Integer);

        try
        {
            List<DbValue[]> rows = await ReadAllAsync(window);

            Assert.Equal(long.MaxValue, rows[0][1].AsInteger);
            Assert.Equal(1, rows[1][1].AsInteger);
        }
        finally
        {
            await window.DisposeAsync();
        }
    }

    [Fact]
    public async Task IntegerOverflowingFrame_FailsWithStableDiagnostic()
    {
        ColumnDefinition[] inputColumns =
        [
            new() { Name = "value", Type = DbType.Integer, Nullable = false },
        ];
        var source = new FixedRowsOperator(
            inputColumns,
            [
                [DbValue.FromInteger(long.MaxValue)],
                [DbValue.FromInteger(1)],
            ]);
        WindowFunctionExpression[] functions =
        [
            CreateAggregate(
                "SUM",
                CreateFrame(
                    WindowFrameBoundKind.UnboundedPreceding,
                    WindowFrameBoundKind.CurrentRow)),
        ];
        var window = CreateWindowOperator(
            source,
            inputColumns,
            functions,
            DbType.Integer);

        try
        {
            CSharpDbException error = await Assert.ThrowsAsync<CSharpDbException>(
                () => window.OpenAsync(TestContext.Current.CancellationToken).AsTask());

            Assert.Equal(ErrorCode.TypeMismatch, error.Code);
            Assert.Contains("overflow", error.Message, StringComparison.OrdinalIgnoreCase);
            Assert.Equal(1, source.DisposeCount);
        }
        finally
        {
            await window.DisposeAsync();
        }

        Assert.Equal(1, source.DisposeCount);
    }

    [Fact]
    public async Task MixedNumericFrame_ReturnsToExactIntegerAfterLastRealLeaves()
    {
        ColumnDefinition[] inputColumns =
        [
            new() { Name = "value", Type = DbType.Real, Nullable = false },
        ];
        var source = new FixedRowsOperator(
            inputColumns,
            [
                [DbValue.FromInteger(10)],
                [DbValue.FromReal(0.5d)],
                [DbValue.FromInteger(20)],
                [DbValue.FromInteger(30)],
            ]);
        WindowFunctionExpression[] functions =
        [
            CreateAggregate(
                "SUM",
                CreateFrame(
                    WindowFrameBoundKind.Preceding,
                    WindowFrameBoundKind.CurrentRow,
                    startOffset: 1)),
        ];
        var window = CreateWindowOperator(
            source,
            inputColumns,
            functions,
            DbType.Real);

        try
        {
            List<DbValue[]> rows = await ReadAllAsync(window);

            Assert.Equal(DbType.Integer, rows[0][1].Type);
            Assert.Equal(10, rows[0][1].AsInteger);
            Assert.Equal(10.5d, rows[1][1].AsReal);
            Assert.Equal(20.5d, rows[2][1].AsReal);
            Assert.Equal(DbType.Integer, rows[3][1].Type);
            Assert.Equal(50, rows[3][1].AsInteger);
        }
        finally
        {
            await window.DisposeAsync();
        }
    }

    [Fact]
    public async Task SlidingMinMax_PreserveCollationEqualTiesUntilTheyLeave()
    {
        ColumnDefinition[] inputColumns =
        [
            new() { Name = "value", Type = DbType.Text, Nullable = false },
        ];
        var source = new FixedRowsOperator(
            inputColumns,
            [
                [DbValue.FromText("z")],
                [DbValue.FromText("alpha")],
                [DbValue.FromText("ALPHA")],
                [DbValue.FromText("beta")],
            ]);
        WindowFrame frame = CreateFrame(
            WindowFrameBoundKind.Preceding,
            WindowFrameBoundKind.CurrentRow,
            startOffset: 1);
        var collatedValue = new CollateExpression
        {
            Operand = new ColumnRefExpression { ColumnName = "value" },
            Collation = "NOCASE",
        };
        WindowFunctionExpression[] functions =
        [
            CreateAggregate("MIN", frame, collatedValue),
            CreateAggregate("MAX", frame, collatedValue),
        ];
        var window = CreateWindowOperator(
            source,
            inputColumns,
            functions,
            DbType.Text,
            DbType.Text);

        try
        {
            List<DbValue[]> rows = await ReadAllAsync(window);

            Assert.Equal(["z", "z"], [rows[0][1].AsText, rows[0][2].AsText]);
            Assert.Equal(["alpha", "z"], [rows[1][1].AsText, rows[1][2].AsText]);
            Assert.Equal(["alpha", "alpha"], [rows[2][1].AsText, rows[2][2].AsText]);
            Assert.Equal(["ALPHA", "beta"], [rows[3][1].AsText, rows[3][2].AsText]);
        }
        finally
        {
            await window.DisposeAsync();
        }
    }

    [Theory]
    [InlineData(1, "MaxBufferedRows")]
    [InlineData(2, "MaxPartitionRows")]
    public async Task ResourceLimitFailure_DisposesSourceExactlyOnce(
        int maxBufferedRows,
        string expectedLimit)
    {
        ColumnDefinition[] inputColumns =
        [
            new() { Name = "value", Type = DbType.Integer, Nullable = false },
        ];
        var source = new FixedRowsOperator(
            inputColumns,
            [
                [DbValue.FromInteger(1)],
                [DbValue.FromInteger(2)],
            ]);
        var rowNumber = new WindowFunctionExpression
        {
            Function = new FunctionCallExpression
            {
                FunctionName = "ROW_NUMBER",
                Arguments = [],
            },
            Window = new WindowSpecification(),
        };
        var window = CreateWindowOperator(
            source,
            inputColumns,
            [rowNumber],
            DbType.Integer,
            new WindowExecutionOptions
            {
                MaxPartitionRows = 1,
                MaxBufferedRows = maxBufferedRows,
            });

        try
        {
            CSharpDbException error = await Assert.ThrowsAsync<CSharpDbException>(
                () => window.OpenAsync(TestContext.Current.CancellationToken).AsTask());

            Assert.Equal(ErrorCode.ResourceLimitExceeded, error.Code);
            Assert.Contains(expectedLimit, error.Message, StringComparison.Ordinal);
            Assert.Equal(1, source.DisposeCount);
        }
        finally
        {
            await window.DisposeAsync();
        }

        Assert.Equal(1, source.DisposeCount);
    }

    private static WindowOperator CreateWindowOperator(
        FixedRowsOperator source,
        ColumnDefinition[] inputColumns,
        IReadOnlyList<WindowFunctionExpression> functions,
        params DbType[] outputTypes) =>
        CreateWindowOperator(source, inputColumns, functions, outputTypes, options: null);

    private static WindowOperator CreateWindowOperator(
        FixedRowsOperator source,
        ColumnDefinition[] inputColumns,
        IReadOnlyList<WindowFunctionExpression> functions,
        DbType outputType,
        WindowExecutionOptions options) =>
        CreateWindowOperator(source, inputColumns, functions, [outputType], options);

    private static WindowOperator CreateWindowOperator(
        FixedRowsOperator source,
        ColumnDefinition[] inputColumns,
        IReadOnlyList<WindowFunctionExpression> functions,
        IReadOnlyList<DbType> outputTypes,
        WindowExecutionOptions? options)
    {
        Assert.Equal(functions.Count, outputTypes.Count);
        var inputSchema = new TableSchema
        {
            TableName = "input",
            Columns = inputColumns,
        };
        ColumnDefinition[] outputColumns =
        [
            .. inputColumns,
            .. outputTypes.Select((type, index) => new ColumnDefinition
            {
                Name = $"window_{index}",
                Type = type,
                Nullable = true,
            }),
        ];
        return new WindowOperator(
            source,
            inputSchema,
            functions,
            outputColumns,
            options: options);
    }

    private static WindowFunctionExpression CreateAggregate(
        string functionName,
        WindowFrame frame,
        Expression? argument = null) =>
        new()
        {
            Function = new FunctionCallExpression
            {
                FunctionName = functionName,
                Arguments =
                [
                    argument ?? new ColumnRefExpression { ColumnName = "value" },
                ],
            },
            Window = new WindowSpecification
            {
                Frame = frame,
            },
        };

    private static WindowFrame CreateFrame(
        WindowFrameBoundKind startKind,
        WindowFrameBoundKind endKind,
        long? startOffset = null,
        long? endOffset = null) =>
        new()
        {
            Start = new WindowFrameBound
            {
                Kind = startKind,
                Offset = startOffset,
            },
            End = new WindowFrameBound
            {
                Kind = endKind,
                Offset = endOffset,
            },
        };

    private static async Task<List<DbValue[]>> ReadAllAsync(WindowOperator window)
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        await window.OpenAsync(ct);
        var rows = new List<DbValue[]>();
        while (await window.MoveNextAsync(ct))
            rows.Add(window.Current);
        return rows;
    }

    private sealed class FixedRowsOperator(
        ColumnDefinition[] outputSchema,
        DbValue[][] rows) : IOperator
    {
        private int _index = -1;

        public ColumnDefinition[] OutputSchema { get; } = outputSchema;
        public bool ReusesCurrentRowBuffer => false;
        public DbValue[] Current { get; private set; } = Array.Empty<DbValue>();
        public int DisposeCount { get; private set; }

        public ValueTask OpenAsync(CancellationToken ct = default)
        {
            ct.ThrowIfCancellationRequested();
            _index = -1;
            Current = Array.Empty<DbValue>();
            return ValueTask.CompletedTask;
        }

        public ValueTask<bool> MoveNextAsync(CancellationToken ct = default)
        {
            ct.ThrowIfCancellationRequested();
            _index++;
            if (_index >= rows.Length)
            {
                Current = Array.Empty<DbValue>();
                return ValueTask.FromResult(false);
            }

            Current = rows[_index];
            return ValueTask.FromResult(true);
        }

        public ValueTask DisposeAsync()
        {
            DisposeCount++;
            Current = Array.Empty<DbValue>();
            return ValueTask.CompletedTask;
        }
    }
}
