using CSharpDB.ImportExport.Models;
using CSharpDB.ImportExport.TableArchives;
using CSharpDB.Primitives;

namespace CSharpDB.Tests;

public sealed class DeclaredTypeTableArchiveTests
{
    [Fact]
    public async Task NativeArchive_RejectsDescriptorWhoseStorageTagDoesNotMatch()
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        var schema = new TableSchema
        {
            TableName = "invalid_typed_archive",
            Columns =
            [
                new ColumnDefinition
                {
                    Name = "amount",
                    Type = DbType.Text,
                    DeclaredType = SqlTypeDescriptor.Create(
                        SqlTypeKind.Decimal,
                        precision: 18,
                        scale: 2),
                    Nullable = false,
                },
            ],
        };
        await using var archive = new MemoryStream();

        InvalidDataException error = await Assert.ThrowsAsync<InvalidDataException>(
            async () => await TableArchiveWriter.WriteAsync(
                archive,
                schema,
                TableArchiveWriter.ToAsyncRows([], ct),
                ct));

        Assert.Contains("declares DECIMAL(18,2)", error.Message, StringComparison.Ordinal);
        Assert.Equal(0, archive.Length);
    }

    [Theory]
    [InlineData(SqlTypeKind.Uuid, DbType.Blob, false, true, "ROWVERSION")]
    public async Task NativeArchive_RejectsLogicalTypeIncompatibleWithGeneratedColumn(
        SqlTypeKind declaredKind,
        DbType storageType,
        bool isIdentity,
        bool isRowVersion,
        string diagnostic)
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        var schema = new TableSchema
        {
            TableName = "invalid_generated_column_archive",
            Columns =
            [
                new ColumnDefinition
                {
                    Name = "generated_value",
                    Type = storageType,
                    DeclaredType = SqlTypeDescriptor.Create(declaredKind),
                    Nullable = false,
                    IsPrimaryKey = isIdentity,
                    IsIdentity = isIdentity,
                    IsRowVersion = isRowVersion,
                },
            ],
        };
        await using var archive = new MemoryStream();

        InvalidDataException error = await Assert.ThrowsAsync<InvalidDataException>(
            async () => await TableArchiveWriter.WriteAsync(
                archive,
                schema,
                TableArchiveWriter.ToAsyncRows([], ct),
                ct));

        Assert.Contains(diagnostic, error.Message, StringComparison.OrdinalIgnoreCase);
        Assert.Equal(0, archive.Length);
    }

    [Fact]
    public async Task NativeArchive_RoundTripsLogicalDescriptorsAndExactDecimalValues()
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        var decimalType = SqlTypeDescriptor.Create(
            SqlTypeKind.Decimal,
            precision: 18,
            scale: 4);
        var varcharType = SqlTypeDescriptor.Create(
            SqlTypeKind.VarChar,
            length: 12);
        var schema = new TableSchema
        {
            TableName = "typed_archive_items",
            Columns =
            [
                new ColumnDefinition
                {
                    Name = "id",
                    Type = DbType.Integer,
                    DeclaredType = SqlTypeDescriptor.Create(SqlTypeKind.Integer),
                    Nullable = false,
                    IsPrimaryKey = true,
                },
                new ColumnDefinition
                {
                    Name = "amount",
                    Type = DbType.Decimal,
                    DeclaredType = decimalType,
                    Nullable = false,
                },
                new ColumnDefinition
                {
                    Name = "code",
                    Type = DbType.Text,
                    DeclaredType = varcharType,
                    Nullable = false,
                },
            ],
        };
        DbValue[][] rows =
        [
            [
                DbValue.FromInteger(1),
                DbValue.FromDecimal(12345678901234.5678m),
                DbValue.FromText("alpha"),
            ],
        ];

        await using var archive = new MemoryStream();
        TableArchiveManifest written = await TableArchiveWriter.WriteAsync(
            archive,
            schema,
            TableArchiveWriter.ToAsyncRows(rows, ct),
            ct);

        Assert.Equal(TableArchiveManifest.LogicalTypesFormatVersion, written.FormatVersion);

        archive.Position = 0;
        (var archivedSchema, var manifest) =
            await TableArchiveReader.ReadMetadataAsync(archive, ct);
        Assert.Equal(TableArchiveManifest.LogicalTypesFormatVersion, manifest.FormatVersion);
        Assert.Equal(decimalType, archivedSchema.Columns[1].DeclaredType);
        Assert.Equal(varcharType, archivedSchema.Columns[2].DeclaredType);

        TableSchema restored = archivedSchema.ToTableSchema();
        Assert.Equal(decimalType, restored.Columns[1].EffectiveType);
        Assert.Equal(varcharType, restored.Columns[2].EffectiveType);

        archive.Position = 0;
        var restoredRows = new List<DbValue[]>();
        await foreach (DbValue[] row in TableArchiveReader.ReadRowsAsync(archive, ct))
            restoredRows.Add(row);

        DbValue[] restoredRow = Assert.Single(restoredRows);
        Assert.Equal(DbType.Decimal, restoredRow[1].Type);
        Assert.Equal(12345678901234.5678m, restoredRow[1].AsDecimal);
    }

    [Fact]
    public async Task NativeArchive_RoundTripsBigIntIdentityDescriptor()
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        var schema = new TableSchema
        {
            TableName = "bigint_identity_archive",
            Columns =
            [
                new ColumnDefinition
                {
                    Name = "id",
                    Type = DbType.Integer,
                    DeclaredType = SqlTypeDescriptor.Create(SqlTypeKind.BigInt),
                    Nullable = false,
                    IsPrimaryKey = true,
                    IsIdentity = true,
                },
            ],
        };

        await using var archive = new MemoryStream();
        await TableArchiveWriter.WriteAsync(
            archive,
            schema,
            TableArchiveWriter.ToAsyncRows(
                [new[] { DbValue.FromInteger(long.MaxValue) }],
                ct),
            ct);

        archive.Position = 0;
        (TableArchiveSchema restored, _) = await TableArchiveReader.ReadMetadataAsync(archive, ct);
        TableArchiveColumn column = Assert.Single(restored.Columns);
        Assert.Equal(SqlTypeKind.BigInt, column.DeclaredType!.Kind);
        Assert.True(column.IsIdentity);
    }
}
