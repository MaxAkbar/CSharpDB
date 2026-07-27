using CSharpDB.Migration.Files.Csv;

namespace CSharpDB.Migration.Files.Tests;

public sealed class CsvExportOrderedContentDigestTests
{
    [Fact]
    public void CurrentPrefixDigest_HasStableEmptyVector_AndDoesNotMutate()
    {
        using var digest = new CsvExportOrderedContentDigest();

        CsvExportHashManifest first = digest.GetCurrentPrefixDigest();
        CsvExportHashManifest repeated = digest.GetCurrentPrefixDigest();

        Assert.Equal(CsvExportHashManifest.Sha256Algorithm, first.Algorithm);
        Assert.Equal(
            "99bc978deb983ed512280c5f295e87b7f75dc644bf77735aeb954fa81dc2573f",
            first.Value);
        Assert.Equal(first.Value, repeated.Value);
        Assert.Equal(0, digest.RowCount);
        Assert.Equal(
            "879a6d96f9dbe682b05f572f0f462ca37a21893f7aa626a93ab8f06acea14550",
            digest.Complete().Value);
    }

    [Fact]
    public void CurrentPrefixDigest_HasStableMultiRowVector_AndPreservesCompletion()
    {
        byte[] firstRowHash = Enumerable.Range(0, 32)
            .Select(static value => checked((byte)value))
            .ToArray();
        byte[] secondRowHash = Enumerable.Repeat((byte)0xa5, 32).ToArray();
        using var withObservations = new CsvExportOrderedContentDigest();
        using var withoutObservations = new CsvExportOrderedContentDigest();

        withObservations.AppendRowHash(firstRowHash);
        withoutObservations.AppendRowHash(firstRowHash);
        _ = withObservations.GetCurrentPrefixDigest();
        withObservations.AppendRowHash(secondRowHash);
        withoutObservations.AppendRowHash(secondRowHash);

        CsvExportHashManifest first = withObservations.GetCurrentPrefixDigest();
        CsvExportHashManifest repeated = withObservations.GetCurrentPrefixDigest();

        Assert.Equal(
            "a855519a9d41338f67e1c92e33208d5c615cd77cd1039252ddc470668cd24677",
            first.Value);
        Assert.Equal(first.Value, repeated.Value);
        Assert.Equal(2, withObservations.RowCount);
        CsvExportHashManifest observedCompletion = withObservations.Complete();
        CsvExportHashManifest unobservedCompletion = withoutObservations.Complete();
        Assert.Equal(
            "669a551e8a0c08ad1964aaa5a9ce0dc45ed4f5f9c37c4831bc57028aa935d810",
            observedCompletion.Value);
        Assert.Equal(unobservedCompletion.Value, observedCompletion.Value);
    }

    [Fact]
    public void CurrentPrefixDigest_RejectsCompletedAndDisposedInstances()
    {
        var completed = new CsvExportOrderedContentDigest();
        _ = completed.Complete();

        Assert.Throws<InvalidOperationException>(
            () => completed.GetCurrentPrefixDigest());
        completed.Dispose();

        var disposed = new CsvExportOrderedContentDigest();
        disposed.Dispose();

        Assert.Throws<ObjectDisposedException>(
            () => disposed.GetCurrentPrefixDigest());
    }
}
