namespace CSharpDB.Api.Tests;

public sealed class ApiTestFileCleanupTests
{
    [Fact]
    public async Task DeleteIfExistsAsync_PermanentLockFailsWithOriginalException()
    {
        Assert.SkipWhen(!OperatingSystem.IsWindows(), "Windows prevents deleting open files.");
        string path = Path.Combine(Path.GetTempPath(), $"csharpdb_api_cleanup_locked_{Guid.NewGuid():N}.db");
        try
        {
            await using var lockStream = new FileStream(
                path, FileMode.CreateNew, FileAccess.ReadWrite, FileShare.None);

            IOException error = await Assert.ThrowsAsync<IOException>(() =>
                ApiTestFileCleanup.DeleteIfExistsAsync(path).AsTask().WaitAsync(
                    TimeSpan.FromSeconds(20), TestContext.Current.CancellationToken));

            Assert.Contains(path, error.Message, StringComparison.Ordinal);
            Assert.Contains("cleanup timeout", error.Message, StringComparison.Ordinal);
            Assert.IsType<IOException>(error.InnerException);
            Assert.True(File.Exists(path));
        }
        finally
        {
            File.Delete(path);
        }
    }
}
