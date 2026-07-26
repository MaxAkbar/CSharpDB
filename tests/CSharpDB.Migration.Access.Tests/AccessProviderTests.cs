using System.Data;
using System.Runtime.InteropServices;

namespace CSharpDB.Migration.Access.Tests;

public sealed class AccessProviderTests
{
    [Fact]
    public void ProviderTableMatchIsCaseInsensitive()
    {
        using var providers = new DataTable();
        providers.Columns.Add(
            "SOURCES_NAME",
            typeof(string));
        providers.Rows.Add(
            "microsoft.ace.oledb.16.0");

        Assert.True(
            AccessProviderProbe.ContainsProvider(
                providers,
                AccessProviderIds.Ace16));
        Assert.False(
            AccessProviderProbe.ContainsProvider(
                providers,
                AccessProviderIds.Ace12));
    }

    [Fact]
    public void MissingProviderIdFailsProbeClosed()
    {
        AccessProviderAvailability result =
            AccessProviderProbe.CheckProviderId(
                AccessOleDbProvider.Ace16,
                "CSharpDB.Missing.Access.Provider");

        Assert.False(result.IsAvailable);
        Assert.Equal(
            "CSharpDB.Missing.Access.Provider",
            result.ProviderId);
        Assert.Equal(
            RuntimeInformation.ProcessArchitecture,
            result.ProcessArchitecture);
        Assert.DoesNotContain(
            "exception",
            result.Reason,
            StringComparison.OrdinalIgnoreCase);
    }

    [Fact]
    public void SelectorDoesNotSilentlyFallback()
    {
        var requested = new List<
            AccessOleDbProvider>();
        AccessMigrationException error =
            Assert.Throws<AccessMigrationException>(
                () => AccessProviderSelector.Select(
                    new AccessSourceOptions
                    {
                        Provider =
                            AccessOleDbProvider.Ace16,
                        AllowAce12Fallback = false,
                    },
                    provider =>
                    {
                        requested.Add(provider);
                        return Unavailable(provider);
                    }));

        Assert.Equal(
            AccessMigrationErrorCode
                .ProviderUnavailable,
            error.ErrorCode);
        Assert.Equal(
            [AccessOleDbProvider.Ace16],
            requested);
    }

    [Fact]
    public void ExplicitFallbackUsesAce12OnlyAfterAce16IsAbsent()
    {
        var requested = new List<
            AccessOleDbProvider>();
        AccessOleDbProvider selected =
            AccessProviderSelector.Select(
                new AccessSourceOptions
                {
                    Provider =
                        AccessOleDbProvider.Ace16,
                    AllowAce12Fallback = true,
                },
                provider =>
                {
                    requested.Add(provider);
                    return provider ==
                        AccessOleDbProvider.Ace12
                        ? Available(provider)
                        : Unavailable(provider);
                });

        Assert.Equal(
            AccessOleDbProvider.Ace12,
            selected);
        Assert.Equal(
            [
                AccessOleDbProvider.Ace16,
                AccessOleDbProvider.Ace12,
            ],
            requested);
    }

    private static AccessProviderAvailability
        Available(
        AccessOleDbProvider provider) =>
        new()
        {
            Provider = provider,
            ProviderId =
                AccessProviderIds.Resolve(provider),
            ProcessArchitecture =
                RuntimeInformation.ProcessArchitecture,
            IsAvailable = true,
            Reason = "available",
        };

    private static AccessProviderAvailability
        Unavailable(
        AccessOleDbProvider provider) =>
        Available(provider) with
        {
            IsAvailable = false,
            Reason = "unavailable",
        };
}
