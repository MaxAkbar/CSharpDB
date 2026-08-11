using System.Net;
using CSharpDB.Api.Security;

namespace CSharpDB.Api.Tests;

public sealed class DiagnosticsAccessPolicyTests
{
    [Fact]
    public void SecurityOptions_DefaultToFailClosedDiagnosticsAcknowledgements()
    {
        var options = new CSharpDbApiSecurityOptions();

        Assert.Equal(CSharpDbRemoteSecurityMode.None, options.Mode);
        Assert.False(options.AllowInsecureRemoteDiagnostics);
        Assert.False(options.AllowSensitiveQueryDetailAccess);
    }

    [Theory]
    [InlineData("127.0.0.1")]
    [InlineData("127.255.255.254")]
    [InlineData("::1")]
    public void SecurityNone_AllowsProvenLoopbackAddresses(string address)
    {
        var options = new CSharpDbApiSecurityOptions();

        CSharpDbDiagnosticsAccessDecision decision = Evaluate(
            options,
            IPAddress.Parse(address));

        Assert.Equal(CSharpDbDiagnosticsAccessDecision.Allowed, decision);
    }

    [Theory]
    [InlineData("192.0.2.10")]
    [InlineData("2001:db8::10")]
    public void SecurityNone_RejectsRemoteAddressesByDefault(string address)
    {
        var options = new CSharpDbApiSecurityOptions();

        CSharpDbDiagnosticsAccessDecision decision = Evaluate(
            options,
            IPAddress.Parse(address));

        Assert.Equal(CSharpDbDiagnosticsAccessDecision.Forbidden, decision);
    }

    [Fact]
    public void SecurityNone_RejectsNullAndUnspecifiedAddresses()
    {
        var options = new CSharpDbApiSecurityOptions
        {
            AllowInsecureRemoteDiagnostics = true,
        };

        Assert.Equal(
            CSharpDbDiagnosticsAccessDecision.Forbidden,
            Evaluate(options, remoteIpAddress: null));
        Assert.Equal(
            CSharpDbDiagnosticsAccessDecision.Forbidden,
            Evaluate(options, IPAddress.Any));
        Assert.Equal(
            CSharpDbDiagnosticsAccessDecision.Forbidden,
            Evaluate(options, IPAddress.IPv6Any));
    }

    [Theory]
    [InlineData("192.0.2.10")]
    [InlineData("2001:db8::10")]
    public void SecurityNone_ExplicitInsecureRemoteAcknowledgementAllowsRemoteAddress(
        string address)
    {
        var options = new CSharpDbApiSecurityOptions
        {
            AllowInsecureRemoteDiagnostics = true,
        };

        CSharpDbDiagnosticsAccessDecision decision = Evaluate(
            options,
            IPAddress.Parse(address));

        Assert.Equal(CSharpDbDiagnosticsAccessDecision.Allowed, decision);
    }

    [Fact]
    public void ApiKeyMode_ClassifiesCorrectMissingAndWrongKeysWithoutSecretEcho()
    {
        const string secret = "ApiKey-Canary-Do-Not-Echo";
        var options = new CSharpDbApiSecurityOptions
        {
            Mode = CSharpDbRemoteSecurityMode.ApiKey,
            ApiKey = secret,
        };

        CSharpDbDiagnosticsAccessDecision correct = Evaluate(
            options,
            remoteIpAddress: null,
            suppliedApiKey: secret);
        CSharpDbDiagnosticsAccessDecision missing = Evaluate(
            options,
            remoteIpAddress: null,
            suppliedApiKey: null);
        CSharpDbDiagnosticsAccessDecision wrong = Evaluate(
            options,
            remoteIpAddress: null,
            suppliedApiKey: "wrong-key");

        Assert.Equal(CSharpDbDiagnosticsAccessDecision.Allowed, correct);
        Assert.Equal(CSharpDbDiagnosticsAccessDecision.Unauthenticated, missing);
        Assert.Equal(CSharpDbDiagnosticsAccessDecision.Unauthenticated, wrong);
        Assert.DoesNotContain(secret, correct.ToString(), StringComparison.Ordinal);
        Assert.DoesNotContain(secret, missing.ToString(), StringComparison.Ordinal);
        Assert.DoesNotContain(secret, wrong.ToString(), StringComparison.Ordinal);
    }

    [Fact]
    public void ApiKeyMode_MissingConfiguredKeyIsUnauthenticated()
    {
        var options = new CSharpDbApiSecurityOptions
        {
            Mode = CSharpDbRemoteSecurityMode.ApiKey,
        };

        CSharpDbDiagnosticsAccessDecision decision = Evaluate(
            options,
            IPAddress.Loopback,
            suppliedApiKey: "any-key");

        Assert.Equal(CSharpDbDiagnosticsAccessDecision.Unauthenticated, decision);
    }

    [Theory]
    [InlineData(CSharpDbRemoteSecurityMode.None, "127.0.0.1", null)]
    [InlineData(CSharpDbRemoteSecurityMode.ApiKey, null, "configured-key")]
    public void QueryDetail_RequiresSeparateSensitiveAccessAcknowledgement(
        CSharpDbRemoteSecurityMode mode,
        string? remoteAddress,
        string? suppliedApiKey)
    {
        var options = new CSharpDbApiSecurityOptions
        {
            Mode = mode,
            ApiKey = "configured-key",
        };
        IPAddress? address = remoteAddress is null
            ? null
            : IPAddress.Parse(remoteAddress);

        CSharpDbDiagnosticsAccessDecision denied = Evaluate(
            options,
            address,
            suppliedApiKey,
            CSharpDbDiagnosticsAccessKind.QueryDetail);

        options.AllowSensitiveQueryDetailAccess = true;
        CSharpDbDiagnosticsAccessDecision allowed = Evaluate(
            options,
            address,
            suppliedApiKey,
            CSharpDbDiagnosticsAccessKind.QueryDetail);

        Assert.Equal(CSharpDbDiagnosticsAccessDecision.Forbidden, denied);
        Assert.Equal(CSharpDbDiagnosticsAccessDecision.Allowed, allowed);
    }

    [Fact]
    public void UndefinedSecurityModeAndAccessKind_FailClosed()
    {
        var options = new CSharpDbApiSecurityOptions
        {
            Mode = (CSharpDbRemoteSecurityMode)int.MaxValue,
            AllowInsecureRemoteDiagnostics = true,
            AllowSensitiveQueryDetailAccess = true,
        };

        Assert.Equal(
            CSharpDbDiagnosticsAccessDecision.Forbidden,
            Evaluate(options, IPAddress.Loopback));

        options.Mode = CSharpDbRemoteSecurityMode.None;
        Assert.Equal(
            CSharpDbDiagnosticsAccessDecision.Forbidden,
            Evaluate(
                options,
                IPAddress.Loopback,
                accessKind: (CSharpDbDiagnosticsAccessKind)int.MaxValue));
    }

    private static CSharpDbDiagnosticsAccessDecision Evaluate(
        CSharpDbApiSecurityOptions options,
        IPAddress? remoteIpAddress,
        string? suppliedApiKey = null,
        CSharpDbDiagnosticsAccessKind accessKind = CSharpDbDiagnosticsAccessKind.Runtime)
        => CSharpDbDiagnosticsAccessPolicy.Evaluate(
            options,
            remoteIpAddress,
            suppliedApiKey,
            accessKind);
}
