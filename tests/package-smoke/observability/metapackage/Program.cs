using System.Text.Json;
using CSharpDB.Observability;
using CSharpDB.Sql;

const string secret = "observability-package-canary";
QueryFingerprintResult result = SqlQueryNormalizer.NormalizeAndFingerprint(
    $"SELECT value FROM observations WHERE secret = '{secret}'");
string json = JsonSerializer.Serialize(
    result,
    CSharpDbObservabilityJsonContext.Default.QueryFingerprintResult);

if (!result.Fingerprint.Value.StartsWith(
        QueryFingerprint.Algorithm + ":",
        StringComparison.Ordinal) ||
    result.NormalizedText.Contains(secret, StringComparison.Ordinal) ||
    json.Contains(secret, StringComparison.Ordinal))
{
    throw new InvalidOperationException("The metapackage fingerprint surface was not safe and usable.");
}

Console.WriteLine("CSharpDB metapackage observability surface smoke passed.");
