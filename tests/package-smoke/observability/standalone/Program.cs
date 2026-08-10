using System.Text.Json;
using CSharpDB.Observability;

var options = new CSharpDbObservabilityOptions
{
    Enabled = true,
    DatabaseAlias = "package-smoke",
};
options.Validate();

var fingerprint = new QueryFingerprint(
    $"{QueryFingerprint.Algorithm}:{new string('a', 64)}");

string optionsJson = JsonSerializer.Serialize(
    options,
    CSharpDbObservabilityJsonContext.Default.CSharpDbObservabilityOptions);
string fingerprintJson = JsonSerializer.Serialize(
    fingerprint,
    CSharpDbObservabilityJsonContext.Default.QueryFingerprint);

if (!optionsJson.Contains("package-smoke", StringComparison.Ordinal) ||
    !fingerprintJson.Contains(QueryFingerprint.Algorithm, StringComparison.Ordinal))
{
    throw new InvalidOperationException("The standalone observability package contract was not usable.");
}

Console.WriteLine("Standalone CSharpDB.Observability package smoke passed.");
