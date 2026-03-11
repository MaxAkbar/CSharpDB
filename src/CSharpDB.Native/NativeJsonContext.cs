using System.Text.Json.Serialization;
using DiagnosticsDatabaseInspectReport = CSharpDB.Storage.Diagnostics.DatabaseInspectReport;
using DiagnosticsIndexInspectReport = CSharpDB.Storage.Diagnostics.IndexInspectReport;
using DiagnosticsPageInspectReport = CSharpDB.Storage.Diagnostics.PageInspectReport;
using DiagnosticsWalInspectReport = CSharpDB.Storage.Diagnostics.WalInspectReport;
using EngineMaintenanceReport = CSharpDB.Engine.DatabaseMaintenanceReport;
using EngineReindexResult = CSharpDB.Engine.DatabaseReindexResult;
using EngineVacuumResult = CSharpDB.Engine.DatabaseVacuumResult;

namespace CSharpDB.Native;

[JsonSourceGenerationOptions(
    PropertyNamingPolicy = JsonKnownNamingPolicy.CamelCase,
    UseStringEnumConverter = true)]
[JsonSerializable(typeof(DiagnosticsDatabaseInspectReport))]
[JsonSerializable(typeof(DiagnosticsWalInspectReport))]
[JsonSerializable(typeof(DiagnosticsPageInspectReport))]
[JsonSerializable(typeof(DiagnosticsIndexInspectReport))]
[JsonSerializable(typeof(EngineMaintenanceReport))]
[JsonSerializable(typeof(EngineReindexResult))]
[JsonSerializable(typeof(EngineVacuumResult))]
internal sealed partial class NativeJsonContext : JsonSerializerContext
{
}
