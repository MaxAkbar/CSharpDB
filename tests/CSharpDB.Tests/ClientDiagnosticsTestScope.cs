using System.Reflection;
using CSharpDB.Client.Internal;

namespace CSharpDB.Tests;

internal static class ClientDiagnosticsTestScope
{
    // Preserve a sampled runtime family across a physical owner release. This
    // exercises aggregation without keeping a database transaction open.
    internal static IDisposable RetainRuntimeFamily(EngineTransportClient client)
        => Assert.IsAssignableFrom<IDisposable>(typeof(EngineTransportClient)
            .GetMethod("AcquireRuntimeDiagnosticsFamilyLease", BindingFlags.Instance | BindingFlags.NonPublic)!
            .Invoke(client, null));
}
