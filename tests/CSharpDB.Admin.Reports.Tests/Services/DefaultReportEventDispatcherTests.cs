using CSharpDB.Admin.Reports.Contracts;
using CSharpDB.Admin.Reports.Models;
using CSharpDB.Admin.Reports.Services;
using CSharpDB.Primitives;

namespace CSharpDB.Admin.Reports.Tests.Services;

public sealed class DefaultReportEventDispatcherTests
{
    [Fact]
    public async Task DispatchAsync_InvokesMatchingCommandsWithRuntimeArgumentsAndMetadata()
    {
        DbCommandContext? captured = null;
        var commands = DbCommandRegistry.Create(builder =>
        {
            builder.AddCommand("AuditReport", context =>
            {
                captured = context;
                return DbCommandResult.Success();
            });
        });

        var dispatcher = new DefaultReportEventDispatcher(commands);
        ReportDefinition report = CreateReport([
            new ReportEventBinding(
                ReportEventKind.BeforeRender,
                "AuditReport",
                new Dictionary<string, object?> { ["Source"] = "configured" }),
        ]);

        ReportEventDispatchResult result = await dispatcher.DispatchAsync(
            report,
            CreateSource(),
            ReportEventKind.BeforeRender,
            new Dictionary<string, object?> { ["RowCount"] = 4, ["Source"] = "runtime" },
            TestContext.Current.CancellationToken);

        Assert.True(result.Succeeded);
        Assert.NotNull(captured);
        Assert.Equal("AdminReports", captured!.Metadata["surface"]);
        Assert.Equal("sales-report", captured.Metadata["reportId"]);
        Assert.Equal("Sales", captured.Metadata["sourceName"]);
        Assert.Equal("BeforeRender", captured.Metadata["event"]);
        Assert.Equal(4, captured.Arguments["RowCount"].AsInteger);
        Assert.Equal("configured", captured.Arguments["Source"].AsText);
    }

    [Fact]
    public async Task DispatchAsync_StopsOnCommandFailureByDefault()
    {
        var commands = DbCommandRegistry.Create(builder =>
            builder.AddCommand("Reject", _ => DbCommandResult.Failure("Report rejected.")));
        var dispatcher = new DefaultReportEventDispatcher(commands);
        ReportDefinition report = CreateReport([new ReportEventBinding(ReportEventKind.OnOpen, "Reject")]);

        ReportEventDispatchResult result = await dispatcher.DispatchAsync(
            report,
            CreateSource(),
            ReportEventKind.OnOpen,
            ct: TestContext.Current.CancellationToken);

        Assert.False(result.Succeeded);
        Assert.Equal("Report rejected.", result.Message);
    }

    private static ReportDefinition CreateReport(IReadOnlyList<ReportEventBinding> eventBindings)
        => new(
            "sales-report",
            "Sales Report",
            new ReportSourceReference(ReportSourceKind.Table, "Sales"),
            DefinitionVersion: 1,
            SourceSchemaSignature: "sales:v1",
            PageSettings: ReportPageSettings.DefaultLetterPortrait,
            Groups: [],
            Sorts: [],
            Bands: [],
            EventBindings: eventBindings);

    private static ReportSourceDefinition CreateSource()
        => new(
            ReportSourceKind.Table,
            "Sales",
            "Sales",
            "SELECT * FROM Sales",
            "sales:v1",
            []);
}
