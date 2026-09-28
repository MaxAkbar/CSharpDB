using System.Reflection;
using System.Text.Json;
using CSharpDB.Admin.Components.Shared;
using CSharpDB.Admin.Helpers;
using Microsoft.AspNetCore.Components;
using Microsoft.AspNetCore.Components.Rendering;
using Microsoft.AspNetCore.Components.Web;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.JSInterop;

namespace CSharpDB.Admin.Forms.Tests.Components.Shared;

public sealed class SqlEditorCompletionTests
{
    [Theory]
    [InlineData("SELECT * FROM Customers WHERE ", false)]
    [InlineData("SELECT * FROM \"Customers\" WHERE ", false)]
    [InlineData("SELECT * FROM Customers WHERE ", true)]
    public async Task TypingOrRequestingCompletion_RendersColumns_AndAcceptsWithoutReplacingTheQuery(string sql, bool explicitTrigger)
    {
        var js = new EditorJs(sql);
        using var services = new ServiceCollection().AddSingleton<IJSRuntime>(js).BuildServiceProvider();
        await using var renderer = new HtmlRenderer(services, NullLoggerFactory.Instance);
        await renderer.Dispatcher.InvokeAsync(async () =>
        {
            SqlEditor? editor = null;
            var root = await renderer.RenderComponentAsync<Host>(ParameterView.FromDictionary(new Dictionary<string, object?>
            {
                [nameof(Host.Sql)] = sql,
                [nameof(Host.Capture)] = (Action<SqlEditor>)(component => editor = component),
            }));
            Assert.NotNull(editor);
            if (explicitTrigger) await editor.OnSqlEditorShortcut("Complete");
            else await (Task)typeof(SqlEditor).GetMethod("OnInput", BindingFlags.Instance | BindingFlags.NonPublic)!.Invoke(editor, null)!;
            Assert.Contains("sql-completion-popup", root.ToHtmlString());
            Assert.Contains(">Email</span>", root.ToHtmlString());
            await editor.OnSqlEditorCompletionKey("Tab");
            Assert.Equal(sql + "Email", js.Value);
            Assert.Equal(js.Value.Length, js.Caret);
        });
    }

    public sealed class Host : ComponentBase
    {
        [Parameter] public string Sql { get; set; } = "";
        [Parameter] public Action<SqlEditor>? Capture { get; set; }
        protected override void BuildRenderTree(RenderTreeBuilder builder)
        {
            builder.OpenComponent<SqlEditor>(0);
            builder.AddAttribute(1, nameof(SqlEditor.Value), Sql);
            builder.AddAttribute(2, nameof(SqlEditor.CompletionCatalog), new SqlCompletionCatalog
            {
                Sources = [new("Customers", SqlCompletionSourceKind.Table)],
                ColumnsBySource = new Dictionary<string, IReadOnlyList<SqlCompletionColumn>> { ["Customers"] = [new("Email", "TEXT", "Customers")] },
            });
            builder.AddComponentReferenceCapture(3, value => Capture?.Invoke((SqlEditor)value));
            builder.CloseComponent();
        }
    }

    private sealed class EditorJs(string sql) : IJSRuntime
    {
        public string Value = sql;
        public int Caret = sql.Length;
        public ValueTask<TValue> InvokeAsync<TValue>(string identifier, object?[]? args)
        {
            if (identifier == "editorInterop.replaceEditorText")
            {
                Value = Value[..(int)args![1]!] + (string)args[3]! + Value[(int)args[2]! ..];
                Caret = (int)args[4]!;
            }
            if (identifier is "editorInterop.getEditorState" or "editorInterop.replaceEditorText")
                return ValueTask.FromResult(JsonSerializer.Deserialize<TValue>(JsonSerializer.Serialize(new { Value, SelectionStart = Caret, SelectionEnd = Caret }))!);
            if (identifier == "editorInterop.getCaretCoordinates")
                return ValueTask.FromResult(JsonSerializer.Deserialize<TValue>("{\"Left\":12,\"Top\":30}")!);
            return ValueTask.FromResult(default(TValue)!);
        }
        public ValueTask<TValue> InvokeAsync<TValue>(string identifier, CancellationToken ct, object?[]? args) => InvokeAsync<TValue>(identifier, args);
    }
}
