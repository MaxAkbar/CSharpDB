using System.Text;
using CSharpDB.Admin.Forms.Models;
using CSharpDB.CodeModules;
using CSharpDB.CodeModules.Runtime;

namespace CSharpDB.Admin.Forms.Services;

public interface IFormCodeModuleDesignerService
{
    Task<CodeModuleHandler> CreateHandlerAsync(
        FormCodeModuleHandlerRequest request,
        CancellationToken ct = default);
}

public sealed record FormCodeModuleHandlerRequest(
    string FormId,
    string FormName,
    string TableName,
    string EventName,
    bool IsCancelable,
    string? ControlId = null,
    string? ControlType = null);

public sealed class NullFormCodeModuleDesignerService : IFormCodeModuleDesignerService
{
    public static NullFormCodeModuleDesignerService Instance { get; } = new();

    private NullFormCodeModuleDesignerService()
    {
    }

    public Task<CodeModuleHandler> CreateHandlerAsync(
        FormCodeModuleHandlerRequest request,
        CancellationToken ct = default)
        => Task.FromException<CodeModuleHandler>(
            new InvalidOperationException("C# code module designer support is not configured for this host."));
}

public sealed class CSharpDbFormCodeModuleDesignerService(CSharpDbCodeModuleClient codeModules) : IFormCodeModuleDesignerService
{
    private const string NamespaceName = "CSharpDB.UserCode.Forms";

    public async Task<CodeModuleHandler> CreateHandlerAsync(
        FormCodeModuleHandlerRequest request,
        CancellationToken ct = default)
    {
        ArgumentNullException.ThrowIfNull(request);
        if (string.IsNullOrWhiteSpace(request.FormId))
            throw new InvalidOperationException("Save the form before creating C# handlers.");

        string moduleId = $"form:{request.FormId}";
        string typeName = $"{NamespaceName}.{ToIdentifier(request.FormName, "Form")}Module";
        string methodName = string.IsNullOrWhiteSpace(request.ControlId)
            ? $"On{ToIdentifier(request.EventName, "Event")}"
            : $"{ToIdentifier(request.ControlId, "Control")}_{ToIdentifier(request.EventName, "Event")}";
        string contextType = string.IsNullOrWhiteSpace(request.ControlId)
            ? request.IsCancelable ? nameof(FormBeforeEventContext) : nameof(FormEventContext)
            : nameof(FormControlEventContext);

        CodeModuleDefinition? existing = await codeModules.GetAsync(moduleId, ct);
        string source = existing is null
            ? CreateFormModuleSource(typeName, methodName, contextType)
            : EnsureMethodStub(existing.Source, methodName, contextType);

        await codeModules.UpsertAsync(new CodeModuleDefinition(
            moduleId,
            string.IsNullOrWhiteSpace(request.FormName) ? moduleId : request.FormName,
            CodeModuleKind.Form,
            source,
            OwnerKind: "Form",
            OwnerId: request.FormId,
            TypeName: typeName,
            Metadata: new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase)
            {
                ["tableName"] = request.TableName,
            }), ct);

        return new CodeModuleHandler(moduleId, typeName, methodName);
    }

    private static string CreateFormModuleSource(string typeName, string methodName, string contextType)
    {
        string shortTypeName = typeName.Split('.').Last();
        return $$"""
            using CSharpDB.CodeModules.Runtime;

            namespace {{NamespaceName}};

            public sealed class {{shortTypeName}} : FormCodeModule
            {
                public void {{methodName}}({{contextType}} context)
                {
                }
            }
            """;
    }

    private static string EnsureMethodStub(string source, string methodName, string contextType)
    {
        if (source.Contains($" {methodName}(", StringComparison.Ordinal) ||
            source.Contains($"\t{methodName}(", StringComparison.Ordinal))
        {
            return source;
        }

        int insertAt = source.LastIndexOf('}');
        if (insertAt < 0)
            return source + Environment.NewLine + CreateDetachedMethod(methodName, contextType);

        var builder = new StringBuilder(source.Length + 160);
        builder.Append(source.AsSpan(0, insertAt));
        if (!source[..insertAt].EndsWith(Environment.NewLine, StringComparison.Ordinal))
            builder.AppendLine();

        builder.AppendLine();
        builder.AppendLine($"    public void {methodName}({contextType} context)");
        builder.AppendLine("    {");
        builder.AppendLine("    }");
        builder.Append(source.AsSpan(insertAt));
        return builder.ToString();
    }

    private static string CreateDetachedMethod(string methodName, string contextType)
        => $$"""

            public void {{methodName}}({{contextType}} context)
            {
            }
            """;

    private static string ToIdentifier(string? value, string fallback)
    {
        string text = string.IsNullOrWhiteSpace(value) ? fallback : value;
        var builder = new StringBuilder(text.Length + 1);
        foreach (char ch in text)
        {
            if (char.IsLetterOrDigit(ch) || ch == '_')
                builder.Append(ch);
        }

        if (builder.Length == 0)
            builder.Append(fallback);
        if (!char.IsLetter(builder[0]) && builder[0] != '_')
            builder.Insert(0, '_');

        return builder.ToString();
    }
}
