using CSharpDB.Primitives;

namespace CSharpDB.Admin.Forms.Models;

public sealed record FormDefinition(
    string FormId,
    string Name,
    string TableName,
    int DefinitionVersion,
    string SourceSchemaSignature,
    LayoutDefinition Layout,
    IReadOnlyList<ControlDefinition> Controls,
    IReadOnlyDictionary<string, object?>? RendererHints = null,
    IReadOnlyList<FormEventBinding>? EventBindings = null,
    DbAutomationMetadata? Automation = null,
    IReadOnlyList<DbActionSequence>? ActionSequences = null,
    IReadOnlyList<ControlRuleDefinition>? Rules = null);
