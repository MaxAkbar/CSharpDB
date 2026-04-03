using CSharpDB.Admin.Forms.Models;

namespace CSharpDB.Admin.Forms.Contracts;

public interface IValidationInferenceService
{
    IReadOnlyList<ValidationRule> InferRules(FormFieldDefinition field);
    IReadOnlyList<ValidationError> Evaluate(FormDefinition form, IDictionary<string, object?> record);
}
