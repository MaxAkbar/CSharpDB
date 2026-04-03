using CSharpDB.Admin.Forms.Models;

namespace CSharpDB.Admin.Forms.Contracts;

public interface IFormRepository
{
    Task<FormDefinition?> GetAsync(string formId);
    Task<FormDefinition> CreateAsync(FormDefinition form);
    Task<UpdateResult> TryUpdateAsync(string formId, int expectedVersion, FormDefinition updated);
    Task<IReadOnlyList<FormDefinition>> ListAsync(string? tableName = null);
    Task<bool> DeleteAsync(string formId);
}
