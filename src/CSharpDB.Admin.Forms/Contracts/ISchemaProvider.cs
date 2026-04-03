using CSharpDB.Admin.Forms.Models;

namespace CSharpDB.Admin.Forms.Contracts;

public interface ISchemaProvider
{
    Task<FormTableDefinition?> GetTableDefinitionAsync(string tableName);
    Task<IReadOnlyList<string>> ListTableNamesAsync();
}
