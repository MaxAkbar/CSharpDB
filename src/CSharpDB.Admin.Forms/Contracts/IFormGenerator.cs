using CSharpDB.Admin.Forms.Models;

namespace CSharpDB.Admin.Forms.Contracts;

public interface IFormGenerator
{
    FormDefinition GenerateDefault(FormTableDefinition table);
}
