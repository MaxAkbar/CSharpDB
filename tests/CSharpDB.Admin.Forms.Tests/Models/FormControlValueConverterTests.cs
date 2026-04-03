using CSharpDB.Admin.Forms.Models;

namespace CSharpDB.Admin.Forms.Tests.Models;

public class FormControlValueConverterTests
{
    [Theory]
    [InlineData(true, true)]
    [InlineData(false, false)]
    [InlineData("true", true)]
    [InlineData("false", false)]
    [InlineData("1", true)]
    [InlineData("0", false)]
    [InlineData(1, true)]
    [InlineData(0, false)]
    [InlineData(1L, true)]
    [InlineData(0L, false)]
    public void ToBoolean_RecognizesBooleanEncodings(object value, bool expected)
    {
        bool actual = FormControlValueConverter.ToBoolean(value);

        Assert.Equal(expected, actual);
    }

    [Fact]
    public void ConvertCheckboxValue_CastsToBoundFieldType()
    {
        var stringField = new FormFieldDefinition("IsActiveText", FieldDataType.String, false, false);
        var intField = new FormFieldDefinition("IsActiveInt", FieldDataType.Int64, false, false);

        object? stringValue = FormControlValueConverter.ConvertCheckboxValue(true, stringField);
        object? intValue = FormControlValueConverter.ConvertCheckboxValue(false, intField);

        Assert.Equal("true", stringValue);
        Assert.Equal(0L, intValue);
    }

    [Fact]
    public void ConvertChoiceValue_CastsBooleanChoicesForNumericFields()
    {
        var intField = new FormFieldDefinition("Status", FieldDataType.Int64, false, false);
        var boolField = new FormFieldDefinition("Enabled", FieldDataType.Boolean, false, false);

        object? numericChoice = FormControlValueConverter.ConvertChoiceValue("true", intField);
        object? booleanChoice = FormControlValueConverter.ConvertChoiceValue("1", boolField);

        Assert.Equal(1L, numericChoice);
        Assert.Equal(true, booleanChoice);
    }

    [Fact]
    public void ChoiceMatchesValue_TreatsCommonBooleanRepresentationsAsEquivalent()
    {
        var intField = new FormFieldDefinition("Status", FieldDataType.Int64, false, false);

        bool matchesTrue = FormControlValueConverter.ChoiceMatchesValue(1L, "true", intField);
        bool matchesFalse = FormControlValueConverter.ChoiceMatchesValue("0", "false", intField);
        bool mismatch = FormControlValueConverter.ChoiceMatchesValue(0L, "true", intField);

        Assert.True(matchesTrue);
        Assert.True(matchesFalse);
        Assert.False(mismatch);
    }
}
