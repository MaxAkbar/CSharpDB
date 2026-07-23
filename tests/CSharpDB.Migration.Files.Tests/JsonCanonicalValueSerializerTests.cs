using System.Buffers;
using System.Text;
using CSharpDB.Migration.Files.Json;

namespace CSharpDB.Migration.Files.Tests;

public sealed class JsonCanonicalValueSerializerTests
{
    [Fact]
    public void PreservesEncounterOrderExactNumberLexemesAndNesting()
    {
        JsonLogicalValue value = Object(
            ("z", Number("18446744073709551616")),
            ("a", Array(
                Number("-0"),
                Object(
                    ("second", Number("1.2300e+004")),
                    ("first", JsonLogicalValue.CreateNull())),
                JsonLogicalValue.CreateBoolean(false))),
            ("m", JsonLogicalValue.CreateString("text")));

        byte[] actual = JsonCanonicalValueSerializer.SerializeToUtf8Bytes(value);

        Assert.Equal(
            """{"z":18446744073709551616,"a":[-0,{"second":1.2300e+004,"first":null},false],"m":"text"}"""u8.ToArray(),
            actual);
    }

    [Fact]
    public void UsesFrozenMinimalEscapingAndLiteralUnicode()
    {
        string controls = string.Concat(Enumerable.Range(0, 32).Select(index => (char)index));
        JsonLogicalValue value = Object(
            ("control\u0001\"\\é_日本😀", JsonLogicalValue.CreateString(
                controls + "\"\\café_日本😀/\u2028\u2029")));
        const string expected =
            "{\"control\\u0001\\\"\\\\é_日本😀\":\"" +
            "\\u0000\\u0001\\u0002\\u0003\\u0004\\u0005\\u0006\\u0007" +
            "\\b\\t\\n\\u000b\\f\\r\\u000e\\u000f" +
            "\\u0010\\u0011\\u0012\\u0013\\u0014\\u0015\\u0016\\u0017" +
            "\\u0018\\u0019\\u001a\\u001b\\u001c\\u001d\\u001e\\u001f" +
            "\\\"\\\\café_日本😀/\u2028\u2029\"}";

        byte[] actual = JsonCanonicalValueSerializer.SerializeToUtf8Bytes(value);

        Assert.Equal(Encoding.UTF8.GetBytes(expected), actual);
        Assert.False(actual.AsSpan().StartsWith(Encoding.UTF8.Preamble));
    }

    [Theory]
    [InlineData("0")]
    [InlineData("-0")]
    [InlineData("9223372036854775807")]
    [InlineData("9223372036854775808")]
    [InlineData("18446744073709551615")]
    [InlineData("18446744073709551616")]
    [InlineData("12345678901234567890.1234567890123456789000")]
    [InlineData("1e400")]
    [InlineData("1e-4000")]
    [InlineData("1.2300E+004")]
    public void WritesNumberLexemesWithoutNormalization(string lexeme)
    {
        byte[] actual = JsonCanonicalValueSerializer.SerializeToUtf8Bytes(Number(lexeme));

        Assert.Equal(Encoding.ASCII.GetBytes(lexeme), actual);
    }

    [Fact]
    public void WritesEveryLogicalKindIncludingEmptyContainers()
    {
        JsonLogicalValue value = Array(
            JsonLogicalValue.CreateNull(),
            JsonLogicalValue.CreateBoolean(true),
            JsonLogicalValue.CreateBoolean(false),
            JsonLogicalValue.CreateString(string.Empty),
            Object(),
            Array());

        byte[] actual = JsonCanonicalValueSerializer.SerializeToUtf8Bytes(value);

        Assert.Equal("""[null,true,false,"",{},[]]"""u8.ToArray(), actual);
    }

    [Fact]
    public void WriteAndSerializeAreDeterministicAcrossRepeatedCalls()
    {
        JsonLogicalValue value = Object(
            ("β", Array(
                JsonLogicalValue.CreateString("one"),
                Number("2.00"),
                Object(("x", JsonLogicalValue.CreateBoolean(true))))));
        byte[] first = JsonCanonicalValueSerializer.SerializeToUtf8Bytes(value);
        byte[] repeated = JsonCanonicalValueSerializer.SerializeToUtf8Bytes(value);
        var destination = new ArrayBufferWriter<byte>(initialCapacity: 1);

        JsonCanonicalValueSerializer.Write(destination, value);

        Assert.Equal(first, repeated);
        Assert.Equal(first, destination.WrittenSpan.ToArray());
        Assert.Equal("""{"β":["one",2.00,{"x":true}]}"""u8.ToArray(), first);
    }

    private static JsonLogicalValue Number(string lexeme) =>
        JsonLogicalValue.CreateNumber(lexeme);

    private static JsonLogicalValue Array(params JsonLogicalValue[] elements) =>
        JsonLogicalValue.CreateArray(elements);

    private static JsonLogicalValue Object(
        params (string Name, JsonLogicalValue Value)[] properties) =>
        JsonLogicalValue.CreateObject(
            properties
                .Select(
                    static (property, index) =>
                        JsonLogicalProperty.Create(index, property.Name, property.Value))
                .ToArray());
}
