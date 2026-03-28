namespace CSharpDB.Sql;

public enum TokenType
{
    // Literals
    IntegerLiteral,
    RealLiteral,
    StringLiteral,

    // Identifier
    Identifier,
    Parameter,

    // Keywords
    Create,
    Table,
    Insert,
    Into,
    Values,
    Select,
    From,
    Where,
    And,
    Or,
    Not,
    Limit,
    Offset,
    Order,
    By,
    Asc,
    Desc,
    Null,
    Delete,
    Update,
    Set,
    Drop,
    Integer,
    Real,
    Text,
    Blob,
    Primary,
    Key,
    Identity,
    Autoincrement,
    If,
    Exists,
    Like,
    In,
    Between,
    Escape,
    Is,
    Group,
    Having,
    As,
    Distinct,
    Count,
    Sum,
    Avg,
    Min,
    Max,
    Join,
    Inner,
    Left,
    Right,
    Outer,
    Cross,
    On,
    Union,
    Intersect,
    Except,
    Alter,
    Add,
    Column,
    Collate,
    Rename,
    To,
    Index,
    Unique,
    View,
    With,
    Recursive,
    Analyze,
    Trigger,
    Before,
    After,
    For,
    Each,
    Row,
    Begin,
    End,
    New,
    Old,

    // Operators
    Equals,         // =
    NotEquals,      // <> or !=
    LessThan,       // <
    GreaterThan,    // >
    LessOrEqual,    // <=
    GreaterOrEqual, // >=
    Plus,           // +
    Minus,          // -
    Star,           // *
    Slash,          // /

    // Punctuation
    Comma,
    Colon,
    Dot,
    LeftParen,
    RightParen,
    Semicolon,

    // Special
    Eof,
}
