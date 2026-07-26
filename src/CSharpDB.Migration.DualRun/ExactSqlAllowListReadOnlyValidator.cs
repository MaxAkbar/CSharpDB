namespace CSharpDB.Migration.DualRun;

/// <summary>
/// A dialect-neutral fail-closed guard for a reviewed query pack. SQL must
/// match an approved statement byte-for-byte; comments, whitespace changes,
/// additional statements, and unreviewed text are rejected.
/// </summary>
public sealed class ExactSqlAllowListReadOnlyValidator : IDualRunReadOnlyStatementValidator
{
    private const int MaxApprovedStatements = 10_000;
    private readonly HashSet<string> _approved;

    public ExactSqlAllowListReadOnlyValidator(
        string validatorId,
        IEnumerable<string> approvedReadOnlyStatements)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(validatorId);
        ArgumentNullException.ThrowIfNull(approvedReadOnlyStatements);
        ValidatorId = validatorId;
        _approved = new HashSet<string>(StringComparer.Ordinal);

        foreach (string sql in approvedReadOnlyStatements)
        {
            ArgumentException.ThrowIfNullOrWhiteSpace(sql);
            if (!_approved.Add(sql))
                throw new ArgumentException("The approved SQL list contains a duplicate statement.");
            if (_approved.Count > MaxApprovedStatements)
            {
                throw new ArgumentOutOfRangeException(
                    nameof(approvedReadOnlyStatements),
                    $"At most {MaxApprovedStatements} approved statements are allowed.");
            }
        }

        if (_approved.Count == 0)
            throw new ArgumentException("At least one approved read-only statement is required.");
    }

    public string ValidatorId { get; }

    public DualRunReadOnlyValidation Validate(string sql)
    {
        ArgumentNullException.ThrowIfNull(sql);
        return _approved.Contains(sql)
            ? new DualRunReadOnlyValidation { IsReadOnly = true }
            : new DualRunReadOnlyValidation
            {
                IsReadOnly = false,
                RejectionCode = "DUALRUN_SQL_NOT_IN_APPROVED_READ_ONLY_SET",
            };
    }
}
