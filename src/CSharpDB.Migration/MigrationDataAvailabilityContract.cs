namespace CSharpDB.Migration;

/// <summary>
/// Provider-neutral catalog facts that distinguish an inventoried object from
/// an object whose rows or documents are present in a retained source.
/// </summary>
public static class MigrationDataAvailabilityContract
{
    public const string AvailableFacet = "migrationDataAvailable";

    public const string UnavailableReasonFacet =
        "migrationDataUnavailableReason";
}
