namespace CSharpDB.Migration.Access;

using CSharpDB.Migration;

public static class AccessRetainedDataContract
{
    public const string DataContract =
        "csharpdb-access-retained-data/v1";

    public const string ScalarCodecContract =
        "csharpdb-access-scalar/v1";

    public const string RowOrderContract =
        "csharpdb-access-primary-key-order/v1";

    public const string SnapshotIdentityPrefix =
        "access-retained:";

    public const string DataAvailableFacet =
        MigrationDataAvailabilityContract.AvailableFacet;

    public const string DataUnavailableReasonFacet =
        MigrationDataAvailabilityContract
            .UnavailableReasonFacet;
}

public static class AccessRetainedAvailabilityReasons
{
    public const string Available = "available";

    public const string StableOrder =
        "primary-key-required";

    public const string ScalarType =
        "unsupported-scalar-type";
}
