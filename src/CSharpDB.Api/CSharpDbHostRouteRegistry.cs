namespace CSharpDB.Api;

internal sealed class CSharpDbHostRouteRegistry
{
    private readonly object _gate = new();
    private readonly List<Reservation> _reservations = [];

    internal void ReserveSubtree(string path, string owner)
        => Reserve(path, owner, subtree: true);

    internal void ReserveExact(string path, string owner)
        => Reserve(path, owner, subtree: false);

    internal void ThrowIfCollides(string path, string owner)
    {
        var candidate = new PathString(path);
        lock (_gate)
        {
            foreach (Reservation reservation in _reservations)
            {
                var existing = new PathString(reservation.Path);
                bool collision = reservation.Subtree
                    ? candidate.StartsWithSegments(existing)
                    : candidate.Equals(existing);
                if (collision)
                {
                    throw new InvalidOperationException(
                        $"The {owner} path collides with the reserved {reservation.Owner} route.");
                }
            }
        }
    }

    private void Reserve(string path, string owner, bool subtree)
    {
        lock (_gate)
        {
            foreach (Reservation reservation in _reservations)
            {
                if (PathsOverlap(path, subtree, reservation))
                {
                    throw new InvalidOperationException(
                        $"The {owner} route collides with the reserved {reservation.Owner} route.");
                }
            }

            _reservations.Add(new Reservation(path, owner, subtree));
        }
    }

    private static bool PathsOverlap(
        string candidatePath,
        bool candidateSubtree,
        Reservation existing)
    {
        var candidate = new PathString(candidatePath);
        var existingPath = new PathString(existing.Path);

        if (candidate.Equals(existingPath))
            return true;

        if (candidateSubtree && existingPath.StartsWithSegments(candidate))
            return true;

        return existing.Subtree && candidate.StartsWithSegments(existingPath);
    }

    private sealed record Reservation(string Path, string Owner, bool Subtree);
}
