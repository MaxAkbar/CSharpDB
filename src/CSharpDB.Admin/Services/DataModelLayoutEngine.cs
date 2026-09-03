using CSharpDB.Admin.Models;

namespace CSharpDB.Admin.Services;

public static class DataModelCanvasMetrics
{
    public const double NodeWidth = 220;
    public const double NodeHeaderHeight = 36;
    public const double NodeMetaHeight = 24;
    public const double ColumnRowHeight = 28;

    public static IReadOnlyList<DataModelColumn> GetVisibleColumns(DataModelState state, DataModelNode node)
    {
        if (node.DetailLevel == DataModelNodeDetailLevel.Collapsed)
            return [];
        if (node.DetailLevel == DataModelNodeDetailLevel.All)
            return node.Columns;

        var relationshipColumns = state.Relationships
            .Where(relationship =>
                string.Equals(relationship.LeftTable, node.Name, StringComparison.OrdinalIgnoreCase) ||
                string.Equals(relationship.RightTable, node.Name, StringComparison.OrdinalIgnoreCase))
            .Select(relationship => string.Equals(relationship.LeftTable, node.Name, StringComparison.OrdinalIgnoreCase)
                ? relationship.LeftColumn
                : relationship.RightColumn)
            .ToHashSet(StringComparer.OrdinalIgnoreCase);

        return node.Columns
            .Where(column => column.IsPrimaryKey || column.IsForeignKey || relationshipColumns.Contains(column.Name))
            .ToArray();
    }

    public static double NodeHeight(DataModelState state, DataModelNode node)
    {
        if (node.DetailLevel == DataModelNodeDetailLevel.Collapsed)
            return NodeHeaderHeight;
        return NodeHeaderHeight + NodeMetaHeight + GetVisibleColumns(state, node).Count * ColumnRowHeight;
    }

    public static double ColumnCenterY(DataModelState state, DataModelNode node, string columnName)
    {
        if (node.DetailLevel == DataModelNodeDetailLevel.Collapsed)
            return node.Y + NodeHeaderHeight / 2;

        IReadOnlyList<DataModelColumn> visible = GetVisibleColumns(state, node);
        int index = -1;
        for (int i = 0; i < visible.Count; i++)
        {
            if (string.Equals(visible[i].Name, columnName, StringComparison.OrdinalIgnoreCase))
            {
                index = i;
                break;
            }
        }

        return index < 0
            ? node.Y + NodeHeaderHeight / 2
            : node.Y + NodeHeaderHeight + NodeMetaHeight + index * ColumnRowHeight + ColumnRowHeight / 2;
    }
}

public readonly record struct DataModelConnectorPoint(double X, double Y);

public readonly record struct DataModelConnectorEndpoint(
    double X,
    double Y,
    DataModelConnectorSide Side);

public readonly record struct DataModelConnectorObstacle(
    double X,
    double Y,
    double Width,
    double Height);

public sealed record DataModelConnectorRoute(
    IReadOnlyList<DataModelConnectorPoint> Points,
    double LabelX,
    double LabelY,
    bool UsedAutomaticFallback = false);

/// <summary>
/// Produces deterministic orthogonal connector paths that stay outside table cards.
/// The visibility grid intentionally has no dependency on browser geometry or a graph package,
/// so initial rendering and geometry tests use the same rules as the canvas.
/// </summary>
public static class DataModelConnectorRouter
{
    public const double Clearance = 18;
    public const double EndpointStub = 24;
    public const int MaximumWaypoints = 128;
    private const double BendPenalty = 48;
    private const double OuterLaneGap = 24;
    private const double EqualityTolerance = 0.001;

    public static DataModelConnectorRoute Route(
        DataModelConnectorEndpoint start,
        DataModelConnectorEndpoint end,
        IReadOnlyList<DataModelConnectorObstacle> obstacles,
        DataModelConnectorLayout? layout = null)
    {
        DataModelConnectorPoint startPoint = new(start.X, start.Y);
        DataModelConnectorPoint endPoint = new(end.X, end.Y);
        List<RoutingRect> physical = ExpandObstacles(obstacles, 0);
        List<RoutingRect> expanded = ExpandObstacles(obstacles, Clearance);
        DataModelConnectorPoint departure = EndpointDeparture(start, physical);
        DataModelConnectorPoint approach = EndpointDeparture(end, physical);
        List<DataModelConnectorPoint> waypoints = layout?.Waypoints
            .Select(static waypoint => new DataModelConnectorPoint(waypoint.X, waypoint.Y))
            .ToList() ?? [];
        bool custom = layout is not null && waypoints.Count > 0;
        List<DataModelConnectorPoint>? interior = null;
        if (custom && waypoints.Count <= MaximumWaypoints && waypoints.All(point => IsValidWaypoint(point, expanded)))
            interior = FindWaypointPath(departure, approach, waypoints, expanded);
        bool usedAutomaticFallback = custom && interior is null;
        if (interior is null)
        {
            interior = FindPath(departure, approach, expanded)
                // Tight manually positioned cards can leave less than the preferred clearance.
                // Relax the gutter before accepting any path through a physical card.
                ?? FindPath(departure, approach, physical)
                ?? FallbackPath(departure, approach, physical);
        }

        var points = new List<DataModelConnectorPoint>(interior.Count + 2) { startPoint };
        points.AddRange(interior);
        points.Add(endPoint);
        points = Simplify(points, custom && !usedAutomaticFallback ? waypoints : []);

        (double labelX, double labelY) = LabelPoint(points);
        return new DataModelConnectorRoute(points, labelX, labelY, usedAutomaticFallback);
    }

    public static bool IsValidWaypoint(
        DataModelConnectorPoint point,
        IReadOnlyList<DataModelConnectorObstacle> obstacles) =>
        IsValidWaypoint(point, ExpandObstacles(obstacles, Clearance));

    private static bool IsValidWaypoint(DataModelConnectorPoint point, IReadOnlyList<RoutingRect> obstacles) =>
        double.IsFinite(point.X) && double.IsFinite(point.Y)
        && point.X >= 4 && point.Y >= 4 && !InsideAny(point, obstacles);

    private static List<RoutingRect> ExpandObstacles(IReadOnlyList<DataModelConnectorObstacle> obstacles, double clearance) =>
        obstacles.Select(obstacle => new RoutingRect(
            obstacle.X - clearance,
            obstacle.Y - clearance,
            obstacle.X + obstacle.Width + clearance,
            obstacle.Y + obstacle.Height + clearance)).ToList();

    private static DataModelConnectorPoint EndpointDeparture(
        DataModelConnectorEndpoint endpoint,
        IReadOnlyList<RoutingRect> obstacles)
    {
        double nearestObstacle = double.PositiveInfinity;
        foreach (RoutingRect obstacle in obstacles)
        {
            if (endpoint.Y <= obstacle.Top + EqualityTolerance || endpoint.Y >= obstacle.Bottom - EqualityTolerance)
                continue;
            double gap = endpoint.Side == DataModelConnectorSide.Right
                ? obstacle.Left - endpoint.X
                : endpoint.X - obstacle.Right;
            if (gap > EqualityTolerance)
                nearestObstacle = Math.Min(nearestObstacle, gap);
        }
        double distance = nearestObstacle < EndpointStub ? nearestObstacle / 2 : EndpointStub;
        return new(endpoint.X + (endpoint.Side == DataModelConnectorSide.Right ? distance : -distance), endpoint.Y);
    }

    private static List<DataModelConnectorPoint>? FindWaypointPath(
        DataModelConnectorPoint start,
        DataModelConnectorPoint end,
        IReadOnlyList<DataModelConnectorPoint> waypoints,
        IReadOnlyList<RoutingRect> obstacles)
    {
        var result = new List<DataModelConnectorPoint> { start };
        foreach (DataModelConnectorPoint target in waypoints.Append(end))
        {
            List<DataModelConnectorPoint>? leg = FindPath(result[^1], target, obstacles);
            if (leg is null)
                return null;
            result.AddRange(leg.Skip(1));
        }
        return result;
    }

    public static bool IntersectsObstacleInterior(
        DataModelConnectorRoute route,
        DataModelConnectorObstacle obstacle)
    {
        var rect = new RoutingRect(obstacle.X, obstacle.Y, obstacle.X + obstacle.Width, obstacle.Y + obstacle.Height);
        for (int index = 1; index < route.Points.Count; index++)
        {
            if (!SegmentIsClear(route.Points[index - 1], route.Points[index], [rect]))
                return true;
        }
        return false;
    }

    private static List<DataModelConnectorPoint>? FindPath(
        DataModelConnectorPoint start,
        DataModelConnectorPoint end,
        IReadOnlyList<RoutingRect> obstacles)
    {
        if (InsideAny(start, obstacles) || InsideAny(end, obstacles))
            return null;

        var xValues = new SortedSet<double> { start.X, end.X, (start.X + end.X) / 2 };
        var yValues = new SortedSet<double> { start.Y, end.Y, (start.Y + end.Y) / 2 };
        foreach (RoutingRect obstacle in obstacles)
        {
            xValues.Add(obstacle.Left);
            xValues.Add(obstacle.Right);
            yValues.Add(obstacle.Top);
            yValues.Add(obstacle.Bottom);
        }

        double minX = Math.Min(start.X, Math.Min(end.X, obstacles.Select(static obstacle => obstacle.Left).DefaultIfEmpty(start.X).Min()));
        double maxX = Math.Max(start.X, Math.Max(end.X, obstacles.Select(static obstacle => obstacle.Right).DefaultIfEmpty(start.X).Max()));
        double minY = Math.Min(start.Y, Math.Min(end.Y, obstacles.Select(static obstacle => obstacle.Top).DefaultIfEmpty(start.Y).Min()));
        double maxY = Math.Max(start.Y, Math.Max(end.Y, obstacles.Select(static obstacle => obstacle.Bottom).DefaultIfEmpty(start.Y).Max()));
        xValues.Add(Math.Max(4, minX - OuterLaneGap));
        xValues.Add(maxX + OuterLaneGap);
        yValues.Add(Math.Max(4, minY - OuterLaneGap));
        yValues.Add(maxY + OuterLaneGap);

        var points = new List<DataModelConnectorPoint>();
        var pointIndex = new Dictionary<DataModelConnectorPoint, int>();
        foreach (double y in yValues)
        {
            foreach (double x in xValues)
            {
                var point = new DataModelConnectorPoint(x, y);
                if (InsideAny(point, obstacles))
                    continue;
                pointIndex[point] = points.Count;
                points.Add(point);
            }
        }

        if (!pointIndex.TryGetValue(start, out int startIndex) || !pointIndex.TryGetValue(end, out int endIndex))
            return null;

        var adjacency = Enumerable.Range(0, points.Count).Select(_ => new List<RouteEdge>()).ToArray();
        foreach (IGrouping<double, int> row in Enumerable.Range(0, points.Count).GroupBy(index => points[index].Y))
            ConnectVisibleNeighbors(row.OrderBy(index => points[index].X), RouteDirection.Horizontal, points, obstacles, adjacency);
        foreach (IGrouping<double, int> column in Enumerable.Range(0, points.Count).GroupBy(index => points[index].X))
            ConnectVisibleNeighbors(column.OrderBy(index => points[index].Y), RouteDirection.Vertical, points, obstacles, adjacency);

        const int directionCount = 3;
        int stateCount = points.Count * directionCount;
        var distance = Enumerable.Repeat(double.PositiveInfinity, stateCount).ToArray();
        var previous = Enumerable.Repeat(-1, stateCount).ToArray();
        int startState = startIndex * directionCount + (int)RouteDirection.Horizontal;
        distance[startState] = 0;
        int sequence = 0;
        var queue = new PriorityQueue<int, (double Cost, int Sequence)>();
        queue.Enqueue(startState, (0, sequence++));

        while (queue.TryDequeue(out int state, out (double Cost, int Sequence) priority))
        {
            if (priority.Cost > distance[state] + EqualityTolerance)
                continue;

            int currentPointIndex = state / directionCount;
            RouteDirection previousDirection = (RouteDirection)(state % directionCount);
            foreach (RouteEdge edge in adjacency[currentPointIndex]
                         .OrderBy(static edge => edge.Direction)
                         .ThenBy(edge => points[edge.Target].X)
                         .ThenBy(edge => points[edge.Target].Y))
            {
                double bend = previousDirection == edge.Direction ? 0 : BendPenalty;
                double candidate = distance[state] + edge.Length + bend;
                int nextState = edge.Target * directionCount + (int)edge.Direction;
                if (candidate >= distance[nextState] - EqualityTolerance)
                    continue;
                distance[nextState] = candidate;
                previous[nextState] = state;
                queue.Enqueue(nextState, (candidate, sequence++));
            }
        }

        int endState = Enumerable.Range(1, 2)
            .Select(direction => endIndex * directionCount + direction)
            .OrderBy(state => distance[state] + (state % directionCount == (int)RouteDirection.Horizontal ? 0 : BendPenalty))
            .ThenBy(static state => state)
            .First();
        if (double.IsPositiveInfinity(distance[endState]))
            return null;

        var reversed = new List<DataModelConnectorPoint>();
        for (int state = endState; state >= 0; state = previous[state])
        {
            reversed.Add(points[state / directionCount]);
            if (state == startState)
                break;
        }
        reversed.Reverse();
        return reversed;
    }

    private static void ConnectVisibleNeighbors(
        IEnumerable<int> orderedIndices,
        RouteDirection direction,
        IReadOnlyList<DataModelConnectorPoint> points,
        IReadOnlyList<RoutingRect> obstacles,
        IReadOnlyList<List<RouteEdge>> adjacency)
    {
        int? previous = null;
        foreach (int current in orderedIndices)
        {
            if (previous is int prior && SegmentIsClear(points[prior], points[current], obstacles))
            {
                double length = Math.Abs(points[current].X - points[prior].X) + Math.Abs(points[current].Y - points[prior].Y);
                adjacency[prior].Add(new RouteEdge(current, direction, length));
                adjacency[current].Add(new RouteEdge(prior, direction, length));
            }
            previous = current;
        }
    }

    private static List<DataModelConnectorPoint> FallbackPath(
        DataModelConnectorPoint start,
        DataModelConnectorPoint end,
        IReadOnlyList<RoutingRect> obstacles)
    {
        double top = Math.Max(4, Math.Min(start.Y, Math.Min(end.Y, obstacles.Select(static obstacle => obstacle.Top).DefaultIfEmpty(start.Y).Min())) - OuterLaneGap);
        double bottom = Math.Max(start.Y, Math.Max(end.Y, obstacles.Select(static obstacle => obstacle.Bottom).DefaultIfEmpty(start.Y).Max())) + OuterLaneGap;
        double routeY = Math.Abs(start.Y - top) + Math.Abs(end.Y - top) <= Math.Abs(start.Y - bottom) + Math.Abs(end.Y - bottom)
            ? top
            : bottom;
        return [start, new(start.X, routeY), new(end.X, routeY), end];
    }

    private static List<DataModelConnectorPoint> Simplify(
        IReadOnlyList<DataModelConnectorPoint> points,
        IReadOnlyList<DataModelConnectorPoint> protectedPoints)
    {
        var result = new List<DataModelConnectorPoint>();
        foreach (DataModelConnectorPoint point in points)
        {
            if (result.Count > 0 && SamePoint(result[^1], point))
                continue;
            while (result.Count >= 2
                   && !protectedPoints.Any(guide => SamePoint(guide, result[^1]))
                   && Collinear(result[^2], result[^1], point)
                   && Between(result[^2], result[^1], point))
                result.RemoveAt(result.Count - 1);
            result.Add(point);
        }
        return result;
    }

    private static (double X, double Y) LabelPoint(IReadOnlyList<DataModelConnectorPoint> points)
    {
        double longest = -1;
        DataModelConnectorPoint selectedStart = points[0];
        DataModelConnectorPoint selectedEnd = points[^1];
        for (int index = 1; index < points.Count; index++)
        {
            double length = Math.Abs(points[index].X - points[index - 1].X) + Math.Abs(points[index].Y - points[index - 1].Y);
            if (length <= longest)
                continue;
            longest = length;
            selectedStart = points[index - 1];
            selectedEnd = points[index];
        }
        return ((selectedStart.X + selectedEnd.X) / 2, (selectedStart.Y + selectedEnd.Y) / 2);
    }

    private static bool InsideAny(DataModelConnectorPoint point, IReadOnlyList<RoutingRect> obstacles) =>
        obstacles.Any(obstacle => point.X > obstacle.Left + EqualityTolerance
                                  && point.X < obstacle.Right - EqualityTolerance
                                  && point.Y > obstacle.Top + EqualityTolerance
                                  && point.Y < obstacle.Bottom - EqualityTolerance);

    private static bool SegmentIsClear(
        DataModelConnectorPoint start,
        DataModelConnectorPoint end,
        IReadOnlyList<RoutingRect> obstacles)
    {
        if (Math.Abs(start.Y - end.Y) < EqualityTolerance)
        {
            double left = Math.Min(start.X, end.X);
            double right = Math.Max(start.X, end.X);
            return obstacles.All(obstacle =>
                start.Y <= obstacle.Top + EqualityTolerance
                || start.Y >= obstacle.Bottom - EqualityTolerance
                || right <= obstacle.Left + EqualityTolerance
                || left >= obstacle.Right - EqualityTolerance);
        }

        if (Math.Abs(start.X - end.X) < EqualityTolerance)
        {
            double top = Math.Min(start.Y, end.Y);
            double bottom = Math.Max(start.Y, end.Y);
            return obstacles.All(obstacle =>
                start.X <= obstacle.Left + EqualityTolerance
                || start.X >= obstacle.Right - EqualityTolerance
                || bottom <= obstacle.Top + EqualityTolerance
                || top >= obstacle.Bottom - EqualityTolerance);
        }

        return false;
    }

    private static bool SamePoint(DataModelConnectorPoint left, DataModelConnectorPoint right) =>
        Math.Abs(left.X - right.X) < EqualityTolerance && Math.Abs(left.Y - right.Y) < EqualityTolerance;

    private static bool Collinear(DataModelConnectorPoint first, DataModelConnectorPoint second, DataModelConnectorPoint third) =>
        Math.Abs(first.X - second.X) < EqualityTolerance && Math.Abs(second.X - third.X) < EqualityTolerance
        || Math.Abs(first.Y - second.Y) < EqualityTolerance && Math.Abs(second.Y - third.Y) < EqualityTolerance;

    private static bool Between(DataModelConnectorPoint first, DataModelConnectorPoint middle, DataModelConnectorPoint last) =>
        middle.X >= Math.Min(first.X, last.X) - EqualityTolerance
        && middle.X <= Math.Max(first.X, last.X) + EqualityTolerance
        && middle.Y >= Math.Min(first.Y, last.Y) - EqualityTolerance
        && middle.Y <= Math.Max(first.Y, last.Y) + EqualityTolerance;

    private readonly record struct RoutingRect(double Left, double Top, double Right, double Bottom);
    private readonly record struct RouteEdge(int Target, RouteDirection Direction, double Length);

    private enum RouteDirection
    {
        None,
        Horizontal,
        Vertical,
    }
}

public static class DataModelLayoutEngine
{
    private const double Margin = 24;
    private const double RankGap = 160;
    private const double NodeGap = 48;
    private const double ComponentGap = 100;
    private const double IsolatedColumnGap = 90;
    private const int IsolatedColumns = 4;

    public static int CountOverlaps(DataModelState state)
    {
        int overlaps = 0;
        for (int leftIndex = 0; leftIndex < state.Nodes.Count; leftIndex++)
        {
            DataModelNode left = state.Nodes[leftIndex];
            double leftHeight = DataModelCanvasMetrics.NodeHeight(state, left);
            for (int rightIndex = leftIndex + 1; rightIndex < state.Nodes.Count; rightIndex++)
            {
                DataModelNode right = state.Nodes[rightIndex];
                double rightHeight = DataModelCanvasMetrics.NodeHeight(state, right);
                if (left.X < right.X + DataModelCanvasMetrics.NodeWidth
                    && left.X + DataModelCanvasMetrics.NodeWidth > right.X
                    && left.Y < right.Y + rightHeight
                    && left.Y + leftHeight > right.Y)
                {
                    overlaps++;
                }
            }
        }

        return overlaps;
    }

    public static void ArrangeAll(DataModelState state)
    {
        if (state.Nodes.Count == 0)
            return;

        Dictionary<string, DataModelNode> nodes = state.Nodes.ToDictionary(node => node.Name, StringComparer.OrdinalIgnoreCase);
        Dictionary<string, HashSet<string>> directed = CreateAdjacency(nodes.Keys);
        Dictionary<string, HashSet<string>> undirected = CreateAdjacency(nodes.Keys);
        foreach (DataModelRelationship relationship in state.Relationships.Where(static relationship => relationship.IsResolved))
        {
            if (!nodes.ContainsKey(relationship.LeftTable) || !nodes.ContainsKey(relationship.RightTable))
                continue;

            // Relationships are stored child -> parent; layout flows parent -> child.
            directed[relationship.RightTable].Add(relationship.LeftTable);
            undirected[relationship.RightTable].Add(relationship.LeftTable);
            undirected[relationship.LeftTable].Add(relationship.RightTable);
        }

        List<List<string>> components = FindComponents(nodes.Keys, undirected);
        List<List<string>> connected = components
            .Where(component => component.Count > 1 || directed[component[0]].Count > 0)
            .OrderBy(component => component.Min(StringComparer.OrdinalIgnoreCase), StringComparer.OrdinalIgnoreCase)
            .ToList();
        List<string> isolated = components
            .Where(component => component.Count == 1 && directed[component[0]].Count == 0)
            .SelectMany(static component => component)
            .OrderBy(static name => name, StringComparer.OrdinalIgnoreCase)
            .ToList();

        double nextY = Margin;
        foreach (List<string> component in connected)
        {
            double height = LayoutComponent(state, nodes, directed, component, Margin, nextY);
            nextY += height + ComponentGap;
        }

        if (connected.Count > 0 && isolated.Count > 0)
            nextY += ComponentGap / 2;

        double isolatedRowHeight = 0;
        for (int i = 0; i < isolated.Count; i++)
        {
            DataModelNode node = nodes[isolated[i]];
            int column = i % IsolatedColumns;
            if (column == 0 && i > 0)
            {
                nextY += isolatedRowHeight + NodeGap;
                isolatedRowHeight = 0;
            }

            node.X = Margin + column * (DataModelCanvasMetrics.NodeWidth + IsolatedColumnGap);
            node.Y = nextY;
            isolatedRowHeight = Math.Max(isolatedRowHeight, DataModelCanvasMetrics.NodeHeight(state, node));
        }
    }

    public static void PlaceNewNodes(DataModelState state, IReadOnlySet<string> existingNodeNames)
    {
        List<DataModelNode> additions = state.Nodes
            .Where(node => !existingNodeNames.Contains(node.Name))
            .OrderBy(static node => node.Name, StringComparer.OrdinalIgnoreCase)
            .ToList();
        if (additions.Count == 0)
            return;
        if (existingNodeNames.Count == 0)
        {
            ArrangeAll(state);
            return;
        }

        var occupied = state.Nodes
            .Where(node => existingNodeNames.Contains(node.Name))
            .Select(node => RectFor(state, node))
            .ToList();
        var placedNames = existingNodeNames.ToHashSet(StringComparer.OrdinalIgnoreCase);
        double defaultX = occupied.Max(static rect => rect.Right) + RankGap;

        foreach (DataModelNode node in additions)
        {
            List<(double X, double Y)> candidates = [];
            foreach (DataModelRelationship relationship in state.Relationships.Where(relationship => relationship.IsResolved))
            {
                if (string.Equals(relationship.LeftTable, node.Name, StringComparison.OrdinalIgnoreCase))
                {
                    DataModelNode? parent = FindPlacedNode(state, relationship.RightTable, placedNames);
                    if (parent is not null)
                        candidates.Add((parent.X + DataModelCanvasMetrics.NodeWidth + RankGap, parent.Y));
                }
                else if (string.Equals(relationship.RightTable, node.Name, StringComparison.OrdinalIgnoreCase))
                {
                    DataModelNode? child = FindPlacedNode(state, relationship.LeftTable, placedNames);
                    if (child is not null)
                        candidates.Add((Math.Max(Margin, child.X - DataModelCanvasMetrics.NodeWidth - RankGap), child.Y));
                }
            }

            double desiredX = candidates.Count == 0 ? defaultX : candidates.Average(static candidate => candidate.X);
            double desiredY = candidates.Count == 0 ? Margin : candidates.Average(static candidate => candidate.Y);
            ModelRect placed = FindOpenRect(state, node, desiredX, desiredY, occupied);
            node.X = placed.X;
            node.Y = placed.Y;
            occupied.Add(placed);
            placedNames.Add(node.Name);
        }
    }

    private static double LayoutComponent(
        DataModelState state,
        IReadOnlyDictionary<string, DataModelNode> nodes,
        IReadOnlyDictionary<string, HashSet<string>> directed,
        IReadOnlyCollection<string> component,
        double originX,
        double originY)
    {
        HashSet<string> included = component.ToHashSet(StringComparer.OrdinalIgnoreCase);
        List<List<string>> stronglyConnected = FindStronglyConnectedComponents(component, directed);
        var componentByNode = new Dictionary<string, int>(StringComparer.OrdinalIgnoreCase);
        for (int i = 0; i < stronglyConnected.Count; i++)
        {
            foreach (string name in stronglyConnected[i])
                componentByNode[name] = i;
        }

        var dag = Enumerable.Range(0, stronglyConnected.Count).ToDictionary(index => index, _ => new HashSet<int>());
        var incoming = Enumerable.Range(0, stronglyConnected.Count).ToDictionary(index => index, _ => new HashSet<int>());
        foreach (string parent in component)
        {
            foreach (string child in directed[parent].Where(included.Contains))
            {
                int from = componentByNode[parent];
                int to = componentByNode[child];
                if (from == to || !dag[from].Add(to))
                    continue;
                incoming[to].Add(from);
            }
        }

        var ranksByComponent = new int[stronglyConnected.Count];
        var queue = new SortedSet<int>(Comparer<int>.Create((left, right) =>
        {
            int byName = StringComparer.OrdinalIgnoreCase.Compare(
                stronglyConnected[left].Min(StringComparer.OrdinalIgnoreCase),
                stronglyConnected[right].Min(StringComparer.OrdinalIgnoreCase));
            return byName != 0 ? byName : left.CompareTo(right);
        }));
        var indegree = incoming.ToDictionary(static pair => pair.Key, static pair => pair.Value.Count);
        foreach ((int id, int count) in indegree)
        {
            if (count == 0)
                queue.Add(id);
        }

        while (queue.Count > 0)
        {
            int current = queue.Min;
            queue.Remove(current);
            foreach (int child in dag[current])
            {
                ranksByComponent[child] = Math.Max(ranksByComponent[child], ranksByComponent[current] + 1);
                if (--indegree[child] == 0)
                    queue.Add(child);
            }
        }

        var ranks = component
            .GroupBy(name => ranksByComponent[componentByNode[name]])
            .OrderBy(static group => group.Key)
            .ToDictionary(
                static group => group.Key,
                group => group.OrderBy(static name => name, StringComparer.OrdinalIgnoreCase).ToList());
        MinimizeCrossings(ranks, directed, included);

        double componentHeight = ranks.Values.Max(rank => RankHeight(state, nodes, rank));
        foreach ((int rankIndex, List<string> rank) in ranks)
        {
            double rankHeight = RankHeight(state, nodes, rank);
            double y = originY + (componentHeight - rankHeight) / 2;
            foreach (string name in rank)
            {
                DataModelNode node = nodes[name];
                node.X = originX + rankIndex * (DataModelCanvasMetrics.NodeWidth + RankGap);
                node.Y = y;
                y += DataModelCanvasMetrics.NodeHeight(state, node) + NodeGap;
            }
        }

        return componentHeight;
    }

    private static void MinimizeCrossings(
        IDictionary<int, List<string>> ranks,
        IReadOnlyDictionary<string, HashSet<string>> directed,
        IReadOnlySet<string> included)
    {
        int maxRank = ranks.Keys.DefaultIfEmpty(0).Max();
        for (int sweep = 0; sweep < 4; sweep++)
        {
            for (int rank = 1; rank <= maxRank; rank++)
                SortByNeighborBarycenter(ranks, rank, directed, included, parents: true);
            for (int rank = maxRank - 1; rank >= 0; rank--)
                SortByNeighborBarycenter(ranks, rank, directed, included, parents: false);
        }
    }

    private static void SortByNeighborBarycenter(
        IDictionary<int, List<string>> ranks,
        int rank,
        IReadOnlyDictionary<string, HashSet<string>> directed,
        IReadOnlySet<string> included,
        bool parents)
    {
        if (!ranks.TryGetValue(rank, out List<string>? names) || names.Count < 2)
            return;

        var positions = ranks.Values
            .SelectMany(static list => list.Select((name, index) => (name, index)))
            .GroupBy(static item => item.name, StringComparer.OrdinalIgnoreCase)
            .ToDictionary(static group => group.Key, static group => group.First().index, StringComparer.OrdinalIgnoreCase);

        double Score(string name)
        {
            IEnumerable<string> neighbors = parents
                ? directed.Where(pair => pair.Value.Contains(name)).Select(static pair => pair.Key)
                : directed[name];
            int[] values = neighbors.Where(included.Contains).Where(positions.ContainsKey).Select(neighbor => positions[neighbor]).ToArray();
            return values.Length == 0 ? double.MaxValue : values.Average();
        }

        names.Sort((left, right) =>
        {
            int byScore = Score(left).CompareTo(Score(right));
            return byScore != 0 ? byScore : StringComparer.OrdinalIgnoreCase.Compare(left, right);
        });
    }

    private static List<List<string>> FindComponents(
        IEnumerable<string> nodeNames,
        IReadOnlyDictionary<string, HashSet<string>> undirected)
    {
        var remaining = nodeNames.ToHashSet(StringComparer.OrdinalIgnoreCase);
        var result = new List<List<string>>();
        while (remaining.Count > 0)
        {
            string start = remaining.OrderBy(static name => name, StringComparer.OrdinalIgnoreCase).First();
            var component = new List<string>();
            var queue = new Queue<string>();
            queue.Enqueue(start);
            remaining.Remove(start);
            while (queue.Count > 0)
            {
                string current = queue.Dequeue();
                component.Add(current);
                foreach (string neighbor in undirected[current].OrderBy(static name => name, StringComparer.OrdinalIgnoreCase))
                {
                    if (remaining.Remove(neighbor))
                        queue.Enqueue(neighbor);
                }
            }
            result.Add(component);
        }
        return result;
    }

    private static List<List<string>> FindStronglyConnectedComponents(
        IEnumerable<string> nodeNames,
        IReadOnlyDictionary<string, HashSet<string>> directed)
    {
        HashSet<string> included = nodeNames.ToHashSet(StringComparer.OrdinalIgnoreCase);
        var indexByNode = new Dictionary<string, int>(StringComparer.OrdinalIgnoreCase);
        var lowLink = new Dictionary<string, int>(StringComparer.OrdinalIgnoreCase);
        var stack = new Stack<string>();
        var onStack = new HashSet<string>(StringComparer.OrdinalIgnoreCase);
        var result = new List<List<string>>();
        int index = 0;

        void Visit(string node)
        {
            indexByNode[node] = index;
            lowLink[node] = index;
            index++;
            stack.Push(node);
            onStack.Add(node);

            foreach (string child in directed[node].Where(included.Contains).OrderBy(static name => name, StringComparer.OrdinalIgnoreCase))
            {
                if (!indexByNode.ContainsKey(child))
                {
                    Visit(child);
                    lowLink[node] = Math.Min(lowLink[node], lowLink[child]);
                }
                else if (onStack.Contains(child))
                {
                    lowLink[node] = Math.Min(lowLink[node], indexByNode[child]);
                }
            }

            if (lowLink[node] != indexByNode[node])
                return;

            var component = new List<string>();
            string current;
            do
            {
                current = stack.Pop();
                onStack.Remove(current);
                component.Add(current);
            } while (!string.Equals(current, node, StringComparison.OrdinalIgnoreCase));
            component.Sort(StringComparer.OrdinalIgnoreCase);
            result.Add(component);
        }

        foreach (string node in included.OrderBy(static name => name, StringComparer.OrdinalIgnoreCase))
        {
            if (!indexByNode.ContainsKey(node))
                Visit(node);
        }
        return result;
    }

    private static Dictionary<string, HashSet<string>> CreateAdjacency(IEnumerable<string> names) =>
        names.ToDictionary(static name => name, _ => new HashSet<string>(StringComparer.OrdinalIgnoreCase), StringComparer.OrdinalIgnoreCase);

    private static double RankHeight(
        DataModelState state,
        IReadOnlyDictionary<string, DataModelNode> nodes,
        IReadOnlyList<string> rank) =>
        rank.Sum(name => DataModelCanvasMetrics.NodeHeight(state, nodes[name])) + Math.Max(0, rank.Count - 1) * NodeGap;

    private static DataModelNode? FindPlacedNode(
        DataModelState state,
        string name,
        IReadOnlySet<string> placedNames) =>
        state.Nodes.FirstOrDefault(node =>
            string.Equals(node.Name, name, StringComparison.OrdinalIgnoreCase) &&
            placedNames.Contains(node.Name));

    private static ModelRect FindOpenRect(
        DataModelState state,
        DataModelNode node,
        double desiredX,
        double desiredY,
        IReadOnlyList<ModelRect> occupied)
    {
        double x = Math.Max(Margin, desiredX);
        double y = Math.Max(Margin, desiredY);
        double height = DataModelCanvasMetrics.NodeHeight(state, node);
        for (int attempt = 0; attempt < 2000; attempt++)
        {
            var candidate = new ModelRect(x, y, DataModelCanvasMetrics.NodeWidth, height);
            ModelRect? collision = occupied.FirstOrDefault(rect => candidate.Intersects(rect));
            if (collision is null)
                return candidate;
            y = collision.Bottom + NodeGap;
        }
        return new ModelRect(x, y, DataModelCanvasMetrics.NodeWidth, height);
    }

    private static ModelRect RectFor(DataModelState state, DataModelNode node) =>
        new(node.X, node.Y, DataModelCanvasMetrics.NodeWidth, DataModelCanvasMetrics.NodeHeight(state, node));

    private sealed record ModelRect(double X, double Y, double Width, double Height)
    {
        public double Right => X + Width;
        public double Bottom => Y + Height;

        public bool Intersects(ModelRect other) =>
            X < other.Right + NodeGap && Right + NodeGap > other.X &&
            Y < other.Bottom + NodeGap && Bottom + NodeGap > other.Y;
    }
}
