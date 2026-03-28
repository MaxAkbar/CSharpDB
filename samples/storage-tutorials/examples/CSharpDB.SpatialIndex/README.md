# CSharpDB.SpatialIndex

A **spatial index** built on a single CSharpDB.Storage B+tree.
Geographic coordinates (latitude, longitude) are mapped to a single `long` key via a **Hilbert space-filling curve**, so nearby points in 2D have nearby keys in the tree. `BTreeCursor.SeekAsync` + `MoveNextAsync` range scans then approximate spatial proximity queries.

---

## Why a Hilbert Curve as the B+tree Key?

A B+tree is inherently one-dimensional — it stores key–value pairs sorted by a single `long` key. Geographic data, however, is two-dimensional (latitude × longitude). To bridge this gap, we use the **Hilbert curve**, a continuous space-filling curve that maps 2D coordinates to a 1D index while preserving **spatial locality**:

| Property | Detail |
|---|---|
| **Curve type** | Hilbert (better locality than Z-order / Morton) |
| **Resolution** | 28 bits per axis → 56-bit key |
| **Grid cells** | ~268 million per axis (≈ 0.07 m at equator) |
| **Key 0** | Reserved for the superblock (root page IDs) |
| **Key offset** | All Hilbert keys are +1 to avoid collision with the superblock |

Because the Hilbert curve folds 2D space into 1D without the "jump" artefacts of Z-order curves, a B+tree key range `[center - delta, center + delta]` captures most nearby geographic points. The remaining false positives are removed by a post-filter (Haversine distance for nearby queries, coordinate bounds for bounding-box queries).

```
2D geographic space              1D B+tree key space

  ┌─────────────────┐
  │ ╭──╮╭──╮        │            ┌───────────────────────────────────┐
  │ │  ││  │        │   Encode   │ 0 │ ... │ key-δ │ key │ key+δ │ … │
  │ ╰──╯╰──╯        │  ──────▶   └───────────────────────────────────┘
  │ ╭──╮╭──╮        │                        ◄─── range scan ───►
  │ │  ││  │        │
  │ ╰──╯╰──╯        │            SeekAsync(key-δ) → MoveNextAsync
  └─────────────────┘            until key > key+δ
```

---

## Architecture

```
┌──────────────────────────────────────────────────────────────┐
│                        Program.cs                            │
│                  "dotnet run" → CLI REPL                     │
│                  "dotnet run serve" → Web server             │
└──────────┬──────────────────────────────┬────────────────────┘
           │                              │
    ┌──────▼──────┐              ┌────────▼────────┐
    │  CLI / REPL │              │  ASP.NET Minimal│
    │   Commands  │              │    API Host     │
    └──────┬──────┘              └────────┬────────┘
           │                              │
    ┌──────▼──────────────────────────────▼──────┐
    │         ISpatialIndexApi  (interface)      │
    │  ┌──────────────┐   ┌────────────────────┐ │
    │  │ InProcess    │   │ HttpSpatialIndex   │ │
    │  │ ApiClient    │   │ ApiClient          │ │
    │  └──────┬───────┘   └────────┬───────────┘ │
    └─────────┼────────────────────┼─────────────┘
              │                    │
    ┌─────────▼────────────────────▼─────────────┐
    │        SpatialIndexApiService              │
    │        (SemaphoreSlim gate, lazy init)     │
    └─────────────────┬──────────────────────────┘
                      │
    ┌─────────────────▼──────────────────────────┐
    │          SpatialIndexDatabase              │
    │    QueryNearbyAsync / QueryBoundingBoxAsync│
    └─────────────────┬──────────────────────────┘
                      │
    ┌─────────────────▼───────────────────────────┐
    │           SpatialIndexStore                 │
    │   Single B+tree: HilbertKey → JSON(Point)   │
    │   ScanRangeAsync (SeekAsync + MoveNextAsync)│
    └─────────────────┬───────────────────────────┘
                      │
    ┌─────────────────▼──────────────────────────┐
    │          CSharpDB.Storage                  │
    │      BTree · BTreeCursor · Pager           │
    └────────────────────────────────────────────┘
```

### Project Structure

```
CSharpDB.SpatialIndex/
├── Core/
│   ├── HilbertCurve.cs           # 2D → 1D mapping (encode, decode, range estimation)
│   ├── GeoMath.cs                # Haversine distance, bounding-box helpers
│   ├── SpatialPoint.cs           # Data model
│   ├── SpatialQueryResult.cs     # Query result + statistics (scan efficiency)
│   ├── SpatialIndexStore.cs      # B+tree CRUD + ScanRangeAsync (cursor showcase)
│   └── SpatialIndexDatabase.cs   # High-level API (nearby + bbox queries)
├── Api/
│   ├── ISpatialIndexApi.cs       # Interface + request DTOs
│   ├── SpatialIndexApiService.cs # Thread-safe service wrapper
│   └── Clients/
│       ├── InProcessSpatialIndexApiClient.cs
│       └── HttpSpatialIndexApiClient.cs
├── Hosting/
│   └── SpatialIndexWebHost.cs    # ASP.NET Core Minimal API endpoints
├── Infrastructure/
│   └── SpatialIndexDatabaseUtility.cs
├── Cli/
│   ├── AnsiConsoleWriter.cs      # ANSI-coloured terminal output
│   ├── SpatialIndexConsolePresenter.cs  # Tables + ASCII scatter map
│   ├── SpatialIndexSampleRunner.cs      # 60 world POIs demo
│   ├── Commands/
│   │   ├── IReplCommand.cs
│   │   ├── SampleCommand.cs
│   │   ├── SpatialIndexCommand.cs
│   │   └── PrefixedReplCommand.cs
│   └── Repl/
│       └── ReplHost.cs
└── Program.cs                    # Entry point
```

---

## The Single B+tree Design

| B+tree | Key | Value | Purpose |
|---|---|---|---|
| `_data` | `HilbertCurve.Encode(lat, lon)` → `long` | `JSON(SpatialPoint)` | Stores all geographic points |

**Key 0** is reserved as a **superblock** that stores the root page ID. All Hilbert keys are `+1` to avoid collision.

### Core Query Pattern: `ScanRangeAsync`

```csharp
// SpatialIndexStore.cs — the B+tree cursor showcase
public async Task<(List<SpatialPoint> Points, int Scanned)> ScanRangeAsync(
    long startKey, long endKey, string? categoryFilter, int maxResults, CancellationToken ct)
{
    var results = new List<SpatialPoint>();
    var scanned = 0;
    var cursor = _data.CreateCursor();

    if (!await cursor.SeekAsync(startKey, ct))
        return (results, scanned);

    do
    {
        if (cursor.CurrentKey == 0) continue;       // skip superblock
        if (cursor.CurrentKey > endKey) break;       // past the range

        scanned++;

        var point = JsonSerializer.Deserialize<SpatialPoint>(
            cursor.CurrentValue.Span, JsonOptions);

        if (categoryFilter is not null &&
            !string.Equals(point.Category, categoryFilter,
                StringComparison.OrdinalIgnoreCase))
            continue;

        results.Add(point);
        if (results.Count >= maxResults) break;
    }
    while (await cursor.MoveNextAsync(ct));

    return (results, scanned);
}
```

---

## Spatial Query Strategies

### 1. Nearby Query (Radius Search)

```
QueryNearbyAsync(lat, lon, radiusKm)
   │
   ├─ HilbertCurve.Encode(lat, lon) → centerKey
   ├─ HilbertCurve.EstimateRadiusDelta(lat, radiusKm) → delta
   │     • Converts km → degrees → Hilbert grid delta
   │     • Samples 4 cardinal directions, takes max delta
   │     • Applies 4× conservative multiplier (over-scan is OK)
   │
   ├─ ScanRangeAsync(centerKey - delta, centerKey + delta)
   │     • SeekAsync → MoveNextAsync loop
   │
   └─ Post-filter: Haversine distance ≤ radiusKm
      Sort by distance ascending
```

### 2. Bounding Box Query

```
QueryBoundingBoxAsync(minLat, minLon, maxLat, maxLon)
   │
   ├─ HilbertCurve.BoundingBoxRange(minLat, minLon, maxLat, maxLon)
   │     • Samples 16×16 grid points across the box
   │     • Returns [minKey, maxKey] across all samples
   │
   ├─ ScanRangeAsync(minKey, maxKey)
   │
   └─ Post-filter: lat/lon within actual bounds
```

### Scan Efficiency

Both strategies intentionally **over-scan** and **post-filter**. The `SpatialQueryStatistics` object reports:

| Metric | Meaning |
|---|---|
| `TotalResults` | Points that passed the post-filter |
| `ScannedEntries` | Total B+tree entries visited in the range scan |
| `Efficiency` | `TotalResults / ScannedEntries` (1.0 = perfect) |
| `MinDistanceKm` | Nearest result from query centre |
| `MaxDistanceKm` | Farthest result from query centre |
| `BoundingBoxAreaSqKm` | Area covered (bbox queries only) |

---

## Running the Sample

### Prerequisites

- .NET 10 SDK
- CSharpDB.Storage project (referenced in the solution)

### CLI Mode (Interactive REPL)

```bash
dotnet run --project samples/storage-tutorials/examples/CSharpDB.SpatialIndex/CSharpDB.SpatialIndex.csproj
```

```
spatialdb> sample           # Load 60 world POIs (landmarks, cities, restaurants)
spatialdb> nearby 48.8566 2.3522 50        # Points within 50 km of Paris
spatialdb> nearby 40.7128 -74.006 500      # Points within 500 km of NYC
spatialdb> nearby 41.9028 12.4964 200 category:landmark   # Landmarks near Rome
spatialdb> bbox 35 -10 70 40               # Europe bounding box
spatialdb> add 34.0522 -118.2437 "Los Angeles" category:city
spatialdb> count                            # Total stored points
spatialdb> get 123456789                    # Lookup by Hilbert key
spatialdb> delete 123456789                 # Delete by Hilbert key
spatialdb> reset                            # Wipe database
spatialdb> help                             # Show all commands
spatialdb> exit                             # Quit
```

### Web Mode (REST API + Dashboard)

```bash
dotnet run --project samples/storage-tutorials/examples/CSharpDB.SpatialIndex/CSharpDB.SpatialIndex.csproj -- serve
```

Open **http://localhost:62488** for the interactive dashboard with:
- 🗺️ Canvas scatter plot with geographic coordinate visualization
- 📍 Nearby and bounding box query forms with preset quick queries
- 📊 Real-time query statistics (results, scanned entries, efficiency)
- 📋 Sortable results table with category badges and distance
- ⚡ One-click sample data loading (25 landmarks, 20 cities, 15 restaurants)

### REST API Endpoints

| Method | Endpoint | Description |
|---|---|---|
| `POST` | `/api/spatial/points` | Add a geographic point |
| `GET` | `/api/spatial/nearby?lat=…&lon=…&radiusKm=…` | Nearby query (radius search) |
| `GET` | `/api/spatial/bbox?minLat=…&minLon=…&maxLat=…&maxLon=…` | Bounding box query |
| `GET` | `/api/spatial/points/{hilbertKey}` | Get a point by Hilbert key |
| `DELETE` | `/api/spatial/points/{hilbertKey}` | Delete a point |
| `GET` | `/api/spatial/count` | Count stored points |
| `POST` | `/api/spatial/reset` | Reset the database |

#### Add a point

```bash
curl -X POST http://localhost:62488/api/spatial/points \
  -H "Content-Type: application/json" \
  -d '{"latitude":48.8584,"longitude":2.2945,"name":"Eiffel Tower","category":"landmark"}'
```

#### Nearby query

```bash
curl "http://localhost:62488/api/spatial/nearby?lat=48.8566&lon=2.3522&radiusKm=50"
```

#### Bounding box query

```bash
curl "http://localhost:62488/api/spatial/bbox?minLat=35&minLon=-10&maxLat=70&maxLon=40"
```

---

## The Hilbert Curve Implementation

The `HilbertCurve` class implements the standard iterative Hilbert curve algorithm with quadrant rotation:

```csharp
// Encode: (lat, lon) → long key
public static long Encode(double latitude, double longitude)
{
    var x = NormalizeLongitude(longitude);   // [-180, 180] → [0, 2^28 - 1]
    var y = NormalizeLatitude(latitude);      // [-90,  90]  → [0, 2^28 - 1]
    return XYToHilbert(x, y) + 1;            // +1 to reserve key 0 for superblock
}

// Decode: long key → (lat, lon)
public static (double Latitude, double Longitude) Decode(long hilbertKey)
{
    var (x, y) = HilbertToXY(hilbertKey - 1);
    return (DenormalizeLatitude(y), DenormalizeLongitude(x));
}
```

### Coordinate Normalisation

| Input Range | Grid Range | Formula |
|---|---|---|
| Latitude: `[-90, 90]` | `[0, 2²⁸ - 1]` | `(lat + 90) / 180 × GridSize` |
| Longitude: `[-180, 180]` | `[0, 2²⁸ - 1]` | `(lon + 180) / 360 × GridSize` |

### Range Estimation

For **nearby queries**, `EstimateRadiusDelta` converts a radius in km to an approximate Hilbert key delta:

1. Convert km → degrees (1° latitude ≈ 111.32 km)
2. Account for longitude compression at the given latitude (cos factor)
3. Sample the 4 cardinal directions at the radius distance
4. Take the maximum absolute key difference
5. Apply 4× safety multiplier (conservative over-scan)

For **bounding box queries**, `BoundingBoxRange` samples a 16×16 grid of points across the box and returns the [min, max] Hilbert key range.

---

## Sample Data

The `SampleCommand` loads 60 real-world geographic points across three categories:

| Category | Count | Examples |
|---|---|---|
| **Landmarks** | 25 | Eiffel Tower, Colosseum, Taj Mahal, Great Wall, Machu Picchu |
| **Cities** | 20 | Paris, Tokyo, New York, Sydney, Cairo, Singapore, Nairobi |
| **Restaurants** | 15 | Le Jules Verne (Paris), Roscioli (Rome), Curry 36 (Berlin) |

The sample runner then demonstrates:
1. **Nearby Paris (50 km)** — finds restaurants, landmarks, and the city itself
2. **Nearby New York (500 km)** — captures the Statue of Liberty and nearby cities
3. **Europe bounding box (35°N–70°N, 10°W–40°E)** — shows all European POIs
4. **Landmarks near Rome (200 km)** — category-filtered nearby query

---

## Key Takeaways

- **One B+tree, one key** — a Hilbert curve encodes 2D data into a 1D `long` key
- **Range scans are spatial queries** — `SeekAsync` + `MoveNextAsync` approximates proximity
- **Over-scan + post-filter** — conservative range estimation with exact post-filtering guarantees correct results
- **Efficiency metrics** — `ScannedEntries / TotalResults` shows how well the curve approximation performs
- **No external dependencies** — pure C# implementation of both the Hilbert curve and Haversine formula
