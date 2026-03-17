using CSharpDB.SpatialIndex;

internal sealed class SpatialIndexSampleRunner
{
    private readonly AnsiConsoleWriter _console;
    private readonly SpatialIndexConsolePresenter _presenter;

    public SpatialIndexSampleRunner(AnsiConsoleWriter console, SpatialIndexConsolePresenter presenter)
    {
        _console = console;
        _presenter = presenter;
    }

    public async Task RunAsync(ISpatialIndexApi api, CancellationToken ct)
    {
        await api.ResetAsync(ct);

        await _console.WriteSectionAsync("Sample scenario");
        await _console.WriteInfoAsync("Resetting database and loading world geographic data.");
        await _console.WriteBlankLineAsync();

        // ── World Landmarks ──────────────────────────────────
        await _console.WriteSectionAsync("1. Loading world landmarks");
        var landmarks = new (double Lat, double Lon, string Name, string Country)[]
        {
            (48.8584, 2.2945, "Eiffel Tower", "France"),
            (40.6892, -74.0445, "Statue of Liberty", "USA"),
            (51.5007, -0.1246, "Big Ben", "UK"),
            (41.8902, 12.4922, "Colosseum", "Italy"),
            (27.1751, 78.0421, "Taj Mahal", "India"),
            (-33.8568, 151.2153, "Sydney Opera House", "Australia"),
            (-13.1631, -72.5450, "Machu Picchu", "Peru"),
            (40.4319, 116.5704, "Great Wall", "China"),
            (-22.9519, -43.2105, "Christ the Redeemer", "Brazil"),
            (30.3285, 35.4444, "Petra", "Jordan"),
            (20.6843, -88.5678, "Chichen Itza", "Mexico"),
            (37.9715, 23.7267, "Acropolis", "Greece"),
            (48.8606, 2.3376, "Louvre Museum", "France"),
            (41.4036, 2.1744, "Sagrada Familia", "Spain"),
            (52.5163, 13.3777, "Brandenburg Gate", "Germany"),
            (35.6586, 139.7454, "Tokyo Tower", "Japan"),
            (55.7520, 37.6175, "Red Square", "Russia"),
            (29.9792, 31.1342, "Pyramids of Giza", "Egypt"),
            (43.7230, 10.3966, "Leaning Tower of Pisa", "Italy"),
            (59.3293, 18.0686, "Stockholm Palace", "Sweden"),
            (38.8977, -77.0365, "White House", "USA"),
            (51.1789, -1.8262, "Stonehenge", "UK"),
            (64.1466, -21.9426, "Hallgrimskirkja", "Iceland"),
            (-25.3444, 131.0369, "Uluru", "Australia"),
            (47.5576, 10.7498, "Neuschwanstein Castle", "Germany"),
        };

        foreach (var (lat, lon, name, country) in landmarks)
        {
            await api.AddAsync(lat, lon, name, "landmark", null,
                new Dictionary<string, string> { ["country"] = country }, ct);
        }

        await _console.WriteSuccessAsync($"{landmarks.Length} landmarks loaded.");
        await _console.WriteBlankLineAsync();

        // ── Major Cities ─────────────────────────────────────
        await _console.WriteSectionAsync("2. Loading major cities");
        var cities = new (double Lat, double Lon, string Name, string Country)[]
        {
            (40.7128, -74.0060, "New York", "USA"),
            (51.5074, -0.1278, "London", "UK"),
            (35.6762, 139.6503, "Tokyo", "Japan"),
            (48.8566, 2.3522, "Paris", "France"),
            (-33.8688, 151.2093, "Sydney", "Australia"),
            (30.0444, 31.2357, "Cairo", "Egypt"),
            (-22.9068, -43.1729, "Rio de Janeiro", "Brazil"),
            (55.7558, 37.6173, "Moscow", "Russia"),
            (39.9042, 116.4074, "Beijing", "China"),
            (28.6139, 77.2090, "New Delhi", "India"),
            (41.0082, 28.9784, "Istanbul", "Turkey"),
            (-34.6037, -58.3816, "Buenos Aires", "Argentina"),
            (52.5200, 13.4050, "Berlin", "Germany"),
            (41.3851, 2.1734, "Barcelona", "Spain"),
            (45.4642, 9.1900, "Milan", "Italy"),
            (37.7749, -122.4194, "San Francisco", "USA"),
            (1.3521, 103.8198, "Singapore", "Singapore"),
            (25.2048, 55.2708, "Dubai", "UAE"),
            (-1.2921, 36.8219, "Nairobi", "Kenya"),
            (59.9139, 10.7522, "Oslo", "Norway"),
        };

        foreach (var (lat, lon, name, country) in cities)
        {
            await api.AddAsync(lat, lon, name, "city", null,
                new Dictionary<string, string> { ["country"] = country }, ct);
        }

        await _console.WriteSuccessAsync($"{cities.Length} cities loaded.");
        await _console.WriteBlankLineAsync();

        // ── European Restaurants ─────────────────────────────
        await _console.WriteSectionAsync("3. Loading European restaurants");
        var restaurants = new (double Lat, double Lon, string Name, string City)[]
        {
            (48.8645, 2.3500, "Le Comptoir du Pantheon", "Paris"),
            (48.8530, 2.3499, "Chez Janou", "Paris"),
            (48.8738, 2.2950, "Le Jules Verne", "Paris"),
            (41.8925, 12.4853, "Roscioli", "Rome"),
            (41.8986, 12.4733, "Pizzeria da Baffetto", "Rome"),
            (51.5113, -0.1224, "The Barbary", "London"),
            (51.4994, -0.1272, "Regency Cafe", "London"),
            (41.3870, 2.1700, "Can Culleretes", "Barcelona"),
            (41.3920, 2.1650, "La Boqueria", "Barcelona"),
            (52.5230, 13.4015, "Curry 36", "Berlin"),
            (52.5070, 13.3910, "Tim Raue", "Berlin"),
            (45.4640, 9.1880, "Luini Panzerotti", "Milan"),
            (47.3700, 8.5393, "Zeughauskeller", "Zurich"),
            (50.0875, 14.4213, "Lokál", "Prague"),
            (60.1699, 24.9384, "Ravintola Savoy", "Helsinki"),
        };

        foreach (var (lat, lon, name, city) in restaurants)
        {
            await api.AddAsync(lat, lon, name, "restaurant", null,
                new Dictionary<string, string> { ["city"] = city }, ct);
        }

        await _console.WriteSuccessAsync($"{restaurants.Length} restaurants loaded.");
        await _console.WriteBlankLineAsync();

        // ── Statistics ───────────────────────────────────────
        await _console.WriteSectionAsync("4. Database statistics");
        var count = await api.CountAsync(ct);
        await _console.WriteInfoAsync($"Total points stored: {count:N0}");
        await _console.WriteBlankLineAsync();

        // ── Nearby: Paris 50km ───────────────────────────────
        await _console.WriteSectionAsync("5. Nearby query: within 50 km of Paris");
        var nearParis = await api.QueryNearbyAsync(48.8566, 2.3522, 50, null, 100, ct);
        await _presenter.ShowNearbyResultAsync(nearParis, 48.8566, 2.3522);
        await _console.WriteBlankLineAsync();

        // ── Nearby: New York 500km ───────────────────────────
        await _console.WriteSectionAsync("6. Nearby query: within 500 km of New York");
        var nearNY = await api.QueryNearbyAsync(40.7128, -74.0060, 500, null, 100, ct);
        await _presenter.ShowNearbyResultAsync(nearNY, 40.7128, -74.0060);
        await _console.WriteBlankLineAsync();

        // ── Bounding box: Europe ─────────────────────────────
        await _console.WriteSectionAsync("7. Bounding box query: Europe (35N-70N, 10W-40E)");
        var europe = await api.QueryBoundingBoxAsync(35, -10, 70, 40, null, 1000, ct);
        await _presenter.ShowBboxResultAsync(europe);
        await _console.WriteBlankLineAsync();

        // ── Nearby: landmarks only near Rome ─────────────────
        await _console.WriteSectionAsync("8. Nearby landmarks within 200 km of Rome");
        var nearRome = await api.QueryNearbyAsync(41.9028, 12.4964, 200, "landmark", 50, ct);
        await _presenter.ShowNearbyResultAsync(nearRome, 41.9028, 12.4964);
        await _console.WriteBlankLineAsync();

        await _console.WriteSuccessAsync("Sample completed.");
    }
}
