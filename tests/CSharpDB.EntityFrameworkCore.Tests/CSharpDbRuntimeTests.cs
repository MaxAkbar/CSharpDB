using System.Data.Common;
using System.Runtime.CompilerServices;
using CSharpDB.Data;
using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Diagnostics;

namespace CSharpDB.EntityFrameworkCore.Tests;

[Collection("ConnectionPoolState")]
public sealed class CSharpDbRuntimeTests : IAsyncLifetime
{
    private readonly string _workspace = Path.Combine(Path.GetTempPath(), $"csharpdb_efcore_runtime_{Guid.NewGuid():N}");

    private static CancellationToken Ct => TestContext.Current.CancellationToken;

    public async ValueTask InitializeAsync()
    {
        Directory.CreateDirectory(_workspace);
        await CSharpDbConnection.ClearAllPoolsAsync();
    }

    public async ValueTask DisposeAsync()
    {
        await CSharpDbConnection.ClearAllPoolsAsync();
        DeleteDirectoryIfExists(_workspace);
    }

    [Fact]
    public async Task EnsureCreated_FileBackedCrudAndTypeRoundTrip_Succeeds()
    {
        string dbPath = GetDbPath("runtime");

        await using var db = new ProviderRuntimeContext($"Data Source={dbPath}");
        await db.Database.EnsureCreatedAsync(Ct);

        var person = new PersonRecord
        {
            Name = "Ada",
            Active = true,
            Score = 12.5,
            Visits = 9,
            Status = PersonStatus.Active,
            GuidValue = Guid.Parse("11111111-1111-1111-1111-111111111111"),
            CreatedAt = new DateTime(2026, 4, 16, 12, 34, 56, DateTimeKind.Utc),
            ObservedAt = new DateTimeOffset(2026, 4, 16, 5, 34, 56, TimeSpan.FromHours(-7)),
            Birthday = new DateOnly(2000, 1, 2),
            Alarm = new TimeOnly(7, 8, 9),
            Payload = [0x01, 0x02, 0x03, 0xFE],
        };

        db.People.Add(person);
        db.Widgets.Add(new ManualWidget
        {
            Id = 42,
            Name = "manually-assigned",
        });

        await db.SaveChangesAsync(Ct);

        Assert.True(person.Id > 0);

        PersonRecord loaded = await db.People.AsNoTracking().SingleAsync(p => p.Id == person.Id, Ct);
        ManualWidget widget = await db.Widgets.AsNoTracking().SingleAsync(w => w.Id == 42, Ct);

        Assert.Equal("Ada", loaded.Name);
        Assert.True(loaded.Active);
        Assert.Equal(12.5, loaded.Score);
        Assert.Equal(9L, loaded.Visits);
        Assert.Equal(PersonStatus.Active, loaded.Status);
        Assert.Equal(person.GuidValue, loaded.GuidValue);
        Assert.Equal(person.CreatedAt, loaded.CreatedAt);
        Assert.Equal(person.ObservedAt, loaded.ObservedAt);
        Assert.Equal(person.Birthday, loaded.Birthday);
        Assert.Equal(person.Alarm, loaded.Alarm);
        Assert.Equal(person.Payload, loaded.Payload);
        Assert.Equal("manually-assigned", widget.Name);

        person.Name = "Grace";
        await db.SaveChangesAsync(Ct);

        Assert.Equal(1, await db.People.CountAsync(p => p.Name == "Grace", Ct));

        db.Remove(person);
        await db.SaveChangesAsync(Ct);

        Assert.Equal(0, await db.People.CountAsync(Ct));
    }

    [Fact]
    public async Task Queries_IncludePaginationAndContainsOverConstantsAndParameters_Succeed()
    {
        string dbPath = GetDbPath("queries");

        await using var db = new ProviderRuntimeContext($"Data Source={dbPath}");
        await db.Database.EnsureCreatedAsync(Ct);

        db.Blogs.AddRange(
            new Blog
            {
                Name = "Alpha",
                Posts =
                [
                    new Post { Title = "Welcome" },
                    new Post { Title = "GettingStarted" },
                ],
            },
            new Blog
            {
                Name = "Beta",
                Posts =
                [
                    new Post { Title = "Roadmap" },
                ],
            },
            new Blog
            {
                Name = "Gamma",
            });

        await db.SaveChangesAsync(Ct);

        string alphaName = await db.Blogs
            .Where(blog => blog.Name == "Alpha")
            .Select(blog => blog.Name)
            .SingleAsync(Ct);

        List<Blog> blogsWithPosts = await db.Blogs
            .OrderBy(blog => blog.Name)
            .Include(blog => blog.Posts)
            .ToListAsync(Ct);

        List<string> pagedNames = await db.Blogs
            .OrderBy(blog => blog.Name)
            .Skip(1)
            .Take(1)
            .Select(blog => blog.Name)
            .ToListAsync(Ct);

        string[] allowedNames = ["Alpha", "Gamma"];

        List<string> parameterContains = await db.Blogs
            .Where(blog => allowedNames.Contains(blog.Name))
            .OrderBy(blog => blog.Name)
            .Select(blog => blog.Name)
            .ToListAsync(Ct);

        List<string> constantContains = await db.Blogs
            .Where(blog => new[] { "Alpha", "Gamma" }.Contains(blog.Name))
            .OrderBy(blog => blog.Name)
            .Select(blog => blog.Name)
            .ToListAsync(Ct);

        Blog alpha = Assert.Single(blogsWithPosts, blog => blog.Name == "Alpha");

        Assert.Equal("Alpha", alphaName);
        Assert.Equal(2, alpha.Posts.Count);
        Assert.Equal(["Beta"], pagedNames);
        Assert.Equal(["Alpha", "Gamma"], parameterContains);
        Assert.Equal(["Alpha", "Gamma"], constantContains);
        Assert.True(await db.Posts.AnyAsync(post => post.Title == "Welcome", Ct));
        Assert.Equal(3, await db.Posts.CountAsync(Ct));
    }

    [Fact]
    public async Task Queries_StringFunctionsTranslateInPredicatesProjectionsAndOrdering()
    {
        string dbPath = GetDbPath("string-functions");

        await using var db = new ProviderRuntimeContext($"Data Source={dbPath}");
        await db.Database.EnsureCreatedAsync(Ct);
        db.Blogs.AddRange(
            new Blog { Name = "  Alpha  " },
            new Blog { Name = "beta" },
            new Blog { Name = "Gamma" },
            new Blog { Name = "I" });
        await db.SaveChangesAsync(Ct);
        string? removal = null;

        var query = db.Blogs
            .Where(blog => blog.Name.Trim().ToLowerInvariant() == "alpha")
            .OrderBy(blog => blog.Name.Trim().Length)
            .Select(blog => new
            {
                blog.Name,
                Length = blog.Name.Length,
                Lower = blog.Name.ToLower(),
                Upper = blog.Name.ToUpperInvariant(),
                Trimmed = blog.Name.Trim(),
                LeftTrimmed = blog.Name.TrimStart(),
                RightTrimmed = blog.Name.TrimEnd(),
                Replaced = blog.Name.Replace("a", "@"),
                Removed = blog.Name.Replace("a", removal),
                NullableTrimmed = blog.OptionalText!.Trim(),
                FirstTwo = blog.Name.Trim().Substring(0, 2),
                Tail = blog.Name.Trim().Substring(2),
            });

        string sql = query.ToQueryString();
        Assert.Contains("LENGTH(", sql, StringComparison.Ordinal);
        Assert.Contains("LOWER(", sql, StringComparison.Ordinal);
        Assert.Contains("UPPER(", sql, StringComparison.Ordinal);
        Assert.Contains("TRIM(", sql, StringComparison.Ordinal);
        Assert.Contains("LTRIM(", sql, StringComparison.Ordinal);
        Assert.Contains("RTRIM(", sql, StringComparison.Ordinal);
        Assert.Contains("REPLACE(", sql, StringComparison.Ordinal);
        Assert.Contains("COALESCE(", sql, StringComparison.Ordinal);
        Assert.Contains("SUBSTRING(", sql, StringComparison.Ordinal);

        var translated = Assert.Single(await query.ToListAsync(Ct));
        Assert.Equal("  Alpha  ", translated.Name);
        Assert.Equal(9, translated.Length);
        Assert.Equal("  alpha  ", translated.Lower);
        Assert.Equal("  ALPHA  ", translated.Upper);
        Assert.Equal("Alpha", translated.Trimmed);
        Assert.Equal("Alpha  ", translated.LeftTrimmed);
        Assert.Equal("  Alpha", translated.RightTrimmed);
        Assert.Equal("  Alph@  ", translated.Replaced);
        Assert.Equal("  Alph  ", translated.Removed);
        Assert.Null(translated.NullableTrimmed);
        Assert.Equal("Al", translated.FirstTwo);
        Assert.Equal("pha", translated.Tail);

        string serverLower = await db.Blogs
            .Where(blog => blog.Name == "I")
            .Select(blog => blog.Name.ToLower())
            .SingleAsync(Ct);
        Assert.Equal("i", serverLower);
    }

    [Fact]
    public async Task Queries_DateAndTimeComponentsTranslateInPredicatesAndProjections()
    {
        string dbPath = GetDbPath("temporal-components");

        await using var db = new ProviderRuntimeContext($"Data Source={dbPath}");
        await db.Database.EnsureCreatedAsync(Ct);
        db.People.Add(new PersonRecord
        {
            Name = "Temporal",
            Active = true,
            Score = 1.5,
            Visits = 2,
            Status = PersonStatus.Active,
            GuidValue = Guid.Parse("22222222-2222-2222-2222-222222222222"),
            CreatedAt = new DateTime(2026, 7, 18, 9, 23, 45, DateTimeKind.Utc),
            ObservedAt = new DateTimeOffset(2026, 7, 18, 2, 23, 45, TimeSpan.FromHours(-7)),
            Birthday = new DateOnly(2000, 1, 2),
            Alarm = new TimeOnly(7, 8, 9),
            Payload = [0x01],
        });
        await db.SaveChangesAsync(Ct);

        var query = db.People
            .Where(person =>
                person.CreatedAt.Year == 2026 &&
                person.Birthday.Month == 1 &&
                person.Alarm.Hour == 7)
            .Select(person => new
            {
                CreatedYear = person.CreatedAt.Year,
                CreatedMonth = person.CreatedAt.Month,
                CreatedDay = person.CreatedAt.Day,
                CreatedHour = person.CreatedAt.Hour,
                CreatedMinute = person.CreatedAt.Minute,
                CreatedSecond = person.CreatedAt.Second,
                BirthYear = person.Birthday.Year,
                BirthMonth = person.Birthday.Month,
                BirthDay = person.Birthday.Day,
                AlarmHour = person.Alarm.Hour,
                AlarmMinute = person.Alarm.Minute,
                AlarmSecond = person.Alarm.Second,
            });

        string sql = query.ToQueryString();
        Assert.Contains("YEAR(", sql, StringComparison.Ordinal);
        Assert.Contains("MONTH(", sql, StringComparison.Ordinal);
        Assert.Contains("DAY(", sql, StringComparison.Ordinal);
        Assert.Contains("HOUR(", sql, StringComparison.Ordinal);
        Assert.Contains("MINUTE(", sql, StringComparison.Ordinal);
        Assert.Contains("SECOND(", sql, StringComparison.Ordinal);

        var translated = Assert.Single(await query.ToListAsync(Ct));
        Assert.Equal(2026, translated.CreatedYear);
        Assert.Equal(7, translated.CreatedMonth);
        Assert.Equal(18, translated.CreatedDay);
        Assert.Equal(9, translated.CreatedHour);
        Assert.Equal(23, translated.CreatedMinute);
        Assert.Equal(45, translated.CreatedSecond);
        Assert.Equal(2000, translated.BirthYear);
        Assert.Equal(1, translated.BirthMonth);
        Assert.Equal(2, translated.BirthDay);
        Assert.Equal(7, translated.AlarmHour);
        Assert.Equal(8, translated.AlarmMinute);
        Assert.Equal(9, translated.AlarmSecond);
    }

    [Fact]
    public async Task Queries_DoubleMathFunctionsTranslateInPredicatesProjectionsAndOrdering()
    {
        string dbPath = GetDbPath("double-math");

        await using var db = new ProviderRuntimeContext($"Data Source={dbPath}");
        await db.Database.EnsureCreatedAsync(Ct);
        db.People.AddRange(
            CreateNumericPerson("Negative", -12.55, 1),
            CreateNumericPerson("Positive", 3.25, 2, optionalScore: -3.25),
            CreateNumericPerson("Midpoint", 2.5, 3));
        await db.SaveChangesAsync(Ct);

        var query = db.People
            .Where(person => Math.Abs(person.Score) > 10)
            .OrderBy(person => Math.Floor(Math.Abs(person.Score)))
            .Select(person => new
            {
                person.Score,
                Absolute = Math.Abs(person.Score),
                Rounded = Math.Round(Math.Abs(person.Score)),
                Floor = Math.Floor(person.Score),
                Ceiling = Math.Ceiling(person.Score),
                Truncated = Math.Truncate(person.Score),
                Sign = Math.Sign(person.Score),
            });

        string sql = query.ToQueryString();
        Assert.Contains("ABS(", sql, StringComparison.Ordinal);
        Assert.Contains("ROUND(", sql, StringComparison.Ordinal);
        Assert.Contains("FLOOR(", sql, StringComparison.Ordinal);
        Assert.Contains("FIX(", sql, StringComparison.Ordinal);
        Assert.Contains("SGN(", sql, StringComparison.Ordinal);

        var translated = Assert.Single(await query.ToListAsync(Ct));
        Assert.Equal(-12.55, translated.Score, precision: 10);
        Assert.Equal(12.55, translated.Absolute, precision: 10);
        Assert.Equal(13, translated.Rounded);
        Assert.Equal(-13, translated.Floor);
        Assert.Equal(-12, translated.Ceiling);
        Assert.Equal(-12, translated.Truncated);
        Assert.Equal(-1, translated.Sign);

        string optionalMatch = await db.People
            .Where(person => Math.Abs(person.OptionalScore!.Value) > 0)
            .Select(person => person.Name)
            .SingleAsync(Ct);
        Assert.Equal("Positive", optionalMatch);
        Assert.False(await db.People
            .Where(person => person.OptionalScore == null)
            .AnyAsync(
                person => Math.Ceiling(person.OptionalScore!.Value) > 0,
                Ct));

        double midpoint = await db.People
            .Where(person => person.Name == "Midpoint")
            .Select(person => Math.Round(person.Score))
            .SingleAsync(Ct);
        Assert.Equal(2, midpoint);
    }

    [Fact]
    public async Task Queries_ScalarNumericAggregatesTranslateWithExpectedEmptyAndNullableSemantics()
    {
        string dbPath = GetDbPath("aggregates");
        var interceptor = new ReaderCountingInterceptor();

        await using var db = new ProviderRuntimeContext(
            $"Data Source={dbPath}",
            interceptor);
        await db.Database.EnsureCreatedAsync(Ct);
        db.People.AddRange(
            CreateNumericPerson("ActiveOne", 2.5, 2, active: true, optionalScore: 2.5),
            CreateNumericPerson("ActiveTwo", 7.5, 4, active: true),
            CreateNumericPerson("Inactive", -3, 8, optionalScore: null));
        await db.SaveChangesAsync(Ct);
        interceptor.Reset();

        IQueryable<PersonRecord> activePeople =
            db.People.Where(person => person.Active);

        Assert.Equal(2L, await activePeople.LongCountAsync(Ct));
        Assert.True(await activePeople.OrderBy(person => person.Id).Take(1).AnyAsync(Ct));
        Assert.False(await db.People.Where(person => person.Score > 100).AnyAsync(Ct));
        Assert.Equal(3, await activePeople.SumAsync(person => person.Id, Ct));
        Assert.Equal(10, await activePeople.SumAsync(person => person.Score, Ct));
        Assert.Equal(5, await activePeople.AverageAsync(person => person.Score, Ct));
        Assert.Equal(1, await activePeople.MinAsync(person => person.Id, Ct));
        Assert.Equal(2, await activePeople.MaxAsync(person => person.Id, Ct));
        Assert.Equal(2.5, await activePeople.MinAsync(person => person.Score, Ct));
        Assert.Equal(7.5, await activePeople.MaxAsync(person => person.Score, Ct));

        Assert.Equal(2.5, await activePeople.SumAsync(person => person.OptionalScore, Ct));
        Assert.Equal(2.5, await activePeople.AverageAsync(person => person.OptionalScore, Ct));
        Assert.Equal(2.5, await activePeople.MinAsync(person => person.OptionalScore, Ct));
        Assert.Equal(2.5, await activePeople.MaxAsync(person => person.OptionalScore, Ct));

        IQueryable<PersonRecord> empty =
            db.People.Where(person => person.Score > 100);
        IQueryable<PersonRecord> allNull =
            db.People.Where(person => !person.Active);

        Assert.Equal(0, await empty.CountAsync(Ct));
        Assert.Equal(0L, await empty.LongCountAsync(Ct));
        Assert.Equal(0, await empty.SumAsync(person => person.Id, Ct));
        Assert.Equal(0, await empty.SumAsync(person => person.Score, Ct));
        Assert.Equal(0, await empty.SumAsync(person => person.OptionalScore, Ct));
        Assert.Null(await empty.AverageAsync(person => person.OptionalScore, Ct));
        Assert.Null(await empty.MinAsync(person => person.OptionalScore, Ct));
        Assert.Null(await empty.MaxAsync(person => person.OptionalScore, Ct));
        int readerCommandsBeforeEmptyAverage = interceptor.ReaderCommandCount;
        InvalidOperationException emptyAverageException =
            await Assert.ThrowsAsync<InvalidOperationException>(
            () => empty.AverageAsync(person => person.Score, Ct));
        Assert.Equal(
            readerCommandsBeforeEmptyAverage + 1,
            interceptor.ReaderCommandCount);
        Assert.Contains(
            "Nullable object",
            emptyAverageException.Message,
            StringComparison.OrdinalIgnoreCase);

        int readerCommandsBeforeEmptyMin = interceptor.ReaderCommandCount;
        InvalidOperationException emptyMinException =
            await Assert.ThrowsAsync<InvalidOperationException>(
            () => empty.MinAsync(person => person.Id, Ct));
        Assert.Equal(
            readerCommandsBeforeEmptyMin + 1,
            interceptor.ReaderCommandCount);
        Assert.Contains(
            "Nullable object",
            emptyMinException.Message,
            StringComparison.OrdinalIgnoreCase);

        int readerCommandsBeforeEmptyMax = interceptor.ReaderCommandCount;
        InvalidOperationException emptyMaxException =
            await Assert.ThrowsAsync<InvalidOperationException>(
            () => empty.MaxAsync(person => person.Score, Ct));
        Assert.Equal(
            readerCommandsBeforeEmptyMax + 1,
            interceptor.ReaderCommandCount);
        Assert.Contains(
            "Nullable object",
            emptyMaxException.Message,
            StringComparison.OrdinalIgnoreCase);
        Assert.Equal(0, await allNull.SumAsync(person => person.OptionalScore, Ct));
        Assert.Null(await allNull.AverageAsync(person => person.OptionalScore, Ct));
        Assert.Null(await allNull.MinAsync(person => person.OptionalScore, Ct));
        Assert.Null(await allNull.MaxAsync(person => person.OptionalScore, Ct));

        string dispatchedSql = string.Join(
            Environment.NewLine,
            interceptor.ReaderCommandTexts);
        Assert.Contains("COUNT(", dispatchedSql, StringComparison.Ordinal);
        Assert.Contains("SUM(", dispatchedSql, StringComparison.Ordinal);
        Assert.Contains("AVG(", dispatchedSql, StringComparison.Ordinal);
        Assert.Contains("MIN(", dispatchedSql, StringComparison.Ordinal);
        Assert.Contains("MAX(", dispatchedSql, StringComparison.Ordinal);
        Assert.Contains("EXISTS", dispatchedSql, StringComparison.Ordinal);
        Assert.DoesNotContain("CAST(", dispatchedSql, StringComparison.Ordinal);
    }

    [Fact]
    public async Task Queries_GroupedAndDistinctAggregatesTranslateWithExpectedSemantics()
    {
        string dbPath = GetDbPath("grouped-distinct-aggregates");
        var interceptor = new ReaderCountingInterceptor();

        await using var db = new ProviderRuntimeContext(
            $"Data Source={dbPath}",
            interceptor);
        await db.Database.EnsureCreatedAsync(Ct);
        db.People.AddRange(
            CreateNumericPerson(
                "ActiveOne",
                2.5,
                1,
                active: true,
                optionalScore: 2.5,
                optionalRank: 1,
                status: PersonStatus.Active),
            CreateNumericPerson(
                "ActiveTwo",
                7.5,
                2,
                active: true,
                optionalScore: null,
                optionalRank: null,
                status: PersonStatus.Active),
            CreateNumericPerson(
                "ActiveDuplicate",
                7.5,
                3,
                active: true,
                optionalScore: 2.5,
                optionalRank: 1,
                status: PersonStatus.Active),
            CreateNumericPerson(
                "InactiveOne",
                -3,
                4,
                optionalScore: 1,
                optionalRank: 2,
                status: PersonStatus.Suspended),
            CreateNumericPerson(
                "InactiveDuplicate",
                -3,
                5,
                optionalScore: null,
                optionalRank: null,
                status: PersonStatus.Unknown));
        await db.SaveChangesAsync(Ct);
        interceptor.Reset();

        var groupedQuery = db.People
            .GroupBy(person => person.Active)
            .Select(group => new
            {
                Active = group.Key,
                Count = group.Count(),
                LongCount = group.LongCount(),
                Sum = group.Sum(person => person.Score),
                Average = group.Average(person => person.Score),
                Min = group.Min(person => person.Score),
                Max = group.Max(person => person.Score),
                OptionalSum =
                    group.Sum(person => person.OptionalScore),
                OptionalAverage =
                    group.Average(person => person.OptionalScore),
                DistinctCount = group
                    .Select(person => person.Id)
                    .Distinct()
                    .Count(),
                DistinctSum = group
                    .Select(person => person.Id)
                    .Distinct()
                    .Sum(),
            })
            .OrderByDescending(row => row.Count)
            .ThenBy(row => row.Active);

        string groupedSql = groupedQuery.ToQueryString();
        var grouped = await groupedQuery.ToListAsync(Ct);
        var active = Assert.Single(grouped, row => row.Active);
        var inactive = Assert.Single(grouped, row => !row.Active);

        Assert.Contains("GROUP BY", groupedSql, StringComparison.Ordinal);
        Assert.Contains("ORDER BY", groupedSql, StringComparison.Ordinal);
        Assert.Contains("COUNT(DISTINCT", groupedSql, StringComparison.Ordinal);
        Assert.True(grouped[0].Active);
        Assert.Equal(3, active.Count);
        Assert.Equal(3L, active.LongCount);
        Assert.Equal(17.5, active.Sum);
        Assert.Equal(17.5 / 3, active.Average, precision: 10);
        Assert.Equal(2.5, active.Min);
        Assert.Equal(7.5, active.Max);
        Assert.Equal(5, active.OptionalSum);
        Assert.Equal(2.5, active.OptionalAverage);
        Assert.Equal(3, active.DistinctCount);
        Assert.Equal(6, active.DistinctSum);
        Assert.Equal(2, inactive.Count);
        Assert.Equal(2L, inactive.LongCount);
        Assert.Equal(-6, inactive.Sum);
        Assert.Equal(-3, inactive.Average);
        Assert.Equal(-3, inactive.Min);
        Assert.Equal(-3, inactive.Max);
        Assert.Equal(1, inactive.OptionalSum);
        Assert.Equal(1, inactive.OptionalAverage);
        Assert.Equal(2, inactive.DistinctCount);
        Assert.Equal(9, inactive.DistinctSum);

        var filteredCompositeHavingQuery = db.People
            .Where(person => person.Score <= 7.5)
            .GroupBy(person => new
            {
                person.Active,
                person.Status,
            })
            .Where(group => group.Count() > 2)
            .Select(group => new
            {
                group.Key.Active,
                group.Key.Status,
                Count = group.Count(),
                Sum = group.Sum(person => person.Score),
            });
        string filteredCompositeHavingSql =
            filteredCompositeHavingQuery.ToQueryString();
        var filteredCompositeHaving =
            await filteredCompositeHavingQuery.SingleAsync(Ct);

        Assert.Contains(
            "GROUP BY",
            filteredCompositeHavingSql,
            StringComparison.Ordinal);
        Assert.Contains(
            "HAVING",
            filteredCompositeHavingSql,
            StringComparison.Ordinal);
        Assert.True(filteredCompositeHaving.Active);
        Assert.Equal(PersonStatus.Active, filteredCompositeHaving.Status);
        Assert.Equal(3, filteredCompositeHaving.Count);
        Assert.Equal(17.5, filteredCompositeHaving.Sum);

        var nullableKeyGroups = await db.People
            .GroupBy(person => person.OptionalRank)
            .Select(group => new
            {
                group.Key,
                Count = group.Count(),
            })
            .OrderBy(row => row.Key)
            .ToListAsync(Ct);
        Assert.Equal(3, nullableKeyGroups.Count);
        Assert.Null(nullableKeyGroups[0].Key);
        Assert.Equal(2, nullableKeyGroups[0].Count);
        Assert.Equal(1, nullableKeyGroups[1].Key);
        Assert.Equal(2, nullableKeyGroups[1].Count);
        Assert.Equal(2, nullableKeyGroups[2].Key);
        Assert.Equal(1, nullableKeyGroups[2].Count);

        var nullAggregateGroups = await db.People
            .GroupBy(person => person.Status)
            .Where(group =>
                group.Average(person => person.OptionalScore) ==
                null)
            .Select(group => group.Key)
            .ToListAsync(Ct);
        Assert.Equal(
            [PersonStatus.Unknown],
            nullAggregateGroups);

        var emptyGroups = await db.People
            .Where(person => person.Score > 100)
            .GroupBy(person => person.Active)
            .Select(group => new
            {
                group.Key,
                Count = group.Count(),
            })
            .ToListAsync(Ct);
        Assert.Empty(emptyGroups);

        IQueryable<int> distinctRanks =
            db.People.Select(person => person.Id).Distinct();
        IQueryable<int?> distinctOptionalRanks =
            db.People.Select(person => person.OptionalRank).Distinct();
        IQueryable<int> filteredDistinctRanks = db.People
            .Where(person => person.Score > 0)
            .Select(person => person.Id)
            .Distinct();
        IQueryable<int> emptyDistinctRanks = db.People
            .Where(person => person.Score > 100)
            .Select(person => person.Id)
            .Distinct();

        Assert.Equal(5, await distinctRanks.CountAsync(Ct));
        Assert.Equal(5L, await distinctRanks.LongCountAsync(Ct));
        Assert.Equal(15, await distinctRanks.SumAsync(Ct));
        Assert.Equal(1, await distinctRanks.MinAsync(Ct));
        Assert.Equal(5, await distinctRanks.MaxAsync(Ct));
        Assert.Equal(3, await filteredDistinctRanks.CountAsync(Ct));
        Assert.Equal(6, await filteredDistinctRanks.SumAsync(Ct));
        Assert.Equal(0, await emptyDistinctRanks.CountAsync(Ct));
        Assert.Equal(0, await emptyDistinctRanks.SumAsync(Ct));
        await AssertEmptyDistinctThrows(
            () => emptyDistinctRanks.MinAsync(Ct));
        await AssertEmptyDistinctThrows(
            () => emptyDistinctRanks.MaxAsync(Ct));

        int commandsBeforeNullableDistinctCount =
            interceptor.ReaderCommandCount;
        InvalidOperationException nullableDistinctCountException =
            await Assert.ThrowsAsync<InvalidOperationException>(
                () => distinctOptionalRanks.CountAsync(Ct));
        Assert.Contains(
            "CDBEF1004",
            nullableDistinctCountException.Message,
            StringComparison.Ordinal);
        Assert.Contains(
            "counts NULL once",
            nullableDistinctCountException.Message,
            StringComparison.Ordinal);
        Assert.Equal(
            commandsBeforeNullableDistinctCount,
            interceptor.ReaderCommandCount);

        string dispatchedSql = string.Join(
            Environment.NewLine,
            interceptor.ReaderCommandTexts);
        Assert.Contains("GROUP BY", dispatchedSql, StringComparison.Ordinal);
        Assert.Contains("COUNT(DISTINCT", dispatchedSql, StringComparison.Ordinal);
        Assert.Contains("SUM(DISTINCT", dispatchedSql, StringComparison.Ordinal);
        Assert.DoesNotContain("AVG(DISTINCT", dispatchedSql, StringComparison.Ordinal);
        Assert.Contains("MIN(", dispatchedSql, StringComparison.Ordinal);
        Assert.Contains("MAX(", dispatchedSql, StringComparison.Ordinal);
        Assert.DoesNotContain("FROM (", dispatchedSql, StringComparison.Ordinal);
        Assert.DoesNotContain("CAST(", dispatchedSql, StringComparison.Ordinal);

        async Task AssertEmptyDistinctThrows<T>(
            Func<Task<T>> operation)
        {
            int commandsBefore = interceptor.ReaderCommandCount;
            InvalidOperationException exception =
                await Assert.ThrowsAsync<InvalidOperationException>(
                    async () => _ = await operation());
            Assert.Equal(
                commandsBefore + 1,
                interceptor.ReaderCommandCount);
            Assert.Contains(
                "Nullable object",
                exception.Message,
                StringComparison.OrdinalIgnoreCase);
        }
    }

    [Fact]
    public async Task UnsupportedDistinctAggregateShapes_ReportBeforeCommandDispatch()
    {
        string dbPath =
            GetDbPath("unsupported-distinct-aggregates");
        var interceptor = new ReaderCountingInterceptor();

        await using var db = new ProviderRuntimeContext(
            $"Data Source={dbPath}",
            interceptor);
        await db.Database.EnsureCreatedAsync(Ct);
        db.People.Add(
            CreateNumericPerson(
                "Only",
                2.5,
                1,
                active: true,
                optionalScore: null));
        await db.SaveChangesAsync(Ct);
        interceptor.Reset();

        await AssertRejected(
            async () =>
            {
                _ = await db.People
                    .Select(person => person.OptionalScore)
                    .Distinct()
                    .CountAsync(Ct);
            },
            "counts NULL once");
        await AssertRejected(
            async () =>
            {
                _ = await db.People
                    .GroupBy(person => person.Active)
                    .Select(group => group
                        .Select(person => person.OptionalScore)
                        .Distinct()
                        .Count())
                    .ToListAsync(Ct);
            },
            "counts NULL once");
        await AssertRejected(
            async () =>
            {
                _ = await db.People
                    .Select(person => person.Id)
                    .Distinct()
                    .AverageAsync(Ct);
            },
            "Average over Distinct");
        await AssertRejected(
            async () =>
            {
                _ = await db.People
                    .Select(person => person.Score)
                    .Distinct()
                    .CountAsync(Ct);
            },
            "nonnullable int column");
        await AssertRejected(
            async () =>
            {
                _ = await db.People
                    .Select(person => new
                    {
                        person.Active,
                        person.Status,
                    })
                    .Distinct()
                    .CountAsync(Ct);
            },
            "direct member access");
        await AssertRejected(
            async () =>
            {
                _ = await db.People
                    .Select(person => person.Score)
                    .Distinct()
                    .CountAsync(score => score > 0, Ct);
            },
            "predicate after Distinct");
        await AssertRejected(
            async () =>
            {
                _ = await db.People
                    .Select(person => person.Score)
                    .Distinct()
                    .Where(score => score > 0)
                    .SumAsync(Ct);
            },
            "predicate after Distinct");
        await AssertRejected(
            async () =>
            {
                _ = await db.People
                    .Select(person => person.Id)
                    .Distinct()
                    .TagWith("after-distinct")
                    .Where(id => id > 0)
                    .SumAsync(Ct);
            },
            "predicate after Distinct");
        await AssertRejected(
            async () =>
            {
                _ = await db.People
                    .Select(person => person.Id)
                    .Distinct()
                    .Select(id => id)
                    .Where(id => id > 0)
                    .SumAsync(Ct);
            },
            "predicate after Distinct");
        await AssertRejected(
            async () =>
            {
                _ = await db.People
                    .Select(person => person.Score + 1)
                    .Distinct()
                    .SumAsync(Ct);
            },
            "direct member access");
        await AssertRejected(
            async () =>
            {
                _ = await db.People
                    .OrderBy(person => person.Id)
                    .Select(person => person.Id)
                    .Distinct()
                    .SumAsync(Ct);
            },
            "source operator 'OrderBy'");
        await AssertRejected(
            async () =>
            {
                _ = await db.People
                    .Select(person => person.Id)
                    .Select(id => id)
                    .Distinct()
                    .SumAsync(Ct);
            },
            "direct member access");
        await AssertRejected(
            async () =>
            {
                _ = await db.People
                    .Select(person => (int)person.Id)
                    .Distinct()
                    .SumAsync(Ct);
            },
            "without casts");
        await AssertRejected(
            async () =>
            {
                _ = await db.People
                    .OrderBy(person => person.Id)
                    .Take(1)
                    .Select(person => person.Score)
                    .Distinct()
                    .SumAsync(Ct);
            },
            "source operator 'Take'");

        string convertedDbPath =
            GetDbPath("unsupported-converted-distinct-aggregates");
        await using (var convertedDb =
            new ConvertedAggregateStorageContext(
                $"Data Source={convertedDbPath}",
                interceptor))
        {
            await convertedDb.Database.EnsureCreatedAsync(Ct);
            convertedDb.Items.Add(
                new ConvertedAggregateStorageEntity
                {
                    Active = true,
                    Score = 2.5,
                    ShiftedScore = 2.5,
                    IntText = 2,
                });
            await convertedDb.SaveChangesAsync(Ct);
            interceptor.Reset();

            await AssertRejected(
                async () =>
                {
                    _ = await convertedDb.Items
                        .Select(item => item.Score)
                        .Distinct()
                        .SumAsync(Ct);
                },
                "uses a value converter");
            await AssertRejected(
                async () =>
                {
                    _ = await convertedDb.Items
                        .Select(item => item.ShiftedScore)
                        .Distinct()
                        .SumAsync(Ct);
                },
                "uses a value converter");
        }

        Assert.Equal(0, interceptor.ReaderCommandCount);

        async Task AssertRejected(
            Func<Task> operation,
            string expectedReason)
        {
            InvalidOperationException exception =
                await Assert.ThrowsAsync<InvalidOperationException>(
                    operation);
            Assert.Contains(
                "CDBEF1004",
                exception.Message,
                StringComparison.Ordinal);
            Assert.Contains(
                expectedReason,
                exception.Message,
                StringComparison.OrdinalIgnoreCase);
        }
    }

    [Fact]
    public async Task UnsupportedGroupedAggregateShapes_ReportBeforeCommandDispatch()
    {
        string dbPath =
            GetDbPath("unsupported-grouped-aggregates");
        var interceptor = new ReaderCountingInterceptor();

        await using (var db = new ProviderRuntimeContext(
            $"Data Source={dbPath}",
            interceptor))
        {
            await db.Database.EnsureCreatedAsync(Ct);
            db.People.Add(
                CreateNumericPerson(
                    "Only",
                    2.5,
                    1,
                    active: true));
            await db.SaveChangesAsync(Ct);
            interceptor.Reset();

            await AssertRejected(
                async () =>
                {
                    _ = await db.People
                        .GroupBy(person => person.Name.ToLower())
                        .Select(group => new
                        {
                            group.Key,
                            Count = group.Count(),
                        })
                        .ToListAsync(Ct);
                },
                "direct mapped scalar columns");
            await AssertRejected(
                async () =>
                {
                    _ = await db.People
                        .GroupBy(person => (long)person.Id)
                        .Select(group => new
                        {
                            group.Key,
                            Count = group.Count(),
                        })
                        .ToListAsync(Ct);
                },
                "transformed and expression keys");
            await AssertRejected(
                async () =>
                {
                    _ = await db.People
                        .GroupBy(person => person.Score)
                        .Select(group => new
                        {
                            group.Key,
                            Count = group.Count(),
                        })
                        .ToListAsync(Ct);
                },
                "key type 'Double'");
            await AssertRejected(
                async () =>
                {
                    _ = await db.People
                        .GroupBy(person =>
                            new ReferenceEqualityGroupKey(
                                person.Id))
                        .Select(group => group.Count())
                        .ToListAsync(Ct);
                },
                "anonymous types and ValueTuple");
            await AssertRejected(
                async () =>
                {
                    _ = await db.People
                        .GroupBy(person =>
                            new ForgedAnonymousTypeKey(
                                person.Id))
                        .Select(group => group.Count())
                        .ToListAsync(Ct);
                },
                "anonymous types and ValueTuple");
            await AssertRejected(
                async () =>
                {
                    _ = await db.People
                        .GroupBy(person => person.Active)
                        .Select(group => new
                        {
                            group.Key,
                            Count = group.Count(),
                        })
                        .CountAsync(Ct);
                },
                "already-grouped result rows");
            await AssertRejected(
                async () =>
                {
                    _ = await db.People
                        .GroupBy(person => person.Active)
                        .ToListAsync(Ct);
                },
                "Materializing IGrouping");
            await AssertRejected(
                async () =>
                {
                    _ = await db.People
                        .GroupBy(person => person.Active)
                        .AnyAsync(Ct);
                },
                "Grouped sequence operator 'Any'");
            await AssertRejected(
                async () =>
                {
                    _ = await db.People
                        .GroupBy(person => person.Active)
                        .Select(group => new
                        {
                            group.Key,
                            Count = group.Count(),
                        })
                        .SingleAsync(
                            row => row.Count > 0,
                            Ct);
                },
                "Grouped sequence operator 'Single'");
            await AssertRejected(
                async () =>
                {
                    _ = await db.People
                        .GroupBy(person => person.Active)
                        .Select(group => new
                        {
                            group.Key,
                            Count = group.Count(),
                        })
                        .AnyAsync(Ct);
                },
                "Grouped sequence operator 'Any'");
            await AssertRejected(
                async () =>
                {
                    _ = await db.People
                        .Take(1)
                        .GroupBy(person => person.Active)
                        .Select(group => new
                        {
                            group.Key,
                            Count = group.Count(),
                        })
                        .ToListAsync(Ct);
                },
                "source operator 'Take'");
            await AssertRejected(
                async () =>
                {
                    _ = await db.People
                        .GroupBy(person => person.Active)
                        .Take(1)
                        .Select(group => new
                        {
                            group.Key,
                            Count = group.Count(),
                        })
                        .ToListAsync(Ct);
                },
                "Post-GroupBy operator 'Take'");
            await AssertRejected(
                async () =>
                {
                    _ = await db.People
                        .GroupBy(person => person.Active)
                        .Cast<object>()
                        .ToListAsync(Ct);
                },
                "Post-GroupBy operator 'Cast'");
            await AssertRejected(
                async () =>
                {
                    _ = await db.People
                        .Select(person => new
                        {
                            Key = person.Name.ToLower(),
                            person.Score,
                        })
                        .GroupBy(row => row.Key)
                        .Select(group => new
                        {
                            group.Key,
                            Count = group.Count(),
                        })
                        .ToListAsync(Ct);
                },
                "source operator 'Select'");
            await AssertRejected(
                async () =>
                {
                    _ = await db.People
                        .Select(person => new PersonRecord
                        {
                            Active = person.Active,
                            Score =
                                person.Score + int.MaxValue,
                        })
                        .GroupBy(person => person.Active)
                        .Select(group => group.Sum(
                            person => person.Score))
                        .ToListAsync(Ct);
                },
                "source operator 'Select'");
            await AssertRejected(
                async () =>
                {
                    _ = await db.People
                        .GroupBy(person => person.Active)
                        .Select(group => group.Count(
                            person => person.Score > 0))
                        .ToListAsync(Ct);
                },
                "Predicate Count produces CASE");
            await AssertRejected(
                async () =>
                {
                    _ = await db.People
                        .GroupBy(person => person.Active)
                        .Select(group => group.LongCount(
                            person => person.Score > 0))
                        .ToListAsync(Ct);
                },
                "Predicate LongCount produces CASE");
            await AssertRejected(
                async () =>
                {
                    _ = await db.People
                        .GroupBy(person => person.Active)
                        .Select(group => group.Average(
                            person => person.Id))
                        .ToListAsync(Ct);
                },
                "Average over 'Int32'");
            await AssertRejected(
                async () =>
                {
                    _ = await db.People
                        .GroupBy(person => person.Active)
                        .Select(group => group.Sum(
                            person => person.Visits))
                        .ToListAsync(Ct);
                },
                "Sum over 'Int64'");
            await AssertRejected(
                async () =>
                {
                    _ = await db.People
                        .GroupBy(person => person.Active)
                        .Select(group => group.Sum(
                            person => person.Score + 1))
                        .ToListAsync(Ct);
                },
                "selectors must be one directly mapped");
            await AssertRejected(
                async () =>
                {
                    _ = await db.People
                        .GroupBy(person => person.Active)
                        .Select(group => group
                            .Select(person => person.Id)
                            .Distinct()
                            .Average())
                        .ToListAsync(Ct);
                },
                "introduces a CAST");
            await AssertRejected(
                async () =>
                {
                    _ = await db.People
                        .GroupBy(person => person.Active)
                        .Select(group => group.Average(
                            person => (double)person.Id))
                        .ToListAsync(Ct);
                },
                "selectors must be one directly mapped");
            await AssertRejected(
                async () =>
                {
                    _ = await db.People
                        .GroupBy(person => person.Active)
                        .Select(group =>
                            group.Count() > 0
                                ? group.Sum(
                                    person => person.Score)
                                : 0)
                        .ToListAsync(Ct);
                },
                "Grouped projections must contain");
            await AssertRejected(
                async () =>
                {
                    _ = await db.People
                        .GroupBy(person => person.Active)
                        .Where(group =>
                            group.Sum(person => person.Id) +
                            int.MaxValue >
                            0)
                        .Select(group => group.Key)
                        .ToListAsync(Ct);
                },
                "Generated HAVING predicates must use");
            await AssertRejected(
                async () =>
                {
                    _ = await db.People
                        .GroupBy(person => person.Active)
                        .Select(group => new
                        {
                            group.Key,
                            Sum = group.Sum(
                                person => person.Id),
                        })
                        .Where(row =>
                            row.Sum + int.MaxValue > 0)
                        .ToListAsync(Ct);
                },
                "Post-GroupBy operator 'Where'");
            await AssertRejected(
                async () =>
                {
                    _ = await db.People
                        .GroupBy(person => person.Active)
                        .Select(group => new
                        {
                            group.Key,
                            Sum = group.Sum(
                                person => person.Id),
                        })
                        .OrderBy(row =>
                            row.Sum + int.MaxValue)
                        .ToListAsync(Ct);
                },
                "Grouped ordering must use");
            await AssertRejected(
                async () =>
                {
                    _ = await db.People
                        .GroupBy(person => person.Active)
                        .Select(group => new
                        {
                            group.Key,
                            Count = group.Count(),
                        })
                        .OrderBy(row => (long)row.Count)
                        .ToListAsync(Ct);
                },
                "without casts");
            await AssertRejected(
                async () =>
                {
                    _ = await db.People
                        .GroupBy(person => person.Active)
                        .Select(group => group.Sum(
                            person => person.Id))
                        .Select(sum => sum + int.MaxValue)
                        .ToListAsync(Ct);
                },
                "Post-GroupBy operator 'Select'");
            await AssertRejected(
                async () =>
                {
                    IQueryable<int> groupedProjection =
                        db.People
                            .GroupBy(person => person.Active)
                            .Select(group => group.Count());
                    IQueryable<int> plainProjection =
                        db.People.Select(person => person.Id);
                    _ = await plainProjection
                        .Concat(groupedProjection)
                        .ToListAsync(Ct);
                },
                "Post-GroupBy operator 'Concat'");
            await AssertRejected(
                async () =>
                {
                    _ = await db.People
                        .GroupBy(person => person.Active)
                        .Select(group => group.Sum(
                            person => person.Id))
                        .Distinct()
                        .Select(sum => sum + int.MaxValue)
                        .ToListAsync(Ct);
                },
                "Post-GroupBy operator 'Select'");
            await AssertRejected(
                async () =>
                {
                    _ = await db.People
                        .GroupBy(person => person.Active)
                        .Select(group =>
                            new EmptyGroupedProjection())
                        .ToListAsync(Ct);
                },
                "Grouped projections must contain");
            await AssertRejected(
                async () =>
                {
                    _ = await db.People
                        .GroupBy(person => person.Name)
                        .Select(group => new
                        {
                            Derived = group.Key.Length,
                            Count = group.Count(),
                        })
                        .ToListAsync(Ct);
                },
                "Grouped projections must contain");
            await AssertRejected(
                async () =>
                {
                    _ = await db.People
                        .GroupBy(person => person.Active)
                        .Where(group => group.Any())
                        .Select(group => group.Key)
                        .ToListAsync(Ct);
                },
                "Grouped sequence operator 'Any'");
            await AssertRejected(
                async () =>
                {
                    _ = await db.People
                        .GroupBy(person => person.Active)
                        .Select(group =>
                            group.Sum(person => person.Id) +
                            int.MaxValue)
                        .ToListAsync(Ct);
                },
                "Grouped projections must contain");
            await AssertRejected(
                async () =>
                {
                    _ = await db.People
                        .GroupBy(
                            person => person.Active,
                            person => person.Score)
                        .Select(group => group.Sum())
                        .ToListAsync(Ct);
                },
                "without a selector");
            await AssertRejected(
                async () =>
                {
                    _ = await db.People
                        .GroupBy(
                            person => person.Active,
                            person => person.Score,
                            (active, scores) => new
                            {
                                Active = active,
                                Positive = scores.Count(
                                    score => score > 0),
                            })
                        .ToListAsync(Ct);
                },
                "element-selector and result-selector");
            await AssertRejected(
                async () =>
                {
                    _ = await db.People
                        .Join(
                            db.People,
                            left => left.Id,
                            right => right.Id,
                            (left, right) => new
                            {
                                Left = left,
                                Right = right,
                            })
                        .GroupBy(row => row.Left.Active)
                        .Select(group => group.Count())
                        .ToListAsync(Ct);
                },
                "source operator 'Join'");
            await AssertRejected(
                async () =>
                {
                    _ = await db.People
                        .SelectMany(
                            person => db.People
                                .Where(other =>
                                    other.Active ==
                                    person.Active)
                                .GroupBy(other =>
                                    other.Active)
                                .Select(group => new
                                {
                                    group.Key,
                                    Count =
                                        group.Count(),
                                }),
                            (_, group) => group.Count)
                        .ToListAsync(Ct);
                },
                "Post-GroupBy operator 'SelectMany'");
        }

        string convertedAggregateDbPath =
            GetDbPath("unsupported-converted-grouped-aggregates");
        await using (var db = new ConvertedAggregateStorageContext(
            $"Data Source={convertedAggregateDbPath}",
            interceptor))
        {
            await db.Database.EnsureCreatedAsync(Ct);
            db.Items.AddRange(
                new ConvertedAggregateStorageEntity
                {
                    Active = true,
                    Score = 2.5,
                    ShiftedScore = 2.5,
                    IntText = 2,
                },
                new ConvertedAggregateStorageEntity
                {
                    Active = true,
                    Score = 7.5,
                    ShiftedScore = 7.5,
                    IntText = 10,
                });
            await db.SaveChangesAsync(Ct);
            interceptor.Reset();

            await AssertRejected(
                async () =>
                {
                    _ = await db.Items
                        .GroupBy(item => item.Active)
                        .Select(group => group.Sum(
                            item => item.Score))
                        .ToListAsync(Ct);
                },
                "uses a value converter");
            await AssertRejected(
                async () =>
                {
                    _ = await db.Items
                        .GroupBy(item => item.Active)
                        .Select(group => group.Sum(
                            item => item.ShiftedScore))
                        .ToListAsync(Ct);
                },
                "uses a value converter");
            await AssertRejected(
                async () =>
                {
                    _ = await db.Items
                        .GroupBy(item => item.Active)
                        .Select(group => new
                        {
                            Min = group.Min(
                                item => item.IntText),
                            Max = group.Max(
                                item => item.IntText),
                        })
                        .ToListAsync(Ct);
                },
                "uses a value converter");
        }

        string nonBinaryDbPath =
            GetDbPath("unsupported-nonbinary-grouping");
        await using (var db = new NonBinaryGroupingContext(
            $"Data Source={nonBinaryDbPath}",
            interceptor))
        {
            await db.Database.EnsureCreatedAsync(Ct);
            db.Items.AddRange(
                new NonBinaryGroupEntity { Category = "A" },
                new NonBinaryGroupEntity { Category = "a" });
            await db.SaveChangesAsync(Ct);
            interceptor.Reset();

            await AssertRejected(
                async () =>
                {
                    _ = await db.Items
                        .GroupBy(item => item.Category)
                        .Select(group => new
                        {
                            group.Key,
                            Count = group.Count(),
                        })
                        .ToListAsync(Ct);
                },
                "binary equality");
            await AssertRejected(
                async () =>
                {
                    _ = await db.Items
                        .GroupBy(item =>
                            new NonBinaryCompositeGroupKey(
                                item.Category)
                            {
                                Id = item.Id,
                            })
                        .Select(group => group.Count())
                        .ToListAsync(Ct);
                },
                "anonymous types and ValueTuple");
        }

        string convertedNonBinaryDbPath =
            GetDbPath("unsupported-converted-nonbinary-grouping");
        await using (var db = new ConvertedNonBinaryGroupingContext(
            $"Data Source={convertedNonBinaryDbPath}",
            interceptor))
        {
            await db.Database.EnsureCreatedAsync(Ct);
            db.Items.AddRange(
                new ConvertedNonBinaryGroupEntity
                {
                    Kind = ConvertedGroupKind.Upper,
                },
                new ConvertedNonBinaryGroupEntity
                {
                    Kind = ConvertedGroupKind.Lower,
                });
            await db.SaveChangesAsync(Ct);
            interceptor.Reset();

            await AssertRejected(
                async () =>
                {
                    _ = await db.Items
                        .GroupBy(item => item.Kind)
                        .Select(group => new
                        {
                            group.Key,
                            Count = group.Count(),
                        })
                        .ToListAsync(Ct);
                },
                "configured value converter");
        }

        string convertedStringKeyDbPath =
            GetDbPath("unsupported-converted-string-grouping");
        await using (var db = new ConvertedStringGroupingContext(
            $"Data Source={convertedStringKeyDbPath}",
            interceptor))
        {
            await db.Database.EnsureCreatedAsync(Ct);
            db.Items.AddRange(
                new NonBinaryGroupEntity { Category = "A" },
                new NonBinaryGroupEntity { Category = "a" });
            await db.SaveChangesAsync(Ct);
            interceptor.Reset();

            await AssertRejected(
                async () =>
                {
                    _ = await db.Items
                        .GroupBy(item => item.Category)
                        .Select(group => new
                        {
                            group.Key,
                            Count = group.Count(),
                        })
                        .ToListAsync(Ct);
                },
                "configured value converter");
        }

        Assert.Equal(0, interceptor.ReaderCommandCount);

        async Task AssertRejected(
            Func<Task> operation,
            string expectedReason)
        {
            InvalidOperationException exception =
                await Assert.ThrowsAsync<InvalidOperationException>(
                    operation);
            Assert.Contains(
                "CDBEF1005",
                exception.Message,
                StringComparison.Ordinal);
            Assert.Contains(
                expectedReason,
                exception.Message,
                StringComparison.OrdinalIgnoreCase);
            Assert.Equal(0, interceptor.ReaderCommandCount);
        }
    }

    [Fact]
    public async Task UnsupportedLinq_ReportsProviderDiagnosticsBeforeCommandDispatch()
    {
        string dbPath = GetDbPath("unsupported-linq");
        var interceptor = new ReaderCountingInterceptor();

        await using var db = new ProviderRuntimeContext(
            $"Data Source={dbPath}",
            interceptor);
        await db.Database.EnsureCreatedAsync(Ct);
        db.Blogs.Add(new Blog { Name = "Alpha" });
        db.People.Add(new PersonRecord
        {
            Name = "Temporal",
            GuidValue = Guid.Empty,
            CreatedAt = new DateTime(2026, 7, 18, 0, 0, 0, DateTimeKind.Utc),
            ObservedAt = DateTimeOffset.UnixEpoch,
            Birthday = new DateOnly(2000, 1, 1),
            Alarm = new TimeOnly(7, 0),
        });
        await db.SaveChangesAsync(Ct);
        interceptor.Reset();

        InvalidOperationException methodError =
            await Assert.ThrowsAsync<InvalidOperationException>(() =>
                db.Blogs
                    .Where(blog => blog.Name.Contains(
                        "alpha",
                        StringComparison.Ordinal))
                    .ToListAsync(Ct));
        Assert.Contains("CDBEF1001", methodError.Message, StringComparison.Ordinal);
        Assert.Contains("System.String.Contains", methodError.Message, StringComparison.Ordinal);
        Assert.Contains("StringComparison", methodError.Message, StringComparison.Ordinal);
        Assert.Contains("#linq-translation", methodError.Message, StringComparison.Ordinal);

        InvalidOperationException memberError =
            await Assert.ThrowsAsync<InvalidOperationException>(() =>
                db.People
                    .Where(person => person.CreatedAt.DayOfWeek == DayOfWeek.Saturday)
                    .ToListAsync(Ct));
        Assert.Contains("CDBEF1002", memberError.Message, StringComparison.Ordinal);
        Assert.Contains("System.DateTime.DayOfWeek", memberError.Message, StringComparison.Ordinal);
        Assert.Contains("#linq-translation", memberError.Message, StringComparison.Ordinal);

        InvalidOperationException nestedError =
            await Assert.ThrowsAsync<InvalidOperationException>(() =>
                db.People
                    .Where(person =>
                        person.Name.Replace(
                            "x",
                            person.CreatedAt.DayOfWeek.ToString()) == "value")
                    .ToListAsync(Ct));
        Assert.Contains("CDBEF1002", nestedError.Message, StringComparison.Ordinal);
        Assert.Contains("System.DateTime.DayOfWeek", nestedError.Message, StringComparison.Ordinal);
        Assert.DoesNotContain("System.String.Replace", nestedError.Message, StringComparison.Ordinal);

        InvalidOperationException operatorError =
            await Assert.ThrowsAsync<InvalidOperationException>(() =>
                db.Blogs
                    .OrderBy(blog => blog.Id)
                    .TakeWhile(blog => blog.Id < 10)
                    .ToListAsync(Ct));
        Assert.Contains("CDBEF1003", operatorError.Message, StringComparison.Ordinal);
        Assert.Contains("Queryable.TakeWhile", operatorError.Message, StringComparison.Ordinal);
        Assert.Contains("#linq-translation", operatorError.Message, StringComparison.Ordinal);

        InvalidOperationException skipWhileError =
            await Assert.ThrowsAsync<InvalidOperationException>(() =>
                db.Blogs
                    .OrderBy(blog => blog.Id)
                    .SkipWhile(blog => blog.Id < 10)
                    .ToListAsync(Ct));
        Assert.Contains("CDBEF1003", skipWhileError.Message, StringComparison.Ordinal);
        Assert.Contains("Queryable.SkipWhile", skipWhileError.Message, StringComparison.Ordinal);

        InvalidOperationException precisionOverloadError =
            await Assert.ThrowsAsync<InvalidOperationException>(() =>
                db.People
                    .Where(person => Math.Round(person.Score, 2) > 0)
                    .ToListAsync(Ct));
        Assert.Contains("CDBEF1001", precisionOverloadError.Message, StringComparison.Ordinal);
        Assert.Contains(
            "System.Math.Round(System.Double, System.Int32)",
            precisionOverloadError.Message,
            StringComparison.Ordinal);

        InvalidOperationException integralOverloadError =
            await Assert.ThrowsAsync<InvalidOperationException>(() =>
                db.People
                    .Where(person => Math.Abs(person.Visits) > 0)
                    .ToListAsync(Ct));
        Assert.Contains("CDBEF1001", integralOverloadError.Message, StringComparison.Ordinal);
        Assert.Contains(
            "System.Math.Abs(System.Int64)",
            integralOverloadError.Message,
            StringComparison.Ordinal);
        Assert.Equal(0, interceptor.ReaderCommandCount);
    }

    [Fact]
    public async Task SaveChanges_WithConcurrencyToken_ThrowsWhenRowWasModified()
    {
        string dbPath = GetDbPath("concurrency");

        await using (var seed = new ProviderRuntimeContext($"Data Source={dbPath}"))
        {
            await seed.Database.EnsureCreatedAsync(Ct);
            seed.Tickets.Add(new Ticket
            {
                Name = "alpha",
                Version = 1,
            });

            await seed.SaveChangesAsync(Ct);
        }

        await using var first = new ProviderRuntimeContext($"Data Source={dbPath}");
        await using var second = new ProviderRuntimeContext($"Data Source={dbPath}");

        Ticket firstTicket = await first.Tickets.SingleAsync(Ct);
        Ticket secondTicket = await second.Tickets.SingleAsync(Ct);

        firstTicket.Name = "alpha-v2";
        firstTicket.Version = 2;
        await first.SaveChangesAsync(Ct);

        secondTicket.Name = "alpha-stale";
        secondTicket.Version = 2;

        var error = await Assert.ThrowsAsync<DbUpdateConcurrencyException>(() => second.SaveChangesAsync(Ct));
        Assert.Contains("expected to affect 1 row", error.Message, StringComparison.OrdinalIgnoreCase);
    }

    [Fact]
    public async Task UseCSharpDb_WithPrivateMemoryConnection_SupportsRuntimeOperations()
    {
        await using var connection = new CSharpDbConnection("Data Source=:memory:");
        await connection.OpenAsync(Ct);

        await using var db = new ProviderRuntimeContext(connection);
        await db.Database.EnsureCreatedAsync(Ct);

        db.People.Add(new PersonRecord
        {
            Name = "Transient",
            Active = true,
            Score = 1.25,
            Visits = 1,
            Status = PersonStatus.Active,
            GuidValue = Guid.NewGuid(),
            CreatedAt = new DateTime(2026, 4, 16, 0, 0, 0, DateTimeKind.Utc),
            ObservedAt = new DateTimeOffset(2026, 4, 16, 0, 0, 0, TimeSpan.Zero),
            Birthday = new DateOnly(2001, 1, 1),
            Alarm = new TimeOnly(8, 0, 0),
            Payload = [0xCA, 0xFE],
        });

        await db.SaveChangesAsync(Ct);

        Assert.Equal("Transient", await db.People.Select(person => person.Name).SingleAsync(Ct));
    }

    [Fact]
    public async Task ModelValidation_RejectsDecimalWithoutConverter()
    {
        string dbPath = GetDbPath("decimal");

        await using var db = new DecimalModelContext($"Data Source={dbPath}");
        var error = await Assert.ThrowsAsync<NotSupportedException>(() => db.Database.EnsureCreatedAsync(Ct));

        Assert.Contains("decimal without an explicit value converter", error.Message, StringComparison.OrdinalIgnoreCase);
    }

    [Fact]
    public async Task ModelValidation_RejectsSchemas()
    {
        string dbPath = GetDbPath("schema");

        await using var db = new SchemaModelContext($"Data Source={dbPath}");
        var error = await Assert.ThrowsAsync<NotSupportedException>(() => db.Database.EnsureCreatedAsync(Ct));

        Assert.Contains("schemas are not supported", error.Message, StringComparison.OrdinalIgnoreCase);
    }

    [Fact]
    public async Task LiteralDefaultsAndCreateTableChecks_AreApplied()
    {
        await using var defaultValueDb = new DefaultValueModelContext($"Data Source={GetDbPath("defaults")}");
        await defaultValueDb.Database.EnsureCreatedAsync(Ct);
        defaultValueDb.Items.Add(new DefaultValueEntity());
        await defaultValueDb.SaveChangesAsync(Ct);
        Assert.Equal("pending", await defaultValueDb.Items.Select(item => item.Name).SingleAsync(Ct));

        await using var checkConstraintDb = new CheckConstraintModelContext($"Data Source={GetDbPath("checks")}");
        await checkConstraintDb.Database.EnsureCreatedAsync(Ct);
        checkConstraintDb.Items.Add(new CheckConstraintEntity { Value = 1 });
        await checkConstraintDb.SaveChangesAsync(Ct);

        checkConstraintDb.Items.Add(new CheckConstraintEntity { Value = 0 });
        var checkError = await Assert.ThrowsAsync<DbUpdateException>(() => checkConstraintDb.SaveChangesAsync(Ct));
        Assert.Contains("check", checkError.ToString(), StringComparison.OrdinalIgnoreCase);
    }

    [Fact]
    public async Task IdentityOnlyEntity_UsesDefaultValuesAndReturnsGeneratedKey()
    {
        await using var db = new IdentityOnlyModelContext($"Data Source={GetDbPath("identity-only")}");
        await db.Database.EnsureCreatedAsync(Ct);

        var entity = new IdentityOnlyEntity();
        db.Items.Add(entity);
        await db.SaveChangesAsync(Ct);

        Assert.True(entity.Id > 0);
        Assert.Equal(1, await db.Items.CountAsync(Ct));
    }

    [Fact]
    public async Task CompositeKeyEntity_SupportsCrudIndexesAndDuplicateRejection()
    {
        string dbPath = GetDbPath("composite-key");

        await using (var db = new CompositeKeyModelContext($"Data Source={dbPath}"))
        {
            await db.Database.EnsureCreatedAsync(Ct);
            db.OrderLines.AddRange(
                new CompositeOrderLine { OrderId = 10, LineNo = 1, Sku = "alpha", Quantity = 2 },
                new CompositeOrderLine { OrderId = 10, LineNo = 2, Sku = "beta", Quantity = 1 });
            await db.SaveChangesAsync(Ct);

            CompositeOrderLine line = await db.OrderLines.SingleAsync(
                item => item.OrderId == 10 && item.LineNo == 1,
                Ct);
            line.Quantity = 5;
            await db.SaveChangesAsync(Ct);

            db.Remove(await db.OrderLines.SingleAsync(
                item => item.OrderId == 10 && item.LineNo == 2,
                Ct));
            await db.SaveChangesAsync(Ct);
        }

        await using (var duplicate = new CompositeKeyModelContext($"Data Source={dbPath}"))
        {
            duplicate.OrderLines.Add(new CompositeOrderLine
            {
                OrderId = 10,
                LineNo = 1,
                Sku = "duplicate",
                Quantity = 9,
            });

            await Assert.ThrowsAsync<DbUpdateException>(() => duplicate.SaveChangesAsync(Ct));
        }

        await using var verify = new CompositeKeyModelContext($"Data Source={dbPath}");
        CompositeOrderLine persisted = await verify.OrderLines.AsNoTracking().SingleAsync(Ct);
        Assert.Equal(10, persisted.OrderId);
        Assert.Equal(1, persisted.LineNo);
        Assert.Equal(5, persisted.Quantity);
        Assert.Equal("alpha", persisted.Sku);
    }

    [Fact]
    public async Task AlternateKeyRelationship_SupportsEnsureCreatedAndEnforcement()
    {
        string dbPath = GetDbPath("alternate-key");

        await using (var db = new AlternateKeyModelContext($"Data Source={dbPath}"))
        {
            await db.Database.EnsureCreatedAsync(Ct);
            db.Parents.Add(new AlternateKeyParent { Code = "parent-1" });
            db.Children.Add(new AlternateKeyChild { ParentCode = "parent-1" });
            await db.SaveChangesAsync(Ct);
        }

        await using var invalid = new AlternateKeyModelContext($"Data Source={dbPath}");
        invalid.Children.Add(new AlternateKeyChild { ParentCode = "missing" });
        await Assert.ThrowsAsync<DbUpdateException>(() => invalid.SaveChangesAsync(Ct));
    }

    [Fact]
    public async Task CompositeForeignKeyRelationship_SupportsEnsureCreatedAndEnforcement()
    {
        string dbPath = GetDbPath("composite-foreign-key");

        await using (var db = new CompositeForeignKeyModelContext($"Data Source={dbPath}"))
        {
            await db.Database.EnsureCreatedAsync(Ct);
            db.Parents.Add(new CompositeForeignKeyParent { TenantId = 7, ParentNo = 42 });
            db.Children.Add(new CompositeForeignKeyChild
            {
                TenantId = 7,
                ParentNo = 42,
            });
            await db.SaveChangesAsync(Ct);
        }

        await using var invalid = new CompositeForeignKeyModelContext($"Data Source={dbPath}");
        invalid.Children.Add(new CompositeForeignKeyChild
        {
            TenantId = 7,
            ParentNo = 999,
        });
        await Assert.ThrowsAsync<DbUpdateException>(() => invalid.SaveChangesAsync(Ct));
    }

    [Theory]
    [InlineData("Data Source=provider.db;Pooling=true", "pooled connections")]
    [InlineData("Data Source=:memory:shared", "named shared-memory")]
    [InlineData("Data Source=provider.db;Transport=Http", "direct embedded transports")]
    [InlineData("Endpoint=http://localhost:5123;Transport=Http", "Endpoint connections")]
    public async Task ProviderValidation_RejectsUnsupportedConnectionConfigurations(string connectionString, string expectedMessage)
    {
        await using var db = new ProviderRuntimeContext(connectionString);

        var error = await Assert.ThrowsAsync<NotSupportedException>(() => db.Database.EnsureCreatedAsync(Ct));
        Assert.Contains(expectedMessage, error.Message, StringComparison.OrdinalIgnoreCase);
    }

    private string GetDbPath(string name)
        => Path.Combine(_workspace, $"{name}.db");

    private static PersonRecord CreateNumericPerson(
        string name,
        double score,
        long visits,
        bool active = false,
        double? optionalScore = null,
        int? optionalRank = null,
        PersonStatus status = PersonStatus.Unknown) =>
        new()
        {
            Name = name,
            Active = active,
            Score = score,
            OptionalScore = optionalScore,
            OptionalRank = optionalRank,
            Visits = visits,
            Status = status,
            GuidValue = Guid.NewGuid(),
            CreatedAt = DateTime.UnixEpoch,
            ObservedAt = DateTimeOffset.UnixEpoch,
            Birthday = DateOnly.FromDateTime(DateTime.UnixEpoch),
            Alarm = TimeOnly.MinValue,
        };

    private static void DeleteDirectoryIfExists(string path)
    {
        if (Directory.Exists(path))
            Directory.Delete(path, recursive: true);
    }

    private abstract class TestDbContext : DbContext
    {
        private readonly string? _connectionString;
        private readonly DbConnection? _connection;
        private readonly IInterceptor[] _interceptors;

        protected TestDbContext(
            string connectionString,
            params IInterceptor[] interceptors)
        {
            _connectionString = connectionString;
            _interceptors = interceptors;
        }

        protected TestDbContext(DbConnection connection)
        {
            _connection = connection;
            _interceptors = [];
        }

        protected override void OnConfiguring(DbContextOptionsBuilder optionsBuilder)
        {
            if (_connection is not null)
                optionsBuilder.UseCSharpDb(_connection);
            else
                optionsBuilder.UseCSharpDb(_connectionString!);

            if (_interceptors.Length > 0)
                optionsBuilder.AddInterceptors(_interceptors);
        }
    }

    private sealed class ProviderRuntimeContext : TestDbContext
    {
        public ProviderRuntimeContext(string connectionString)
            : base(connectionString)
        {
        }

        public ProviderRuntimeContext(
            string connectionString,
            params IInterceptor[] interceptors)
            : base(connectionString, interceptors)
        {
        }

        public ProviderRuntimeContext(DbConnection connection)
            : base(connection)
        {
        }

        public DbSet<PersonRecord> People => Set<PersonRecord>();

        public DbSet<ManualWidget> Widgets => Set<ManualWidget>();

        public DbSet<Blog> Blogs => Set<Blog>();

        public DbSet<Post> Posts => Set<Post>();

        public DbSet<Ticket> Tickets => Set<Ticket>();

        protected override void OnModelCreating(ModelBuilder modelBuilder)
        {
            modelBuilder.Entity<ManualWidget>()
                .Property(widget => widget.Id)
                .ValueGeneratedNever();

            modelBuilder.Entity<Blog>()
                .HasMany(blog => blog.Posts)
                .WithOne(post => post.Blog)
                .HasForeignKey(post => post.BlogId)
                .OnDelete(DeleteBehavior.Cascade);

            modelBuilder.Entity<Ticket>()
                .Property(ticket => ticket.Version)
                .IsConcurrencyToken();
        }
    }

    private sealed class DecimalModelContext(string connectionString) : TestDbContext(connectionString)
    {
        public DbSet<DecimalEntity> Items => Set<DecimalEntity>();
    }

    private sealed class SchemaModelContext(string connectionString) : TestDbContext(connectionString)
    {
        public DbSet<PersonRecord> People => Set<PersonRecord>();

        protected override void OnModelCreating(ModelBuilder modelBuilder)
            => modelBuilder.Entity<PersonRecord>().ToTable("People", "app");
    }

    private sealed class DefaultValueModelContext(string connectionString) : TestDbContext(connectionString)
    {
        public DbSet<DefaultValueEntity> Items => Set<DefaultValueEntity>();

        protected override void OnModelCreating(ModelBuilder modelBuilder)
            => modelBuilder.Entity<DefaultValueEntity>()
                .Property(item => item.Name)
                .HasDefaultValue("pending");
    }

    private sealed class CheckConstraintModelContext(string connectionString) : TestDbContext(connectionString)
    {
        public DbSet<CheckConstraintEntity> Items => Set<CheckConstraintEntity>();

        protected override void OnModelCreating(ModelBuilder modelBuilder)
            => modelBuilder.Entity<CheckConstraintEntity>()
                .ToTable(tableBuilder => tableBuilder.HasCheckConstraint("CK_CheckConstraintEntity_Value", "Value > 0"));
    }

    private sealed class IdentityOnlyModelContext(string connectionString) : TestDbContext(connectionString)
    {
        public DbSet<IdentityOnlyEntity> Items => Set<IdentityOnlyEntity>();
    }

    private sealed class NonBinaryGroupingContext : TestDbContext
    {
        public NonBinaryGroupingContext(
            string connectionString,
            params IInterceptor[] interceptors)
            : base(connectionString, interceptors)
        {
        }

        public DbSet<NonBinaryGroupEntity> Items =>
            Set<NonBinaryGroupEntity>();

        protected override void OnModelCreating(
            ModelBuilder modelBuilder)
        {
            modelBuilder.Entity<NonBinaryGroupEntity>()
                .Property(item => item.Category)
                .UseCollation("NOCASE");
        }
    }

    private sealed class ConvertedNonBinaryGroupingContext
        : TestDbContext
    {
        public ConvertedNonBinaryGroupingContext(
            string connectionString,
            params IInterceptor[] interceptors)
            : base(connectionString, interceptors)
        {
        }

        public DbSet<ConvertedNonBinaryGroupEntity> Items =>
            Set<ConvertedNonBinaryGroupEntity>();

        protected override void OnModelCreating(
            ModelBuilder modelBuilder)
        {
            modelBuilder.Entity<ConvertedNonBinaryGroupEntity>()
                .Property(item => item.Kind)
                .HasConversion(
                    value => value == ConvertedGroupKind.Upper
                        ? "A"
                        : "a",
                    value => value == "A"
                        ? ConvertedGroupKind.Upper
                        : ConvertedGroupKind.Lower)
                .UseCollation("NOCASE");
        }
    }

    private sealed class ConvertedAggregateStorageContext
        : TestDbContext
    {
        public ConvertedAggregateStorageContext(
            string connectionString,
            params IInterceptor[] interceptors)
            : base(connectionString, interceptors)
        {
        }

        public DbSet<ConvertedAggregateStorageEntity> Items =>
            Set<ConvertedAggregateStorageEntity>();

        protected override void OnModelCreating(
            ModelBuilder modelBuilder)
        {
            modelBuilder.Entity<ConvertedAggregateStorageEntity>()
                .Property(item => item.Score)
                .HasConversion<string>();
            modelBuilder.Entity<ConvertedAggregateStorageEntity>()
                .Property(item => item.IntText)
                .HasConversion<string>();
            modelBuilder.Entity<ConvertedAggregateStorageEntity>()
                .Property(item => item.ShiftedScore)
                .HasConversion(
                    value => value + 1,
                    value => value - 1);
        }
    }

    private sealed class ConvertedStringGroupingContext
        : TestDbContext
    {
        public ConvertedStringGroupingContext(
            string connectionString,
            params IInterceptor[] interceptors)
            : base(connectionString, interceptors)
        {
        }

        public DbSet<NonBinaryGroupEntity> Items =>
            Set<NonBinaryGroupEntity>();

        protected override void OnModelCreating(
            ModelBuilder modelBuilder)
        {
            modelBuilder.Entity<NonBinaryGroupEntity>()
                .Property(item => item.Category)
                .HasConversion(
                    value => value,
                    value => value.ToLowerInvariant());
        }
    }

    private sealed class CompositeKeyModelContext(string connectionString) : TestDbContext(connectionString)
    {
        public DbSet<CompositeOrderLine> OrderLines => Set<CompositeOrderLine>();

        protected override void OnModelCreating(ModelBuilder modelBuilder)
        {
            modelBuilder.Entity<CompositeOrderLine>(entity =>
            {
                entity.HasKey(line => new { line.OrderId, line.LineNo });
                entity.HasIndex(line => new { line.OrderId, line.Sku }).IsUnique();
                entity.HasIndex(line => new { line.Sku, line.LineNo });
            });
        }
    }

    private sealed class AlternateKeyModelContext(string connectionString) : TestDbContext(connectionString)
    {
        public DbSet<AlternateKeyParent> Parents => Set<AlternateKeyParent>();

        public DbSet<AlternateKeyChild> Children => Set<AlternateKeyChild>();

        protected override void OnModelCreating(ModelBuilder modelBuilder)
        {
            modelBuilder.Entity<AlternateKeyParent>()
                .HasAlternateKey(parent => parent.Code);
            modelBuilder.Entity<AlternateKeyChild>()
                .HasOne(child => child.Parent)
                .WithMany(parent => parent.Children)
                .HasForeignKey(child => child.ParentCode)
                .HasPrincipalKey(parent => parent.Code);
        }
    }

    private sealed class CompositeForeignKeyModelContext(string connectionString) : TestDbContext(connectionString)
    {
        public DbSet<CompositeForeignKeyParent> Parents => Set<CompositeForeignKeyParent>();

        public DbSet<CompositeForeignKeyChild> Children => Set<CompositeForeignKeyChild>();

        protected override void OnModelCreating(ModelBuilder modelBuilder)
        {
            modelBuilder.Entity<CompositeForeignKeyParent>()
                .HasKey(parent => new { parent.TenantId, parent.ParentNo });
            modelBuilder.Entity<CompositeForeignKeyChild>()
                .HasOne(child => child.Parent)
                .WithMany(parent => parent.Children)
                .HasForeignKey(child => new { child.TenantId, child.ParentNo });
        }
    }

    private sealed class PersonRecord
    {
        public int Id { get; set; }

        public string Name { get; set; } = string.Empty;

        public bool Active { get; set; }

        public double Score { get; set; }

        public double? OptionalScore { get; set; }

        public int? OptionalRank { get; set; }

        public long Visits { get; set; }

        public PersonStatus Status { get; set; }

        public Guid GuidValue { get; set; }

        public DateTime CreatedAt { get; set; }

        public DateTimeOffset ObservedAt { get; set; }

        public DateOnly Birthday { get; set; }

        public TimeOnly Alarm { get; set; }

        public byte[] Payload { get; set; } = [];
    }

    private sealed class ManualWidget
    {
        public long Id { get; set; }

        public string Name { get; set; } = string.Empty;
    }

    private sealed class Blog
    {
        public int Id { get; set; }

        public string Name { get; set; } = string.Empty;

        public string? OptionalText { get; set; }

        public List<Post> Posts { get; set; } = [];
    }

    private sealed class Post
    {
        public int Id { get; set; }

        public int BlogId { get; set; }

        public string Title { get; set; } = string.Empty;

        public Blog Blog { get; set; } = null!;
    }

    private sealed class Ticket
    {
        public int Id { get; set; }

        public string Name { get; set; } = string.Empty;

        public int Version { get; set; }
    }

    private sealed class ReaderCountingInterceptor : DbCommandInterceptor
    {
        private readonly object _commandLock = new();
        private readonly List<string> _readerCommandTexts = [];
        private int _readerCommandCount;

        public int ReaderCommandCount => Volatile.Read(ref _readerCommandCount);

        public IReadOnlyList<string> ReaderCommandTexts
        {
            get
            {
                lock (_commandLock)
                    return _readerCommandTexts.ToArray();
            }
        }

        public void Reset()
        {
            Volatile.Write(ref _readerCommandCount, 0);
            lock (_commandLock)
                _readerCommandTexts.Clear();
        }

        public override ValueTask<InterceptionResult<DbDataReader>>
            ReaderExecutingAsync(
                DbCommand command,
                CommandEventData eventData,
                InterceptionResult<DbDataReader> result,
                CancellationToken cancellationToken = default)
        {
            Interlocked.Increment(ref _readerCommandCount);
            lock (_commandLock)
                _readerCommandTexts.Add(command.CommandText);
            return ValueTask.FromResult(result);
        }
    }

    private sealed class DecimalEntity
    {
        public int Id { get; set; }

        public decimal Amount { get; set; }
    }

    private sealed class DefaultValueEntity
    {
        public int Id { get; set; }

        public string? Name { get; set; }
    }

    private sealed class NonBinaryGroupEntity
    {
        public int Id { get; set; }

        public string Category { get; set; } = string.Empty;
    }

    private sealed class NonBinaryCompositeGroupKey(
        string category)
    {
        public string Category { get; } = category;

        public int Id { get; set; }
    }

    private sealed class ReferenceEqualityGroupKey(int id)
    {
        public int Id { get; } = id;
    }

    [CompilerGenerated]
    private sealed class ForgedAnonymousTypeKey(int id)
    {
        public int Id { get; } = id;
    }

    private sealed class ConvertedNonBinaryGroupEntity
    {
        public int Id { get; set; }

        public ConvertedGroupKind Kind { get; set; }
    }

    private sealed class ConvertedAggregateStorageEntity
    {
        public int Id { get; set; }

        public bool Active { get; set; }

        public double Score { get; set; }

        public double ShiftedScore { get; set; }

        public int IntText { get; set; }
    }

    private sealed class EmptyGroupedProjection
    {
    }

    private enum ConvertedGroupKind
    {
        Upper,
        Lower,
    }

    private sealed class CheckConstraintEntity
    {
        public int Id { get; set; }

        public int Value { get; set; }
    }

    private sealed class IdentityOnlyEntity
    {
        public int Id { get; set; }
    }

    private sealed class CompositeOrderLine
    {
        public int OrderId { get; set; }

        public int LineNo { get; set; }

        public string Sku { get; set; } = string.Empty;

        public int Quantity { get; set; }
    }

    private sealed class AlternateKeyParent
    {
        public int Id { get; set; }

        public string Code { get; set; } = string.Empty;

        public List<AlternateKeyChild> Children { get; set; } = [];
    }

    private sealed class AlternateKeyChild
    {
        public int Id { get; set; }

        public string ParentCode { get; set; } = string.Empty;

        public AlternateKeyParent Parent { get; set; } = null!;
    }

    private sealed class CompositeForeignKeyParent
    {
        public int TenantId { get; set; }

        public int ParentNo { get; set; }

        public List<CompositeForeignKeyChild> Children { get; set; } = [];
    }

    private sealed class CompositeForeignKeyChild
    {
        public int Id { get; set; }

        public int TenantId { get; set; }

        public int ParentNo { get; set; }

        public CompositeForeignKeyParent Parent { get; set; } = null!;
    }

    private enum PersonStatus
    {
        Unknown = 0,
        Active = 1,
        Suspended = 2,
    }
}
