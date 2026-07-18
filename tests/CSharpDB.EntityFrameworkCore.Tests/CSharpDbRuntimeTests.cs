using System.Data.Common;
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
        double? optionalScore = null) =>
        new()
        {
            Name = name,
            Active = active,
            Score = score,
            OptionalScore = optionalScore,
            Visits = visits,
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
