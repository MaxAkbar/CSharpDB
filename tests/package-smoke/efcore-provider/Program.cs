using CSharpDB.EntityFrameworkCore.PackageSmoke;
using Microsoft.EntityFrameworkCore;

if (args.Length != 2
    || (args[0] is not "crud" and not "verify-migration"))
{
    Console.Error.WriteLine(
        "Usage: CSharpDB.EntityFrameworkCore.PackageSmoke <crud|verify-migration> <database-path>");
    return 2;
}

string mode = args[0];
string databasePath = Path.GetFullPath(args[1]);
string? databaseDirectory = Path.GetDirectoryName(databasePath);
if (!string.IsNullOrWhiteSpace(databaseDirectory))
    Directory.CreateDirectory(databaseDirectory);

if (mode == "crud")
{
    File.Delete(databasePath);

    int itemId;
    await using (var db = new PackageSmokeContext(databasePath))
    {
        await db.Database.EnsureCreatedAsync();
        var item = new PackageSmokeItem { Name = "created" };
        db.Items.Add(item);
        await db.SaveChangesAsync();

        item.Name = "updated";
        await db.SaveChangesAsync();
        itemId = item.Id;
    }

    await using (var reopened = new PackageSmokeContext(databasePath))
    {
        PackageSmokeItem item = await reopened.Items.SingleAsync();
        if (item.Id != itemId || item.Name != "updated")
            throw new InvalidOperationException("The updated row did not survive reopen.");

        reopened.Items.Remove(item);
        await reopened.SaveChangesAsync();
    }

    await using (var reopened = new PackageSmokeContext(databasePath))
    {
        if (await reopened.Items.AnyAsync())
            throw new InvalidOperationException("The deleted row survived reopen.");
    }

    Console.WriteLine("Package-only CRUD and reopen smoke passed.");
    return 0;
}

await using (var migrated = new PackageSmokeContext(databasePath))
{
    if (!await migrated.Database.CanConnectAsync())
        throw new InvalidOperationException("The migrated database could not be opened.");

    migrated.Items.Add(new PackageSmokeItem { Name = "migration-update" });
    await migrated.SaveChangesAsync();
}

await using (var reopened = new PackageSmokeContext(databasePath))
{
    if (await reopened.Items.CountAsync() != 1)
        throw new InvalidOperationException(
            "The row written through the migrated schema did not survive reopen.");
}

Console.WriteLine("Package-only migration database update smoke passed.");
return 0;
