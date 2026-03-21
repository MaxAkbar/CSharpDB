# CSharpDB Storage Internals - Video Tutorial Walkthrough

> A beginner-friendly guided tour of how CSharpDB stores data on disk. No prior database
> engine knowledge required. We start from "what even is an embedded database?" and work
> our way down to the raw bytes on disk.

---

## Part 1: What Is an Embedded Database?

Most databases you've probably worked with -- SQL Server, PostgreSQL, MySQL -- run as a
**separate process** (a server). Your app connects to that server over a network or socket,
sends it queries, and gets results back. You have to install it, configure it, manage it.

An **embedded database** is completely different. There is no separate server. The database
engine is just a **library** that lives inside your application's process. Your app talks
to it by calling C# methods directly -- no network, no sockets, no installation.

```
Traditional (Server) Database:
  [Your App] ---network---> [Database Server Process] ---> [Files on Disk]
                 TCP/IP           separate program

Embedded Database (CSharpDB):
  [Your App + CSharpDB library] ---> [Files on Disk]
         same process                  just one file!
```

### Why would you want this?

- **Zero setup** -- just add a NuGet package
- **No server to manage** -- no ports, no configuration, no Docker containers
- **Fast** -- no network round-trips, data goes straight from disk to your objects
- **Portable** -- your entire database is a single file you can copy, email, or back up
- **Great for** -- desktop apps, mobile apps, CLI tools, IoT, unit tests, prototyping

### The tradeoff?

Embedded databases are designed for **one application at a time**. They're not meant to
handle hundreds of separate client connections like PostgreSQL does. But for single-app
scenarios, they're simpler and often faster.

### SQLite is the most famous example

If you've used SQLite before, CSharpDB follows the same philosophy -- but it's written
entirely in C# with zero native dependencies, so it runs anywhere .NET runs (including
NativeAOT, WebAssembly, etc.)

---

## Part 2: What's on Disk? The Two-File System

When you create a CSharpDB database, you get **at most two files**:

```
mydata.cdb        <-- the main database file (all your tables, indexes, schemas)
mydata.cdb.wal    <-- the write-ahead log (temporary, used for crash safety)
```

That's it. Your entire database -- every table, every index, every row of data, every
schema definition -- lives inside that single `.cdb` file.

The `.wal` file is a temporary helper file used for crash safety (more on that in Part 7).
It comes and goes. If CSharpDB shuts down cleanly, the WAL gets merged back into the main
file and disappears.

### Opening a database is one line of code:

```csharp
// File-backed (persistent) -- data survives app restarts
await using var db = await Database.OpenAsync("mydata.cdb");

// In-memory (great for tests) -- data disappears when the app exits
await using var db = await Database.OpenInMemoryAsync();

// Hybrid: lives in memory for speed, auto-persists to disk for durability
await using var db = await Database.OpenHybridAsync("mydata.cdb");
```

If the file doesn't exist yet, `OpenAsync` creates it automatically. If it already exists,
it opens it and validates that it's a real CSharpDB file. That's it -- you're ready to
run SQL.

> **Source:** `src/CSharpDB.Engine/Database.cs`

---

## Part 3: Pages -- The Building Blocks of Everything

Here's the first core concept: the database file is **not** one big blob of text or JSON.
It's divided into fixed-size chunks called **pages**, each exactly **4,096 bytes** (4 KB).

### Why 4 KB?

This isn't an arbitrary number. Hard drives, SSDs, and operating systems all move data
around in chunks. The most common chunk size (called a "sector" or "block") is 4 KB.
By aligning our database pages to that size, we make sure that:

- Reading one page = one efficient disk read
- Writing one page = one efficient disk write
- No wasted partial reads

### Visualizing the file

Think of the database file as a long row of numbered boxes, each exactly 4 KB:

```
|  Page 0  |  Page 1  |  Page 2  |  Page 3  |  Page 4  | ...
|  4096 B  |  4096 B  |  4096 B  |  4096 B  |  4096 B  |
```

When the database needs more space, it simply adds a new page to the end of the file.
When data is deleted and a page becomes empty, CSharpDB puts it on a **freelist** (a
list of recycled pages) so it can be reused later without growing the file.

### How this looks in code

Page size and structure are defined in a single constants file:

```csharp
// src/CSharpDB.Storage/Paging/PageConstants.cs

public static class PageConstants
{
    public const int PageSize = 4096;           // every page is exactly this many bytes
    public const int FileHeaderSize = 100;      // the first 100 bytes of page 0 are special

    // Usable space within a page (page 0 has less because of the file header)
    public static int UsableSpace(uint pageId) =>
        pageId == 0 ? PageSize - FileHeaderSize : PageSize;
}
```

---

## Part 4: The File Header -- The First Thing in the Database

Page 0 is special. Its first **100 bytes** contain a **file header** -- a small block of
metadata that describes the entire database. Think of it as the "table of contents" or the
"passport" of your database file.

When CSharpDB opens a file, the very first thing it does is read these 100 bytes to make
sure it's looking at a real database (not a random file someone renamed).

### What's in the header?

```
Byte offset  Size     What it stores
-----------  ----     --------------
0            4 bytes  Magic bytes: "CSDB"
                      These are the literal ASCII characters C, S, D, B. If
                      CSharpDB opens a file and doesn't see "CSDB" at the start,
                      it knows this isn't a valid database and refuses to continue.

4            4 bytes  Format version (currently 1)
                      Allows future versions to know how to read older files.

8            4 bytes  Page size (4096)
                      Recorded here so the reader knows how big each page is.

12           4 bytes  Total page count
                      How many pages the database file currently contains.
                      A brand-new database has 1 page (page 0 itself).

16           4 bytes  Schema catalog root page
                      The page number where table/index/view definitions are
                      stored. This is the entry point to find any table's data.

20           4 bytes  Freelist head page
                      The first page in the "recycling bin" of deleted pages.
                      0 means no pages are available for reuse.

24           4 bytes  Change counter
                      Incremented every time a transaction commits. Useful for
                      detecting whether the database has changed since last read.

28           72 bytes Reserved (zeroed out)
                      Reserved for future features. Currently all zeros.
```

### Here's the code that defines these offsets:

```csharp
// src/CSharpDB.Storage/Paging/PageConstants.cs

public static class PageConstants
{
    // Magic bytes: "CSDB"
    public static readonly byte[] MagicBytes = "CSDB"u8.ToArray();
    public const int FormatVersion = 1;

    // File header layout offsets (within page 0)
    public const int MagicOffset = 0;           // 4 bytes
    public const int VersionOffset = 4;          // 4 bytes
    public const int PageSizeOffset = 8;         // 4 bytes
    public const int PageCountOffset = 12;       // 4 bytes
    public const int SchemaRootPageOffset = 16;  // 4 bytes - root of schema catalog B+tree
    public const int FreelistHeadOffset = 20;    // 4 bytes - first page of freelist, 0 = none
    public const int ChangeCounterOffset = 24;   // 4 bytes
}
```

After the 100-byte header, the remaining 3,996 bytes of page 0 are used for actual data
just like any other page -- nothing goes to waste.

---

## Part 5: Slotted Pages -- How Data Is Organized Inside a Page

Now we know the file is made of 4 KB pages. But how is data organized **inside** each
page? CSharpDB uses a classic technique called a **slotted page**.

### The real-world analogy

Imagine a notebook page where you:
1. Write a **directory** at the top of the page (listing where each note is)
2. Write your actual **notes** starting from the bottom of the page, working upward

As you add more entries, the directory grows **downward** and the notes grow **upward**.
When they meet in the middle, the page is full.

### The actual layout

```
+-------------------------------------------------------------+
|  PAGE HEADER (9 bytes)                                      |
|  [PageType: 1 byte]   What kind of page is this?            |
|  [CellCount: 2 bytes] How many data entries on this page?   |
|  [ContentStart: 2 bytes] Where does the data area begin?    |
|  [RightChild/NextLeaf: 4 bytes] Pointer to related page     |
+-------------------------------------------------------------+
|  CELL POINTER ARRAY  (grows downward --->)                  |
|  [ptr0: 2 bytes] [ptr1: 2 bytes] [ptr2: 2 bytes] ...        |
|                                                             |
|  Each pointer is a 2-byte offset that says "cell #N is      |
|  located at byte position X on this page."                  |
+-------------------------------------------------------------+
|                                                             |
|                      FREE SPACE                             |
|                                                             |
|  (unused area between the pointers and the data)            |
|                                                             |
+-------------------------------------------------------------+
|  CELL CONTENT AREA  (<--- grows upward)                     |
|  ... [cell2 data] [cell1 data] [cell0 data]                 |
|                                                             |
|  Each "cell" is one data entry (like a row in a table).     |
|  New cells are added from the bottom of the page upward.    |
+-------------------------------------------------------------+
```

### Why this design?

1. **Fast lookups** -- Want cell #5? Read the pointer at position 5, jump straight there.
   No need to scan through all the other cells.
2. **Easy reordering** -- To sort cells, you just rearrange the pointer array. The actual
   cell data doesn't move. This is much cheaper than physically shuffling bytes around.
3. **Space efficient** -- Free space is consolidated in one contiguous block in the middle,
   making it easy to tell exactly how much room is left.
4. **Page is full when they meet** -- When the pointer array (growing forward) bumps into
   the cell data (growing backward), the page is full and needs to be split into two pages.

### Page types

The first byte of the header tells CSharpDB what kind of page this is:

| Byte value | Type         | What it holds                                        |
|------------|--------------|------------------------------------------------------|
| `0x0D`     | **Leaf**     | Actual data -- key-value pairs (rows in your table)  |
| `0x05`     | **Interior** | Routing signposts that point to child pages          |
| `0x00`     | **Freelist** | An empty recycled page, waiting to be reused         |

Don't worry about Interior vs Leaf yet -- that's explained in the B+Tree section next.

### The code that manages this:

```csharp
// src/CSharpDB.Storage/Paging/SlottedPage.cs

public struct SlottedPage
{
    private readonly byte[] _data;       // the raw 4096-byte page buffer
    private readonly int _baseOffset;    // where the page header starts

    public SlottedPage(byte[] pageData, uint pageId)
    {
        _data = pageData;
        // Page 0 starts at byte 100 (after the file header); all others start at 0
        _baseOffset = PageConstants.ContentOffset(pageId);
    }

    // Read the page type (leaf, interior, or freelist)
    public byte PageType
    {
        get => _data[_baseOffset + PageConstants.PageTypeOffset];
        set => _data[_baseOffset + PageConstants.PageTypeOffset] = value;
    }

    // How many cells (data entries) are on this page?
    public ushort CellCount
    {
        get => BinaryPrimitives.ReadUInt16LittleEndian(
            Span[(_baseOffset + PageConstants.CellCountOffset)..]);
        set => BinaryPrimitives.WriteUInt16LittleEndian(
            Span[(_baseOffset + PageConstants.CellCountOffset)..], value);
    }
}
```

Reading a specific cell is a two-step process -- look up the offset in the pointer array,
then read the cell data at that offset:

```csharp
public Span<byte> GetCell(int index)
{
    // Step 1: "Where is cell #index on this page?"
    ushort offset = GetCellOffset(index);

    // Step 2: Read the cell at that offset
    //         The cell starts with a varint-encoded size, then the actual data
    var cellData = Span[offset..];
    ulong cellSize = Varint.Read(cellData, out int headerBytes);
    return Span[offset..(offset + headerBytes + (int)cellSize)];
}
```

---

## Part 6: B+Trees -- How Tables and Indexes Are Organized

This is the most important data structure in the entire engine. **Every table, every
index, and even the schema catalog** is stored as a B+tree.

### What problem does it solve?

Imagine you have a table with 1 million rows. If they were stored in a flat list, finding
row #500,000 would mean scanning through 499,999 rows first. That's way too slow.

A **B+tree** (pronounced "bee plus tree") is a tree-shaped structure that lets you find
any row by its key in just a handful of steps -- even with millions of rows. Think of it
like the index at the back of a textbook: instead of reading every page to find a topic,
you look it up in the index and jump straight to the right page.

### How it works: two types of pages

A B+tree uses the two page types we saw earlier:

**Leaf pages** (`0x0D`) -- These hold the actual data. Each cell in a leaf page contains
a key (like a row ID) and the row's data (the actual column values).

**Interior pages** (`0x05`) -- These are like signposts at a highway intersection. They
don't hold data themselves -- they hold keys and pointers that say "if you're looking for
a key less than 50, go to page 3; if between 50 and 100, go to page 7; if over 100, go
to page 12."

### Visual example

Let's say you have a `users` table with IDs 1 through 150:

```
                         [Interior Page (root)]
                         "keys < 50 → left"
                         "keys < 100 → middle"
                         "keys >= 100 → right"
                        /         |           \
                       /          |            \
              [Leaf Page A]  [Leaf Page B]  [Leaf Page C]
              rows 1-49      rows 50-99     rows 100-150
                   |              |              |
                   +--->nextLeaf--+--->nextLeaf---+
```

**To find row #75:**
1. Start at the root (interior page)
2. 75 is between 50 and 100, so follow the middle pointer
3. Arrive at Leaf Page B -- scan it to find row 75
4. Done! Only 2 page reads instead of scanning everything.

**The `nextLeaf` links** are important too: leaf pages are chained together left to right.
This means if you need to scan all rows (like `SELECT * FROM users`), you can walk through
the leaves in order without ever going back up through the interior pages. Very efficient
for range queries like `WHERE id BETWEEN 10 AND 80`.

### What's stored in each cell?

**Leaf cell** (holds actual data):
```
[totalSize: varint]  [key: 8 bytes]  [row payload: variable length]
        |                  |                    |
  "This cell is         The row ID         The actual column values
   N bytes total"       (64-bit integer)   (encoded as binary -- see Part 7)
```

**Interior cell** (holds routing info):
```
[totalSize: varint]  [leftChild: 4 bytes]  [key: 8 bytes]
        |                   |                     |
  "This cell is        Page number of        "Keys less than this
   N bytes total"      the left subtree       value go to leftChild"
```

### What happens when a page gets full?

When you insert a new row and the leaf page doesn't have enough free space, the page
**splits**:

1. A new page is allocated
2. Roughly half the cells stay on the old page, half move to the new page
3. A new routing entry is added to the parent interior page, pointing to the new page
4. If the parent is also full, it splits too (this can ripple up to the root)

This is what keeps the tree balanced -- every leaf is always the same number of levels
from the root, so lookups are always fast.

> **Source:** `src/CSharpDB.Storage/BTree/BTree.cs`

---

## Part 7: Record Encoding -- How Rows Become Bytes

When you run:
```sql
INSERT INTO users VALUES (1, 'Alice', 30);
```

Those values (the integer `1`, the string `'Alice'`, and the integer `30`) need to be
converted into raw bytes before they can be stored in a B+tree leaf cell. This process is
called **encoding** (or serialization).

### The format (a worked example)

Let's trace exactly how `(1, 'Alice', 30)` gets encoded:

```
Step 1: Write the column count
  We have 3 columns, so write: [0x03]  (the number 3 as a single byte)

Step 2: For each column, write a type tag (1 byte) then the data

  Column 1: integer 1
    Type tag: [0x01]  (means INTEGER)
    Data:     [0x01 0x00 0x00 0x00 0x00 0x00 0x00 0x00]  (the number 1 as 8 bytes)

  Column 2: text 'Alice'
    Type tag: [0x03]  (means TEXT)
    Data:     [0x05]  (length of "Alice" in bytes = 5)
              [0x41 0x6C 0x69 0x63 0x65]  (the UTF-8 bytes for "Alice")

  Column 3: integer 30
    Type tag: [0x01]  (means INTEGER)
    Data:     [0x1E 0x00 0x00 0x00 0x00 0x00 0x00 0x00]  (the number 30 as 8 bytes)
```

Final encoded bytes (26 bytes total):
```
03 01 01 00 00 00 00 00 00 00 03 05 41 6C 69 63 65 01 1E 00 00 00 00 00 00 00
```

### The five data types

CSharpDB supports five fundamental types. Everything maps to one of these:

| Type tag | Name        | How it's stored                         | Example              |
|----------|-------------|-----------------------------------------|----------------------|
| `0x00`   | **NULL**    | Nothing! 0 bytes. Just the tag.         | `NULL`               |
| `0x01`   | **INTEGER** | 8 bytes, little-endian signed int64     | `42`, `-7`, `1000000`|
| `0x02`   | **REAL**    | 8 bytes, IEEE 754 double-precision      | `3.14`, `0.001`      |
| `0x03`   | **TEXT**    | Length prefix (varint) + UTF-8 bytes    | `'Hello'`, `'Alice'` |
| `0x04`   | **BLOB**    | Length prefix (varint) + raw bytes      | Binary data, images  |

### The encoder code:

```csharp
// src/CSharpDB.Storage/Serialization/RecordEncoder.cs

public static byte[] Encode(ReadOnlySpan<DbValue> values)
{
    // First pass: calculate total size so we can allocate one buffer
    int size = Varint.SizeOf((ulong)values.Length);  // space for column count
    foreach (var v in values)
        size += 1 + ValueDataSize(v);                // 1 byte type tag + data

    var buffer = new byte[size];
    int pos = Varint.Write(buffer, (ulong)values.Length);  // write column count

    // Second pass: write each column
    foreach (var v in values)
    {
        buffer[pos++] = (byte)v.Type;  // write the type tag byte

        switch (v.Type)
        {
            case DbType.Null:
                break;  // NULL = just the tag, no data bytes needed

            case DbType.Integer:
                BinaryPrimitives.WriteInt64LittleEndian(buffer.AsSpan(pos), v.AsInteger);
                pos += 8;
                break;

            case DbType.Real:
                BinaryPrimitives.WriteInt64LittleEndian(
                    buffer.AsSpan(pos),
                    BitConverter.DoubleToInt64Bits(v.AsReal));
                pos += 8;
                break;

            case DbType.Text:
                string text = v.AsText;
                int byteCount = Utf8.GetByteCount(text);
                pos += Varint.Write(buffer.AsSpan(pos), (ulong)byteCount); // write length
                pos += Utf8.GetBytes(text.AsSpan(), buffer.AsSpan(pos, byteCount)); // write string
                break;

            case DbType.Blob:
                var blob = v.AsBlob;
                pos += Varint.Write(buffer.AsSpan(pos), (ulong)blob.Length);
                blob.CopyTo(buffer.AsSpan(pos));
                pos += blob.Length;
                break;
        }
    }
    return buffer;
}
```

### What's a "varint"?

You'll notice `Varint.Write` and `Varint.Read` used in several places. A **varint**
(variable-length integer) is a clever encoding trick. Instead of always using 8 bytes for
every number, small numbers use fewer bytes:

| Value       | Bytes needed | Savings vs fixed 8-byte |
|-------------|-------------|------------------------|
| 0-127       | 1 byte      | 87.5% smaller          |
| 128-16383   | 2 bytes     | 75% smaller            |
| 16384+      | 3+ bytes    | Still smaller          |

Since most numbers in databases are small (column counts, string lengths, small IDs), this
saves a lot of space. The encoding uses **LEB128** format -- each byte uses 7 bits for
data and 1 "continuation" bit that says "there's more bytes coming":

```csharp
// src/CSharpDB.Storage/Serialization/Varint.cs

public static int Write(Span<byte> buffer, ulong value)
{
    int i = 0;
    do
    {
        byte b = (byte)(value & 0x7F);   // grab the lowest 7 bits
        value >>= 7;                     // shift the remaining bits down
        if (value != 0) b |= 0x80;      // if there are more bits, set the high bit
        buffer[i++] = b;                 // write this byte
    } while (value != 0);
    return i;  // return how many bytes we wrote
}
```

For example, the number `300`:
- Binary: `100101100` (9 bits -- doesn't fit in 7)
- Byte 1: `0101100` + continuation bit = `10101100` = `0xAC`
- Byte 2: `0000010` + no continuation  = `00000010` = `0x02`
- Result: `[0xAC, 0x02]` -- just 2 bytes instead of 8!

---

## Part 8: The Write-Ahead Log (WAL) -- Crash Safety

### The problem: what if your app crashes mid-write?

Imagine you're updating a table that requires modifying 3 pages. Your app writes page 1,
writes page 2, and then... the power goes out. Page 3 never got written. Now your database
is in a **half-updated state** -- some pages reflect the new data, some don't. This is
called **corruption**, and it's the worst thing that can happen to a database.

### The solution: write to a separate log first

Instead of modifying the main `.cdb` file directly, CSharpDB writes all changes to a
separate **Write-Ahead Log** (the `.wal` file) first. Only after the WAL is safely on disk
does it consider the transaction committed. Later, the WAL changes are merged back into
the main file during a **checkpoint**.

```
Without WAL (dangerous):
  [Your App] --writes directly--> [mydata.cdb]
                                  (if crash mid-write = corruption!)

With WAL (safe):
  [Your App] --writes to--> [mydata.cdb.wal] --checkpoint--> [mydata.cdb]
                            (if crash here,    (can be redone safely
                             WAL is discarded)  from the WAL)
```

### Why is this safe?

- If the app crashes **before** the WAL commit: the partial WAL entries are thrown away,
  and the main file is untouched. No corruption.
- If the app crashes **after** the WAL commit but before checkpoint: when CSharpDB opens
  next, it detects the WAL, replays the committed changes, and merges them into the main
  file. No data lost.
- If the app crashes **during** checkpoint: the WAL still has the complete data. It just
  does the checkpoint again on next open. No problem.

### WAL file structure

The WAL file has its own header (32 bytes) followed by a series of **frames**. Each frame
is a snapshot of one modified page:

```
+---------------------------------------------+
| WAL HEADER (32 bytes)                       |
| [magic: "CWAL"]    Identifies WAL file      |
| [version: 4 bytes] Format version           |
| [pageSize: 4 bytes] Should match main file  |
| [dbPageCount: 4 bytes]                      |
| [salt1, salt2: 4 bytes each] For integrity  |
| [checksumSeed: 4 bytes]                     |
| [reserved: 4 bytes]                         |
+---------------------------------------------+
|                                             |
| FRAME 0 (4120 bytes)                        |
| +----------------------------------------+  |
| | Frame Header (24 bytes)                |  |
| | [pageId: 4 bytes]                      |  |
| |   "This frame is a copy of page #N"    |  |
| | [dbPageCount: 4 bytes]                 |  |
| |   "If nonzero, this is a COMMIT frame" |  |
| | [salt1, salt2: 4 bytes each]           |  |
| | [headerChecksum, dataChecksum: 4 each] |  |
| |   Integrity checks to detect corruption|  |
| +----------------------------------------+  |
| | Page Data (4096 bytes)                 |  |
| |   The full contents of that page       |  |
| +----------------------------------------+  |
|                                             |
| FRAME 1 (4120 bytes) ...                    |
| FRAME 2 (4120 bytes) ...                    |
| FRAME N (commit frame -- dbPageCount > 0)   |
+---------------------------------------------+
```

Each frame is exactly **4,120 bytes** (24-byte header + 4,096-byte page copy).

A **commit frame** is simply a frame where the `dbPageCount` field is nonzero. This marks
the end of a transaction -- all frames from the previous commit up to this one belong to
the same atomic unit of work.

### The constants in code:

```csharp
// src/CSharpDB.Storage/Paging/PageConstants.cs

// WAL file header
public static readonly byte[] WalMagic = "CWAL"u8.ToArray();
public const int WalHeaderSize = 32;

// Each WAL frame = header + one full page
public const int WalFrameHeaderSize = 24;
public const int WalFrameSize = WalFrameHeaderSize + PageSize;  // 24 + 4096 = 4120

// Auto-checkpoint when WAL gets this many committed frames
public const int DefaultCheckpointThreshold = 1000;
```

### Transaction lifecycle (step by step)

```
1. BEGIN TRANSACTION
   - Acquire the writer lock
     (CSharpDB allows only ONE writer at a time -- this prevents conflicts)
   - Record where we currently are in the WAL file

2. MAKE CHANGES
   - Modify pages in the in-memory cache
   - Track which pages are "dirty" (modified but not yet saved)

3. COMMIT
   - Write all dirty pages to the WAL as new frames
   - Mark the last frame as a commit frame (set dbPageCount > 0)
   - Call fsync() to force the OS to flush the WAL to physical disk
     (this is the moment the data becomes truly durable)
   - Release the writer lock

4. CHECKPOINT (happens periodically, not on every commit)
   - Copy all committed WAL pages back into the main .cdb file
   - Truncate the WAL file (it starts fresh)
   - By default, this happens after 1000 committed frames
```

### Bonus: how the WAL enables concurrent readers

The WAL isn't just for crash safety -- it also lets **readers and writers work at the same
time** without blocking each other. Here's how:

When a reader starts a query, it takes a "snapshot" of the WAL index at that moment. It
will only see data that was committed **before** its snapshot, even if a writer commits new
data while the reader is working.

```
Timeline:
  Writer:   [write page 5] [write page 12] [COMMIT] [write page 5 again...]
  Reader A:     starts here ----snapshot----->  sees the COMMIT changes
  Reader B:              starts here ----------> doesn't see the COMMIT yet
```

This is called **snapshot isolation** -- each reader sees a frozen, consistent view of the
database. No locking, no waiting, no conflicts.

> **Source:** `src/CSharpDB.Storage/Wal/WriteAheadLog.cs`

---

## Part 9: The Storage Device Layer -- Abstracting Disk I/O

At the very bottom of the architecture, CSharpDB needs to read and write raw bytes to
files. Rather than scattering `File.Read` and `File.Write` calls everywhere, all disk I/O
goes through a clean interface called `IStorageDevice`:

```csharp
// src/CSharpDB.Storage/Device/IStorageDevice.cs

public interface IStorageDevice : IAsyncDisposable, IDisposable
{
    long Length { get; }
    ValueTask<int> ReadAsync(long offset, Memory<byte> buffer, CancellationToken ct = default);
    ValueTask WriteAsync(long offset, ReadOnlyMemory<byte> buffer, CancellationToken ct = default);
    ValueTask FlushAsync(CancellationToken ct = default);
    ValueTask SetLengthAsync(long length, CancellationToken ct = default);
}
```

Just four operations: read bytes, write bytes, flush to disk, and resize the file. That's
all a storage engine needs.

### Why use an interface?

Because CSharpDB has **multiple implementations**:

| Implementation          | Used for                                                 |
|-------------------------|----------------------------------------------------------|
| `FileStorageDevice`     | Real files on disk (the default for persistent databases)|
| `MemoryStorageDevice`   | In-memory databases and unit tests                       |

This means the entire storage engine can run against in-memory "files" for testing, with
**zero code changes** to the B+tree, WAL, or pager. Swap the device, everything else
stays the same.

### The file-backed implementation

`FileStorageDevice` uses .NET's `RandomAccess` API for high-performance async I/O:

```csharp
// src/CSharpDB.Storage/Device/FileStorageDevice.cs

public sealed class FileStorageDevice : IStorageDevice
{
    public FileStorageDevice(string filePath, bool createNew = false)
    {
        // Open with both Asynchronous (non-blocking) and RandomAccess (seek anywhere)
        _handle = File.OpenHandle(
            filePath,
            createNew ? FileMode.CreateNew : FileMode.OpenOrCreate,
            FileAccess.ReadWrite,
            FileShare.ReadWrite,
            FileOptions.Asynchronous | FileOptions.RandomAccess);
    }

    public async ValueTask<int> ReadAsync(long offset, Memory<byte> buffer, ...)
    {
        int totalRead = 0;
        while (totalRead < buffer.Length)
        {
            int read = await RandomAccess.ReadAsync(
                _handle, buffer[totalRead..], offset + totalRead, ct);
            if (read == 0) break;  // reached end of file
            totalRead += read;
        }

        // If we tried to read past the end of the file, fill the rest with zeros
        // (this happens when the database is brand new and the file is small)
        if (totalRead < buffer.Length)
            buffer[totalRead..].Span.Clear();

        return totalRead;
    }

    // FlushAsync forces the OS to write buffered data to the physical disk
    // This is critical for durability -- without it, data could be lost in a crash
    public ValueTask FlushAsync(CancellationToken ct = default)
    {
        RandomAccess.FlushToDisk(_handle);
        return ValueTask.CompletedTask;
    }
}
```

Notice the zero-fill behavior when reading past end of file -- this is a safety net so the
rest of the engine always gets a clean, zeroed page buffer even for newly allocated pages.

---

## Part 10: The Pager -- The Traffic Controller

We've seen pages, B+trees, the WAL, and the storage device. But what sits in the middle
and coordinates everything? That's the **Pager**.

The Pager is the central manager of all page I/O. Every time any part of the engine needs
to read or write a page, it goes through the Pager. Its responsibilities include:

### 1. Page cache (avoid re-reading from disk)

When a page is read from disk, the Pager keeps a copy in memory. If the same page is
requested again, it returns the cached copy instantly -- no disk I/O needed.

CSharpDB offers two caching strategies:

| Cache type            | Behavior                                             |
|-----------------------|------------------------------------------------------|
| `DictionaryPageCache` | Unlimited -- caches every page forever (default)     |
| `LruPageCache`        | Bounded -- evicts least-recently-used pages when full|

### 2. Dirty page tracking

When a page is modified in memory, the Pager marks it as "dirty." On commit, it knows
exactly which pages need to be written to the WAL -- no need to scan every cached page.

### 3. Page allocation and freelist management

When the B+tree needs a new page (e.g., for a page split), the Pager first checks the
**freelist** (recycled pages from deleted data). If the freelist is empty, it extends the
file by one page. This means the database file only grows when it genuinely needs more
space.

### 4. Writer lock enforcement

Only one transaction can write at a time. The Pager enforces this with a
`SemaphoreSlim(1, 1)` -- a lock that allows exactly one holder. Readers don't need the
lock at all (they use WAL snapshots), so reads never block.

### 5. Snapshot readers for MVCC

When a reader starts, the Pager creates a "snapshot reader" that sees a frozen view of the
database at that point in time. This is how multiple readers and one writer can all work
concurrently without interfering with each other.

---

## Part 11: The Schema Catalog -- Where Table Definitions Live

When you write `CREATE TABLE users (id INTEGER, name TEXT)`, that table definition needs
to be stored somewhere persistent. CSharpDB stores all metadata in a **schema catalog**,
which is itself a B+tree (stored in the main database file alongside your data).

The schema catalog stores:

| Category       | What it tracks                                        |
|---------------|-------------------------------------------------------|
| **Tables**    | Table name, column names/types, constraints, which page holds the B+tree root |
| **Indexes**   | Index name, which columns, uniqueness, the index's B+tree root page          |
| **Views**     | View name, the SQL definition                         |
| **Triggers**  | Trigger name, event type, timing, body                |
| **Statistics**| Row counts, column min/max values (used by the query planner) |

When the database opens, the entire catalog is loaded into memory for fast lookups. The
file header's `SchemaRootPageOffset` (byte 16) tells the Pager which page to start reading
from.

> **Source:** `src/CSharpDB.Storage/Catalog/SchemaCatalog.cs`

---

## Part 12: Putting It All Together -- Full Journey of an INSERT

Let's trace the complete path of a simple session:

```csharp
await using var db = await Database.OpenAsync("mydata.cdb");

await db.ExecuteAsync("CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT, age INTEGER)");
await db.ExecuteAsync("INSERT INTO users VALUES (1, 'Alice', 30)");
```

### Step 1: Opening the database

```
Database.OpenAsync("mydata.cdb")
  |
  +-> Create a FileStorageDevice for "mydata.cdb"
  |     Opens the file with async + random access flags
  |
  +-> Read page 0 (the first 4096 bytes)
  |     Check for "CSDB" magic bytes at offset 0
  |     Read page count, schema root page, freelist head
  |
  +-> Open the WAL ("mydata.cdb.wal")
  |     If WAL exists from a previous crash, replay committed frames
  |     If no WAL, create a fresh one
  |
  +-> Load the schema catalog
  |     Follow the schema root page pointer from the file header
  |     Read the catalog B+tree and cache all table/index definitions
  |
  +-> Return the Database object, ready for queries
```

### Step 2: Creating a table

```
db.ExecuteAsync("CREATE TABLE users (...)")
  |
  +-> SQL Parser: tokenize "CREATE TABLE users ..." into an AST
  |
  +-> Allocate a new page for the table's B+tree root
  |     Check freelist first, then extend file if needed
  |
  +-> Add entry to the schema catalog:
  |     "users" -> { columns: [id INTEGER, name TEXT, age INTEGER],
  |                  rootPage: 3,
  |                  primaryKey: id }
  |
  +-> Write dirty pages to WAL
  +-> Commit (fsync the WAL to disk)
```

### Step 3: Inserting a row

```
db.ExecuteAsync("INSERT INTO users VALUES (1, 'Alice', 30)")
  |
  +-> SQL Parser: parse into an INSERT AST
  |
  +-> Look up "users" in the schema catalog
  |     Found: root page = 3, columns = [id, name, age]
  |
  +-> Encode the row values into binary:
  |     [0x03]                                    column count = 3
  |     [0x01][0x01 0x00 0x00 0x00 0x00 0x00 0x00 0x00]   INTEGER 1
  |     [0x03][0x05][0x41 0x6C 0x69 0x63 0x65]            TEXT "Alice"
  |     [0x01][0x1E 0x00 0x00 0x00 0x00 0x00 0x00 0x00]   INTEGER 30
  |
  +-> B+tree insert: starting from root page 3
  |     Descend through interior pages (if any) to find the right leaf
  |     Insert the cell [key=1, payload=encoded bytes] into the leaf page
  |     If the leaf is full, split it into two pages
  |
  +-> Mark modified pages as dirty
  +-> Write dirty pages to WAL as frames
  +-> Mark the last frame as a commit frame
  +-> fsync the WAL --> data is now durable on disk!
  |
  +-> (Later) Checkpoint: copy WAL pages into mydata.cdb, truncate WAL
```

---

## Part 13: The Architecture Layer Cake

Here's how all the pieces stack together:

```
+-------------------------------------------------------------+
|  YOUR APPLICATION CODE                                      |
|  db.ExecuteAsync("SELECT * FROM users WHERE age > 25")      |
+-------------------------------------------------------------+
           |
+-------------------------------------------------------------+
|  ENGINE LAYER                                               |
|  Database class (entry point), Collection<T> (NoSQL API)    |
+-------------------------------------------------------------+
           |
+-------------------------------------------------------------+
|  SQL LAYER                                                  |
|  Tokenizer -> Parser -> AST -> Query Planner -> Operators   |
+-------------------------------------------------------------+
           |
+-------------------------------------------------------------+
|  STORAGE LAYER (everything in this walkthrough)             |
|                                                             |
|  +----------+  +-----------+  +----------------+            |
|  | B+Tree   |  | Schema    |  | Record Encoder |            |
|  | find     |  | Catalog   |  | encode/decode  |            |
|  | insert   |  | tables    |  | rows to bytes  |            |
|  | delete   |  | indexes   |  +----------------+            |
|  | split    |  | views     |                                |
|  +----------+  +-----------+                                |
|                                                             |
|  +----------+  +------------------+                         |
|  | Pager    |  | Write-Ahead Log  |                         |
|  | caching  |  | crash recovery   |                         |
|  | dirty    |  | concurrent reads |                         |
|  | tracking |  | checkpointing    |                         |
|  +----------+  +------------------+                         |
|                                                             |
|  +-------------------------------------------------------+  |
|  | Storage Device (IStorageDevice)                       |  |
|  | FileStorageDevice (disk) or MemoryStorageDevice (RAM) |  |
|  +-------------------------------------------------------+  |
+-------------------------------------------------------------+
           |
+-------------------------------------------------------------+
|  DISK                                                       |
|  mydata.cdb  +  mydata.cdb.wal                              |
+-------------------------------------------------------------+
```

---

## Part 14: NoSQL Collections (Bonus)

Besides SQL, CSharpDB has a **document-style API** for when you just want a key-value
store without writing SQL:

```csharp
// Get or create a typed collection
var users = await db.GetCollectionAsync<User>("users");

// Store a document (key = string, value = any C# object)
await users.PutAsync("user:1", new User("Alice", 30, "alice@example.com"));

// Retrieve by key
var alice = await users.GetAsync("user:1");

// Query with a lambda predicate
await foreach (var kvp in users.FindAsync(u => u.Age > 25))
    Console.WriteLine(kvp.Value.Name);
```

Under the hood, this uses the **exact same B+tree storage** as SQL tables:
- Your C# object is serialized to JSON using `System.Text.Json`
- The string key (like `"user:1"`) is hashed to a 64-bit integer
- That integer becomes the B+tree key, and the JSON bytes become the payload
- Hash collisions are resolved with linear probing (checking the next slot)

So whether you use SQL or the collection API, your data takes the same path through the
Pager, WAL, B+tree, and storage device.

> **Source:** `src/CSharpDB.Engine/Collection.cs`

---

## Quick Reference: Project Structure

```
src/
  CSharpDB.Primitives/        Shared types (DbValue, DbType, Schema, ErrorCode)
                               No dependencies -- everything else depends on this

  CSharpDB.Storage/            ** THE STORAGE ENGINE (this walkthrough) **
    Paging/                      Page layout, Pager, page cache
    BTree/                       B+tree operations (find, insert, delete, split)
    Wal/                         Write-ahead log, transactions, recovery
    Serialization/               Record encoding, varint, schema serialization
    Catalog/                     Schema catalog (table/index/view definitions)
    Device/                      File I/O abstraction (FileStorageDevice, etc.)
    Caching/                     LRU and dictionary page caches
    Checkpointing/               WAL checkpoint policies and execution
    Indexing/                    Secondary index management

  CSharpDB.Sql/                SQL tokenizer, parser, and AST definitions
  CSharpDB.Execution/          Query planner and operator tree executors
  CSharpDB.Engine/             Top-level Database and Collection<T> API
  CSharpDB.Client/             Client SDK with transport abstraction
  CSharpDB.Data/               ADO.NET provider (for familiar DbConnection usage)
  CSharpDB.Native/             NativeAOT C FFI library (use from C, Python, etc.)
  CSharpDB.Storage.Diagnostics/ Read-only tooling for inspecting database files
```

---

## Key Takeaways for the Video

1. **Embedded = no server** -- CSharpDB is a library, not a separate process
2. **Single file** -- your entire database is one `.cdb` file (+ a temporary `.wal`)
3. **4 KB pages** -- the file is divided into fixed-size blocks, aligned to disk sectors
4. **File header** -- first 100 bytes identify the file and point to all the metadata
5. **Slotted pages** -- pointers grow forward, data grows backward, efficient lookups
6. **B+trees everywhere** -- tables, indexes, and the schema catalog are all B+trees
7. **Record encoding** -- rows become compact binary with type tags and varint compression
8. **WAL for crash safety** -- changes go to the log first, then get checkpointed
9. **Concurrent readers** -- WAL snapshots let readers and writers work at the same time
10. **Clean abstractions** -- `IStorageDevice` lets the same engine run on disk or in memory
11. **Zero dependencies** -- pure .NET, runs anywhere .NET runs
