# Sample Datasets

This folder contains SQL scripts that create realistic database schemas with sample data. Each script generates tables, indexes, views, and triggers for a specific business domain.

## Available Samples

### Northwind Electronics — `ecommerce-store.sql`

An online electronics retailer with customers, product categories, orders, reviews, and shipping addresses.

| Tables | Indexes | Views | Triggers | Statements |
|--------|---------|-------|----------|------------|
| 7 | 4 | 2 | 1 | 84 |

**Tables:** `customers`, `categories`, `products`, `orders`, `order_items`, `reviews`, `shipping_addresses`
**Views:** `order_summary`, `product_catalog`
**Trigger:** `trg_update_stock` — automatically decrements product stock on order item insert

---

### Riverside Health Center — `medical-clinic.sql`

A medical clinic with departments, doctors, patients, appointments, prescriptions, medical records, and billing.

| Tables | Indexes | Views | Triggers | Statements |
|--------|---------|-------|----------|------------|
| 7 | 4 | 2 | 1 | 75 |

**Tables:** `departments`, `doctors`, `patients`, `appointments`, `prescriptions`, `medical_records`, `billing`
**Views:** `upcoming_appointments`, `outstanding_bills`
**Trigger:** `trg_record_after_appointment` — automatically creates a medical record when an appointment is completed

---

### Maplewood Unified School District — `school-district.sql`

A school district with teachers, students, courses, enrollments, classrooms, scheduling, and attendance tracking.

| Tables | Indexes | Views | Triggers | Statements |
|--------|---------|-------|----------|------------|
| 7 | 4 | 2 | 1 | 116 |

**Tables:** `teachers`, `students`, `courses`, `enrollments`, `classrooms`, `course_schedule`, `attendance`
**Views:** `class_roster`, `teacher_schedule`
**Trigger:** `trg_enrollment_attendance` — automatically creates attendance records when a student enrolls

---

## Running a Sample

### Option 1: Via the REST API

Start the API server, then use the script runner:

```bash
# 1. Start the API
dotnet run --project src/CSharpDB.Api

# 2. Run a sample (requires dotnet-script: dotnet tool install -g dotnet-script)
dotnet script samples/run-sample.csx -- samples/ecommerce-store.sql
```

The script expects the API at `http://localhost:61818`.

### Option 2: Via the CLI

```bash
dotnet run --project src/CSharpDB.Cli -- mydata.db

csdb> .read samples/ecommerce-store.sql
```

### Option 3: Via C# code

```csharp
using CSharpDB.Engine;

await using var db = await Database.OpenAsync("sample.db");
var sql = File.ReadAllText("samples/ecommerce-store.sql");

// Split and execute each statement
foreach (var stmt in sql.Split(';', StringSplitOptions.RemoveEmptyEntries))
{
    var trimmed = stmt.Trim();
    if (!string.IsNullOrEmpty(trimmed) && !trimmed.StartsWith("--"))
        await db.ExecuteAsync(trimmed);
}
```

> **Note:** For trigger blocks (`CREATE TRIGGER ... BEGIN ... END`), you need a statement splitter that respects `BEGIN...END` boundaries. The `run-sample.csx` script handles this automatically.

## Notes

- Running the same sample twice against the same database will fail on `CREATE TABLE` statements since the scripts do not include cleanup. Delete the `.db` file or use a fresh database for a clean run.
- The API uses `Data Source=csharpdb.db` by default (`src/CSharpDB.Api/appsettings.json`).

## See Also

- [Getting Started Tutorial](../docs/getting-started.md) — Learn the CSharpDB API step by step
- [REST API Reference](../docs/rest-api.md) — Full endpoint documentation
- [CLI Reference](../docs/cli.md) — Interactive REPL commands
