# Sample Datasets

This folder contains SQL scripts that create realistic database schemas with sample data. Each script generates tables, indexes, views, and triggers for a specific business domain.

For API workflows, each SQL sample also has a companion `*.procedures.json` file that defines table-backed stored procedures for `/api/procedures`.

## Available Samples

### Northwind Electronics — `ecommerce-store.sql`

An online electronics retailer with customers, product categories, orders, reviews, and shipping addresses.

| Tables | Indexes | Views | Triggers | Statements | Procedures |
|--------|---------|-------|----------|------------|------------|
| 7 | 4 | 2 | 1 | 84 | 2 |

**Tables:** `customers`, `categories`, `products`, `orders`, `order_items`, `reviews`, `shipping_addresses`
**Views:** `order_summary`, `product_catalog`
**Trigger:** `trg_update_stock` — automatically decrements product stock on order item insert
**Procedure file:** `ecommerce-store.procedures.json`
**Procedures:** `GetCustomerOrderHistory`, `AdjustProductStock`

---

### Riverside Health Center — `medical-clinic.sql`

A medical clinic with departments, doctors, patients, appointments, prescriptions, medical records, and billing.

| Tables | Indexes | Views | Triggers | Statements | Procedures |
|--------|---------|-------|----------|------------|------------|
| 7 | 4 | 2 | 1 | 75 | 2 |

**Tables:** `departments`, `doctors`, `patients`, `appointments`, `prescriptions`, `medical_records`, `billing`
**Views:** `upcoming_appointments`, `outstanding_bills`
**Trigger:** `trg_record_after_appointment` — automatically creates a medical record when an appointment is completed
**Procedure file:** `medical-clinic.procedures.json`
**Procedures:** `GetPatientOutstandingBills`, `MarkBillPaid`

---

### Maplewood Unified School District — `school-district.sql`

A school district with teachers, students, courses, enrollments, classrooms, scheduling, and attendance tracking.

| Tables | Indexes | Views | Triggers | Statements | Procedures |
|--------|---------|-------|----------|------------|------------|
| 7 | 4 | 2 | 1 | 116 | 2 |

**Tables:** `teachers`, `students`, `courses`, `enrollments`, `classrooms`, `course_schedule`, `attendance`
**Views:** `class_roster`, `teacher_schedule`
**Trigger:** `trg_enrollment_attendance` — automatically creates attendance records when a student enrolls
**Procedure file:** `school-district.procedures.json`
**Procedures:** `GetStudentCourses`, `RecordAttendance`

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

The script expects the API at `http://localhost:61818` by default and also imports the companion procedure file (`samples/ecommerce-store.procedures.json`) using create-or-update behavior.

Override API URL:

```bash
CSHARPDB_API_BASEURL=http://localhost:5000 dotnet script samples/run-sample.csx -- samples/ecommerce-store.sql
```

Override procedure file:

```bash
dotnet script samples/run-sample.csx -- samples/ecommerce-store.sql samples/ecommerce-store.procedures.json
```

### Option 2: Via the CLI

```bash
dotnet run --project src/CSharpDB.Cli -- mydata.db

csdb> .read samples/ecommerce-store.sql
```

This loads schema/data only. Procedure catalogs are loaded through the API runner above.

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
- Procedure imports are idempotent in the API runner (`POST` then `PUT` on conflict).
- The API uses `Data Source=csharpdb.db` by default (`src/CSharpDB.Api/appsettings.json`).

## See Also

- [Getting Started Tutorial](../docs/getting-started.md) — Learn the CSharpDB API step by step
- [REST API Reference](../docs/rest-api.md) — Full endpoint documentation
- [CLI Reference](../docs/cli.md) — Interactive REPL commands
