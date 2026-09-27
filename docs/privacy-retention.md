# Privacy & Retention

Privacy & Retention updates selected live fields while retaining records and their relationships. Open **Data Hygiene → Privacy & Retention**, use a table's **Privacy & Retention** action, or search the command palette. The existing duplicate, validation, and orphan tools remain available.

## Find and manage saved rules

One policy applies to every main record matching its conditions. You do not create a rule per customer. For example, a single policy can select `Customers.Email IS NOT NULL` and generate an anonymous placeholder for each matching email, even when thousands of customers match. Date rules are optional; status flags, NULL checks, comparisons, and related records can define the group instead. Execution limits still apply to each run.

**Saved privacy rules** lists all saved policies for the current database in a searchable table. Each row shows the policy name, main table, selected fields and operations, eligibility summary, and revision. Search by name, table, field (such as `Email`), operation, or condition. Multiple search words narrow the results together. The table filter includes main, target, and configured related tables. Results are sorted by policy name and displayed 20 per page; searching covers all saved policies, including those on other pages.

Choose **Open / edit** to load a policy into the editor, then preview its current matching records. Use **New policy**, **Duplicate**, **Save policy**, or import/export to manage reusable configurations. Filtering the list does not change the open policy or execute a run.

## Manual workflow

1. Find a saved rule in the searchable table and choose **Open / edit**, or choose **New policy**, name it, and select its main table.
2. Add the relationship mappings required by conditions and related targets. Declared foreign keys are suggested in both directions. Explicit mappings use ordered column lists; `TenantId, CustomerId` matches the corresponding two target columns. Adding a mapping does not automatically select fields for modification.
3. Build eligibility with AND/OR groups, typed comparisons, NULL checks, age conditions, and “has/has no related records.” Conditions inside a related predicate apply to the related table. An empty related predicate checks for any related record.
4. Add each target table, select its path from the main table, and choose each field and operation.
5. Save the policy, then preview. Review eligible main records, eligible and changed counts per table, preserved fields, warnings, and bounded before/after samples. Blocking issues prevent preparation.
6. Apply and confirm the displayed database, policy revision, and counts. Progress reports uncommitted updates. Cancellation before commit rolls back the entire update transaction. Once commit starts, wait for a confirmed outcome or reconcile an uncertain outcome.
7. Download the run summary or inspect recent committed runs. Edit and save policies, duplicate them, or export/import JSON. Import creates a new policy identity. Reopen a database and select its saved policy to reuse it.

For example, select customers whose `LastSeen` is older than 365 days, with no orders in the last 365 days and no open disputes. Replace their names and emails, and erase addresses on explicitly mapped orders. Leave customer/order keys, amounts, taxes, and ledger fields unselected.

## Individual customer requests using stored procedures

For an individual customer erasure request, use a reusable, parameterized stored procedure when you need application-specific handling. Define the fields and table relationships once, then supply the customer selection for each request. Thousands of requests can call the same procedure; they do not require thousands of saved privacy policies. Keep request intake, identity verification, approval, and status tracking in your existing application or support process. A dedicated request-management feature is outside this workflow.

Use an email address to find and verify the customer in that process, then pass their stable customer ID to the procedure. Include a tenant ID where customer IDs are only unique within a tenant. Do not embed an individual's email or other personal values in the procedure definition, parameter defaults, or saved policy configuration. An internal customer ID is still a sensitive reference when it can be linked to a person.

### Example: clear customer and copied shipping fields

This is an illustrative schema, not a procedure installed by Privacy & Retention. Assume `Customers` has `TenantId`, `CustomerId`, `Name`, and `Email`; `Orders` has the same tenant/customer references plus `ShippingName` and `ShippingAddress`. The four personal fields must accept NULL, and their constraints must permit the resulting values. Review the actual schema, dependent tables, triggers, callbacks, and any fields that must be retained before adapting this example.

In Studio's procedure editor, create `ClearCustomerPersonalFields` and add these parameters with no defaults:

| Parameter | Type | Required |
| --- | --- | --- |
| tenantId | INTEGER | Yes |
| customerId | INTEGER | Yes |

Paste the following into **Body SQL**, then save. CSharpDB stores the SQL body and parameter definitions through its procedure editor or `CreateProcedureAsync`; this example is not a `CREATE PROCEDURE` SQL script.

```sql
UPDATE Orders
SET ShippingName = NULL, ShippingAddress = NULL
WHERE TenantId = @tenantId AND CustomerId = @customerId
  AND (ShippingName IS NOT NULL OR ShippingAddress IS NOT NULL);

UPDATE Customers
SET Name = NULL, Email = NULL
WHERE TenantId = @tenantId AND CustomerId = @customerId
  AND (Name IS NOT NULL OR Email IS NOT NULL);
```

Both statements identify the same customer within the tenant. They leave row counts, customer/order keys, relationships, amounts, taxes, and accounting entries intact, subject to any additional database side effects. The NULL checks avoid updating already-cleared rows on a repeat call. For nonnullable fields, design type-compatible replacements and check unique constraints instead of substituting this NULL example unchanged.

### Review and execute each request

1. Verify the requested customer and the fields approved for clearing in the existing request process. Identify every applicable copy explicitly; this procedure does not discover them.
2. Review matching counts using a separate read-only procedure with the same required parameters and the SELECT body below. Confirm that the customer exists and that the related-row scope is expected. These counts show rows with selected values still present.
3. Open `ClearCustomerPersonalFields`, verify the database and procedure, enter the approved IDs in **Args JSON**, and choose **Run**. For example: `{"tenantId": 7, "customerId": 12345}`. The client equivalent is `ExecuteProcedureAsync("ClearCustomerPersonalFields", args, cancellationToken)` with a dictionary containing those two arguments.
4. Check the execution outcome and each statement's affected count. Record the request reference, procedure/version used, timestamps, counts, and confirmed outcome in the existing request process, without copying original field values. A successful execution with zero changes can mean already-cleared fields or a nonexistent customer; resolve that distinction before closing the request.

Read-only review procedure body:

```sql
SELECT COUNT(*) AS CustomerMatches
FROM Customers
WHERE TenantId = @tenantId AND CustomerId = @customerId;

SELECT COUNT(*) AS CustomersToChange
FROM Customers
WHERE TenantId = @tenantId AND CustomerId = @customerId
  AND (Name IS NOT NULL OR Email IS NOT NULL);

SELECT COUNT(*) AS OrdersToChange
FROM Orders
WHERE TenantId = @tenantId AND CustomerId = @customerId
  AND (ShippingName IS NOT NULL OR ShippingAddress IS NOT NULL);
```

The current direct procedure executor starts one transaction for the body and commits after all statements succeed; it attempts rollback on failure or cancellation. Do not add `BEGIN`, `COMMIT`, or `ROLLBACK` to these bodies or wrap the call in another client transaction. Statement results from a failed run are not evidence of committed changes. If commit acknowledgement is lost, investigate the database state and request record before retrying; procedure execution has no privacy run-ID reconciliation protocol.

**Procedure execution is separate from Privacy & Retention.** The review queries are not a prepared preview: data may change before Run, which acts on the then-current matches. Privacy policy limits, stale-preview checks, protected-key/shared-row checks, trigger/callback rejection, and `__privacy_runs` receipts do not automatically apply to a custom procedure. Implement any required guards and durable audit records in your application/procedure design. Test the adapted procedure on synthetic data, including another tenant with the same customer ID, repeated calls, and rollback after a later statement fails.

This scenario clears selected live fields only. It neither establishes that an entire person is unidentifiable nor completes all aspects of a GDPR request. Unmapped copies, other systems, and backups remain separate responsibilities, as do decisions about which records or fields must be retained.

## Eligibility and dates

The reference time is frozen at preview in UTC. “Older than N days” means **strictly before** `referenceUtc - N days`. “Within last N days” includes both the cutoff and the reference time; future dates do not match it. Each age condition requires an explicit column and a positive day count.

ISO storage accepts ISO dates and timestamps; timestamps with offsets are normalized to UTC, and values without offsets use UTC. Unix seconds and milliseconds require explicit selection. Other local date formats require an exact .NET date format and a time-zone identifier. Ambiguous or nonexistent daylight-saving times are invalid.

Missing dates do not satisfy an age condition. Invalid dates generate a field-level warning without including their values. Unknown comparisons propagate through conditions: in particular, a missing/invalid related activity date cannot establish “no recent activity.” An independent true OR branch can still establish eligibility. Use explicit NULL checks when missing data itself is an intended condition.

## Field operations

| Operation | Behavior |
| --- | --- |
| Erase | Sets a nullable field to NULL. |
| Constant | Parses a replacement according to the declared SQL type and enforces its facets. Binary constants use hexadecimal; UUID constants use UUID text. |
| Anonymous | Generates a stable text placeholder from the policy ID, stable schema identities, and record key. It does not hash the original personal field or store a reversible mapping. The current placeholder needs 74 characters. |
| Partial | Retains a configured number of Unicode text elements at either end and replaces the middle with `*`. Rejects settings that reveal an entire nonempty value. |
| Email | Replaces the local part with `*` and retains the domain. Invalid email shapes block preparation; choose full replacement instead. |

All operations preserve existing NULLs. An unchanged policy applied again to unchanged data produces zero additional field changes. Partial masks retain length and visible information; the email preset retains the domain. Stable placeholders and preserved relationships also retain linkability. A successful run describes changed fields, **not a guarantee that a person or database is unidentifiable**.

## Preservation and preflight

- Only configured columns are assigned. Engine-generated rowversion values can advance normally and are excluded from the preview's preserved-column list.
- Primary keys, selected record-locator keys, identity columns, declared incoming/outgoing foreign-key columns, and all configured relationship columns are protected. Blocking messages identify the dependency.
- Each target needs a primary key or a nonnullable unique key/index to locate rows safely. A chosen unique locator is protected even without a primary key.
- Standalone unique constraints/indexes are checked against the proposed final values, including existing unmodified records and collations. Type, NULL, and supported CHECK constraints are validated before writes; the engine enforces constraints during apply as well.
- First-version CHECK preflight supports row-local literal/column, binary/unary, NULL, BETWEEN, IN, and COLLATE expressions. Other CHECK expressions block preparation.
- UPDATE triggers and reachable host mutation callbacks block target updates. A provider that cannot verify callback safety is rejected. Merely registering an unused scalar function does not block a table.
- Related target paths may contain multiple explicit mappings. A related row reached from both eligible and ineligible main records blocks the run. Correct eligibility or mapping before previewing again.
- Records are never deleted. Unselected personal-data copies are not discovered or modified automatically.

## Transaction and recovery contract

The initial implementation requires a standard file-backed direct client with transaction-consistent schema reads, exclusive sessions, and streaming queries against one database. Direct and durable hybrid storage are supported; remote/sharded, memory-only, custom storage factories, and snapshot-only hybrid execution are not enabled.

Privacy operations reserve the originating connection and open a private session with an operating-system file-writer barrier. Other operations on that connection wait; another writable file handle prevents preparation/apply from starting. Close other applications using the file and finish active readers/transactions before retrying. This prevents independent engine handles from bypassing the transaction's writer lock. Private maintenance SQL is excluded from host SQL diagnostics. Each new privacy session opens a fresh database view, so committed changes between preview and apply invalidate the prepared fingerprint.

Preview reads within a transaction and rolls it back without writing metadata or business records. It retains transient proposed changes and samples, plus a fingerprint covering relevant source rows, eligibility inputs, relationships, and database schema. Schema reads include incoming dependencies from other tables. Changing policy, database, schema, or snapshot data requires another preview. Editing, closing, or invalidating the preview releases its samples.

Apply uses the same pinned client. It first persists a value-free `Pending` run intent for crash recovery. Then it opens one update transaction, rechecks the saved policy revision and full snapshot fingerprint, and writes bounded batches. It verifies affected counts, final values, unchanged fields/keys, and row counts before recording the successful receipt and committing **in that same transaction**. The preliminary run intent is the only separate commit; masking batches never commit separately.

Failures or cancellation before commit roll back all masking updates. If commit/rollback acknowledgement is uncertain, the UI reports **Unknown**, retains the run ID, and blocks further runs. **Reconcile run** acquires the database writer lock and checks the durable journal: a committed receipt confirms success; a still-pending intent proves that no masking commit was recorded and is marked rolled back. If the original session still holds the writer lock, close that session and retry reconciliation. Pending runs remain discoverable after reopening the application/database. Never retry an uncertain apply without reconciliation and a fresh preview.

`__privacy_policies` stores versioned policy JSON and optimistic revision numbers. `__privacy_runs` stores pending/outcome metadata and successful receipts. Both are registered internal tables, omitted from ordinary user-table listings. Neither stores original field values, preview samples, or reversible mappings. Run downloads contain IDs, revision, reference/completion timestamps, status, and aggregate changed counts. Policy exports contain the configured conditions and constants; avoid putting unnecessary personal values in configuration. Errors do not expose source values or generated SQL.

## Configurable limits

Studio binds the `DataPrivacy` configuration section; the library accepts `PrivacyLimits` in the `PrivacyService` constructor.

```json
{
  "DataPrivacy": {
    "PreviewRows": 25,
    "MaxAffectedRows": 10000,
    "MaxEvaluatedRows": 100000,
    "MaxPreparedBytes": 33554432,
    "TimeoutSeconds": 120,
    "BatchSize": 100,
    "MaxStatementBytes": 262144
  }
}
```

Preview rows are limited per table. Evaluated rows include every row read from configured main, related, and target tables; eligibility is evaluated against that bounded snapshot. Prepared-data accounting includes captured values, replacements, relation lookups, and samples; it is not a cap on the whole application's managed heap. Exceeding a preparation/execution limit blocks the run or rolls back pending updates. Timeout/cancellation does not interrupt the commit acknowledgement or rollback cleanup, because their outcome must be established separately.

## Library usage

```csharp
using CSharpDB.DataPrivacy;

var store = new PrivacyPolicyStore();
var service = new PrivacyService();
var policy = new PrivacyPolicy
{
    Name = "Inactive customers",
    RootTable = "Customers",
    Eligibility = new()
    {
        Children = [new() { Kind = PrivacyConditionKind.OlderThan,
            Column = "LastSeen", Days = 365 }]
    },
    Targets = [new()
    {
        Table = "Customers",
        Columns = [new() { Column = "Name", Kind = PrivacyMaskKind.Constant,
            Replacement = "Anonymous" }]
    }]
};

policy = await store.SaveAsync(client, policy, cancellationToken);
using var preview = await service.PreviewAsync(client, policy, ct: cancellationToken);
// Display preview.Tables and obtain the user's confirmation before calling ApplyAsync.
var receipt = await service.ApplyAsync(client, preview, policy, ct: cancellationToken);
if (receipt.Status == "Unknown")
    receipt = await service.ReconcileAsync(client, receipt.RunId, cancellationToken);
```

`ValidateAsync` performs read-only validation of a saved policy against current data. Prepared previews are single-use and must be disposed when abandoned. They are not portable to another client/database. Do not log their sample collections. Razor concerns are confined to Studio, outside `CSharpDB.DataPrivacy`.

## Validation and boundaries

`tests/CSharpDB.DataPrivacy.Tests` covers retention/date conditions, Unicode and repeated masks, composite/multihop propagation, shared rows, keys and constraints, preservation of eCommerce accounting, stale inputs, cancellation/target changes, durable policy/receipt storage, limits, trigger/callback rejection, and commit-acknowledgement recovery. It also renders the Studio component with the normal host function registration. Browser and Desktop WebView2 smoke checks exercise policy creation/import, save, preview, apply, receipts, and repeat preview on isolated synthetic databases.

Implementation verification (2026-09-20): 44 privacy tests, 56 existing hygiene/catalog/transactional-snapshot tests, 32 Admin hygiene/tab-management tests, and 27 data-generation tests passed. The Desktop build completed with zero warnings/errors. The final WebView2 hybrid-database run changed one customer and one related order; subsequent desktop and browser previews showed zero further changes. Earlier direct-database browser checks also verified policy creation, commit, reload, and a value-free receipt download. This is targeted validation, not a claim that the entire repository test suite was run.

Scheduling, cross-database/shard execution, key remapping, reversible masking, and automatic discovery of personal-data copies are deferred. Replacing live values does not securely erase backups, historical copies, transaction logs, or storage remnants.
