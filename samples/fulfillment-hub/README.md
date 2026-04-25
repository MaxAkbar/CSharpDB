# Fulfillment Hub

Fulfillment Hub is a guided operations sample for CSharpDB. Instead of showing isolated features, it gives you one warehouse story and lets the database follow that story from inbound receiving to outbound shipment to returns, with forms, reports, pipelines, collections, saved queries, procedures, triggers, and full-text search all attached to the same working set.

If you want one sample that teaches the platform as a whole, start here.

## Start Here

Run the seeder:

```bash
dotnet run --project samples/fulfillment-hub/FulfillmentHubSample.csproj
```

Each run creates a fresh demo database here:

```text
samples/fulfillment-hub/bin/Debug/net10.0/fulfillment-hub-demo.db
```

The seeder does more than load SQL. It also:

- creates the relational schema and fixed snapshot data
- imports stored procedures and saved queries
- stores Admin forms and reports
- stores and runs pipeline packages
- seeds typed collections
- builds a full-text index over operational playbooks

When the run finishes, keep that database and explore it in the Admin UI, the CLI, or through `CSharpDB.Client`.

## How To Use This Sample

Use the sample in this order:

1. Run the seeder.
2. Open the generated database in the Admin project or query it through the CLI.
3. Follow the walkthrough below in sequence.
4. After each step, inspect the related table, view, saved query, form, report, or collection.
5. Change a few rows and rerun the same step so you can see which parts of the platform are relational, which parts are metadata-driven, and which parts are procedural.

This sample is easiest to learn if you treat it like a live operations system, not a pile of setup files.

## The Story

It is Friday morning. Seattle and Denver are trying to clear outbound demand before the weekend, Atlanta is handling returns, and a few SKUs are already running tight. Your job is to operate the day using the features seeded by this sample.

### 1. Start With The Live Queue

First, look at the order board. This is the operational heartbeat of the sample.

```sql
SELECT order_number, customer_name, warehouse_code, order_status, required_ship_date
FROM order_fulfillment_board
WHERE order_status IN ('released', 'allocated', 'picking')
ORDER BY required_ship_date, priority_code DESC, order_number;
```

What to learn here:

- `order_fulfillment_board` is a view used as a planner-facing queue
- the data model is relational, but the workflow is operational
- this same queue also feeds the saved query and one report source

Then compare it to the saved query:

```sql
EXEC RefreshOperationalStats;
```

That procedure returns the current table stats, shortage watch, and open order board in one call. It is the simplest way to see how stored procedures can package operational read models for Admin users.

### 2. Find The Inventory Problem Before You Allocate Anything

Now switch to the shortage watch.

```sql
SELECT warehouse_code, sku, product_name, available_qty, inbound_qty, reorder_point, shortage_qty
FROM low_stock_watch
WHERE shortage_qty > 0
ORDER BY shortage_qty DESC, warehouse_code, sku;
```

This is where the sample starts teaching you to combine features:

- `low_stock_watch` is a view
- it is also a saved query target
- it is also a report source
- it is also exported by a stored pipeline package

If you are in the Admin UI, open the `Low Stock Watch` report after querying this view. You should see the same shortage story represented as a print-style artifact instead of a SQL result set.

### 3. Pull In New Supply Through Pipelines

The warehouse receives new inbound files from external systems. The sample already stores and runs these pipeline packages for you:

- `supplier-receipts-import`
- `marketplace-orders-import`
- `low-stock-export`

Inspect the staged results:

```sql
SELECT *
FROM supplier_receipts_stage
ORDER BY id;

SELECT *
FROM marketplace_orders_stage
ORDER BY id;
```

What to learn here:

- pipelines are not separate from the database story; they feed the same operational model
- one package imports CSV, one imports JSON, and one exports a query result to CSV
- the run history is stored, so pipeline execution becomes part of the system record

Check the generated export file too:

```text
samples/fulfillment-hub/bin/Debug/net10.0/generated-output/low-stock-watch.csv
```

### 4. Receive A Purchase Order

Now imagine Seattle receives the replenishment shipment for `PO-9001`.

Run:

```sql
EXEC ReceivePurchaseOrder purchaseOrderId=9001;
```

Then inspect:

```sql
SELECT *
FROM purchase_order_receiving_board
WHERE purchase_order_id = 9001
ORDER BY sku;
```

What to learn here:

- procedures can coordinate multi-statement updates
- one procedure can update base tables, write an event row, and return useful follow-up result sets
- the `Purchase Order Receiving` form is bound to the same purchase-order model and child lines

In the Admin UI, open the `Purchase Order Receiving` form. This is where the sample becomes more than SQL: you can see lookups, computed totals, and a child data grid hanging off the same underlying schema.

### 5. Allocate A Waiting Order

Now that inbound supply is available, allocate a waiting order:

```sql
EXEC AllocateOrder orderId=7005;
```

Then inspect the order again:

```sql
SELECT *
FROM order_fulfillment_board
WHERE order_id = 7005;
```

This step teaches a few things at once:

- procedures can reserve inventory by updating multiple related tables
- the order workflow is stateful, but the state is still inspectable with plain SQL
- the `Order Workbench` form is the human-facing surface for the same workflow

Open `orders-workbench` in the Admin UI and look at the order line child tab. That is the same order you just changed procedurally, now visible through form metadata rather than hand-written UI code.

### 6. Create The Shipment

Once an order is allocated, ship it:

```sql
EXEC CreateShipment orderId=7001, shipmentId=8101, shipmentNumber='SHP-8101', carrierId=2;
```

Then inspect the shipment report source:

```sql
SELECT *
FROM shipment_manifest_report_source
WHERE shipment_number = 'SHP-8101'
ORDER BY sku;
```

Now open the `Shipment Manifest` report in Admin.

What to learn here:

- a report source can be a view tailored for printing
- reports do not need a separate reporting database
- the same operational transaction that creates shipment rows also produces report-ready output

### 7. Process A Return

The day is not only outbound. Atlanta receives returns too.

Run:

```sql
EXEC RecordReturn returnId=8502, newStatus='closed', disposition='restock';
```

Then inspect:

```sql
SELECT *
FROM return_queue
ORDER BY requested_date DESC, return_number;
```

This is where the sample shows that reverse logistics is not a separate subsystem. Returns live in the same operational model, can update stock, and can be managed with the `Return Intake` form.

### 8. Check The Operational Audit Trail

Several actions in the sample write to `ops_events` through triggers and procedures.

Inspect that stream:

```sql
SELECT entity_type, entity_id, event_type, event_date, actor_name, details
FROM ops_events
ORDER BY id DESC
LIMIT 20;
```

What to learn here:

- triggers are used for automatic audit-style events
- procedures can add richer business events with actor and narrative context
- the event log becomes a simple operational history that can be queried without extra infrastructure

### 9. Use Full-Text Search To Find The Playbook

At some point, the operator needs guidance, not just data. That is why the sample also seeds `ops_playbooks` and creates a full-text index.

Search for the receiving issue through the engine API:

```csharp
await using var db = await Database.OpenAsync("samples/fulfillment-hub/bin/Debug/net10.0/fulfillment-hub-demo.db");
var hits = await db.SearchAsync("fts_ops_playbooks", "partial receipt");
```

This part is important. The sample is not just showing full-text search in isolation. It is showing how documentation, runbooks, and live operations can sit in the same database.

### 10. Look At The Collections Side

Not every useful artifact belongs in a rigid relational table. The sample also seeds two collections:

- `scanner_sessions`
- `webhook_archive`

These are there to show where typed or semi-structured operational data fits.

The sample seeder verifies them with collection path lookups such as:

- `CurrentWave.OrderNumber`
- `$.tags[]`

What to learn here:

- relational tables still own the transactional model
- collections work well for transient device state, webhook payloads, and nested documents
- indexed path queries let those documents stay queryable without forcing them into awkward relational tables

## How The Files Map To The Story

- [schema.sql](C:/Users/maxim/source/Code/CSharpDB/samples/fulfillment-hub/schema.sql) creates the relational model, views, triggers, and base snapshot
- [procedures.json](C:/Users/maxim/source/Code/CSharpDB/samples/fulfillment-hub/procedures.json) defines the main operational actions
- [saved-queries.json](C:/Users/maxim/source/Code/CSharpDB/samples/fulfillment-hub/saved-queries.json) stores the queue-style queries that make sense in Admin
- [queries.sql](C:/Users/maxim/source/Code/CSharpDB/samples/fulfillment-hub/queries.sql) is the workbook for exploration and learning
- [pipelines](C:/Users/maxim/source/Code/CSharpDB/samples/fulfillment-hub/pipelines/low-stock-export.json) contains stored pipeline definitions
- [imports](C:/Users/maxim/source/Code/CSharpDB/samples/fulfillment-hub/imports/supplier-receipts.csv) contains source files for those pipelines
- [Program.cs](C:/Users/maxim/source/Code/CSharpDB/samples/fulfillment-hub/Program.cs) seeds everything that is not pure DDL: forms, reports, pipeline packages, collections, and full-text indexes

## A Good Learning Loop

If you want to really learn the platform instead of just running the sample once, use this loop:

1. Run the sample.
2. Read one procedure in `procedures.json`.
3. Execute it.
4. Inspect the changed tables and views.
5. Open the related form or report in Admin.
6. Look at the same behavior in `Program.cs` to see how metadata was seeded.
7. Change the schema or procedure and rerun the sample.

That loop will teach you more about CSharpDB than reading the feature list in isolation.
