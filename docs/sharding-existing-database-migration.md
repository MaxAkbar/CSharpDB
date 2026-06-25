# Sharding Existing Databases

Internal guidance for turning a large existing unsharded CSharpDB database into
a sharded deployment.

This is separate from creating a new sharded database. The Admin **Create
Sharding** workflow can seed an active shard map into an opened local master DB,
but it does not split or move existing monolithic data.

## Current Support

Supported today:

- Create a new sharded setup from Admin by opening a local direct master DB,
  creating a shard-map draft, writing that map into the master DB, and reopening
  Admin in sharded mode.
- Open an existing master DB that already contains an active shard map.
- Manage an existing sharded deployment with route-aware Admin tabs, catalog
  draft updates, read-only fan-out SQL, and shard migration APIs.
- Move data between shards for an already-sharded deployment with exact-key and
  bucket-range migration APIs.

Not supported yet:

- Splitting one existing large unsharded database into multiple shard databases
  from Admin.
- Automatically inferring a shard key from SQL or table definitions.
- Treating the original monolithic database as a data shard just because a shard
  map was written somewhere.

## Why A Map Alone Is Not Enough

The master DB is metadata, not a data shard. It owns the active map, directory
metadata, catalog history, migration checkpoints, and future failover metadata.
Normal routed reads and writes go to the shard DB resolved by the route key.

If an operator only seeds a shard map while all rows still live in the old
unsharded DB, routed queries will look in the configured shard DBs and will not
find the old rows. Fan-out also targets shards, not the master DB. Existing data
must be copied into shard DBs and verified before cutover.

## Required Flow

An existing monolithic database needs a split/backfill/cutover workflow:

1. Choose the route model.
   Decide the keyspace and route key, such as `tenant_id`, `account_id`, or
   `order_month`.

2. Define route-owned objects.
   For each table or collection, identify the route-key column or property and
   primary key. Objects that cannot be assigned to one route need an explicit
   strategy before migration.

3. Create the master DB and shard DBs.
   The master DB stores sharding metadata only. Each shard DB is a normal
   CSharpDB database with its own WAL and commit path.

4. Copy schema to every shard that may own the object.
   Route-owned tables and collections need compatible schema on their target
   shards before data movement starts.

5. Backfill data in batches.
   Read the source DB by route-owned object, resolve each row or document to a
   shard from the proposed map, and insert it into the destination shard. Large
   migrations need durable checkpoints so the copy can resume after failure.

6. Verify every copied scope.
   Compare counts and checksums per table, collection, route range, and shard.
   Verification failure must leave the old monolithic DB as the active source of
   truth.

7. Fence or stop writes for cutover.
   Once the bulk copy is complete, stop writes or run an explicit write-fence
   window, copy the final delta, and verify again.

8. Seed the master catalog.
   Only after data is in the shard DBs and verified should the active shard map
   be written into the master DB.

9. Cut over clients and hosts.
   Admin, daemon, API, and applications should open the master DB through the
   normal database locator, such as `ConnectionStrings:CSharpDB`.

10. Retain the source DB.
    Keep the original monolithic DB as a rollback/forensics copy until the
    sharded deployment passes operational validation.

## Data Modeling Checks

Before splitting an existing DB, confirm:

- Each route-owned table has a stable route-key column.
- Each route-owned collection has a stable route-key property.
- Route-key values are immutable or have an explicit move workflow.
- Primary keys are stable and can be used for idempotent batch copy/retry.
- Foreign-key relationships do not require cross-shard enforcement.
- Global/reference tables have a deliberate strategy, such as duplicate to every
  shard or keep outside route-owned flows.
- Alternate lookup keys have a shard-directory plan when callers do not already
  know the route key.

## Admin UX Needed

The missing Admin workflow should be named separately from **Create Sharding**,
for example **Shard Existing Database**. It should collect:

- source DB path;
- master DB path;
- shard definitions;
- route keyspace;
- route-key column per table;
- route-key property per collection;
- batch size;
- verification mode;
- write-fence or cutover mode.

It should preview the resulting map, copy schema, backfill data, checkpoint
progress, verify results, seed the master catalog only after verification, and
then reopen Admin against the master DB.

## Acceptance Gates

Do not mark an existing DB split complete until:

- all route-owned objects have copied row/document counts matching the source;
- checksum validation passes for every copied scope;
- failed or interrupted batches can resume without duplicating data;
- the final write-fence/delta copy has run and passed verification;
- the master DB contains the active shard map;
- Admin can open the master DB and resolve representative route keys;
- read-only fan-out shows expected shard row counts;
- the original source DB has been retained for rollback.
