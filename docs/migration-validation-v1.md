# Migration validation v1

Phase 3 defines validation as the boundary between a completed copy and an
activated CSharpDB target. The runner opens one source validation snapshot and
one target validation snapshot, then uses those same instances for normalized
schema evidence, 64-bit counts, and canonical checksums.

## Snapshot outcome

Each snapshot reports one of three consistency states:

- `Established`: an immutable source, backup, snapshot, transaction, or bound
  watermark and the CSharpDB reader snapshot provide one coherent view.
- `NotEstablished`: reads can complete, but a best-effort source cannot prove
  that counts and rows came from one view.
- `Unavailable`: the provider cannot state a consistency contract.

Only `Established` evidence can produce a passing activation report. Matching
data under either other state is `Inconclusive`, never `Passed`.

## Schema and object contracts

`csharpdb-migration-schema/v1` hashes sorted, persisted schema identities and
their target-inspectable definitions. The target builds its evidence from the
database catalog rather than echoing the plan. Differences retain the object
identity and source/target definition digests, without embedding SQL bodies.

Each included table or collection also receives an object-contract digest. It
binds the `csharpdb-canon-v1` contract, ordered field identities, target names,
stored and logical types, conversions, exclusions, and primary-key ordinals.

## Partitioned checksum

Every row is projected and streamed through `csharpdb-canon-v1`. The validator
stores one fixed 64-byte record per occurrence, so duplicates are retained:

```text
keyed object:   key-hash[32] || row-hash[32]
unkeyed object: row-hash[32] || row-hash[32]
```

The first hash byte selects one of exactly 256 partitions. Within a partition,
records are sorted by unsigned lexicographic byte order and identical tuples
are reduced to `(first-hash, second-hash, multiplicity-u64)`. Partitions are
processed from 0 through 255, which is also global tuple order.

Object digests use the ASCII domain `csharpdb-validation-object/v1`, followed
by the raw canonical contract hash, raw object-contract digest, keyed flag,
row count as u64 big-endian, and every grouped tuple with an u64 big-endian
multiplicity. Partition digests use
`csharpdb-validation-partition/v1` with the same prefix plus the one-byte
partition ID. Hashes are SHA-256 and serialized as lowercase hexadecimal.

Mismatch details contain hashes and multiplicities only. A keyed difference
can identify source-only, target-only, or changed keys. An unkeyed difference
identifies missing or extra row-hash multiplicities. Raw keys and values never
enter the report.

These hashes are pseudonymous evidence, not anonymization. A low-entropy key
such as a small integer can be guessed by enumeration, and hashes can be
linkable within the same canonical contract. Validation reports therefore need
the same access controls as other operational migration records.

## Bounded spill

Raw partition spools and sorted runs have explicit versioned, big-endian
headers. The 256 partitions use fixed 4 KiB buffers, so a low file-handle cap
does not cause a reopen for nearly every uniformly distributed row. Run
generation uses two fixed memory buffers; generated runs are compacted online
through at most 64 bounded merge levels, and merge fan-in/open files are
capped. The workspace is created under an explicit root with an exclusive
reservation and durable ownership marker. A configurable hard byte limit
rejects excess spill. Cancellation and failure cleanup is non-cancelable and
removes only the verified owned workspace.

## Report and activation ordering

`csharpdb-migration-validation/v1` is a deterministic, self-digesting JSON
artifact. It binds plan, catalog, capability, source, target, both snapshot
identities, target CSharpDB version, canonical contract, schema evidence, object counts/checksums,
partitions, mismatches, and stable diagnostics. It excludes times, machine
names, temporary paths, SQL bodies, raw keys, and raw values. A deterministic
text projection is available for operators. The serializer rejects credential-
shaped identity values and any outcome that contradicts its schema, count,
checksum, partition, or consistency evidence.

The success order is strict:

1. finish schema, count, and checksum evidence;
2. normalize and digest the report;
3. durably write and atomically publish the report file;
4. have the target reopen and verify the published `Passed` report through a
   runner-issued activation permit;
5. atomically store its activation receipt and change the staged target from
   `awaiting-validation` to `activated`;
6. report success.

An exact retry reuses the existing report and activation receipt. A changed
report, binding, validation level, snapshot, or digest is rejected. A report
write failure leaves the target awaiting validation.
