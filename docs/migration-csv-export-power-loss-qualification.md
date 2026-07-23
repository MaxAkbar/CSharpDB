# Migration CSV Export Power-Loss Qualification

## Status And Purpose

This is the release-gate runbook for abrupt-power-loss qualification of the
CSV prepared-output checkpoint journal and manifest-last publication sequence.
The gate is **open**. Passing process-termination tests is necessary, but is
not evidence that namespace changes survive removal of power.

The automated child-process tests stop an export process with
`Process.Kill`. They qualify fresh-process handle release, stale-state
classification, exact retry, and the application-level ordering around the
injected boundaries. They do not remove power from the operating system,
storage controller, host cache, or device cache. They therefore cannot expose
acknowledged writes or renames that disappear when those caches lose power and
cannot close this gate.

Closing the gate requires the external hard-power procedure below on every
filesystem and cache configuration in the supported matrix. If that matrix
cannot be qualified, the implementation's support checks and documentation
must first be narrowed to the tested boundary, for example local fixed NTFS
volumes with the qualified cache policy.

## Durability Boundary Under Test

Prepared CSV bytes, pending checkpoint bytes, publication staging data, and
publication staging manifests are flushed through their file handles before
the related handle-based rename. The implementation does not call
`FlushFileBuffers` on the parent directory after checkpoint, CSV, or manifest
namespace replacement. Consequently, a successful file flush does not by
itself prove that the directory entry or replacement is stable after power
loss.

The current runtime admission is broader than a single filesystem: it accepts
eligible local Windows paths while rejecting UNC paths, mapped network
volumes, reparses, links, and unsafe files. That admission must not be treated
as a durability qualification for every local Windows filesystem, storage
stack, or cache policy.

The test outcome must establish both:

1. an interruption before the operation reports success leaves only a
   fail-closed state from which the exact retry converges; and
2. an operation that has reported success remains committed after an immediate
   hard power cut.

Recovery after a reported success is not a substitute for the second
property. A caller may already have acted on the success result.

## Required Test Environment

Use a disposable, self-hosted Windows virtual machine on a hypervisor whose
host controller can cut power without requesting guest shutdown. Hosted CI
agents and an in-guest reboot or shutdown command are not suitable.

Provision:

- a disposable operating-system disk;
- a separate persistent local data volume for the retained snapshot, export
  journal, output pair, run descriptor, and barrier;
- host-controlled hard-off and boot operations that do not invoke guest
  shutdown, service stop, process cleanup, or filesystem dismount;
- a way to clone or snapshot the persistent volume immediately after hard-off
  and before the export is retried;
- fixed, recorded virtual controller, virtual disk, host-cache, guest disk
  write-cache, filesystem, allocation-unit, and hypervisor settings; and
- a clean baseline image that can be restored for each trial.

Do not place the output on a shared folder, network filesystem, mapped drive,
temporary OS disk, or volume whose contents are reconstructed at boot.

For each supported matrix cell, record at minimum:

- Windows edition, build, and patch level;
- .NET runtime and CSharpDB build/commit;
- hypervisor and virtual storage-controller versions;
- virtual disk format and dynamic/fixed allocation;
- filesystem and allocation-unit size;
- guest write-cache and write-cache-buffer-flushing settings;
- host write-through/write-back cache mode;
- physical device model, firmware, cache mode, and power-loss-protection
  status; and
- whether the volume is encrypted, compressed, deduplicated, or layered over
  another storage system.

Changing any of these durability-relevant values creates a new matrix cell.

## Durable Run Descriptor And Barrier

Create the run directory, immutable run descriptor, and a fixed-size barrier
file before starting the export. Flush both files through their handles and
complete a setup reboot before arming the trial. The descriptor binds:

- a unique run ID and matrix-cell ID;
- the retained snapshot identity;
- exact source table, profile, resource limits, and checkpoint interval;
- fully normalized CSV and manifest paths;
- expected canonical final CSV and manifest lengths and SHA-256 digests;
- the selected boundary and pre-boundary state; and
- the test executable and commit.

The barrier file is evidence, not checkpoint or publication authority. It must
be precreated so reaching a boundary does not depend on creating another
directory entry. At the injected boundary, the child:

1. overwrites the fixed-size barrier record with the run ID, boundary ID, and
   monotonically increasing attempt number;
2. calls `FlushFileBuffers` on the barrier handle;
3. signals the host over an out-of-band host-visible control channel; and
4. remains paused without closing export handles or exiting.

The host must wait for the explicit "barrier flushed and paused"
acknowledgement. It must then issue the hypervisor's immediate power-off
operation. It must not ask the guest to shut down, reboot, terminate the
process, flush volumes, or quiesce a snapshot. Record the host's barrier
observation and hard-off acknowledgement.

The barrier proves where the child paused; it does not prove the export's
directory entries were durable. A missing or invalid barrier after reboot
makes the trial inconclusive rather than a pass.

## Trial Procedure

Run every trial from a restored baseline:

1. Materialize the exact retained snapshot and independently pinned identity.
   Produce a no-fault reference export and retain the expected canonical CSV,
   manifest, checkpoint, lengths, and SHA-256 digests outside the guest.
2. Restore the clean persistent-volume baseline appropriate to the boundary.
   For later boundaries, construct the required exact predecessor state by
   running normal product operations, not by fabricating private journal
   files.
3. Verify and flush the immutable run descriptor and precreated barrier.
4. Start the instrumented export child. Arm exactly one boundary and wait for
   its flushed barrier acknowledgement.
5. From the host, hard-power off the VM while the child is still paused.
6. Capture a block-level snapshot or clone of the persistent volume before
   booting the export VM. Preserve its hash with the run evidence.
7. Boot Windows and allow only normal filesystem mount/replay. Before invoking
   any CSharpDB export or recovery API, capture the raw application-visible
   state described below. Prefer inspecting a cloned volume read-only from a
   separate disposable VM. If the inspection process or filesystem mount can
   mutate the volume, preserve a separate untouched clone.
8. Compare the raw state with the accepted-state table. Any unsafe or
   unclassified state fails the trial; do not retry over it as though it were
   accepted.
9. On a writable clone of that exact post-power-loss state, run the exact same
   export request: same retained snapshot and identity, table, profile,
   resource limits, checkpoint interval, CSV path, and manifest path.
10. Require the retry to succeed without manual deletion, repair, path
    changes, or private-file fabrication. Reopen once more in a fresh process
    and require idempotent exact-pair reuse.
11. Capture the final pair and retained private journal. Verify canonical
    parsing, exact lengths, SHA-256 digests, source binding, row count, and
    logical evidence against the no-fault reference.

"Exact retry" means a product-supported rerun. An operator deleting a final,
pending, active, prepared, or staging file invalidates the trial.

## Boundary Matrix

Qualify each boundary in every supported filesystem/cache matrix cell. For
checkpoint boundaries, run at least a mid-stream `Writing` transition and the
terminal `Writing` to `DataComplete` transition. Include the first checkpoint
transition as an additional case because it has no predecessor active
checkpoint.

| Area | Hard-off boundary | Required raw-state classification | Required exact-retry result |
| --- | --- | --- | --- |
| Checkpoint | After prepared data is flushed, before pending checkpoint work | The old canonical active checkpoint remains authority, or no active exists for the first generation. Any preexisting pending file remains non-authority. Prepared data may contain the complete new record prefix beyond the active boundary. | Verify the old active prefix, safely truncate/replay the durable tail, and converge to the reference terminal checkpoint and pair. For the first generation, classify the bytes as uncheckpointed data and use the defined reset path before replay. |
| Checkpoint | After pending checkpoint bytes are flushed, before active replacement | The old canonical active checkpoint remains authority, or no active exists for the first generation. A pending checkpoint may contain the new canonical generation but remains non-authority. Prepared data must contain a prefix valid for the authoritative active checkpoint; a longer complete-record tail is allowed. | Ignore stale pending authority, verify the active prefix, safely truncate/replay any tail, and converge to the reference terminal checkpoint and pair. |
| Checkpoint | After active checkpoint handle rename, before the operation returns | The authoritative active is either the old or new complete canonical generation; it is never torn. For the first generation, no active may survive. Any pending file is non-authority. Prepared data must match at least the selected active prefix; without a first-generation active, it is classified as uncheckpointed data. | Resume from whichever complete active generation survived, or use the defined uncheckpointed-data reset for the first generation, and converge without missing or duplicate rows. |
| Publication | Before final CSV rename | Neither final exists. A private data staging file may exist. A manifest final must not exist. | Requalify or reclaim staging and publish the exact pair data-first, manifest-last. |
| Publication | After final CSV rename, before manifest work | Either neither final exists or only the exact final CSV exists. A manifest final must not exist. | Publish or requalify the exact CSV, then publish the exact manifest without replacing different bytes. |
| Publication | Before manifest rename | Either neither final exists or only the exact final CSV exists. A private manifest staging file may exist. A manifest final without its exact CSV is forbidden. | Recover the exact CSV-only state if present and publish the exact manifest last. |
| Publication | After manifest rename, before result | Neither final, the exact CSV alone, or the exact CSV plus exact canonical manifest may remain. Manifest-only, different finals, and torn files are forbidden. | Converge to or idempotently reuse the exact pair. |
| Publication | Immediately after success is returned | The exact CSV and exact canonical manifest both exist and match the reference. | A fresh exact retry idempotently reuses both. Missing CSV, missing manifest, or a changed artifact fails the durable-success requirement even if retry could reconstruct it. |

Also exercise manifest publication from a preexisting exact CSV-only recovery
state at the before-manifest, after-manifest, and after-success boundaries.
Exercise an exact-pair idempotent reopen followed by immediate hard-off after
success. These cases verify that recovery and reuse do not weaken the same
durability rule.

At every boundary, the following are always failures:

- manifest-only final state;
- noncanonical or digest-mismatched final manifest;
- final CSV bytes that differ from the terminal checkpoint;
- an authoritative checkpoint that is torn, noncanonical, skips a generation,
  or names a prepared prefix that is absent or digest-mismatched;
- a reparse, linked, non-private, special, or unexpectedly replaceable
  authority/final file;
- retry overwriting or deleting different final bytes;
- retry producing missing or duplicate rows; or
- success that depends on manual cleanup.

Private staging files and a longer uncheckpointed prepared tail are acceptable
only where the product already classifies them as non-authority and the exact
retry safely requalifies, reclaims, or truncates them.

## Raw-State And Evidence Capture

Capture evidence before the recovery retry:

- the block-level image or clone identifier and digest;
- the durable run descriptor and barrier bytes;
- host controller logs for the hard-off and reboot;
- a recursive directory listing with exact names, sizes, timestamps,
  attributes, file IDs, link counts, reparse status, and owner/ACL summary;
- raw bytes and SHA-256 for the prepared data, active and pending checkpoints,
  data and manifest staging files, final CSV, and final manifest;
- canonical decode results for every checkpoint and manifest;
- the active checkpoint's recorded generation, phase, row boundary, byte
  boundary, and prefix digest;
- whether each prepared/final prefix independently rehashes to its recorded
  digest; and
- filesystem mount/recovery events and storage errors.

After the exact retry, additionally capture:

- command line with secrets and user-specific paths redacted where necessary;
- child standard output/error, exit code, and reuse flags;
- all final and private-file evidence listed above;
- a second fresh-process exact-retry result; and
- comparison with the no-fault reference pair and source/logical proofs.

Keep one machine-readable result per trial with `pass`, `fail`, or
`inconclusive`, plus a stable reason code. Retain failed and inconclusive disk
images until the issue is explained.

## Repetition And Gate Decision

Run at least 25 independent hard-off trials for every boundary, transition
variant, and supported filesystem/cache matrix cell. Use at least 100 trials
for the after-active-checkpoint-rename and immediate-post-publication-success
cases before a release claim. Restore the baseline between trials; repeated
crashes against one ever-mutating volume do not count as independent
repetitions.

A matrix cell passes only when:

- every trial reaches its intended durable barrier;
- every raw state is one of the accepted states;
- every interrupted trial converges under exact retry;
- every reported-success trial retains its committed authority or exact pair;
- every final pair equals the no-fault reference; and
- no trial requires manual repair or produces an unexplained storage event.

An inconclusive trial must be rerun and does not count toward the repetition
minimum. Any forbidden raw state or final mismatch fails that matrix cell and
reopens implementation analysis.

The Phase 4A gate may be checked only after the evidence package names the
exact qualified matrix. Before that status changes, choose one of:

1. qualify every filesystem, controller, and cache-policy combination admitted
   by the current broad local-Windows checks; or
2. narrow runtime admission, documentation, and support claims to the matrix
   actually qualified, such as a local fixed NTFS volume with the recorded
   write-cache policy, and fail closed elsewhere.

Process-kill coverage, successful normal tests, file-handle flushes, or a
single clean reboot are not substitutes for this external matrix.
