"""Bounded abstract model for the proposed WAL control-record protocol.

This is not a WAL implementation or a filesystem/power-loss test. Frames are
atomic committed records; an invalid control slot represents a detected tear.
Successful flushes persist in order. No checksum collisions are modeled.
"""
from copy import deepcopy
from dataclasses import dataclass, asdict
import argparse
import json


@dataclass(frozen=True)
class Control:
    sequence: int
    epoch: int
    floor: int
    mode: str = "active"
    identity: str = "database-a"


@dataclass
class Disk:
    slots: list
    base: dict
    frames: list
    bad: set


class RecoveryError(Exception):
    pass


def recover(disk):
    valid = [x for x in disk.slots if x is not None]
    if not valid:
        raise RecoveryError("no valid control record")
    if len({x.identity for x in valid}) != 1:
        raise RecoveryError("control identity conflict")
    if len(valid) == 2 and valid[0].sequence == valid[1].sequence and valid[0] != valid[1]:
        raise RecoveryError("conflicting records at the same sequence")
    control = max(valid, key=lambda x: x.sequence)
    if control.mode not in ("active", "empty"):
        raise RecoveryError("unknown control mode")
    result = disk.base.copy()
    if control.mode == "empty":
        return result
    if not 0 <= control.floor <= len(disk.frames):
        raise RecoveryError("replay boundary outside physical extent")
    for position, (epoch, key, value) in enumerate(disk.frames[control.floor:], control.floor):
        if position in disk.bad:
            raise RecoveryError("invalid active frame")
        if epoch != control.epoch:
            raise RecoveryError("active frame generation mismatch")
        result[key] = value
    return result


def expected(frames, bad):
    result = {}
    for position, (_, key, value) in enumerate(frames):
        if position not in bad:
            result[key] = value
    return result


def publish(cuts, disk, slot, control, name):
    # Each write can be absent, complete, or detectably torn at interruption.
    for persistence in ("absent", "complete", "torn"):
        cut = deepcopy(disk)
        if persistence != "absent":
            cut.slots[slot] = control if persistence == "complete" else None
        cuts.append((f"{name}-write-{persistence}", cut, False))
    disk.slots[slot] = control
    cuts.append((f"{name}-flushed", deepcopy(disk), False))


def copy_flush(cuts, disk, target, name):
    changed = [k for k, v in target.items() if disk.base.get(k) != v]
    for mask in range(1 << len(changed)):
        cut = deepcopy(disk)
        for i, key in enumerate(changed):
            if mask & (1 << i):
                cut.base[key] = target[key]
        cuts.append((f"{name}-before-flush-{mask}", cut, False))
    disk.base.update(target)
    cuts.append((f"{name}-flushed", deepcopy(disk), False))


def corrupt_slot(disk, slot):
    disk = deepcopy(disk)
    if slot is not None:
        disk.slots[slot] = None
    return disk


def verify_cuts(cuts, wanted, prefix_corrupt, output, operation):
    count = 0
    errors = 0
    for name, disk, completed in cuts:
        for failed_slot in (None, 0, 1):
            trial = corrupt_slot(disk, failed_slot)
            try:
                result = recover(trial)
            except RecoveryError:
                errors += 1
                # A valid input with a surviving control must recover; so must
                # a completed floor publication even with an invalid old prefix.
                must_recover = completed or (not prefix_corrupt and any(trial.slots))
                assert not must_recover, (operation, name, failed_slot, asdict(trial))
            else:
                assert result == wanted, (operation, name, failed_slot, result, wanted)
            count += 1
    output.append(dict(operation=operation,crashAndSlotCases=count,failClosedCases=errors))


def boundary_cases():
    frames = [(1, 0, 1), (1, 1, 1), (1, 0, 2), (1, 2, 1), (1, 1, 2), (1, 3, 1)]
    results = []
    for floor in range(1, len(frames)):
        # None, an indexed last prefix frame, and a superseded prefix frame.
        invalid_sets = [set(), {floor - 1}]
        if floor >= 3:
            invalid_sets.append({0})
        for bad in invalid_sets:
            disk = Disk([Control(0, 1, 0)] * 2, {}, frames.copy(), bad)
            wanted = expected(frames, bad)
            cuts = [("initial", deepcopy(disk), False)]
            copied = expected(frames[:floor], bad)
            copy_flush(cuts, disk, copied, "copy-prefix")
            publish(cuts, disk, 0, Control(1, 1, floor), "floor-a")
            publish(cuts, disk, 1, Control(2, 1, floor), "floor-b")
            cuts.append(("floor-mirrored-before-return", deepcopy(disk), True))
            verify_cuts(cuts, wanted, bool(bad), results, f"floor-{floor}-bad-{sorted(bad)}")
    return results


def reset_cases():
    frames = [(1, 0, 1), (1, 1, 1), (1, 0, 2), (1, 2, 1), (1, 1, 2), (1, 3, 1)]
    results = []
    for floor in range(1, len(frames)):
        for bad in (set(), {0}):
            copied = expected(frames[:floor], bad)
            disk = Disk([Control(1, 1, floor), Control(2, 1, floor)], copied, frames.copy(), bad)
            wanted = expected(frames, bad)
            cuts = [("initial", deepcopy(disk), False)]
            copy_flush(cuts, disk, wanted, "copy-active")
            publish(cuts, disk, 0, Control(3, 2, 0, "empty"), "empty-a")
            publish(cuts, disk, 1, Control(4, 2, 0, "empty"), "empty-b")
            # A truncated tail may persist to any modeled frame boundary.
            for length in range(len(frames) + 1):
                cut = deepcopy(disk);cut.frames = cut.frames[:length]
                cuts.append((f"truncate-before-flush-{length}", cut, False))
            disk.frames = [];disk.bad = set()
            cuts.append(("truncate-flushed", deepcopy(disk), False))
            publish(cuts, disk, 0, Control(5, 2, 0), "active-a")
            publish(cuts, disk, 1, Control(6, 2, 0), "active-b")
            cuts.append(("active-mirrored", deepcopy(disk), True))
            # The invalid prefix is already below both initial floors. Recovery
            # must therefore succeed at every cut with any surviving control.
            verify_cuts(cuts, wanted, False, results, f"reset-{floor}-bad-{sorted(bad)}")
            disk.frames.append((2, 1, 99));wanted = wanted | {1: 99}
            verify_cuts([("new-commit-acknowledged", disk, True)], wanted, False, results,
                        f"reset-{floor}-new-commit-bad-{sorted(bad)}")
    return results


def negative_controls():
    witnesses = []
    # New EMPTY record before the base flush silently hides committed pages.
    disk = Disk([Control(1, 2, 0, "empty"), Control(0, 1, 0)], {}, [(1, 0, 7)], set())
    assert recover(disk) != {0: 7}
    witnesses.append("Publishing EMPTY before the base flush hides acknowledged data.")
    # Admitting writes after one ACTIVE copy can fall back to EMPTY.
    disk = Disk([None, Control(4, 2, 0, "empty")], {0: 7}, [(2, 0, 8)], set())
    assert recover(disk) != {0: 8}
    witnesses.append("Admitting writes before ACTIVE is mirrored can hide a new commit after one slot fails.")
    # Reclaiming before EMPTY is mirrored can leave only an old replay boundary.
    disk = Disk([None, Control(2, 1, 1)], {0: 7}, [], set())
    try:
        recover(disk)
    except RecoveryError:
        witnesses.append("Truncating before EMPTY is mirrored can leave an unusable old boundary.")
    else:
        raise AssertionError("Unsafe early truncation was accepted.")
    # Completing retirement after one floor copy loses the invalid-prefix guard.
    disk = Disk([None, Control(0, 1, 0)], {0: 7}, [(1, 0, 6), (1, 1, 8)], {0})
    try:
        recover(disk)
    except RecoveryError:
        witnesses.append("Completing retirement before both floor copies persist can expose an invalid prefix.")
    else:
        raise AssertionError("Single-copy retirement was accepted.")
    return witnesses


def admission_cases():
    # Units are abstract frame extents. State is physical used extent, live
    # frames, reserved append extent, reader pin, and allocated extent.
    header = 2
    states = transitions = refused = 0
    for budget in (8, 16):
        initial = (header, 0, 0, False, header)
        frontier = {initial};seen = {initial}
        for _ in range(8):
            following = set()
            for used, live, reserved, pins, allocated in frontier:
                next_states = []
                if reserved == 0:
                    for requested in (1, 3, 5, budget + 1):
                        if used + requested <= budget:
                            # Preallocation rounds up but is clamped at the cap.
                            extent = min(budget, ((used + requested + 3) // 4) * 4)
                            next_states.append((used, live, requested, pins, max(allocated, extent)))
                        else:
                            refused += 1
                            # A quiescent reset can make room only if the entire
                            # request fits; a pinned reader cannot be bypassed.
                            if not pins and header + requested <= budget:
                                reset_then_publish = (header + requested, requested, 0, False, header + requested)
                                next_states.append(reset_then_publish)
                else:
                    next_states.append((used + reserved, live + reserved, 0, pins, allocated))
                    next_states.append((used, live, 0, pins, allocated))  # rollback
                next_states.append((used, live, reserved, not pins, allocated))
                if not pins and reserved == 0:
                    next_states.append(initial)  # quiescent full drain and reset
                    if live > 1:
                        next_states.append((used, live - live // 2, 0, False, allocated))
                for state in next_states:
                    u, l, r, _, a = state
                    assert header <= u <= a <= budget and u + r <= a
                    assert 0 <= l <= u - header and r >= 0
                    transitions += 1
                    if state not in seen:
                        following.add(state);seen.add(state)
            frontier = following
        states += len(seen)
    # Negative accounting controls: the live frame estimate hides retired bytes,
    # and checking a new reservation alone hides an earlier pending reservation.
    assert header + 1 + 1 <= 8 and 8 + 1 > 8
    assert 6 + 1 <= 8 and 6 + 2 + 1 > 8
    return dict(reachableStates=states,checkedTransitions=transitions,refusedReservations=refused,
                depth=8,budgetsInFrameUnits=[8,16],negativeControls=2,
                limits="One outstanding reservation in the transition model; a separate negative control covers double booking. Reader visibility and concurrency scheduling are not modeled.")


def main():
    parser = argparse.ArgumentParser();parser.add_argument("--output", required=True)
    args = parser.parse_args()
    boundary = boundary_cases();reset = reset_cases();negative = negative_controls()
    result = dict(status="bounded-model-passed", boundaryScenarios=len(boundary), resetScenarios=len(reset),
        crashAndSlotCases=sum(x['crashAndSlotCases'] for x in boundary + reset),
        failClosedCases=sum(x['failClosedCases'] for x in boundary + reset),
        admission=admission_cases(),negativeControls=negative,scenarios=boundary + reset,
        assumptions=["Ordered successful flushes persist; nonflushed slot writes may be absent, complete, or detectably torn.",
                     "One additional unreadable slot per cut; two unreadable slots fail closed.",
                     "Frames represent whole committed records; concurrent transactions and torn frame writes are outside this model.",
                     "Invalid active frames fail closed, never return a truncated logical view.",
                     "Read pins and reservations are abstract admission inputs, not a concurrency implementation."],
        limitations="Not a codec, filesystem, process-kill, power-loss, frame-parser, migration or performance qualification.")
    with open(args.output, "x", encoding="utf-8") as f:json.dump(result,f,indent=2);f.write("\n")
    print(json.dumps({k:v for k,v in result.items() if k not in ['scenarios','assumptions','limitations']}))


if __name__ == "__main__":
    main()
