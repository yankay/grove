# KAI and Volcano Capability Validation Experiments

Experiment owner: Kay. Original execution and transfer date: September 22, 2026 (Asia/Shanghai).

## Scope

This collection contains the Grove PCSG scale-to-zero experiments, standalone KAI and Volcano PodGroup capability probes, subgroup enforcement checks, deterministic stale-policy injection, no-delay controls, unit tests, and local evidence instructions. It preserves both successful and failed results.

The experiments were copied from an earlier PR 686 validation task into this pull request worktree. Historical results retain their original source revisions, scheduler versions, timestamps, and object identities. They are not cluster test results for the destination worktree. No production implementation was copied or changed as part of this transfer.

## Programs And Reports

| Area | Program or guide | Report |
| --- | --- | --- |
| Grove PCSG `3 -> 0 -> 3`, live minimum, ScaleOut ordering, router survival, restart, and native PodGroup reconstruction | [Suite guide](../../operator/hack/gang-validation/README.md), [driver](../../operator/hack/gang-validation/run.py), [audit](../../operator/hack/gang-validation/audit.py) | [Historical Grove report](pr686-gang-validation-20260922.md) |
| KAI native PodGroup recreation and in-place recovery | [KAI guide](../../operator/hack/gang-validation/KAI_ONLY.md), [probe](../../operator/hack/gang-validation/kai_only.py) | [KAI report](kai-only-validation-20260922.md) |
| Volcano native PodGroup recreation and in-place recovery | [Volcano guide](../../operator/hack/gang-validation/VOLCANO_ONLY.md), [probe](../../operator/hack/gang-validation/volcano_only.py) | [Strengthened Volcano results](../../operator/hack/gang-validation/VOLCANO_ONLY.md#strengthened-validation) |
| Insufficient capacity, missing subgroup, and multi-Pod subgroup enforcement | Both native probes above | Scheduler-specific reports above |
| Delayed PodGroup policy delivery with normal Pod delivery and a live canary | [Shared fault proxy](../../operator/hack/gang-validation/volcano_fault_proxy.py) | Scheduler-specific reports above |

`test_run.py`, `test_kai_only.py`, `test_volcano_only.py`, and `test_volcano_fault_proxy.py` cover the test drivers and fault proxy. The historical Grove report also records implementation unit tests; those Go tests remain part of their original implementation checkout, not this copied Python suite.

## Historical Results

The recorded cluster experiments used Kubernetes v1.37.0, KAI v0.17.0, and Volcano v1.15.2.

| Experiment | KAI | Volcano |
| --- | --- | --- |
| Unchanged PR 686 runtime, Grove scale-to-zero baseline | FAIL: missing-anchor dependency keeps restored Pods gated | FAIL: same Grove dependency failure |
| Grove prototype `d94776b6`, ordinary recovery and disruption scenarios | PASS within the recorded scope | PASS within the recorded scope |
| Native PodGroup deletion/recreation and router-preserving mutation | PASS | PASS |
| Insufficient-capacity and missing-subgroup blocking | PASS | PASS |
| Multi-Pod worker subgroup with enough aggregate schedulable Pods | PASS | PASS |
| Two deterministic stale-policy injections | PASS: zero waking bindings; unknown subgroup rejected | FAIL twice: two of four waking members bound early |
| Same proxy without policy delay | PASS | PASS |

The standalone KAI suite recorded eight passing runs. The Volcano stale-policy failures remain failures even though all members eventually became Ready after releasing the delay and capacity. API readback is not proof that a scheduler has adopted a new policy.

The native probes do not exercise Grove PCSG, PodGangMap, PodGang reconciliation, or ScaleOut release ordering. The KAI delayed-policy result covers restoring removed subgroup names, not arbitrary minimum increases within subgroup names that remain present. None of these results establish native Kubernetes CompositePodGroup support, a complete compatibility matrix, or merge readiness for the destination branch.

## Destination Worktree Rerun

The destination worktree reran the native scheduler matrix on September 22, 2026 with Kubernetes v1.37.0, a 7-CPU workload node, KAI v0.17.0, and Volcano v1.15.2. Each negative window requested 45 seconds of observation.

Every capacity-sensitive case kept a 3-CPU blocker and persistent 1-CPU probe running, then required a second temporary 1-CPU Pod to become Ready and be deleted before creating the gang members. This proves that the scheduler had residual capacity for partial placement and prevents a full-node condition from producing a false PASS.

| Scenario | KAI | Volcano |
| --- | --- | --- |
| Singleton, PodGroup recreated | PASS | PASS |
| Singleton, PodGroup updated in place | PASS | PASS |
| Two-Pod worker subgroup, PodGroup recreated | PASS | PASS |
| Two-Pod worker subgroup, PodGroup updated in place | PASS | PASS |
| Fault proxy installed without delay, in-place control | PASS | PASS |
| Stale-policy injection 1 | PASS: zero partial bindings for 45.52 seconds | FAIL: `Forbidden partial gang binding: leader-0` |
| Stale-policy injection 2 | PASS: zero partial bindings for 45.89 seconds | FAIL: `Forbidden partial gang binding: leader-0` |
| Fault proxy installed without delay, recreation control | PASS | PASS |

KAI passed all eight runs. Volcano passed six controls and ordinary scenarios; both deterministic stale-policy runs reproduced the same safety failure. All twelve capacity-sensitive runs recorded the `remaining-capacity` proof, six per backend. Releasing the held policy and capacity recovered all four workers in every stale-policy run, so eventual readiness does not change either Volcano verdict.

## Local Evidence

Evidence is intentionally stored under `.ai-tmp/` and is not part of the pull request. The copied historical evidence is under `.ai-tmp/20260922-pcsg-gang-validation/`. The destination rerun is under `.ai-tmp/20260922-kai-volcano-capacity-proof/`.

| Evidence | Contents |
| --- | --- |
| `.ai-tmp/20260922-pcsg-gang-validation/evidence-summary.json` | Baseline failures, prototype results, observation windows, and binding-order audits |
| `.ai-tmp/20260922-pcsg-gang-validation/kai-only/summary.json` | Eight native KAI runs and evidence index |
| `.ai-tmp/20260922-pcsg-gang-validation/volcano-only/strengthened-summary.json` | Subgroup checks, two reproduced safety failures, and no-delay controls |
| `.ai-tmp/20260922-kai-volcano-capacity-proof/{kai,volcano}/*/result.json` | Sixteen destination-worktree native scheduler verdicts |
| `prototype-kai-*`, `prototype-volcano-*`, `baseline-*` | Grove lifecycle snapshots, Pod watches, configuration, and logs |
| `kai-only/`, `volcano-only/` | Native policy and Pod snapshots, fault records, logs, source bundles, and scheduler-only evidence archives |
| `.ai-tmp/20260922-pcsg-gang-validation/TRANSFER.md` | Provenance and excluded credentials, caches, binaries, and sensitive archive |

Historical logs, manifests, checksums, and archived scripts retain their original absolute paths for provenance. Use the current programs linked above for a new run; do not reuse old kubeconfig paths or task identifiers from evidence.

## Local Verification

After adding the residual-capacity proof, all 37 Python unit tests passed in the destination worktree. Ruff lint and format checks passed for all nine Python files, and `git diff --check` passed. The two dedicated Kind clusters and loopback port forwards were removed after the live matrix completed.

From the destination repository root:

```bash
PYTHONDONTWRITEBYTECODE=1 python3 -m unittest discover \
  -s operator/hack/gang-validation -p 'test_*.py' -v
ruff check --no-cache operator/hack/gang-validation
ruff format --check --no-cache operator/hack/gang-validation
```

These commands validate the test harness, not live scheduler behavior. Rerunning cluster experiments requires a fresh task-owned cluster, explicit kubeconfig, current image inputs, and new output directories as described in the scheduler guides.
