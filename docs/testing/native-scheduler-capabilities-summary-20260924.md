# Native Scheduler Capabilities: 3 Backends x 12 Scenarios

Test date: September 24, 2026 (Asia/Shanghai).

## Scope

This summary compares the native capabilities of KAI Scheduler, Volcano, and Kubernetes Workload-Aware Scheduling (WAS). No Grove installation, resources, controllers, or integration are involved.

WAS uses a Workload template tree, a root CompositePodGroup, and leaf PodGroups, following [Grove PR 605](https://github.com/ai-dynamo/grove/pull/605) at commit `531a6120c4e13213cfbe43c657bb4f3be4a96a84`. It is not modeled as one flat PodGroup.

The table combines measured results with explicitly labeled configuration and roadmap options. The full test report and original execution evidence remain local and are not included in this documentation-only update. This summary does not replace those records or their original result counts. No additional tests were run to produce this summary.

## Capability Matrix

| ID | Scenario | KAI v0.17.0 | Volcano v1.15.2 | WAS v1.37.0 + CompositePodGroup |
| --- | --- | --- | --- | --- |
| 01 | Singleton subgroups, recreate | PASS | PASS | PASS |
| 02 | Singleton subgroups, in place | PASS | PASS | PASS |
| 03 | Multi-Pod subgroup, recreate | PASS | PASS | PASS |
| 04 | Multi-Pod subgroup, in place | PASS | PASS [1] | PASS |
| 05 | No-delay proxy control, in place | PASS | PASS | PASS |
| 06 | Strict stale-policy injection 1 | PASS | FAIL | PASS [5] |
| 07 | Strict stale-policy injection 2 | PASS | FAIL | PASS [5] |
| 08 | No-delay proxy control, recreate | PASS | PASS | PASS |
| 09 | Eventual policy adoption | PASS | PASS [2] | PASS [5] |
| 10 | Three membership removal/restoration cycles | PASS | PASS | ROADMAP [4] |
| 11 | Router recreation while idle | PASS | PASS | ROADMAP [4] |
| 12 | Router continuity under pressure, configuration allowed | CONFIGURATION WORKAROUND [3] | PASS | PASS |

### Status Definitions

- `PASS`: The recorded execution met the scenario's bounded requirements.
- `FAIL`: The recorded execution violated those requirements.
- `CONFIGURATION WORKAROUND`: A configuration change addresses the identified failure mechanism; the complete scenario still needs a retest with that configuration.
- `ROADMAP`: An explicit upstream plan covers the API capability currently blocking the scenario. Implementation and a complete scenario retest are still required.

Configuration workarounds and roadmap entries are not counted as measured passes. Scenario 12 in this capability view allows configuration changes; the original execution tested default termination policies.

### Result Notes

**[1] Volcano scenario 04:** The original attempt encountered a test-client API `Conflict` before entering the target observation window. After adding bounded conflict retries, a retest passed on a fresh cluster without the fault proxy. The original failure record remains preserved.

**[2] Volcano scenario 09:** Partial binding occurred while the scheduler still observed the stale policy. After policy release, a new Pod batch passed the eventual-adoption check. This establishes eventual policy adoption for the tested transition, not continuous safety.

**[3] KAI scenario 12:** The configuration workaround is `stalenessGracePeriod: -1s`, already used in scenarios 01-11. The original scenario 12 omitted this override and observed default stale-gang eviction disrupting the router after approximately 60 seconds. Scenario 12 has not been rerun with the override, so this cell is not a measured pass. The original failure describes a mismatch with the router-survival requirement, not a claim that the default eviction behavior is a bug.

**[4] WAS scenarios 10 and 11:** The tested API rejected the root-preserving change from `minGroupCount: 3` to `1` because `spec.schedulingPolicy` is immutable. Neither execution reached subsequent worker-group deletion/restoration or idle router replacement. [KEP-6012, Beta graduation criteria](https://github.com/kubernetes/enhancements/blob/52be95021474b1f2be560a9cb3c342c94a122a5d/keps/sig-scheduling/6012-composite-podgroup-api/README.md#beta) explicitly plans runtime mutability of `CompositePodGroup.minGroupCount`. The roadmap covers this blocking prerequisite, not a guarantee that the complete scenarios already work. The original measured results remain `UNSUPPORTED`; no root recreation was substituted.

**[5] WAS stale-policy scope:** The capacity-calibrated tests demonstrated enough available CPU for the complete stale minimum. The injected delay affected a leader leaf's `minCount: 1 -> 2` while retaining the hierarchy. Passes apply to that transition, Pod set, and arrival order, not every possible hierarchy or membership change. Scenario 09's measured interval before the new batch was approximately 10.09 seconds, including router verification; its configured five-second wait does not establish adoption within five seconds.

## Scenario Introductions

| ID | What the scenario does | What it checks |
| --- | --- | --- |
| 01 | Remove and recreate the native group and worker Pods, with no retained router. Test insufficient total capacity and a missing required subgroup separately. | No waking worker binds until the gang requirements can be met; workers become Ready after the constraint is removed. |
| 02 | Repeat scenario 01 while retaining the native group and running router. WAS retains the hierarchy and adjusts leaf minima. | In-place policy changes preserve group/router identity while maintaining gang blocking and recovery. |
| 03 | Recreate a gang containing a required two-Pod worker subgroup. Make one required worker unschedulable while enough other members exist to satisfy a simple aggregate minimum. | Surplus members of another subgroup cannot substitute for the missing required worker. |
| 04 | Repeat scenario 03 while retaining the native group or WAS hierarchy and the running router. | Multi-Pod subgroup requirements survive in-place transitions without replacing the retained resources. |
| 05 | Repeat scenario 02 through the installed fault proxy with no policy delay enabled. | The proxy installation and scheduler restart do not themselves invalidate the in-place baseline. |
| 06 | Delay the restored policy reaching the scheduler while allowing new worker Pod events through. Verify an independent scheduler canary remains Ready. | No target worker binds during the stale-policy window; workers recover after policy and capacity release. |
| 07 | Repeat scenario 06 in a fresh namespace with new object identities. | The strict stale-policy result is reproducible; this is a repeat, not a separate capability dimension. |
| 08 | Repeat scenario 01 through the installed fault proxy with no policy delay enabled. | The recreate baseline still works after instrumentation, complementing the in-place control in scenario 05. |
| 09 | Record stale-window behavior, release the policy, remove the first worker batch, wait, then create a new batch while capacity is still insufficient. | The scheduler eventually applies the restored policy to new Pods. Earlier safety violations remain separate findings. |
| 10 | Perform three idle/wake cycles while keeping the same native root and router. Actually remove and restore worker subgroup membership instead of only lowering minima. | Repeated membership transitions preserve identity, block partial placement under pressure, and recover to Ready. |
| 11 | Remove worker membership, delete and recreate the router while idle, then restore workers. Retain the native root. | A new router UID becomes Ready without worker groups, followed by correct gang blocking and recovery when workers return. |
| 12 | Hold an in-place wake under resource pressure for a planned 120-second window, then release capacity. This capability view allows termination-policy configuration. | The router remains alive, workers do not partially bind, and the gang recovers. The original KAI run stopped at its first disruption; the configured variant remains untested. |

## Interpretation

- KAI passed scenarios 01-11 with the explicit negative staleness override. Scenario 12 has a configuration workaround pending a focused retest.
- **Volcano's strict wake-up support still depends on upstream changes.** Updating `spec.minMember` and `spec.subGroupPolicy` together does not by itself guarantee strict wake-up safety: the scheduler can observe waking Pods while still using the stale policy. Volcano passed the ordinary lifecycle cases, but both strict stale-policy injections allowed forbidden partial binding. Eventual adoption in scenario 09 does not erase those failures. Strict support requires an upstream solution and successful retesting of these cases; this statement does not imply an announced upstream roadmap or delivery date.
- WAS passed the retained-hierarchy cases tested here. True membership removal and idle router recreation depend on the planned runtime mutability of the root minimum, followed by full lifecycle retesting.

These are bounded native CPU scheduling experiments, not an overall scheduler ranking, GPU/inference benchmark, or long-duration reliability qualification. The default negative window was at least 45 seconds, the extended pressure window was planned for 120 seconds, and membership cycling used three repetitions. The full test report, environment details, reproduction commands, original counts, and raw evidence remain in the test owner's local worktree; this update publishes only the capability summary.
