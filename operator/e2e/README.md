# E2E Test Infrastructure

This directory contains the E2E test infrastructure for the Grove Operator, including dependency management for external components.

## Running E2E Tests

### Prerequisites

The following tools must be installed:
- **kubectl** and **Helm** - For accessing the test cluster
- **Go** (1.26.3+) - For running the tests

For a locally provisioned cluster, also install:
- **Docker** and **k3d** (v5.x) - For running the cluster
- **skaffold** (v2.x) - For deploying Grove operator
- **jq** and **uv** - For the infrastructure scripts

On macOS with Homebrew:
```bash
brew install docker k3d skaffold helm kubectl go jq uv
```


### Running Locally

Against an existing, dedicated test cluster:
```bash
KUBECONFIG=/path/to/test.kubeconfig make -C operator run-e2e
```

To provision the infrastructure, run tests, and clean up:

```bash
make -C operator run-e2e-full
```

The default `operator/hack/e2e.yaml` preset installs Grove and KAI in k3d and
creates 30 KWOK nodes. `run-e2e` itself only runs tests; it does not provision
or remove a cluster. For real worker containers, use `run-e2e-real-full`.
The hibernation recovery tests below require real workers and Ready Pods,
not simulated KWOK readiness.

### Hibernation Backend Tests

The `Test_ZR*`, `Test_GT7*`, and `Test_GS13*` tests also run against an existing cluster with KAI or Volcano.
Use a dedicated cluster: these tests delete Grove workloads, cordon worker nodes,
and restart the Grove operator.

Install Grove in `grove-system` as `grove-operator`, enable the matching scheduler
profile, and provide at least six Ready worker nodes labeled
`node_role.e2e.grove.nvidia.com=agent`. For KAI, install v0.17.0 or newer, including
its updated PodGroup CRD, and create the `test` queue. Keep the scheduler's global
stale-gang eviction default: Grove sets `spec.stalenessGracePeriod: "-1s"` only on
its own PodGroups. The backend fails startup if the served CRD lacks this field.

From the `operator` directory:

```bash
KUBECONFIG=/path/to/test.kubeconfig \
GROVE_E2E_SCHEDULER=volcano \
GROVE_E2E_WORKLOAD_IMAGE=registry:5001/busybox:latest \
go test -tags=e2e ./e2e/tests -run '^(Test_GS13|Test_GT7|Test_ZR)' -count=1 -v -timeout=45m
```

Use `kai-scheduler` for KAI. The two `GROVE_E2E_*` overrides apply only to the
hibernation fixture, not the rest of the E2E suite. Set `E2E_REGISTRY_PORT` when
the host-side test image push endpoint uses a port other than `5001`; the workload
image must be reachable from every worker.

`Test_ZR8` covers automatic gang recovery with runtime targets different from the
templates: an active standalone clique initialized at zero, an idle standalone
clique initialized positive, and a scaling group scaled above its template.
It holds one Pod's deletion, restarts the operator during the partial drain,
accepts a concurrent scale update, and requires Ready replacements at the new
target. A second recovery verifies re-arming and an idle scaling group across a
restart during recreation. `Test_GT7` verifies that a first wake only arms
termination after actual health and that recovery retains its positive target.

`Test_ZR9` scales a PCSG to zero while an old member Pod is held by a finalizer.
It requires recovery to remain in Draining across an operator restart, even with
capacity available, until that Pod and its owning PodClique disappear.
`Test_ZR10` models the inter-controller gap in a fast scale-in/out cycle by
emptying the ScaleOut slot while its old members still exist, then explicitly
triggering PCS reconciliation. It requires fresh member PodCliques and Ready Pods
in the new epoch, matching native PodGroup membership, and undisturbed anchor
Pods.

`Test_ZR11` drives real PCSG `/scale` updates: it grows from two to three
replicas, then exercises `3 -> 2 -> 3` and `3 -> 0 -> 3` while holding an old
ScaleOut Pod. It restarts Grove during deletion and requires fresh scheduling
identity only after the old member drains. No synthetic PGM update or enqueue is
used. `Test_ZR12` deletes membership while the idle scale target survives, then
explicitly deletes that target. Membership reconstruction must preserve the live
zero; target recreation must initialize from the template without a deadlock.

`Test_ZR13` models two `/scale` writers using an explicit `resourceVersion`.
After one writer changes the target, the stale writer must receive a Conflict
without undoing the accepted active or idle intent. Both kinds retain their target
UID across Grove restarts, and a refetched write must subsequently converge to
the new replica count with Ready Pods.

`Test_ZR14_MemberScalingLifecycle` exercises non-zero PCSG member scaling under
both RollingRecreate and OnDelete: `3 -> 4 -> 3`, restart with a retained target
at four, and group idle/wake. It checks Ready Pods, member/group identity,
undisturbed siblings, and complete native subgroup policies. Group hibernation
deletes its members; the test distinguishes a newly template-initialized member
on wake from recovery of a retained scale object. This is workload lifecycle
coverage, not inference engine-world resize validation.
PodGang member accounting follows each existing member's replica target rather
than its template, so scale-out Pods remain referenced and scale-in does not wait
for obsolete template-sized membership. Templates remain the fallback until a
member object exists.

### Experimental Recovery Scope

The prototype's PCS-replica-wide automatic recovery retains the PodClique and
PodCliqueScalingGroup scale objects. General replica recovery policy is deferred
from GREP-0677 as of revision `3bee0ccd`; this prototype behavior is not a
normative requirement of that GREP.
PCS annotations record each replica's recovery epoch and phase; replacement Pods
carry that epoch, so a controller restart or late old-epoch Pod creation cannot
reset replica intent. Old Pods must drain before recreation, and recovery remains
disarmed until replacement Pods satisfy the active components' availability
thresholds. These controller-owned annotations are not a user-facing scale API.
During cascading deletion, member PodCliques retain their finalizer until an
uncached read confirms that all owned Pods are gone, so concurrent PCSG scale-in
cannot bypass the drain. Explicit orphan deletion still leaves Pods to the caller.
Replacement Pods resolve startup dependencies from current gang membership rather
than retained dependencies on components that have since gone idle.
Explicitly deleting a scale object or scaling in a top-level PCS replica is outside
this recovery guarantee; a new logical component is initialized from its template.
PCSG-local gang termination is also outside the retained-object guarantee: when
the group can still satisfy its minimum, its controller deletes the failed
replica's member PodCliques and recreates them from the template. This existing
path can reset an independently scaled member. The prototype does not establish
a universal replica-preservation policy for every recovery path.

### Direct-Object Admission

`PodCliqueSpec.Replicas` is now `*int32` with `json:"replicas,omitempty"`. Go clients must use a pointer for explicit replica targets, including zero; nil omits the field and defaults to one. JSON/YAML replica values remain integers. Serialization, defaulting, and API-server tests cover both direct PodCliques and embedded PodCliqueSet templates.

New PodCliques default an omitted `minAvailable` to `max(1, replicas)` without
changing explicit zero replicas. PCSGs retain their existing minimum default of
one. Explicit non-positive minima are rejected on create; positive minima cannot
be changed or removed on update, including while idle.

Legacy missing or non-positive minima are not silently changed on unrelated
updates. They may be explicitly repaired to a valid positive value, after which
that value is immutable. Existing positive below-quorum targets can still update
unrelated fields without changing replicas or their minimum. Admission tests use
the production webhook and Kubernetes API server; CRD upgrade tests also cover
legacy updates, repairs, and finalizer removal.

### KEDA Integration

The `Test_KEDA_*` tests are opt-in. Install KEDA with its CRDs,
operator, admission webhook, and external metrics API in the same dedicated
real-worker cluster described above. The test expects a single KEDA operator
Pod with `app.kubernetes.io/name=keda-operator`. It restarts both Grove and KEDA;
do not run it on a shared or production cluster.

The verified configuration uses Kubernetes 1.35.0, KEDA 2.20.2, and Redis 7.4.2.
Install KEDA into `keda`, or set `GROVE_E2E_KEDA_NAMESPACE` to its namespace:

```bash
helm repo add kedacore https://kedacore.github.io/charts
helm repo update kedacore
helm --kubeconfig /path/to/test.kubeconfig upgrade --install keda kedacore/keda \
  --version 2.20.2 --namespace keda --create-namespace --wait --timeout=8m
kubectl --kubeconfig /path/to/test.kubeconfig wait \
  --for=condition=Available apiservice/v1beta1.external.metrics.k8s.io --timeout=120s
```

From `operator`, run against each configured scheduler separately:

```bash
KUBECONFIG=/path/to/test.kubeconfig \
GROVE_E2E_KEDA=true \
GROVE_E2E_SCHEDULER=volcano \
GROVE_E2E_WORKLOAD_IMAGE=registry:5001/busybox:latest \
go test -tags=e2e ./e2e/tests -run '^Test_KEDA_' -count=2 -v -timeout=45m
```

Each test creates an ephemeral unauthenticated Redis queue and a ScaledObject for
each target kind, standalone PodClique and PodCliqueScalingGroup. Override
`GROVE_E2E_REDIS_IMAGE` when workers use a local registry. No KEDA Go module
dependency is added to the operator, and ordinary E2E runs skip these tests.

In `Test_KEDA_ActiveFloorAndHibernation`, each target completes two demand cycles
through `0 -> 2 -> 4 -> 2 -> 0`, using
`minReplicaCount: 2`, `idleReplicaCount: 0`, and `maxReplicaCount: 4`.
Assertions inspect the real `/scale` target, Ready workload Pods, stable target
UIDs, and an unaffected always-active component. Idle targets must remain zero
after Grove and KEDA restarts. A deliberately incompatible active floor of one
must surface `KEDAScaleTargetActivationFailed` without changing the target;
restoring two must reactivate it.

Additional failure cases exercise both target kinds:

| Test | Fault | Required Result |
| --- | --- | --- |
| `Test_KEDA_ActiveDownscaleRejected` | Active target at two; change KEDA's active floor to one with nonzero demand. | A direct below-quorum `/scale` probe must return `Invalid` with a `spec` cause. HPA reports `AbleToScale=False/FailedUpdateScale` and at least two `FailedRescale` occurrences. Replicas remain two, Pods retain their UIDs and placement, and correcting the floor permits scale-out and hibernation. |
| `Test_KEDA_MetricFailureRecovery` | Temporarily deny Redis `LLEN`, first at four replicas and then at zero. | KEDA reports `Ready=False/TriggerError`; active HPA reports `ScalingActive=False/FailedGetExternalMetric`. Replicas and Pod identities remain unchanged for 20 seconds after failure is observed. Restoring read permission permits real scaling to two Ready replicas. |

The metric-failure case preserves the Redis queue and uses no fallback policy.
It exercises a scaler read error without changing the ScaledObject or Redis
connectivity, not Redis data loss or an external metrics API outage.

This is Redis-triggered activation and HPA integration, not coverage of every
KEDA scaler, fallback policy, authentication mode, or rolling-update workflow.

### Repeated And Randomized Tests

Increase `-count` to repeat lifecycle scenarios against an existing dedicated
cluster. Runs sharing a cluster must remain serial because cleanup deletes Grove
workloads and tests change node availability.

The membership state-machine fuzzer runs without a cluster. From `operator`:

```bash
go test ./internal/controller/podcliqueset/components/podgangmap \
  -run '^$' -fuzz '^FuzzHibernationMembership$' -fuzztime=3m -parallel=2
go test -tags=e2e ./e2e/grove/workload ./e2e/k8s ./e2e/k8s/pods -count=10
```

Randomized sequences cover two independent scaling groups and a standalone
clique: idle/wake/scale transitions, complete and unique membership, stable
anchors, monotonic epochs when an empty ScaleOut slot is reused, input
immutability, observation-order independence, and steady-state idempotence.
Helper unit tests ensure asynchronous waits do not mistake stale HPA
configuration, unrelated conditions, or an old/terminating controller Pod for
successful convergence. Invalid negative queue lengths are rejected before any
Redis command runs. Warning-event helpers filter by namespace, target UID, type,
and reason, and count aggregated occurrences without double-counting the legacy
and series counters. Unit tests cover those filters, empty results, missing
target UIDs, and API read failures.

### Running in CI/CD

The E2E jobs in `.github/workflows/build-check-test.yaml` run from trusted
`pull-request/<number>` branches when the path filter detects relevant changes
under `operator/` or `.github/`. Adding a label to a draft PR alone does not
override these gates.

## Managing Dependencies

E2E test dependencies (container images and Helm charts) are managed in
`operator/hack/infra_manager/dependencies.yaml`.

### File: `dependencies.yaml`

This file defines all external dependencies used in E2E tests:

- **Container Images**: Images that are pre-pulled into the test cluster to speed up test execution
- **Helm Charts**: External Helm charts (Kai Scheduler, NVIDIA GPU Operator) with their versions and configuration

### Updating Dependencies

To update a dependency version:

1. Edit `operator/hack/infra_manager/dependencies.yaml`
2. Update the `version` field for the desired component
3. Recreate the dedicated cluster to install the new dependencies, then run
   `make -C operator run-e2e` from the repository root.

#### Example: Updating Kai Scheduler

```yaml
kai_scheduler:
  version: "v0.17.0"
```

#### Example: Adding a KAI Component Image to Pre-pull

```yaml
kai_scheduler:
  version: "v0.17.0"
  images:
    # Keep the other component images in this list.
    - "ghcr.io/kai-scheduler/kai-scheduler/crd-upgrader"
```

Component image entries omit tags; the pre-pull code appends the component's
`version`. Arbitrary workload images need a corresponding pre-pull group in
`operator/hack/infra_manager/orchestrator.py`; a top-level `images` list is not
read by the infrastructure scripts.

## Troubleshooting

### Stale k3d Cluster

If cluster creation fails, delete only the disposable cluster belonging to this
test run, using its configured name:
```bash
k3d cluster delete <test-cluster-name>
```

### Test Timeout

The Makefile uses a 45-minute timeout. To set an explicit timeout against an
existing cluster, run from the repository root:
```bash
cd operator
KUBECONFIG=/path/to/test.kubeconfig \
go test -count=1 -tags=e2e ./e2e/tests/... -v -timeout=60m
```
