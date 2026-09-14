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
go test -tags=e2e ./e2e/tests -run '^(Test_GS13|Test_GT7|Test_ZR)' -count=1 -v -timeout=25m
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

Automatic recovery retains the PodClique and PodCliqueScalingGroup scale objects.
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
