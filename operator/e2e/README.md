# E2E Test Infrastructure

This directory contains the E2E test infrastructure for the Grove Operator, including dependency management for external components.

## Running E2E Tests

### Prerequisites

The following tools must be installed:
- **Docker** - For running containers and k3d
- **skaffold** (v2.x) - For deploying Grove operator
- **helm** - For deploying Helm charts
- **Go** (1.26.3+) - For running the tests

The following tools are nice to have
- **k3d** (v5.x) - For creating local Kubernetes clusters

On macOS with Homebrew:
```bash
brew install docker skaffold helm go
```
To install k3d
```bash
brew install k3d
```


### Running Locally

From the repository root:
```bash
make run-e2e
```

Or directly from the operator directory:
```bash
cd operator
make run-e2e
```

The test suite will:
1. Create a k3d cluster with 28 worker nodes
2. Install Grove, Kai Scheduler, and GPU Operator
3. Run all e2e testing suites
4. Clean up the cluster

### Running in CI/CD

E2E tests are automatically run on GitHub Actions for:
- **All non-draft pull requests** to `main`
- **Draft pull requests** with the `run-e2e` label

To trigger e2e tests on a draft PR:
1. Add the `run-e2e` label to the pull request
2. The workflow will run automatically

The CI workflow is defined in `.github/workflows/e2e-test.yaml`.

### Workload-Aware Scheduling (default-scheduler gang) tests

The `Test_WAS*` cases in `tests/was_scheduling_test.go` exercise the
default-scheduler hierarchical gang scheduling backend (GREP-531), which relies
on the upstream Kubernetes Workload-Aware Scheduling APIs (`Workload`,
`CompositePodGroup`, `PodGroup`). Hierarchical WAS requires Kubernetes >= 1.37
and the feature gates listed in the
[installation guide](../../docs/installation.md#default-scheduler-hierarchical-gang-scheduling).
Grove's gang scheduling option is **off by default**, and the tests skip unless
the cluster serves all three required APIs.

Use a dedicated cluster with at least three Ready worker nodes labeled
`node_role.e2e.grove.nvidia.com=agent`, a Grove operator built from this checkout
installed as release `grove-operator` in `grove-system`, and a test registry
reachable as `registry:5001` from the nodes. Set `E2E_REGISTRY_PORT` to the host
port of that registry (default `5001`).

For Kind, use >= v0.33.0 with `kindest/node:v1.37.0`. The existing
`hack/kind-up.sh --enable-was` helper configures the control-plane feature gates
and API versions, but only creates a single control-plane node. It is not by
itself a complete WAS E2E environment: worker nodes, their labels, registry
mapping, and operator installation are also required.

From `operator/`, run against the dedicated kubeconfig:

```bash
KUBECONFIG=/path/to/was-kubeconfig make run-e2e TEST_PATTERN='^Test_WAS[123]_'
```

With such a cluster, `Test_WAS1/2/3` verify the
generated Workload hierarchy, cross-group gang holding, Pod-derived PodGang
conditions before and after scheduling, and the preferred-topology fail-closed
path against the real scheduler. The tests enable the operator's
`default-scheduler` gang profile through Helm, including its conditional RBAC.
Use a dedicated test cluster because this changes the installed operator
configuration.

API-server integration tests also cover startup with missing WAS capabilities
and owner-watch recovery after deleting generated objects. They do not start
a scheduler and are separate from scheduling E2E tests. From `operator/`,
point to locally installed Kubernetes >= 1.37 envtest binaries:

```bash
GROVE_WAS_ENVTEST_ASSETS=/path/to/k8s/1.37.0-linux-amd64 \
  go test -count=1 ./internal/scheduler/kube ./internal/controller/podgang
```

These integration tests skip when `GROVE_WAS_ENVTEST_ASSETS` is unset, preserving
the ordinary unit-test baseline. When set, API-server startup and test failures
are reported as failures rather than skipped.

### Running in CI/CD

E2E tests are automatically run on GitHub Actions for:

## Managing Dependencies

E2E test dependencies (container images and Helm charts) are managed in `dependencies.yaml`, similar to how Go dependencies are managed in `go.mod`.

### File: `dependencies.yaml`

This file defines all external dependencies used in E2E tests:

- **Container Images**: Images that are pre-pulled into the test cluster to speed up test execution
- **Helm Charts**: External Helm charts (Kai Scheduler, NVIDIA GPU Operator) with their versions and configuration

### Updating Dependencies

To update a dependency version:

1. Edit `e2e/dependencies.yaml`
2. Update the `version` field for the desired component
3. Run tests to verify: `cd e2e && go test -v`

#### Example: Updating Kai Scheduler

```yaml
helmCharts:
  kaiScheduler:
    releaseName: kai-scheduler
    chartRef: oci://ghcr.io/kai-scheduler/kai-scheduler/kai-scheduler
    version: v0.16.9  # <- Update this version
    namespace: kai-scheduler
```

#### Example: Adding a New Image to Pre-pull

```yaml
images:
  # ... existing images ...
  - name: docker.io/myorg/myimage
    version: v1.2.3
```

## Troubleshooting

### Stale k3d Cluster

If tests fail with cluster creation errors, clean up any existing cluster:
```bash
k3d cluster delete shared-e2e-test-cluster
```

### Test Timeout

E2E tests can take 10-15 minutes. If tests timeout, increase the timeout:
```bash
cd e2e && go test -tags=e2e ./tests/... -v -timeout 45m
```
