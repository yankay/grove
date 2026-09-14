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

### Hibernation Backend Tests

The `Test_ZR*` and `Test_GS13*` tests also run against an existing cluster with KAI or Volcano.
Use a dedicated cluster: these tests delete Grove workloads, cordon worker nodes,
and restart the Grove operator.

Install Grove in `grove-system` as `grove-operator`, enable the matching scheduler
profile, and provide at least six Ready worker nodes labeled
`node_role.e2e.grove.nvidia.com=agent`. For KAI, create the `test` queue and disable
stale-gang eviction with `scheduler.args.default-staleness-grace-period=-1s`.

From the `operator` directory:

```bash
KUBECONFIG=/path/to/test.kubeconfig \
GROVE_E2E_SCHEDULER=volcano \
GROVE_E2E_WORKLOAD_IMAGE=registry:5001/busybox:latest \
go test -tags=e2e ./e2e/tests -run '^(Test_GS13|Test_ZR)' -count=1 -v -timeout=25m
```

Use `kai-scheduler` for KAI. The two `GROVE_E2E_*` overrides apply only to the
hibernation fixture, not the rest of the E2E suite. Set `E2E_REGISTRY_PORT` when
the host-side test image push endpoint uses a port other than `5001`; the workload
image must be reachable from every worker.

### Running in CI/CD

E2E tests are automatically run on GitHub Actions for:
- **All non-draft pull requests** to `main`
- **Draft pull requests** with the `run-e2e` label

To trigger e2e tests on a draft PR:
1. Add the `run-e2e` label to the pull request
2. The workflow will run automatically

The CI workflow is defined in `.github/workflows/e2e-test.yaml`.

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
