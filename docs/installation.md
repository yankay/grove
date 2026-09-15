# Installation

To install Grove, you can choose one of the following options:
- Install Grove from the published Helm charts under the [GitHub packages section](https://github.com/orgs/ai-dynamo/packages?repo_name=grove).
- Build from source and install Grove using the `make` targets we provide as a part of the repository.

## Install Grove from published packages

You can directly install Grove in your cluster using the published [`grove-charts`](https://github.com/ai-dynamo/grove/pkgs/container/grove%2Fgrove-charts) Helm packages.
Locate the [release tag](https://github.com/ai-dynamo/grove/releases) to install.
Set the `KUBECONFIG` in your shell session, and run the following:

```bash
helm upgrade -i grove oci://ghcr.io/ai-dynamo/grove/grove-charts --version <version>
```

## Build and install Grove from source (*for developers*)

You can build and deploy Grove to your local kind cluster or remote cluster using the provided `make` targets in the repository.
All grove operator `make` targets are located in [Operator Makefile](../operator/Makefile).

### Local Kind cluster set-up

In case you wish to develop Grove using a local [kind](https://kind.sigs.k8s.io/) cluster or are following along with our tutorials on your local machine, please do the following:

- **Navigate to the operator directory:**

  ```bash
  cd operator
  ```

- **Set up a KIND cluster with local docker registry:**

  ```bash
  make kind-up
  ```

- **Optional**: To create a KIND cluster with fake nodes for testing at scale, specify the number of fake nodes:

  ```bash
  # Create a cluster with 20 fake nodes
  make kind-up FAKE_NODES=20
  ```

  This will automatically install [KWOK](https://kwok.sigs.k8s.io/) (Kubernetes WithOut Kubelet) and create the specified number of fake nodes. These fake nodes are tainted with `fake-node=true:NoSchedule`, so you'll need to add the following toleration to your pod specs to schedule on them:

  ```yaml
  tolerations:
  - key: fake-node
    operator: Exists
    effect: NoSchedule
  ```

- Specify the `KUBECONFIG` environment variable in your shell session to the path printed out at the end of the previous step:

  ```bash
  # You would see something like `export KUBECONFIG=/path-to-your-grove-clone/grove/operator/hack/kind/kubeconfig` printed.
  # If you are already in `/path-to-your-grove-clone/grove/operator`, then you can simply:
  export KUBECONFIG=./hack/kind/kubeconfig
  ```

### Remote cluster set-up

If you wish to use your own Kubernetes cluster instead of the local KIND cluster, follow these steps:

- **Set the KUBECONFIG environment variable** to point to your Kubernetes cluster configuration:

  ```bash
  # Set KUBECONFIG to use your Kubernetes cluster kubeconfig
  export KUBECONFIG=/path/to/your/kubernetes/kubeconfig
  ```

- **Set the CONTAINER_REGISTRY environment variable** to specify your container registry:

  ```bash
  # Set a container registry to push your images to
  export CONTAINER_REGISTRY=your-container-registry
  ```

### Installation using make targets

> **Important:** All commands in this section must be run from the `operator/` directory.

```bash
# Navigate to the operator directory (if not already there)
cd operator

# Optional: Deploy to a custom namespace
export NAMESPACE=custom-ns

# Deploy Grove operator and all resources
make deploy
```

This make target installs all relevant CRDs, builds `grove-operator`, `grove-initc`, and deploys the operator to the cluster.
You can configure the Grove operator by modifying the [values.yaml](../operator/charts/values.yaml).

This make target leverages Grove [Helm](https://helm.sh/) charts and [Skaffold](https://skaffold.dev/) to install the following resources to the cluster:

- [CRDs](../operator/charts):
  - Grove operator CRD - `podcliquesets.grove.io`, `podcliques.grove.io` and `podcliquescalinggroups.grove.io`.
  - Grove Scheduler CRDs - `podgangs.scheduler.grove.io`.
- All Grove operator resources defined as a part of [Grove Helm chart templates](../operator/charts/templates).

## Certificate Management

By default, Grove automatically generates and manages TLS certificates for its webhook server. For production environments, you may want to use certificates from your organization's PKI or a certificate manager like cert-manager.

See the [Certificate Management Guide](user-guide/certificate-management.md) for detailed configuration options.

## Auto MNNVL (Multi-Node NVLink)

On clusters with NVIDIA MNNVL support, you can enable automatic Multi-Node NVLink for GPU workloads by setting `config.network.autoMNNVLEnabled: true` in the operator configuration (e.g. via Helm `--set config.network.autoMNNVLEnabled=true`). See the [Auto MNNVL user guide](user-guide/auto-mnnvl.md) for prerequisites, enabling the feature, and usage.

## Verify Installation

Follow the instructions in the [quickstart guide](quickstart.md) to deploy a PodCliqueSet and validate your installation.

## Upgrade Notes

### ClusterTopology renamed to ClusterTopologyBinding

Grove does not provide automatic migration for existing `ClusterTopology`
resources.

If `ClusterTopology` resources already exist in the cluster:

1. Re-create them manually as `ClusterTopologyBinding` resources.
2. Delete the old `ClusterTopology` instances.
3. Delete the old `ClusterTopology` CRD.

Example:

```bash
# 1. Verify any old ClusterTopology instances that still exist
kubectl get clustertopologies.grove.io

# 2. Re-create any ClusterTopology resources you want to keep as
#    ClusterTopologyBinding resources

# 3. Delete the old ClusterTopology instances
kubectl delete clustertopologies.grove.io --all

# 4. Delete the old ClusterTopology CRD
kubectl delete crd clustertopologies.grove.io
```

This is expected to be a low-impact change because Grove has not yet had a
release containing the update that allowed administrators to create
`ClusterTopology` resources freely.

## Advanced: `helm template` and GitOps

`helm template` does not render the chart's `crds/` directory unless
`--include-crds` is passed, so installs via `helm template | kubectl apply`,
ArgoCD, Flux, or Kustomize fail with missing CRDs by default. Set
`crdInstaller.enabled=true` to install and upgrade CRDs from an init
container instead.

In the same workflows you should also set `webhookServerSecret.enabled=false`:
the chart otherwise renders an empty `grove-webhook-server-cert` Secret that
overwrites the auto-generated TLS material on every re-apply or GitOps sync,
breaking the webhook. With it disabled, the operator creates and manages the
Secret itself.

```bash
helm template grove oci://ghcr.io/ai-dynamo/grove/grove-charts \
  --version <version> \
  --set crdInstaller.enabled=true \
  --set webhookServerSecret.enabled=false \
  | kubectl apply -f -
```

See [GREP-436](proposals/436-crd-upgrader/README.md#note-for-helm-template--gitops-users)
for the CRD design details and the alternative `--include-crds` workflow.

## Troubleshooting

### Deployment Issues

#### `make deploy` fails with "No rule to make target 'deploy'"

**Cause:** You're running the command from the wrong directory.

**Solution:** Ensure you're in the `operator/` directory:
```bash
cd operator
make deploy
```

#### `make deploy` fails with "unable to connect to Kubernetes"

**Cause:** The `KUBECONFIG` environment variable is not set correctly.

**Solution:** Export the kubeconfig for your kind cluster:
```bash
kind get kubeconfig --name grove-test-cluster > hack/kind/kubeconfig
export KUBECONFIG=$(pwd)/hack/kind/kubeconfig
make deploy
```

#### Grove operator pod is in `CrashLoopBackOff`

**Cause:** Check the operator logs for specific errors.

**Solution:**
```bash
kubectl logs -l app.kubernetes.io/name=grove-operator
```

#### `kubectl edit cm grove-operator-cm-*` fails with `field is immutable`

**Cause:** The operator ConfigMap is rendered with `immutable: true` by design and cannot be edited in place.

**Solution:** Change configuration via `helm upgrade`; a new ConfigMap is created and the operator rolls automatically.

```bash
helm upgrade grove oci://ghcr.io/ai-dynamo/grove/grove-charts \
  --version <version> \
  --set config.network.autoMNNVLEnabled=false
```

### Runtime Issues

#### Pods stuck in `Pending` state

**Cause:** Gang scheduling requirements might not be met, or there aren't enough resources.

**Solution:**
1. Check PodGang status:
   ```bash
   kubectl get pg -o yaml
   ```
2. Check if MinAvailable requirements can be satisfied by your cluster resources
3. Check node resources:
   ```bash
   kubectl describe nodes
   ```

#### `kubectl scale` command fails with "not found"

**Cause:** The resource name might be incorrect.

**Solution:** List the actual resource names first:
```bash
# For PodCliqueScalingGroups
kubectl get pcsg

# For PodCliqueSets
kubectl get pcs
```

Then use the exact name from the output.

#### PodCliqueScalingGroup not auto-scaling

**Cause:** HPA might not be created or metrics-server might be missing.

**Solution:**
1. Verify HPA exists:
   ```bash
   kubectl get hpa
   ```
2. Check if metrics-server is running (required for HPA):
   ```bash
   kubectl get deployment metrics-server -n kube-system
   ```
3. For kind clusters, you may need to install metrics-server separately (*choose one of following methods*):
  * Use `operator/Makefile` target
  ```bash
  make deploy-addons
  ```
  * Manual setup
  ```bash
  kubectl apply -f https://github.com/kubernetes-sigs/metrics-server/releases/latest/download/components.yaml
  ```

### Getting Help

If you encounter issues not covered here:
1. Check the [GitHub Issues](https://github.com/NVIDIA/grove/issues) for similar problems
2. Join the [Grove mailing list](https://groups.google.com/g/grove-k8s)
3. Start a [discussion thread](https://github.com/NVIDIA/grove/discussions)

## Supported Schedulers

Currently the following schedulers support gang scheduling of `PodGang`s created by the Grove operator:

- [kai-scheduler/kai-scheduler](https://github.com/kai-scheduler/kai-scheduler)
  - Topology Aware Scheduling (TAS) requires [v0.15.2](https://github.com/kai-scheduler/kai-scheduler/releases/tag/v0.15.2)+
  - Disable KAI stale-gang eviction with
    `--set-string scheduler.args.default-staleness-grace-period=-1`. KAI's default eviction can terminate a
    partially scheduled gang before Grove's controller-owned termination delay.
- Kubernetes `default-scheduler`, with the opt-in Workload-Aware Scheduling configuration below.

### Default-scheduler hierarchical gang scheduling

Grove's `default-scheduler` profile leaves `config.gangScheduling` disabled by default. Ordinary default-scheduler operation retains Grove's Kubernetes >= 1.36 baseline. Enabling hierarchical gang scheduling requires Kubernetes >= 1.37 and the following feature gates:

| Component | Required feature gates |
| --- | --- |
| kube-apiserver | `GenericWorkload`, `CompositePodGroup`, `TopologyAwareWorkloadScheduling` |
| kube-scheduler | `GenericWorkload`, `CompositePodGroup`, `TopologyAwareWorkloadScheduling` |
| kube-controller-manager | `GenericWorkload` |

The API server must serve `workloads` and `podgroups` in `scheduling.k8s.io/v1beta1`, and `compositepodgroups` in `scheduling.k8s.io/v1alpha3`. These are built-in Kubernetes APIs, not CRDs installed by the Grove chart.

Enable the backend with these Helm values:

```yaml
config:
  scheduler:
    defaultProfileName: default-scheduler
    profiles:
      - name: default-scheduler
        config:
          gangScheduling: true
```

Apply the values through `helm upgrade`; do not edit the immutable operator ConfigMap:

```bash
helm upgrade -i grove oci://ghcr.io/ai-dynamo/grove/grove-charts \
  --version <version> -f values.yaml
```

The `profiles` list replaces the chart default list. Include any other scheduler profiles used by your workloads.

At startup, Grove uses an uncached client to list all three scheduling resources. The LIST probes share a 30-second context deadline; REST discovery follows the provided client's discovery timeouts. Missing APIs, denied access, or a failed request prevent the operator from starting. There is no fallback to independent Pod scheduling. Restore the prerequisites or set `gangScheduling: false` in the profile and apply another Helm upgrade. The chart grants access to these APIs only while this option is enabled.

API availability verifies API-server support, not the configuration of other control-plane components. The cluster administrator must enable the required kube-scheduler and kube-controller-manager feature gates before enabling the Grove option.

#### Scheduling objects and lifecycle

Each Grove `PodGang` produces one `Workload`, a root `CompositePodGroup`, and a leaf `PodGroup` for each Grove PodGroup. Topology subgroups produce child `CompositePodGroup`s. Required topology constraints apply at their corresponding level, and all runtime groups use the PodGang's priority class.

Grove creates Pod membership through the immutable `spec.schedulingGroup.podGroupName` field before creating the Pod. Pods that predate feature enablement cannot be adopted in place: recreate them through a Grove rollout. Enabling the operator option alone does not migrate existing Pods.

Pod-count changes update leaf gang minimums. PCSG scale-out creates new PodGang hierarchies; scale-in removes the corresponding hierarchies through owner references. Grove watches generated resources and recreates missing objects. A released `MinReplicas=0` retains the last positive upstream minimum from the Workload or a surviving leaf PodGroup. If both copies are lost, reconciliation fails closed instead of guessing a minimum.

#### Status and troubleshooting

Grove's PodCliqueSet reconciler continues to derive `Initialized`, `Scheduled`, and `Ready` from member Pod creation, association, scheduling, and per-clique `MinAvailable`. The backend does not translate upstream group conditions into `PodGang.status`.

`Workload` has no status. Scheduler-specific diagnostics remain on runtime groups and Pods. Their `PodGroupInitiallyScheduled` and `CompositePodGroupInitiallyScheduled` conditions record initial placement, not current workload readiness.

Inspect a PodGang's hierarchy using its `grove.io/podgang` label:

```bash
kubectl get podgangs.scheduler.grove.io <podgang> -n <namespace> -o yaml
kubectl get workloads.scheduling.k8s.io,compositepodgroups.scheduling.k8s.io,podgroups.scheduling.k8s.io \
  -n <namespace> -l grove.io/podgang=<podgang> -o yaml
kubectl get pods -n <namespace> -l grove.io/podgang=<podgang> -o yaml
kubectl describe podgangs.scheduler.grove.io <podgang> -n <namespace>
```

Backend synchronization failures emit `KubeBackendSyncFailed` warning events on the PodGang. For pending Pods, inspect scheduler events and available capacity as well as group conditions.

#### Limitations

- Initial `MinReplicas=0` is unsupported because WAS requires `minCount >= 1`.
- Each generated template list supports at most eight entries, with hierarchy depth limited to four.
- Preferred topology constraints are rejected; a generated group supports one required topology key.
- Changing immutable hierarchy structure requires replacing generated scheduling objects. In-flight hierarchy replacement still requires further end-to-end validation.

See [GREP-531](https://github.com/ai-dynamo/grove/pull/605) for the design and the [WAS test instructions](../operator/e2e/README.md#workload-aware-scheduling-default-scheduler-gang-tests) for local validation.
