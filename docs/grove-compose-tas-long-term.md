# Grove, CompositePodGroup, and TAS

## Goal

Grove keeps one user API and supports multiple scheduler backends.

```text
PodCliqueSet
  -> Grove hierarchical scheduling IR
      -> upstream Workload / CompositePodGroup / PodGroup
      -> KAI PodGroup / SubGroup
      -> Volcano PodGroup / subGroupPolicy
      -> backend TAS constraints
```

Grove owns AI workload structure. Backends own placement.

## Grove Input and IR

Users submit `PodCliqueSet`. Grove converts it into a backend-neutral hierarchy.

```text
PodCliqueSet: deepseek-serving
  replicas: 2
  groups:
    prefill:
      - pleader
      - pworker
    decode:
      - dleader
      - dworker

Grove IR:
  Replica 0
    Group: prefill
      Leaf: pleader, minPods: 1
      Leaf: pworker, minPods: 4
    Group: decode
      Leaf: dleader, minPods: 1
      Leaf: dworker, minPods: 2
  Replica 1
    ...
```

## Upstream Compose

CompositePodGroup can express hierarchy without gang, but Grove's main path uses compose with gang because PCSG groups roles that should be scheduled together.

Grove input:

```yaml
# Modeled after operator/samples/user-guide/01_core-concepts/multi-node-disaggregated.yaml.
apiVersion: grove.io/v1alpha1
kind: PodCliqueSet
metadata:
  name: serving-compose-gang
  namespace: default
spec:
  replicas: 1
  template:
    cliques:
      - name: pleader
        spec:
          roleName: pleader
          replicas: 1
          minAvailable: 1
          podSpec:
            containers:
              - name: prefill-leader
                image: nginx:latest
                command: ["/bin/sh"]
                args: ["-c", "echo prefill leader && sleep infinity"]
      - name: pworker
        spec:
          roleName: pworker
          replicas: 4
          minAvailable: 4
          podSpec:
            containers:
              - name: prefill-worker
                image: nginx:latest
                command: ["/bin/sh"]
                args: ["-c", "echo prefill worker && sleep infinity"]
      - name: dleader
        spec:
          roleName: dleader
          replicas: 1
          minAvailable: 1
          podSpec:
            containers:
              - name: decode-leader
                image: nginx:latest
                command: ["/bin/sh"]
                args: ["-c", "echo decode leader && sleep infinity"]
      - name: dworker
        spec:
          roleName: dworker
          replicas: 2
          minAvailable: 2
          podSpec:
            containers:
              - name: decode-worker
                image: nginx:latest
                command: ["/bin/sh"]
                args: ["-c", "echo decode worker && sleep infinity"]
    podCliqueScalingGroups:
      - name: prefill
        replicas: 1
        minAvailable: 1
        # Grove groups these cliques together. The upstream compose backend
        # treats all listed cliques as required child groups in this example.
        cliqueNames:
          - pleader
          - pworker
      - name: decode
        replicas: 1
        minAvailable: 1
        # Same: both decode leader and worker are required child groups.
        cliqueNames:
          - dleader
          - dworker
```

Generated upstream objects include gang thresholds:

```text
PCSG cliqueNames -> child PodGroups under one CompositePodGroup
all listed cliques required -> CompositePodGroup gang.minGroupCount = len(cliqueNames)
PCLQ minAvailable -> PodGroup gang.minCount
```

```yaml
apiVersion: scheduling.k8s.io/v1alpha3
kind: Workload
metadata:
  name: serving-compose-gang
spec:
  compositePodGroupTemplates:
    - name: serving-root
      schedulingPolicy:
        gang:
          minGroupCount: 2
      compositePodGroupTemplates:
        - name: prefill
          schedulingPolicy:
            gang:
              minGroupCount: 2
          podGroupTemplates:
            - name: pleader
              schedulingPolicy:
                gang:
                  minCount: 1
            - name: pworker
              schedulingPolicy:
                gang:
                  minCount: 4
        - name: decode
          schedulingPolicy:
            gang:
              minGroupCount: 2
          podGroupTemplates:
            - name: dleader
              schedulingPolicy:
                gang:
                  minCount: 1
            - name: dworker
              schedulingPolicy:
                gang:
                  minCount: 2
```

Shape:

```text
CompositePodGroup: serving-root
  gang: 2 child groups
  CompositePodGroup: prefill
    gang: 2 child groups
    PodGroup: pleader, gang: 1 pod
    PodGroup: pworker, gang: 4 pods
  CompositePodGroup: decode
    gang: 2 child groups
    PodGroup: dleader, gang: 1 pod
    PodGroup: dworker, gang: 2 pods
```

## TAS Layer

TAS is added after compose. The tree decides where constraints attach; TAS decides placement.

Grove resolves topology domains before writing backend keys.

```text
block -> nvidia.com/topology-block
rack  -> nvidia.com/nvl-rack
host  -> kubernetes.io/hostname
```

### Upstream Compose With TAS

Grove input:

```yaml
apiVersion: grove.io/v1alpha1
kind: ClusterTopologyBinding
metadata:
  name: gb200-topology
spec:
  levels:
    - domain: block
      key: nvidia.com/topology-block
    - domain: rack
      key: nvidia.com/nvl-rack
    - domain: host
      key: kubernetes.io/hostname
---
apiVersion: grove.io/v1alpha1
kind: PodCliqueSet
metadata:
  name: serving-compose-gang-tas
  namespace: default
spec:
  replicas: 1
  template:
    topologyConstraint:
      topologyName: gb200-topology
      pack:
        required: block
        preferred: rack
    cliques:
      - name: pleader
        spec:
          roleName: pleader
          replicas: 1
          minAvailable: 1
          podSpec:
            containers:
              - name: prefill-leader
                image: nginx:latest
      - name: pworker
        topologyConstraint:
          pack:
            required: rack
            preferred: host
        spec:
          roleName: pworker
          replicas: 4
          minAvailable: 4
          podSpec:
            containers:
              - name: prefill-worker
                image: nginx:latest
      - name: dleader
        spec:
          roleName: dleader
          replicas: 1
          minAvailable: 1
          podSpec:
            containers:
              - name: decode-leader
                image: nginx:latest
      - name: dworker
        spec:
          roleName: dworker
          replicas: 2
          minAvailable: 2
          podSpec:
            containers:
              - name: decode-worker
                image: nginx:latest
    podCliqueScalingGroups:
      - name: prefill
        replicas: 1
        minAvailable: 1
        topologyConstraint:
          pack:
            required: rack
            preferred: host
        cliqueNames:
          - pleader
          - pworker
      - name: decode
        replicas: 1
        minAvailable: 1
        topologyConstraint:
          pack:
            required: block
            preferred: rack
        cliqueNames:
          - dleader
          - dworker
```

Generated upstream Workload:

```yaml
apiVersion: scheduling.k8s.io/v1alpha3
kind: Workload
metadata:
  name: serving-compose-gang-tas
spec:
  compositePodGroupTemplates:
    - name: serving-root
      schedulingPolicy:
        gang:
          minGroupCount: 2
      schedulingConstraints:
        topology:
          - key: nvidia.com/topology-block
      compositePodGroupTemplates:
        - name: prefill
          schedulingPolicy:
            gang:
              minGroupCount: 2
          schedulingConstraints:
            topology:
              - key: nvidia.com/nvl-rack
          podGroupTemplates:
            - name: pleader
              schedulingPolicy:
                gang:
                  minCount: 1
            - name: pworker
              schedulingPolicy:
                gang:
                  minCount: 4
              schedulingConstraints:
                topology:
                  - key: nvidia.com/nvl-rack
        - name: decode
          schedulingPolicy:
            gang:
              minGroupCount: 2
          schedulingConstraints:
            topology:
              - key: nvidia.com/topology-block
          podGroupTemplates:
            - name: dleader
              schedulingPolicy:
                gang:
                  minCount: 1
            - name: dworker
              schedulingPolicy:
                gang:
                  minCount: 2
```

## KAI Reference

Same Grove IR, KAI-native compose output. This is a reference backend, not the main example.

```yaml
apiVersion: scheduling.run.ai/v2alpha2
kind: PodGroup
metadata:
  name: deepseek-serving-0
spec:
  topology: gb200-topology
  minSubGroup: 2
  topologyConstraint:
    requiredTopologyLevel: nvidia.com/topology-block
    preferredTopologyLevel: nvidia.com/nvl-rack
  subGroups:
    - name: prefill
      minSubGroup: 2
      topologyConstraint:
        requiredTopologyLevel: nvidia.com/nvl-rack
        preferredTopologyLevel: kubernetes.io/hostname
    - name: pleader
      parent: prefill
      minMember: 1
    - name: pworker
      parent: prefill
      minMember: 4
      topologyConstraint:
        requiredTopologyLevel: nvidia.com/nvl-rack
        preferredTopologyLevel: kubernetes.io/hostname
    - name: decode
      minSubGroup: 2
      topologyConstraint:
        requiredTopologyLevel: nvidia.com/topology-block
        preferredTopologyLevel: nvidia.com/nvl-rack
    - name: dleader
      parent: decode
      minMember: 1
    - name: dworker
      parent: decode
      minMember: 2
```

## Rule

Required semantics must not be silently dropped.

```text
required gang/TAS unsupported -> reject
preferred TAS unsupported     -> explicit downgrade + status
```

## Outcome

```text
one Grove API
one hierarchical scheduling IR
many native scheduler backends
```
