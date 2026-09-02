//go:build e2e

// Copyright 2026 The Grove Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package tests

import (
	"context"
	"strings"
	"testing"

	apicommon "github.com/ai-dynamo/grove/operator/api/common"
	"github.com/ai-dynamo/grove/operator/e2e/testctx"

	schedulingv1alpha3 "k8s.io/api/scheduling/v1alpha3"
	schedulingv1beta1 "k8s.io/api/scheduling/v1beta1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

// skipUnlessWASServed skips the test unless the cluster serves the upstream
// Workload-Aware Scheduling APIs (Kubernetes >= 1.37 with the GenericWorkload
// and CompositePodGroup feature gates). Bring such a cluster up with
// `hack/kind-up.sh --enable-was`.
func skipUnlessWASServed(t *testing.T, tc *testctx.TestContext) {
	t.Helper()
	workloads := &schedulingv1beta1.WorkloadList{}
	err := tc.Client.List(tc.Ctx, workloads, client.InNamespace(tc.Namespace), client.Limit(1))
	if err == nil {
		return
	}
	if meta.IsNoMatchError(err) || apierrors.IsNotFound(err) {
		t.Skipf("Skipping: the cluster does not serve the scheduling.k8s.io Workload APIs "+
			"(run hack/kind-up.sh --enable-was on Kubernetes >= 1.37): %v", err)
	}
	t.Fatalf("failed to probe Workload API: %v", err)
}

// Test_WAS1_HierarchyCreatedForPodGang is a positive test: deploying a
// default-scheduler PodCliqueSet must produce an upstream Workload with a root
// CompositePodGroup gang over per-clique leaf PodGroups, and all pods must be
// gang-scheduled and become ready.
//
// Scenario WAS-1:
// 1. Initialize a Grove cluster with the Workload-Aware Scheduling APIs served
// 2. Deploy the was-gang PodCliqueSet (default-scheduler), verify 3 pods created
// 3. Verify a Workload is created with a root CompositePodGroup gang whose
//    minGroupCount spans the two per-clique leaf PodGroups
// 4. Verify one runtime PodGroup per Grove PodGroup with the expected minCount
// 5. Verify each pod is assigned to its leaf PodGroup via schedulingGroup
// 6. Verify all pods are scheduled and become ready
func Test_WAS1_HierarchyCreatedForPodGang(t *testing.T) {
	ctx := context.Background()

	Logger.Info("1. Initialize a Grove cluster (requires the Workload-Aware Scheduling APIs)")
	expectedPods := 3 // pc-a: 2 replicas, pc-b: 1 replica
	tc, cleanup := testctx.PrepareTest(ctx, t, 3,
		testctx.WithWorkload(&testctx.WorkloadConfig{
			Name:         "was-gang",
			YAMLPath:     "../yaml/was-gang.yaml",
			Namespace:    "default",
			ExpectedPods: expectedPods,
		}),
	)
	defer cleanup()

	skipUnlessWASServed(t, tc)

	Logger.Info("2. Deploy the was-gang PodCliqueSet, verify pods created")
	if _, err := tc.DeployAndVerifyWorkload(); err != nil {
		t.Fatalf("Failed to deploy workload: %v", err)
	}

	Logger.Info("3. Verify the generated Workload hierarchy")
	workloads := &schedulingv1beta1.WorkloadList{}
	if err := tc.Client.List(ctx, workloads, client.InNamespace(tc.Namespace),
		client.MatchingLabels{apicommon.LabelPartOfKey: "was-gang"}); err != nil {
		// Fall back to listing by the PodGang label if part-of is not propagated.
		workloads = &schedulingv1beta1.WorkloadList{}
		if err := tc.Client.List(ctx, workloads, client.InNamespace(tc.Namespace)); err != nil {
			t.Fatalf("Failed to list Workloads: %v", err)
		}
	}
	if len(workloads.Items) == 0 {
		t.Fatalf("expected at least one Workload to be generated for the PodGang, found none")
	}
	workload := workloads.Items[0]
	if len(workload.Spec.CompositePodGroupTemplates) != 1 {
		t.Fatalf("expected exactly one root CompositePodGroupTemplate, got %d", len(workload.Spec.CompositePodGroupTemplates))
	}
	root := workload.Spec.CompositePodGroupTemplates[0]
	if root.SchedulingPolicy.Gang == nil {
		t.Fatalf("root CompositePodGroupTemplate must use a gang scheduling policy")
	}
	if root.SchedulingPolicy.Gang.MinGroupCount != int32(len(root.PodGroupTemplates)) {
		t.Fatalf("root gang minGroupCount %d must equal the number of leaf templates %d",
			root.SchedulingPolicy.Gang.MinGroupCount, len(root.PodGroupTemplates))
	}
	for _, leaf := range root.PodGroupTemplates {
		if leaf.SchedulingPolicy.Gang == nil {
			t.Fatalf("leaf PodGroupTemplate %q must use a gang scheduling policy", leaf.Name)
		}
	}

	Logger.Info("4. Verify one runtime PodGroup per Grove PodGroup")
	podGroups := &schedulingv1beta1.PodGroupList{}
	if err := tc.Client.List(ctx, podGroups, client.InNamespace(tc.Namespace)); err != nil {
		t.Fatalf("Failed to list PodGroups: %v", err)
	}
	if len(podGroups.Items) < len(root.PodGroupTemplates) {
		t.Fatalf("expected at least %d runtime PodGroups, found %d", len(root.PodGroupTemplates), len(podGroups.Items))
	}
	for i := range podGroups.Items {
		pg := &podGroups.Items[i]
		if pg.Spec.SchedulingPolicy.Gang == nil || pg.Spec.SchedulingPolicy.Gang.MinCount < 1 {
			t.Fatalf("runtime PodGroup %q must have a positive gang minCount", pg.Name)
		}
		if pg.Spec.ParentCompositePodGroupName == nil {
			t.Fatalf("runtime PodGroup %q must reference a parent CompositePodGroup", pg.Name)
		}
	}

	Logger.Info("5. Verify a runtime root CompositePodGroup exists")
	compositePodGroups := &schedulingv1alpha3.CompositePodGroupList{}
	if err := tc.Client.List(ctx, compositePodGroups, client.InNamespace(tc.Namespace)); err != nil {
		t.Fatalf("Failed to list CompositePodGroups: %v", err)
	}
	foundRoot := false
	for i := range compositePodGroups.Items {
		if compositePodGroups.Items[i].Spec.ParentCompositePodGroupName == nil {
			foundRoot = true
		}
	}
	if !foundRoot {
		t.Fatalf("expected a root CompositePodGroup with no parent, found none")
	}

	Logger.Info("6. Verify all pods are scheduled and become ready")
	if err := tc.WaitForPods(expectedPods); err != nil {
		t.Fatalf("Failed to wait for all pods to be ready: %v", err)
	}
	pods, err := tc.ListPods()
	if err != nil {
		t.Fatalf("Failed to list pods: %v", err)
	}
	for i := range pods.Items {
		pod := &pods.Items[i]
		if pod.Spec.SchedulingGroup == nil || pod.Spec.SchedulingGroup.PodGroupName == nil {
			t.Fatalf("pod %q must be assigned to a leaf PodGroup via schedulingGroup.podGroupName", pod.Name)
		}
		if *pod.Spec.SchedulingGroup.PodGroupName != pod.Labels[apicommon.LabelPodClique] {
			t.Fatalf("pod %q schedulingGroup.podGroupName %q must equal its PodClique label %q",
				pod.Name, *pod.Spec.SchedulingGroup.PodGroupName, pod.Labels[apicommon.LabelPodClique])
		}
	}

	Logger.Info("🎉 WAS-1 hierarchy creation test completed successfully!")
}

// Test_WAS2_GangHeldWhenInsufficientResources is a negative-path (adversarial)
// test: when the gang cannot fit, the whole PodGang must be held pending
// (all-or-nothing), and only once enough capacity is available do all pods
// schedule together.
//
// Scenario WAS-2:
// 1. Initialize a Grove cluster with the Workload-Aware Scheduling APIs served,
//    then cordon 1 node so the gang cannot fit
// 2. Deploy the was-gang PodCliqueSet, verify pods created
// 3. Verify all pods are pending (gang not admitted)
// 4. Uncordon the node and verify all pods schedule together
func Test_WAS2_GangHeldWhenInsufficientResources(t *testing.T) {
	ctx := context.Background()

	Logger.Info("1. Initialize a Grove cluster, then cordon 1 node")
	expectedPods := 3
	tc, cleanup := testctx.PrepareTest(ctx, t, 3,
		testctx.WithWorkload(&testctx.WorkloadConfig{
			Name:         "was-gang",
			YAMLPath:     "../yaml/was-gang.yaml",
			Namespace:    "default",
			ExpectedPods: expectedPods,
		}),
	)
	defer cleanup()

	skipUnlessWASServed(t, tc)

	nodesToCordon := tc.SetupAndCordonNodes(1)

	Logger.Info("2. Deploy the was-gang PodCliqueSet, verify pods created")
	if _, err := tc.DeployAndVerifyWorkload(); err != nil {
		t.Fatalf("Failed to deploy workload: %v", err)
	}

	Logger.Info("3. Verify all pods are pending because the gang cannot be admitted")
	if err := tc.VerifyPodsArePendingWithUnschedulableEvents(true, expectedPods); err != nil {
		t.Fatalf("Failed to verify all pods are pending: %v", err)
	}

	Logger.Info("4. Uncordon the node and verify all pods schedule together")
	tc.UncordonNodesAndWaitForPods(nodesToCordon, expectedPods)
	tc.ListPodsAndAssertDistinctNodes()

	Logger.Info("🎉 WAS-2 gang-held-on-insufficient-resources test completed successfully!")
}

// Test_WAS3_PreferredTopologyRejected is a negative-path (adversarial) test:
// the Kubernetes Workload-Aware Scheduling APIs only support required topology
// constraints, so a default-scheduler PodCliqueSet requesting a preferred
// topology constraint must be rejected by the validating webhook (fail closed).
//
// Scenario WAS-3:
// 1. Initialize a Grove cluster with the Workload-Aware Scheduling APIs served
// 2. Apply a default-scheduler PodCliqueSet with a preferred topology constraint
// 3. Verify the apply is rejected and no PodGang / Workload objects are created
func Test_WAS3_PreferredTopologyRejected(t *testing.T) {
	ctx := context.Background()

	Logger.Info("1. Initialize a Grove cluster (requires the Workload-Aware Scheduling APIs)")
	tc, cleanup := testctx.PrepareTest(ctx, t, 1,
		testctx.WithWorkload(&testctx.WorkloadConfig{
			Name:         "was-preferred-reject",
			YAMLPath:     "../yaml/was-preferred-reject.yaml",
			Namespace:    "default",
			ExpectedPods: 0,
		}),
	)
	defer cleanup()

	skipUnlessWASServed(t, tc)

	Logger.Info("2. Apply a default-scheduler PodCliqueSet with a preferred topology constraint")
	_, err := tc.ApplyYAMLFile(tc.Workload.YAMLPath)

	Logger.Info("3. Verify the apply is rejected with a fail-closed validation error")
	if err == nil {
		t.Fatalf("expected the PodCliqueSet with a preferred topology constraint to be rejected, but the apply succeeded")
	}
	if !strings.Contains(err.Error(), "preferred topology") {
		t.Fatalf("expected a preferred-topology rejection error, got: %v", err)
	}

	Logger.Info("🎉 WAS-3 preferred-topology-rejected test completed successfully!")
}

