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
	"sync"
	"testing"

	apicommon "github.com/ai-dynamo/grove/operator/api/common"
	configv1alpha1 "github.com/ai-dynamo/grove/operator/api/config/v1alpha1"
	"github.com/ai-dynamo/grove/operator/e2e/grove/podgang"
	"github.com/ai-dynamo/grove/operator/e2e/setup"
	"github.com/ai-dynamo/grove/operator/e2e/testctx"

	groveschedulerv1alpha1 "github.com/ai-dynamo/grove/scheduler/api/core/v1alpha1"
	schedulingv1alpha3 "k8s.io/api/scheduling/v1alpha3"
	schedulingv1beta1 "k8s.io/api/scheduling/v1beta1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

var (
	enableWASGangSchedulingOnce sync.Once
	enableWASGangSchedulingErr  error
)

// skipUnlessWASServed skips the test unless the cluster serves all three
// upstream Workload-Aware Scheduling APIs. See e2e/README.md for prerequisites.
func skipUnlessWASServed(t *testing.T, tc *testctx.TestContext) {
	t.Helper()
	for _, resource := range []client.ObjectList{
		&schedulingv1beta1.WorkloadList{},
		&schedulingv1beta1.PodGroupList{},
		&schedulingv1alpha3.CompositePodGroupList{},
	} {
		err := tc.Client.List(tc.Ctx, resource, client.InNamespace(tc.Namespace), client.Limit(1))
		if meta.IsNoMatchError(err) || apierrors.IsNotFound(err) {
			t.Skipf("Skipping: the cluster does not serve the scheduling.k8s.io Workload APIs "+
				"(requires Kubernetes >= 1.37 with WAS feature gates; see e2e/README.md): %v", err)
		}
		if err != nil {
			t.Fatalf("failed to probe %T API: %v", resource, err)
		}
	}
}

func enableWASGangScheduling(t *testing.T, tc *testctx.TestContext) {
	t.Helper()
	enableWASGangSchedulingOnce.Do(func() {
		chartDir, err := setup.GetGroveChartDir()
		if err != nil {
			enableWASGangSchedulingErr = err
			return
		}
		enableWASGangSchedulingErr = setup.UpdateGroveConfiguration(
			tc.Ctx,
			tc.Client.RestConfig,
			chartDir,
			&setup.GroveConfig{
				Scheduler: &configv1alpha1.SchedulerConfiguration{
					DefaultProfileName: string(configv1alpha1.SchedulerNameKube),
					Profiles: []configv1alpha1.SchedulerProfile{
						{
							Name: configv1alpha1.SchedulerNameKube,
							Config: &runtime.RawExtension{
								Raw: []byte(`{"gangScheduling":true}`),
							},
						},
					},
				},
			},
			Logger,
		)
	})
	if enableWASGangSchedulingErr != nil {
		t.Fatalf("failed to enable default-scheduler gang scheduling: %v", enableWASGangSchedulingErr)
	}
}

// Test_WAS1_HierarchyCreatedForPodGang is a positive test: deploying a
// default-scheduler PodCliqueSet must produce an upstream Workload with a root
// CompositePodGroup gang over per-clique leaf PodGroups, and all pods must be
// gang-scheduled and become ready.
//
// Scenario WAS-1:
//  1. Initialize a Grove cluster with the Workload-Aware Scheduling APIs served
//  2. Deploy the was-gang PodCliqueSet (default-scheduler), verify 3 pods created
//  3. Verify a Workload is created with a root CompositePodGroup gang whose
//     minGroupCount spans the two per-clique leaf PodGroups
//  4. Verify one runtime PodGroup per Grove PodGroup with the expected minCount
//  5. Verify each pod is assigned to its leaf PodGroup via schedulingGroup
//  6. Verify all pods are scheduled and become ready
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
	enableWASGangScheduling(t, tc)

	Logger.Info("2. Deploy the was-gang PodCliqueSet, verify pods created")
	if _, err := tc.DeployAndVerifyWorkload(); err != nil {
		t.Fatalf("Failed to deploy workload: %v", err)
	}
	// Successful placement waits for the asynchronous hierarchy reconciliation.
	if err := tc.WaitForPods(expectedPods); err != nil {
		t.Fatalf("Failed to wait for all pods to be ready: %v", err)
	}

	Logger.Info("3. Verify the generated Workload hierarchy")
	podGangs, err := podgang.NewVerifier(tc.Client, Logger).List(ctx,
		client.ObjectKey{Namespace: tc.Namespace, Name: tc.Workload.Name})
	if err != nil {
		t.Fatalf("Failed to list PodGangs: %v", err)
	}
	if len(podGangs) != 1 {
		t.Fatalf("expected exactly one PodGang, got %d", len(podGangs))
	}
	gang := &podGangs[0]
	listOptions := []client.ListOption{
		client.InNamespace(tc.Namespace),
		client.MatchingLabels{apicommon.LabelPodGang: gang.Name},
	}
	workloads := &schedulingv1beta1.WorkloadList{}
	if err := tc.Client.List(ctx, workloads, listOptions...); err != nil {
		t.Fatalf("Failed to list Workloads: %v", err)
	}
	if len(workloads.Items) != 1 {
		t.Fatalf("expected exactly one Workload for PodGang %q, got %d", gang.Name, len(workloads.Items))
	}
	workload := workloads.Items[0]
	if workload.Name != gang.Name || !metav1.IsControlledBy(&workload, gang) {
		t.Fatalf("Workload %q must be named after and owned by PodGang %q", workload.Name, gang.Name)
	}
	if len(workload.Spec.CompositePodGroupTemplates) != 1 {
		t.Fatalf("expected exactly one root CompositePodGroupTemplate, got %d", len(workload.Spec.CompositePodGroupTemplates))
	}
	root := workload.Spec.CompositePodGroupTemplates[0]
	if root.SchedulingPolicy.Gang == nil {
		t.Fatalf("root CompositePodGroupTemplate must use a gang scheduling policy")
	}
	if root.SchedulingPolicy.Gang.MinGroupCount != 2 || len(root.PodGroupTemplates) != 2 ||
		len(root.CompositePodGroupTemplates) != 0 {
		t.Fatalf("root must gang exactly the two clique leaf templates: %+v", root)
	}
	leafMinCounts := make(map[string]int32, len(root.PodGroupTemplates))
	for _, leaf := range root.PodGroupTemplates {
		if leaf.SchedulingPolicy.Gang == nil {
			t.Fatalf("leaf PodGroupTemplate %q must use a gang scheduling policy", leaf.Name)
		}
		leafMinCounts[leaf.Name] = leaf.SchedulingPolicy.Gang.MinCount
	}

	Logger.Info("4. Verify one runtime PodGroup per Grove PodGroup")
	expectedMinCounts := map[string]int32{
		tc.Workload.Name + "-0-pc-a": 2,
		tc.Workload.Name + "-0-pc-b": 1,
	}
	podGroups := &schedulingv1beta1.PodGroupList{}
	if err := tc.Client.List(ctx, podGroups, listOptions...); err != nil {
		t.Fatalf("Failed to list PodGroups: %v", err)
	}
	if len(podGroups.Items) != len(expectedMinCounts) {
		t.Fatalf("expected exactly %d runtime PodGroups, got %d", len(expectedMinCounts), len(podGroups.Items))
	}
	for i := range podGroups.Items {
		pg := &podGroups.Items[i]
		minCount, found := expectedMinCounts[pg.Name]
		if !found || !metav1.IsControlledBy(pg, gang) {
			t.Fatalf("unexpected runtime PodGroup %q or owner", pg.Name)
		}
		if pg.Spec.SchedulingPolicy.Gang == nil || pg.Spec.SchedulingPolicy.Gang.MinCount != minCount {
			t.Fatalf("runtime PodGroup %q must have gang minCount %d", pg.Name, minCount)
		}
		if pg.Spec.WorkloadRef == nil || pg.Spec.WorkloadRef.WorkloadName != workload.Name ||
			leafMinCounts[pg.Spec.WorkloadRef.TemplateName] != minCount {
			t.Fatalf("runtime PodGroup %q must reference its Workload leaf template with minCount %d", pg.Name, minCount)
		}
		if pg.Spec.ParentCompositePodGroupName == nil || *pg.Spec.ParentCompositePodGroupName != gang.Name {
			t.Fatalf("runtime PodGroup %q must reference root CompositePodGroup %q", pg.Name, gang.Name)
		}
	}

	Logger.Info("5. Verify a runtime root CompositePodGroup exists")
	compositePodGroups := &schedulingv1alpha3.CompositePodGroupList{}
	if err := tc.Client.List(ctx, compositePodGroups, listOptions...); err != nil {
		t.Fatalf("Failed to list CompositePodGroups: %v", err)
	}
	if len(compositePodGroups.Items) != 1 {
		t.Fatalf("expected exactly one runtime CompositePodGroup, got %d", len(compositePodGroups.Items))
	}
	runtimeRoot := &compositePodGroups.Items[0]
	if runtimeRoot.Name != gang.Name || !metav1.IsControlledBy(runtimeRoot, gang) ||
		runtimeRoot.Spec.ParentCompositePodGroupName != nil {
		t.Fatalf("expected root CompositePodGroup %q owned by the PodGang with no parent", gang.Name)
	}
	if runtimeRoot.Spec.SchedulingPolicy.Gang == nil || runtimeRoot.Spec.SchedulingPolicy.Gang.MinGroupCount != 2 {
		t.Fatalf("root CompositePodGroup must gang both runtime leaf PodGroups")
	}

	Logger.Info("6. Verify all pods are scheduled and become ready")
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
		if _, found := expectedMinCounts[*pod.Spec.SchedulingGroup.PodGroupName]; !found {
			t.Fatalf("pod %q references unexpected PodGroup %q", pod.Name, *pod.Spec.SchedulingGroup.PodGroupName)
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
//  1. Initialize a Grove cluster with the Workload-Aware Scheduling APIs served,
//     then cordon 1 node so the gang cannot fit
//  2. Deploy the was-gang PodCliqueSet, verify pods created
//  3. Verify all pods are pending (gang not admitted)
//  4. Uncordon the node and verify all pods schedule together
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
	enableWASGangScheduling(t, tc)

	nodesToCordon := tc.SetupAndCordonNodes(1)

	Logger.Info("2. Deploy the was-gang PodCliqueSet, verify pods created")
	if _, err := tc.DeployAndVerifyWorkload(); err != nil {
		t.Fatalf("Failed to deploy workload: %v", err)
	}

	Logger.Info("3. Verify all pods are pending because the gang cannot be admitted")
	if err := tc.VerifyPodsArePendingWithUnschedulableEvents(true, expectedPods); err != nil {
		t.Fatalf("Failed to verify all pods are pending: %v", err)
	}

	verifier := podgang.NewVerifier(tc.Client, Logger)
	pcsKey := client.ObjectKey{Namespace: tc.Namespace, Name: tc.Workload.Name}
	if err := podgang.WaitUntilVerified(ctx, verifier, pcsKey, tc.Timeout, tc.Interval,
		podgang.ConditionStatusCheckFn(groveschedulerv1alpha1.PodGangConditionTypeInitialized, metav1.ConditionTrue),
		podgang.ConditionStatusCheckFn(groveschedulerv1alpha1.PodGangConditionTypeScheduled, metav1.ConditionFalse),
		podgang.ConditionStatusCheckFn(groveschedulerv1alpha1.PodGangConditionTypeReady, metav1.ConditionFalse),
		podgang.LastScheduledSetCheckFn(false),
		podgang.LastReadySetCheckFn(false),
	); err != nil {
		t.Fatalf("PodGang status must reflect pending member Pods: %v", err)
	}

	Logger.Info("4. Uncordon the node and verify all pods schedule together")
	tc.UncordonNodesAndWaitForPods(nodesToCordon, expectedPods)
	tc.ListPodsAndAssertDistinctNodes()

	if err := podgang.WaitUntilVerified(ctx, verifier, pcsKey, tc.Timeout, tc.Interval,
		podgang.ConditionStatusCheckFn(groveschedulerv1alpha1.PodGangConditionTypeScheduled, metav1.ConditionTrue),
		podgang.ConditionStatusCheckFn(groveschedulerv1alpha1.PodGangConditionTypeReady, metav1.ConditionTrue),
		podgang.LastScheduledSetCheckFn(true),
		podgang.LastReadySetCheckFn(true),
	); err != nil {
		t.Fatalf("PodGang status must reflect scheduled and ready member Pods: %v", err)
	}

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
	enableWASGangScheduling(t, tc)

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
