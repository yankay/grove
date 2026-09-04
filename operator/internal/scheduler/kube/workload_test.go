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

package kube

import (
	"context"
	"testing"

	apicommon "github.com/ai-dynamo/grove/operator/api/common"
	configv1alpha1 "github.com/ai-dynamo/grove/operator/api/config/v1alpha1"

	groveschedulerv1alpha1 "github.com/ai-dynamo/grove/scheduler/api/core/v1alpha1"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	corev1 "k8s.io/api/core/v1"
	schedulingv1alpha3 "k8s.io/api/scheduling/v1alpha3"
	schedulingv1beta1 "k8s.io/api/scheduling/v1beta1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/tools/record"
	workloadbuilder "k8s.io/component-helpers/scheduling/schedulingv1/workloadbuilder"
	"k8s.io/utils/ptr"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
)

const (
	testNamespace   = "test-ns"
	testPodGangName = "test-pcs-0"
)

// newWorkloadTestScheme builds a fresh scheme with the schemes the gang
// scheduling translation needs, without touching the process-global scheme.
func newWorkloadTestScheme(t *testing.T) *runtime.Scheme {
	t.Helper()
	scheme := runtime.NewScheme()
	require.NoError(t, groveschedulerv1alpha1.AddToScheme(scheme))
	require.NoError(t, schedulingv1beta1.AddToScheme(scheme))
	require.NoError(t, schedulingv1alpha3.AddToScheme(scheme))
	return scheme
}

func newGangBackend(t *testing.T, existingObjects ...client.Object) (*schedulerBackend, client.Client) {
	t.Helper()
	scheme := newWorkloadTestScheme(t)
	cl := fake.NewClientBuilder().WithScheme(scheme).WithObjects(existingObjects...).Build()
	backend := &schedulerBackend{
		client:                cl,
		scheme:                scheme,
		name:                  string(configv1alpha1.SchedulerNameKube),
		eventRecorder:         record.NewFakeRecorder(10),
		gangSchedulingEnabled: true,
	}
	return backend, cl
}

func newTestPodGang(mutators ...func(*groveschedulerv1alpha1.PodGang)) *groveschedulerv1alpha1.PodGang {
	podGang := &groveschedulerv1alpha1.PodGang{
		ObjectMeta: metav1.ObjectMeta{
			Name:      testPodGangName,
			Namespace: testNamespace,
			UID:       types.UID("test-uid"),
		},
		Spec: groveschedulerv1alpha1.PodGangSpec{
			PodGroups: []groveschedulerv1alpha1.PodGroup{
				{Name: "test-pcs-0-prefill", MinReplicas: 2},
				{Name: "test-pcs-0-decode", MinReplicas: 1},
			},
		},
	}
	for _, mutate := range mutators {
		mutate(podGang)
	}
	return podGang
}

func withTopologyGroup(name string, podGroupNames []string, requiredKey string) func(*groveschedulerv1alpha1.PodGang) {
	return func(podGang *groveschedulerv1alpha1.PodGang) {
		var topologyConstraint *groveschedulerv1alpha1.TopologyConstraint
		if requiredKey != "" {
			topologyConstraint = &groveschedulerv1alpha1.TopologyConstraint{
				PackConstraint: &groveschedulerv1alpha1.TopologyPackConstraint{Required: ptr.To(requiredKey)},
			}
		}
		podGang.Spec.TopologyConstraintGroupConfigs = append(podGang.Spec.TopologyConstraintGroupConfigs,
			groveschedulerv1alpha1.TopologyConstraintGroupConfig{
				Name:               name,
				PodGroupNames:      podGroupNames,
				TopologyConstraint: topologyConstraint,
			})
	}
}

func TestBuildWorkloadForPodGang_FlatHierarchy(t *testing.T) {
	workload, err := buildWorkloadForPodGang(newTestPodGang(), nil)
	require.NoError(t, err)

	require.Len(t, workload.Spec.CompositePodGroupTemplates, 1)
	root := workload.Spec.CompositePodGroupTemplates[0]
	assert.Equal(t, rootTemplateName, root.Name)
	require.NotNil(t, root.SchedulingPolicy.Gang)
	assert.Equal(t, int32(2), root.SchedulingPolicy.Gang.MinGroupCount)
	assert.Empty(t, root.CompositePodGroupTemplates)

	require.Len(t, root.PodGroupTemplates, 2)
	leafMinCounts := map[string]int32{}
	for _, leaf := range root.PodGroupTemplates {
		require.NotNil(t, leaf.SchedulingPolicy.Gang, "leaf %q must use gang policy", leaf.Name)
		leafMinCounts[leaf.Name] = leaf.SchedulingPolicy.Gang.MinCount
	}
	assert.Equal(t, map[string]int32{"test-pcs-0-prefill": 2, "test-pcs-0-decode": 1}, leafMinCounts)

	require.Len(t, workload.OwnerReferences, 1)
	assert.Equal(t, "PodGang", workload.OwnerReferences[0].Kind)
	assert.Equal(t, testPodGangName, workload.OwnerReferences[0].Name)
	assert.Equal(t, testPodGangName, workload.Labels[apicommon.LabelPodGang])
}

func TestBuildWorkloadForPodGang_TopologyGroupHierarchy(t *testing.T) {
	podGang := newTestPodGang(withTopologyGroup("tcg-a", []string{"test-pcs-0-prefill"}, "topology.kubernetes.io/rack"))

	workload, err := buildWorkloadForPodGang(podGang, nil)
	require.NoError(t, err)

	require.Len(t, workload.Spec.CompositePodGroupTemplates, 1)
	root := workload.Spec.CompositePodGroupTemplates[0]
	// Root gang spans its two direct children: the tcg-a composite and the ungrouped decode leaf.
	require.NotNil(t, root.SchedulingPolicy.Gang)
	assert.Equal(t, int32(2), root.SchedulingPolicy.Gang.MinGroupCount)

	require.Len(t, root.CompositePodGroupTemplates, 1)
	group := root.CompositePodGroupTemplates[0]
	assert.Equal(t, "tcg-a", group.Name)
	require.NotNil(t, group.SchedulingPolicy.Gang)
	assert.Equal(t, int32(1), group.SchedulingPolicy.Gang.MinGroupCount)
	require.NotNil(t, group.SchedulingConstraints)
	require.Len(t, group.SchedulingConstraints.Topology, 1)
	assert.Equal(t, "topology.kubernetes.io/rack", group.SchedulingConstraints.Topology[0].Key)
	require.Len(t, group.PodGroupTemplates, 1)
	assert.Equal(t, "test-pcs-0-prefill", group.PodGroupTemplates[0].Name)

	require.Len(t, root.PodGroupTemplates, 1)
	assert.Equal(t, "test-pcs-0-decode", root.PodGroupTemplates[0].Name)
}

func TestBuildWorkloadForPodGang_FailClosed(t *testing.T) {
	tests := []struct {
		name        string
		mutate      func(*groveschedulerv1alpha1.PodGang)
		wantErrPart string
	}{
		{
			name: "preferred topology constraint",
			mutate: func(podGang *groveschedulerv1alpha1.PodGang) {
				podGang.Spec.TopologyConstraint = &groveschedulerv1alpha1.TopologyConstraint{
					PackConstraint: &groveschedulerv1alpha1.TopologyPackConstraint{Preferred: ptr.To("kubernetes.io/hostname")},
				}
			},
			wantErrPart: "preferred topology constraint",
		},
		{
			name: "zero minReplicas on creation",
			mutate: func(podGang *groveschedulerv1alpha1.PodGang) {
				podGang.Spec.PodGroups[0].MinReplicas = 0
			},
			wantErrPart: "minCount >= 1",
		},
		{
			name: "no pod groups",
			mutate: func(podGang *groveschedulerv1alpha1.PodGang) {
				podGang.Spec.PodGroups = nil
			},
			wantErrPart: "no PodGroups",
		},
		{
			name: "unknown pod group in topology group",
			mutate: func(podGang *groveschedulerv1alpha1.PodGang) {
				withTopologyGroup("tcg-a", []string{"missing"}, "")(podGang)
			},
			wantErrPart: "unknown PodGroup",
		},
		{
			name: "pod group in multiple topology groups",
			mutate: func(podGang *groveschedulerv1alpha1.PodGang) {
				withTopologyGroup("tcg-a", []string{"test-pcs-0-prefill"}, "")(podGang)
				withTopologyGroup("tcg-b", []string{"test-pcs-0-prefill"}, "")(podGang)
			},
			wantErrPart: "multiple topology constraint groups",
		},
		{
			name: "too many pod groups for one template list",
			mutate: func(podGang *groveschedulerv1alpha1.PodGang) {
				podGang.Spec.PodGroups = nil
				for i := 0; i < schedulingv1beta1.WorkloadMaxPodGroupTemplates+1; i++ {
					podGang.Spec.PodGroups = append(podGang.Spec.PodGroups, groveschedulerv1alpha1.PodGroup{
						Name:        string(rune('a' + i)),
						MinReplicas: 1,
					})
				}
			},
			wantErrPart: "at most",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			podGang := newTestPodGang(tt.mutate)
			_, err := buildWorkloadForPodGang(podGang, nil)
			require.Error(t, err)
			assert.ErrorContains(t, err, tt.wantErrPart)
		})
	}
}

func TestSyncPodGang_CreatesHierarchy(t *testing.T) {
	podGang := newTestPodGang(withTopologyGroup("tcg-a", []string{"test-pcs-0-prefill"}, "topology.kubernetes.io/rack"))
	backend, cl := newGangBackend(t, podGang)
	ctx := context.Background()

	require.NoError(t, backend.SyncPodGang(ctx, podGang))

	workload := &schedulingv1beta1.Workload{}
	require.NoError(t, cl.Get(ctx, client.ObjectKey{Namespace: testNamespace, Name: testPodGangName}, workload))
	require.Len(t, workload.Spec.CompositePodGroupTemplates, 1)

	rootCompositePodGroup := &schedulingv1alpha3.CompositePodGroup{}
	require.NoError(t, cl.Get(ctx, client.ObjectKey{Namespace: testNamespace, Name: testPodGangName}, rootCompositePodGroup))
	assert.Nil(t, rootCompositePodGroup.Spec.ParentCompositePodGroupName)
	require.NotNil(t, rootCompositePodGroup.Spec.WorkloadRef)
	assert.Equal(t, testPodGangName, rootCompositePodGroup.Spec.WorkloadRef.WorkloadName)

	groupCompositePodGroup := &schedulingv1alpha3.CompositePodGroup{}
	require.NoError(t, cl.Get(ctx, client.ObjectKey{Namespace: testNamespace, Name: testPodGangName + "-tcg-a"}, groupCompositePodGroup))
	require.NotNil(t, groupCompositePodGroup.Spec.ParentCompositePodGroupName)
	assert.Equal(t, testPodGangName, *groupCompositePodGroup.Spec.ParentCompositePodGroupName)

	prefillPodGroup := &schedulingv1beta1.PodGroup{}
	require.NoError(t, cl.Get(ctx, client.ObjectKey{Namespace: testNamespace, Name: "test-pcs-0-prefill"}, prefillPodGroup))
	require.NotNil(t, prefillPodGroup.Spec.ParentCompositePodGroupName)
	assert.Equal(t, testPodGangName+"-tcg-a", *prefillPodGroup.Spec.ParentCompositePodGroupName)
	require.NotNil(t, prefillPodGroup.Spec.SchedulingPolicy.Gang)
	assert.Equal(t, int32(2), prefillPodGroup.Spec.SchedulingPolicy.Gang.MinCount)

	decodePodGroup := &schedulingv1beta1.PodGroup{}
	require.NoError(t, cl.Get(ctx, client.ObjectKey{Namespace: testNamespace, Name: "test-pcs-0-decode"}, decodePodGroup))
	require.NotNil(t, decodePodGroup.Spec.ParentCompositePodGroupName)
	assert.Equal(t, testPodGangName, *decodePodGroup.Spec.ParentCompositePodGroupName)
}

func TestSyncPodGang_UpdatesMutableMinCount(t *testing.T) {
	podGang := newTestPodGang()
	backend, cl := newGangBackend(t, podGang)
	ctx := context.Background()
	require.NoError(t, backend.SyncPodGang(ctx, podGang))

	podGang.Spec.PodGroups[0].MinReplicas = 3
	require.NoError(t, backend.SyncPodGang(ctx, podGang))

	prefillPodGroup := &schedulingv1beta1.PodGroup{}
	require.NoError(t, cl.Get(ctx, client.ObjectKey{Namespace: testNamespace, Name: "test-pcs-0-prefill"}, prefillPodGroup))
	require.NotNil(t, prefillPodGroup.Spec.SchedulingPolicy.Gang)
	assert.Equal(t, int32(3), prefillPodGroup.Spec.SchedulingPolicy.Gang.MinCount)

	workload := &schedulingv1beta1.Workload{}
	require.NoError(t, cl.Get(ctx, client.ObjectKey{Namespace: testNamespace, Name: testPodGangName}, workload))
	require.Len(t, workload.Spec.CompositePodGroupTemplates, 1)
	var found bool
	for _, leaf := range workload.Spec.CompositePodGroupTemplates[0].PodGroupTemplates {
		if leaf.Name == "test-pcs-0-prefill" {
			found = true
			require.NotNil(t, leaf.SchedulingPolicy.Gang)
			assert.Equal(t, int32(3), leaf.SchedulingPolicy.Gang.MinCount)
		}
	}
	assert.True(t, found, "prefill leaf template must exist in workload")
}

func TestSyncPodGang_RetainsPositiveMinCountOnZeroRelease(t *testing.T) {
	podGang := newTestPodGang()
	backend, cl := newGangBackend(t, podGang)
	ctx := context.Background()
	require.NoError(t, backend.SyncPodGang(ctx, podGang))

	// Grove releases MinReplicas to zero after initial placement; the upstream
	// gang minCount must retain the last positive value.
	podGang.Spec.PodGroups[1].MinReplicas = 0
	require.NoError(t, backend.SyncPodGang(ctx, podGang))

	decodePodGroup := &schedulingv1beta1.PodGroup{}
	require.NoError(t, cl.Get(ctx, client.ObjectKey{Namespace: testNamespace, Name: "test-pcs-0-decode"}, decodePodGroup))
	require.NotNil(t, decodePodGroup.Spec.SchedulingPolicy.Gang)
	assert.Equal(t, int32(1), decodePodGroup.Spec.SchedulingPolicy.Gang.MinCount)
}

func TestSyncPodGang_RebuildsHierarchyOnStructureChange(t *testing.T) {
	podGang := newTestPodGang()
	backend, cl := newGangBackend(t, podGang)
	ctx := context.Background()
	require.NoError(t, backend.SyncPodGang(ctx, podGang))

	// Adding a group changes the immutable template structure and must rebuild
	// the hierarchy.
	podGang.Spec.PodGroups = append(podGang.Spec.PodGroups, groveschedulerv1alpha1.PodGroup{Name: "test-pcs-0-router", MinReplicas: 1})
	require.NoError(t, backend.SyncPodGang(ctx, podGang))

	workload := &schedulingv1beta1.Workload{}
	require.NoError(t, cl.Get(ctx, client.ObjectKey{Namespace: testNamespace, Name: testPodGangName}, workload))
	require.Len(t, workload.Spec.CompositePodGroupTemplates, 1)
	assert.Len(t, workload.Spec.CompositePodGroupTemplates[0].PodGroupTemplates, 3)
	require.NotNil(t, workload.Spec.CompositePodGroupTemplates[0].SchedulingPolicy.Gang)
	assert.Equal(t, int32(3), workload.Spec.CompositePodGroupTemplates[0].SchedulingPolicy.Gang.MinGroupCount)

	routerPodGroup := &schedulingv1beta1.PodGroup{}
	require.NoError(t, cl.Get(ctx, client.ObjectKey{Namespace: testNamespace, Name: "test-pcs-0-router"}, routerPodGroup))
}

// TestValidateWorkloadLimits_Depth verifies the explicit tree-depth guard fails
// closed once a template tree exceeds WorkloadMaxTreeDepth levels, independent
// of how the tree was produced.
func TestValidateWorkloadLimits_Depth(t *testing.T) {
	podGang := newTestPodGang()

	// Build a chain deeper than the allowed maximum: root -> c1 -> ... -> leaf.
	leaf := &workloadbuilder.WorkloadItem{Name: "leaf"}
	node := leaf
	for i := 0; i < schedulingv1beta1.WorkloadMaxTreeDepth; i++ {
		node = &workloadbuilder.WorkloadItem{Name: "node", Children: []*workloadbuilder.WorkloadItem{node}}
	}

	err := validateWorkloadLimits(podGang, node, 1)
	require.Error(t, err)
	assert.ErrorContains(t, err, "deeper than")

	// A chain exactly at the limit passes.
	withinLimit := &workloadbuilder.WorkloadItem{Name: "leaf"}
	for i := 0; i < schedulingv1beta1.WorkloadMaxTreeDepth-1; i++ {
		withinLimit = &workloadbuilder.WorkloadItem{Name: "node", Children: []*workloadbuilder.WorkloadItem{withinLimit}}
	}
	require.NoError(t, validateWorkloadLimits(podGang, withinLimit, 1))
}

// TestValidateWorkloadLimits_ChildWidth verifies the per-list template count
// guard fails closed on a nested composite child, not just the root.
func TestValidateWorkloadLimits_ChildWidth(t *testing.T) {
	podGang := newTestPodGang()

	children := make([]*workloadbuilder.WorkloadItem, 0, schedulingv1beta1.WorkloadMaxPodGroupTemplates+1)
	for i := 0; i < schedulingv1beta1.WorkloadMaxPodGroupTemplates+1; i++ {
		children = append(children, &workloadbuilder.WorkloadItem{Name: string(rune('a' + i))})
	}
	child := &workloadbuilder.WorkloadItem{Name: "tcg-a", Children: children}
	root := &workloadbuilder.WorkloadItem{Name: rootTemplateName, Children: []*workloadbuilder.WorkloadItem{child}}

	err := validateWorkloadLimits(podGang, root, 1)
	require.Error(t, err)
	assert.ErrorContains(t, err, "at most")
}

// TestBuildWorkloadForPodGang_TooManyGroupsForOneList verifies that too many
// topology constraint groups under the root fail closed on the template-count
// limit.
func TestBuildWorkloadForPodGang_TooManyGroupsForOneList(t *testing.T) {
	podGang := newTestPodGang(func(podGang *groveschedulerv1alpha1.PodGang) {
		podGang.Spec.PodGroups = nil
		for i := 0; i < schedulingv1beta1.WorkloadMaxPodGroupTemplates+1; i++ {
			name := "grp" + string(rune('a'+i))
			podGang.Spec.PodGroups = append(podGang.Spec.PodGroups, groveschedulerv1alpha1.PodGroup{Name: name, MinReplicas: 1})
			withTopologyGroup("tcg-"+string(rune('a'+i)), []string{name}, "")(podGang)
		}
	})

	_, err := buildWorkloadForPodGang(podGang, nil)
	require.Error(t, err)
	assert.ErrorContains(t, err, "at most")
}

// TestBuildWorkloadForPodGang_RootRequiredTopology verifies a required topology
// constraint on the PodGang itself lands on the root composite template.
func TestBuildWorkloadForPodGang_RootRequiredTopology(t *testing.T) {
	podGang := newTestPodGang(func(podGang *groveschedulerv1alpha1.PodGang) {
		podGang.Spec.TopologyConstraint = &groveschedulerv1alpha1.TopologyConstraint{
			PackConstraint: &groveschedulerv1alpha1.TopologyPackConstraint{Required: ptr.To("topology.kubernetes.io/zone")},
		}
	})

	workload, err := buildWorkloadForPodGang(podGang, nil)
	require.NoError(t, err)

	root := workload.Spec.CompositePodGroupTemplates[0]
	require.NotNil(t, root.SchedulingConstraints)
	require.Len(t, root.SchedulingConstraints.Topology, 1)
	assert.Equal(t, "topology.kubernetes.io/zone", root.SchedulingConstraints.Topology[0].Key)
}

// TestBuildWorkloadForPodGang_NestedTopologyLevels verifies TAS constraints at
// all three levels (root PodGang, topology group, leaf PodGroup) map to the
// corresponding template level, resolved parent-to-child.
func TestBuildWorkloadForPodGang_NestedTopologyLevels(t *testing.T) {
	podGang := newTestPodGang(
		func(podGang *groveschedulerv1alpha1.PodGang) {
			podGang.Spec.TopologyConstraint = &groveschedulerv1alpha1.TopologyConstraint{
				PackConstraint: &groveschedulerv1alpha1.TopologyPackConstraint{Required: ptr.To("topology.kubernetes.io/zone")},
			}
			podGang.Spec.PodGroups[0].TopologyConstraint = &groveschedulerv1alpha1.TopologyConstraint{
				PackConstraint: &groveschedulerv1alpha1.TopologyPackConstraint{Required: ptr.To("kubernetes.io/hostname")},
			}
		},
		withTopologyGroup("tcg-a", []string{"test-pcs-0-prefill"}, "topology.kubernetes.io/rack"),
	)

	workload, err := buildWorkloadForPodGang(podGang, nil)
	require.NoError(t, err)

	root := workload.Spec.CompositePodGroupTemplates[0]
	require.NotNil(t, root.SchedulingConstraints)
	require.Len(t, root.SchedulingConstraints.Topology, 1)
	assert.Equal(t, "topology.kubernetes.io/zone", root.SchedulingConstraints.Topology[0].Key, "root carries the PodGang constraint")

	require.Len(t, root.CompositePodGroupTemplates, 1)
	group := root.CompositePodGroupTemplates[0]
	require.NotNil(t, group.SchedulingConstraints)
	require.Len(t, group.SchedulingConstraints.Topology, 1)
	assert.Equal(t, "topology.kubernetes.io/rack", group.SchedulingConstraints.Topology[0].Key, "child carries the topology group constraint")

	require.Len(t, group.PodGroupTemplates, 1)
	leaf := group.PodGroupTemplates[0]
	assert.Equal(t, "test-pcs-0-prefill", leaf.Name)
	require.NotNil(t, leaf.SchedulingConstraints)
	require.Len(t, leaf.SchedulingConstraints.Topology, 1)
	assert.Equal(t, "kubernetes.io/hostname", leaf.SchedulingConstraints.Topology[0].Key, "leaf carries the PodGroup constraint")
}

// TestBuildWorkloadForPodGang_PreferredFailsClosedAtEveryLevel verifies a
// preferred topology constraint fails closed whether it is set on the PodGang,
// a topology group, or a PodGroup.
func TestBuildWorkloadForPodGang_PreferredFailsClosedAtEveryLevel(t *testing.T) {
	preferred := func() *groveschedulerv1alpha1.TopologyConstraint {
		return &groveschedulerv1alpha1.TopologyConstraint{
			PackConstraint: &groveschedulerv1alpha1.TopologyPackConstraint{Preferred: ptr.To("kubernetes.io/hostname")},
		}
	}
	tests := []struct {
		name   string
		mutate func(*groveschedulerv1alpha1.PodGang)
	}{
		{
			name: "on PodGang",
			mutate: func(podGang *groveschedulerv1alpha1.PodGang) {
				podGang.Spec.TopologyConstraint = preferred()
			},
		},
		{
			name: "on PodGroup",
			mutate: func(podGang *groveschedulerv1alpha1.PodGang) {
				podGang.Spec.PodGroups[0].TopologyConstraint = preferred()
			},
		},
		{
			name: "on topology group",
			mutate: func(podGang *groveschedulerv1alpha1.PodGang) {
				podGang.Spec.TopologyConstraintGroupConfigs = append(podGang.Spec.TopologyConstraintGroupConfigs,
					groveschedulerv1alpha1.TopologyConstraintGroupConfig{
						Name:               "tcg-a",
						PodGroupNames:      []string{"test-pcs-0-prefill"},
						TopologyConstraint: preferred(),
					})
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := buildWorkloadForPodGang(newTestPodGang(tt.mutate), nil)
			require.Error(t, err)
			assert.ErrorContains(t, err, "preferred topology constraint")
		})
	}
}

// TestSyncPodGang_RootCompositeCarriesTopology verifies the runtime root
// CompositePodGroup materialized from a PodGang with a required topology
// constraint carries that constraint.
func TestSyncPodGang_RootCompositeCarriesTopology(t *testing.T) {
	podGang := newTestPodGang(func(podGang *groveschedulerv1alpha1.PodGang) {
		podGang.Spec.TopologyConstraint = &groveschedulerv1alpha1.TopologyConstraint{
			PackConstraint: &groveschedulerv1alpha1.TopologyPackConstraint{Required: ptr.To("topology.kubernetes.io/zone")},
		}
	})
	backend, cl := newGangBackend(t, podGang)
	ctx := context.Background()
	require.NoError(t, backend.SyncPodGang(ctx, podGang))

	rootComposite := &schedulingv1alpha3.CompositePodGroup{}
	require.NoError(t, cl.Get(ctx, client.ObjectKey{Namespace: testNamespace, Name: testPodGangName}, rootComposite))
	require.NotNil(t, rootComposite.Spec.SchedulingConstraints)
	require.Len(t, rootComposite.Spec.SchedulingConstraints.Topology, 1)
	assert.Equal(t, "topology.kubernetes.io/zone", rootComposite.Spec.SchedulingConstraints.Topology[0].Key)
}

func TestSyncPodGang_NoOpWithoutGangScheduling(t *testing.T) {
	backend, cl := newGangBackend(t)
	backend.gangSchedulingEnabled = false
	ctx := context.Background()

	require.NoError(t, backend.SyncPodGang(ctx, newTestPodGang()))

	workloads := &schedulingv1beta1.WorkloadList{}
	require.NoError(t, cl.List(ctx, workloads))
	assert.Empty(t, workloads.Items)
}

func TestPreparePod_GangScheduling(t *testing.T) {
	backend, _ := newGangBackend(t)

	pod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "test-pod",
			Namespace: testNamespace,
			Labels:    map[string]string{apicommon.LabelPodClique: "test-pcs-0-prefill"},
		},
	}
	require.NoError(t, backend.PreparePod(pod))

	assert.Equal(t, string(configv1alpha1.SchedulerNameKube), pod.Spec.SchedulerName)
	require.NotNil(t, pod.Spec.SchedulingGroup)
	require.NotNil(t, pod.Spec.SchedulingGroup.PodGroupName)
	assert.Equal(t, "test-pcs-0-prefill", *pod.Spec.SchedulingGroup.PodGroupName)
}

func TestPreparePod_GangSchedulingMissingCliqueLabel(t *testing.T) {
	backend, _ := newGangBackend(t)

	pod := &corev1.Pod{ObjectMeta: metav1.ObjectMeta{Name: "test-pod", Namespace: testNamespace}}
	err := backend.PreparePod(pod)
	require.Error(t, err)
	assert.ErrorContains(t, err, apicommon.LabelPodClique)
}

func TestPreparePod_NoGangSchedulingSetsSchedulerNameOnly(t *testing.T) {
	backend, _ := newGangBackend(t)
	backend.gangSchedulingEnabled = false

	pod := &corev1.Pod{ObjectMeta: metav1.ObjectMeta{Name: "test-pod", Namespace: testNamespace}}
	require.NoError(t, backend.PreparePod(pod))

	assert.Equal(t, string(configv1alpha1.SchedulerNameKube), pod.Spec.SchedulerName)
	assert.Nil(t, pod.Spec.SchedulingGroup, "without gang scheduling no PodGroup membership is assigned")
}

// TestBuildWorkloadForPodGang_AllPodGroupsGrouped verifies the hierarchy when
// every PodGroup belongs to a topology constraint group, so the root composite
// has only composite children and no direct leaf templates.
func TestBuildWorkloadForPodGang_AllPodGroupsGrouped(t *testing.T) {
	podGang := newTestPodGang(
		withTopologyGroup("tcg-a", []string{"test-pcs-0-prefill"}, "topology.kubernetes.io/rack"),
		withTopologyGroup("tcg-b", []string{"test-pcs-0-decode"}, "topology.kubernetes.io/block"),
	)

	workload, err := buildWorkloadForPodGang(podGang, nil)
	require.NoError(t, err)

	require.Len(t, workload.Spec.CompositePodGroupTemplates, 1)
	root := workload.Spec.CompositePodGroupTemplates[0]
	require.NotNil(t, root.SchedulingPolicy.Gang)
	assert.Equal(t, int32(2), root.SchedulingPolicy.Gang.MinGroupCount)
	assert.Empty(t, root.PodGroupTemplates, "all leaves live under topology groups")
	require.Len(t, root.CompositePodGroupTemplates, 2)

	byName := map[string]schedulingv1beta1.CompositePodGroupTemplate{}
	for _, group := range root.CompositePodGroupTemplates {
		byName[group.Name] = group
	}
	require.Contains(t, byName, "tcg-a")
	require.Contains(t, byName, "tcg-b")
	require.Len(t, byName["tcg-a"].PodGroupTemplates, 1)
	assert.Equal(t, "test-pcs-0-prefill", byName["tcg-a"].PodGroupTemplates[0].Name)
	require.NotNil(t, byName["tcg-b"].SchedulingConstraints)
	require.Len(t, byName["tcg-b"].SchedulingConstraints.Topology, 1)
	assert.Equal(t, "topology.kubernetes.io/block", byName["tcg-b"].SchedulingConstraints.Topology[0].Key)
}

// TestBuildWorkloadForPodGang_LeafRequiredTopology verifies a required topology
// constraint set directly on a PodGroup lands on its leaf template.
func TestBuildWorkloadForPodGang_LeafRequiredTopology(t *testing.T) {
	podGang := newTestPodGang(func(podGang *groveschedulerv1alpha1.PodGang) {
		podGang.Spec.PodGroups[0].TopologyConstraint = &groveschedulerv1alpha1.TopologyConstraint{
			PackConstraint: &groveschedulerv1alpha1.TopologyPackConstraint{Required: ptr.To("kubernetes.io/hostname")},
		}
	})

	workload, err := buildWorkloadForPodGang(podGang, nil)
	require.NoError(t, err)

	root := workload.Spec.CompositePodGroupTemplates[0]
	var prefill *schedulingv1beta1.PodGroupTemplate
	for i := range root.PodGroupTemplates {
		if root.PodGroupTemplates[i].Name == "test-pcs-0-prefill" {
			prefill = &root.PodGroupTemplates[i]
		}
	}
	require.NotNil(t, prefill)
	require.NotNil(t, prefill.SchedulingConstraints)
	require.Len(t, prefill.SchedulingConstraints.Topology, 1)
	assert.Equal(t, "kubernetes.io/hostname", prefill.SchedulingConstraints.Topology[0].Key)
}

// TestBuildWorkloadForPodGang_PriorityClassPropagates verifies the PodGang
// priority class name is copied onto the leaf templates.
func TestBuildWorkloadForPodGang_PriorityClassPropagates(t *testing.T) {
	podGang := newTestPodGang(func(podGang *groveschedulerv1alpha1.PodGang) {
		podGang.Spec.PriorityClassName = "high-priority"
	})

	workload, err := buildWorkloadForPodGang(podGang, nil)
	require.NoError(t, err)

	root := workload.Spec.CompositePodGroupTemplates[0]
	require.NotEmpty(t, root.PodGroupTemplates)
	for _, leaf := range root.PodGroupTemplates {
		assert.Equal(t, "high-priority", leaf.PriorityClassName, "leaf %q inherits the PodGang priority class", leaf.Name)
	}
}

// TestBuildWorkloadForPodGang_ZeroMinReplicasRetainsFallback verifies that a
// zero MinReplicas resolves to the supplied fallback minCount rather than
// failing closed.
func TestBuildWorkloadForPodGang_ZeroMinReplicasRetainsFallback(t *testing.T) {
	podGang := newTestPodGang(func(podGang *groveschedulerv1alpha1.PodGang) {
		podGang.Spec.PodGroups[1].MinReplicas = 0
	})

	workload, err := buildWorkloadForPodGang(podGang, map[string]int32{"test-pcs-0-decode": 4})
	require.NoError(t, err)

	root := workload.Spec.CompositePodGroupTemplates[0]
	var decode *schedulingv1beta1.PodGroupTemplate
	for i := range root.PodGroupTemplates {
		if root.PodGroupTemplates[i].Name == "test-pcs-0-decode" {
			decode = &root.PodGroupTemplates[i]
		}
	}
	require.NotNil(t, decode)
	require.NotNil(t, decode.SchedulingPolicy.Gang)
	assert.Equal(t, int32(4), decode.SchedulingPolicy.Gang.MinCount)
}

// TestSyncPodGang_Idempotent verifies a second sync with an unchanged PodGang
// does not error and does not alter the persisted Workload.
func TestSyncPodGang_Idempotent(t *testing.T) {
	podGang := newTestPodGang(withTopologyGroup("tcg-a", []string{"test-pcs-0-prefill"}, "topology.kubernetes.io/rack"))
	backend, cl := newGangBackend(t, podGang)
	ctx := context.Background()
	require.NoError(t, backend.SyncPodGang(ctx, podGang))

	before := &schedulingv1beta1.Workload{}
	require.NoError(t, cl.Get(ctx, client.ObjectKey{Namespace: testNamespace, Name: testPodGangName}, before))

	require.NoError(t, backend.SyncPodGang(ctx, podGang))

	after := &schedulingv1beta1.Workload{}
	require.NoError(t, cl.Get(ctx, client.ObjectKey{Namespace: testNamespace, Name: testPodGangName}, after))
	assert.Equal(t, before.ResourceVersion, after.ResourceVersion, "idempotent sync must not rewrite the Workload")
}

// TestSyncPodGang_GeneratedObjectsOwnedByPodGang verifies every generated
// object carries a controller owner reference to the PodGang for garbage
// collection, plus the grove.io/podgang label used for cleanup.
func TestSyncPodGang_GeneratedObjectsOwnedByPodGang(t *testing.T) {
	podGang := newTestPodGang(withTopologyGroup("tcg-a", []string{"test-pcs-0-prefill"}, ""))
	backend, cl := newGangBackend(t, podGang)
	ctx := context.Background()
	require.NoError(t, backend.SyncPodGang(ctx, podGang))

	assertOwnedByPodGang := func(obj client.Object) {
		owners := obj.GetOwnerReferences()
		require.Len(t, owners, 1)
		assert.Equal(t, "PodGang", owners[0].Kind)
		assert.Equal(t, testPodGangName, owners[0].Name)
		require.NotNil(t, owners[0].Controller)
		assert.True(t, *owners[0].Controller)
		assert.Equal(t, testPodGangName, obj.GetLabels()[apicommon.LabelPodGang])
	}

	workload := &schedulingv1beta1.Workload{}
	require.NoError(t, cl.Get(ctx, client.ObjectKey{Namespace: testNamespace, Name: testPodGangName}, workload))
	require.Len(t, workload.OwnerReferences, 1)
	assert.Equal(t, "PodGang", workload.OwnerReferences[0].Kind)

	rootComposite := &schedulingv1alpha3.CompositePodGroup{}
	require.NoError(t, cl.Get(ctx, client.ObjectKey{Namespace: testNamespace, Name: testPodGangName}, rootComposite))
	assertOwnedByPodGang(rootComposite)

	prefill := &schedulingv1beta1.PodGroup{}
	require.NoError(t, cl.Get(ctx, client.ObjectKey{Namespace: testNamespace, Name: "test-pcs-0-prefill"}, prefill))
	assertOwnedByPodGang(prefill)
}

// TestSyncPodGang_NilPodGangErrors verifies a nil PodGang is rejected.
func TestSyncPodGang_NilPodGangErrors(t *testing.T) {
	backend, _ := newGangBackend(t)
	err := backend.SyncPodGang(context.Background(), nil)
	require.Error(t, err)
	assert.ErrorContains(t, err, "nil")
}

// TestSyncPodGang_ShrinkRebuildsHierarchy verifies removing a PodGroup rebuilds
// the immutable hierarchy and deletes the stale runtime PodGroup.
func TestSyncPodGang_ShrinkRebuildsHierarchy(t *testing.T) {
	podGang := newTestPodGang(func(podGang *groveschedulerv1alpha1.PodGang) {
		podGang.Spec.PodGroups = append(podGang.Spec.PodGroups, groveschedulerv1alpha1.PodGroup{Name: "test-pcs-0-router", MinReplicas: 1})
	})
	backend, cl := newGangBackend(t, podGang)
	ctx := context.Background()
	require.NoError(t, backend.SyncPodGang(ctx, podGang))

	router := &schedulingv1beta1.PodGroup{}
	require.NoError(t, cl.Get(ctx, client.ObjectKey{Namespace: testNamespace, Name: "test-pcs-0-router"}, router))

	// Remove the router PodGroup: structure shrinks and must rebuild.
	podGang.Spec.PodGroups = podGang.Spec.PodGroups[:2]
	require.NoError(t, backend.SyncPodGang(ctx, podGang))

	workload := &schedulingv1beta1.Workload{}
	require.NoError(t, cl.Get(ctx, client.ObjectKey{Namespace: testNamespace, Name: testPodGangName}, workload))
	assert.Len(t, workload.Spec.CompositePodGroupTemplates[0].PodGroupTemplates, 2)

	staleRouter := &schedulingv1beta1.PodGroup{}
	err := cl.Get(ctx, client.ObjectKey{Namespace: testNamespace, Name: "test-pcs-0-router"}, staleRouter)
	assert.True(t, apierrors.IsNotFound(err), "stale router PodGroup must be deleted on rebuild, got %v", err)
}
