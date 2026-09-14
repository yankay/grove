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

package podcliquesetreplica

import (
	"context"
	"fmt"
	"testing"

	apicommon "github.com/ai-dynamo/grove/operator/api/common"
	apiconstants "github.com/ai-dynamo/grove/operator/api/common/constants"
	grovecorev1alpha1 "github.com/ai-dynamo/grove/operator/api/core/v1alpha1"
	componentutils "github.com/ai-dynamo/grove/operator/internal/controller/common/component/utils"
	testutils "github.com/ai-dynamo/grove/operator/test/utils"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/utils/ptr"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

func TestGangRecoveryPreservesRuntimeTargetsAcrossRestart(t *testing.T) {
	for _, tc := range []struct {
		template int32
		runtime  int32
	}{
		{template: 0, runtime: 3},
		{template: 3, runtime: 0},
		{template: 2, runtime: 5},
	} {
		t.Run(fmt.Sprintf("template-%d-runtime-%d", tc.template, tc.runtime), func(t *testing.T) {
			ctx := context.Background()
			pcs := testutils.NewPodCliqueSetBuilder("pcs", "default", "pcs-uid").
				WithReplicas(2).
				WithPodCliqueTemplateSpec(testutils.NewPodCliqueTemplateSpecBuilder("worker").WithReplicas(tc.template).Build()).
				Build()
			pclq := recoveryClique(pcs, "pcs-0-worker", tc.runtime)
			other := recoveryClique(pcs, "pcs-1-worker", 2)
			other.Labels[apicommon.LabelPodCliqueSetReplicaIndex] = "1"
			oldA, oldB := recoveryPod(pclq, "old-a", "", true), recoveryPod(pclq, "old-b", "", true)
			cl := testutils.NewTestClientBuilder().WithObjects(pcs, pclq, other, oldA, oldB).
				WithStatusSubresource(&grovecorev1alpha1.PodClique{}).Build()
			r := _resource{client: cl}
			stalePCS := pcs.DeepCopy()
			require.NoError(t, r.startGangRecovery(ctx, pcs, 0))
			require.NoError(t, cl.Get(ctx, client.ObjectKeyFromObject(pcs), pcs))
			recovery, err := componentutils.GetGangRecovery(pcs, 0)
			require.NoError(t, err)
			require.NoError(t, r.advanceGangRecovery(ctx, pcs, 0, recovery))
			require.NoError(t, cl.Delete(ctx, oldA))

			// A new reconciler resumes a partially completed drain using only API state.
			restarted := _resource{client: cl}
			require.NoError(t, restarted.advanceGangRecovery(ctx, pcs, 0, recovery))
			require.NoError(t, cl.Get(ctx, client.ObjectKeyFromObject(pcs), pcs))
			got, err := componentutils.GetGangRecovery(pcs, 0)
			require.NoError(t, err)
			assert.Equal(t, componentutils.GangRecoveryDraining, got.Phase)
			require.NoError(t, cl.Delete(ctx, oldB))
			require.NoError(t, restarted.advanceGangRecovery(ctx, pcs, 0, recovery))
			require.NoError(t, cl.Get(ctx, client.ObjectKeyFromObject(pcs), pcs))
			recovery, err = componentutils.GetGangRecovery(pcs, 0)
			require.NoError(t, err)
			assert.Equal(t, componentutils.GangRecoveryRecreating, recovery.Phase)

			var replacements []*corev1.Pod
			for i := range tc.runtime {
				pod := recoveryPod(pclq, fmt.Sprintf("new-%d", i), recovery.Epoch, false)
				require.NoError(t, cl.Create(ctx, pod))
				replacements = append(replacements, pod)
			}
			if tc.runtime > 0 {
				// Stale healthy child status is not evidence that replacements are Ready.
				require.NoError(t, restarted.advanceGangRecovery(ctx, pcs, 0, recovery))
				require.NoError(t, cl.Get(ctx, client.ObjectKeyFromObject(pcs), pcs))
				got, err = componentutils.GetGangRecovery(pcs, 0)
				require.NoError(t, err)
				assert.Equal(t, componentutils.GangRecoveryRecreating, got.Phase)
				for _, pod := range replacements {
					pod.Status.Conditions[0].Status = corev1.ConditionTrue
					require.NoError(t, cl.Status().Update(ctx, pod))
				}
			}
			require.NoError(t, restarted.advanceGangRecovery(ctx, pcs, 0, recovery))
			require.NoError(t, cl.Get(ctx, client.ObjectKeyFromObject(pcs), pcs))
			got, err = componentutils.GetGangRecovery(pcs, 0)
			require.NoError(t, err)
			assert.Equal(t, componentutils.GangRecoveryComplete, got.Phase)
			assert.Equal(t, recovery.Epoch, got.Epoch)
			// An old recovery task cannot replay a completed drain.
			require.NoError(t, r.startGangRecovery(ctx, stalePCS, 0))
			require.NoError(t, cl.Get(ctx, client.ObjectKeyFromObject(pcs), pcs))
			replayed, err := componentutils.GetGangRecovery(pcs, 0)
			require.NoError(t, err)
			assert.Equal(t, got, replayed)
			for _, want := range []*grovecorev1alpha1.PodClique{pclq, other} {
				actual := &grovecorev1alpha1.PodClique{}
				require.NoError(t, cl.Get(ctx, client.ObjectKeyFromObject(want), actual))
				assert.Equal(t, want.UID, actual.UID)
				assert.Equal(t, want.Spec.Replicas, actual.Spec.Replicas)
			}
			untouched, err := componentutils.GetGangRecovery(pcs, 1)
			require.NoError(t, err)
			assert.Empty(t, untouched)
		})
	}
}

func TestGangRecoveryHonorsScaleAcceptedDuringDrain(t *testing.T) {
	ctx := context.Background()
	pcs := testutils.NewPodCliqueSetBuilder("pcs", "default", "pcs-uid").WithReplicas(1).
		WithPodCliqueTemplateSpec(testutils.NewPodCliqueTemplateSpecBuilder("worker").WithReplicas(2).Build()).Build()
	pclq := recoveryClique(pcs, "pcs-0-worker", 4)
	cl := testutils.NewTestClientBuilder().WithObjects(pcs, pclq).Build()
	r := _resource{client: cl}
	require.NoError(t, r.startGangRecovery(ctx, pcs, 0))
	require.NoError(t, cl.Get(ctx, client.ObjectKeyFromObject(pcs), pcs))
	draining, err := componentutils.GetGangRecovery(pcs, 0)
	require.NoError(t, err)
	require.NoError(t, cl.Get(ctx, client.ObjectKeyFromObject(pclq), pclq))
	pclq.Spec.Replicas = 0
	require.NoError(t, cl.Update(ctx, pclq))
	require.NoError(t, r.advanceGangRecovery(ctx, pcs, 0, draining))
	require.NoError(t, cl.Get(ctx, client.ObjectKeyFromObject(pcs), pcs))
	recreating, err := componentutils.GetGangRecovery(pcs, 0)
	require.NoError(t, err)
	require.NoError(t, r.advanceGangRecovery(ctx, pcs, 0, recreating))
	require.NoError(t, cl.Get(ctx, client.ObjectKeyFromObject(pcs), pcs))
	finished, err := componentutils.GetGangRecovery(pcs, 0)
	require.NoError(t, err)
	assert.Equal(t, componentutils.GangRecoveryComplete, finished.Phase)
	require.NoError(t, cl.Get(ctx, client.ObjectKeyFromObject(pclq), pclq))
	assert.Zero(t, pclq.Spec.Replicas)
	assert.Equal(t, types.UID("pcs-0-worker-uid"), pclq.UID)
}

func TestRecoveryAvailabilityRequiresExistingOwnedTargets(t *testing.T) {
	ctx := context.Background()
	pcs := testutils.NewPodCliqueSetBuilder("pcs", "default", "pcs-uid").WithReplicas(1).
		WithPodCliqueTemplateSpec(testutils.NewPodCliqueTemplateSpecBuilder("worker").WithReplicas(0).Build()).Build()
	pclq := recoveryClique(pcs, "pcs-0-worker", 0)
	pclq.OwnerReferences[0].UID = "another-pcs"
	cl := testutils.NewTestClientBuilder().WithObjects(pcs, pclq, recoveryPod(pclq, "foreign", "", true)).Build()
	r := _resource{client: cl}
	snapshot, err := r.getRecoverySnapshot(ctx, pcs, 0)
	require.NoError(t, err)
	assert.Empty(t, snapshot.pclqs)
	assert.Empty(t, snapshot.pods)
	assert.False(t, snapshot.available(pcs, 0, "epoch"), "missing or foreign clique is not an idle target")
}

func TestPruneGangRecoveriesOnScaleIn(t *testing.T) {
	ctx := context.Background()
	pcs := testutils.NewPodCliqueSetBuilder("pcs", "default", "pcs-uid").WithReplicas(1).Build()
	recovery := componentutils.GangRecovery{Epoch: "epoch", Phase: componentutils.GangRecoveryDraining}
	require.NoError(t, componentutils.SetGangRecovery(pcs, 0, recovery))
	require.NoError(t, componentutils.SetGangRecovery(pcs, 1, recovery))
	pcs.Annotations["example.com/keep"] = "value"
	cl := testutils.NewTestClientBuilder().WithObjects(pcs).Build()
	require.NoError(t, cl.Get(ctx, client.ObjectKeyFromObject(pcs), pcs))
	r := _resource{client: cl}
	require.NoError(t, r.pruneGangRecoveries(ctx, pcs))
	require.NoError(t, cl.Get(ctx, client.ObjectKeyFromObject(pcs), pcs))
	kept, err := componentutils.GetGangRecovery(pcs, 0)
	require.NoError(t, err)
	assert.Equal(t, recovery, kept)
	removed, err := componentutils.GetGangRecovery(pcs, 1)
	require.NoError(t, err)
	assert.Empty(t, removed)
	assert.Equal(t, "value", pcs.Annotations["example.com/keep"])
	beforeVersion := pcs.ResourceVersion
	require.NoError(t, r.pruneGangRecoveries(ctx, pcs))
	assert.Equal(t, beforeVersion, pcs.ResourceVersion, "stable records must not churn")
}

func TestRecoveryScalingGroupAvailability(t *testing.T) {
	for _, tc := range []struct {
		name      string
		replicas  int32
		ready     int32
		observed  int32
		foreign   bool
		missing   bool
		wantReady bool
	}{
		{name: "idle target needs no members", replicas: 0, wantReady: true},
		{name: "missing target is not idle", missing: true},
		{name: "foreign target is not idle", foreign: true},
		{name: "one ready replica is below minimum", replicas: 3, ready: 1, observed: 3},
		{name: "healthy status cannot replace ready pods", replicas: 3, observed: 3},
		{name: "wait for group status to catch up", replicas: 3, ready: 2},
		{name: "two ready replicas satisfy minimum", replicas: 3, ready: 2, observed: 2, wantReady: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			pcs := testutils.NewPodCliqueSetBuilder("pcs", "default", "pcs-uid").WithReplicas(1).
				WithPodCliqueParameters("worker", 1, nil).Build()
			pcs.Spec.Template.PodCliqueScalingGroupConfigs = []grovecorev1alpha1.PodCliqueScalingGroupConfig{{
				Name: "group", Replicas: ptr.To(int32(2)), MinAvailable: ptr.To(int32(2)), CliqueNames: []string{"worker"},
			}}
			group := &grovecorev1alpha1.PodCliqueScalingGroup{
				ObjectMeta: metav1.ObjectMeta{
					Name: "pcs-0-group", Namespace: pcs.Namespace, UID: "group-uid",
					Labels: map[string]string{apicommon.LabelPartOfKey: pcs.Name, apicommon.LabelPodCliqueSetReplicaIndex: "0"},
					OwnerReferences: []metav1.OwnerReference{{
						APIVersion: grovecorev1alpha1.SchemeGroupVersion.String(), Kind: "PodCliqueSet",
						Name: pcs.Name, UID: pcs.UID, Controller: ptr.To(true),
					}},
				},
				Spec: grovecorev1alpha1.PodCliqueScalingGroupSpec{
					Replicas: tc.replicas, MinAvailable: ptr.To(int32(2)), CliqueNames: []string{"worker"},
				},
				Status: grovecorev1alpha1.PodCliqueScalingGroupStatus{
					AvailableReplicas: tc.observed,
					Conditions: []metav1.Condition{{
						Type: apiconstants.ConditionTypeMinAvailableBreached, Status: metav1.ConditionFalse,
					}},
				},
			}
			if tc.foreign {
				group.OwnerReferences[0].UID = "old-pcs-uid"
			}
			objects := []client.Object{pcs}
			if !tc.missing {
				objects = append(objects, group)
			}
			for i := range tc.replicas {
				clique := recoveryClique(pcs, fmt.Sprintf("%s-%d-worker", group.Name, i), 1)
				clique.OwnerReferences[0].Kind, clique.OwnerReferences[0].Name, clique.OwnerReferences[0].UID =
					"PodCliqueScalingGroup", group.Name, group.UID
				objects = append(objects, clique, recoveryPod(clique, fmt.Sprintf("pod-%d", i), "epoch", i < tc.ready))
			}
			r := _resource{client: testutils.NewTestClientBuilder().WithObjects(objects...).Build()}
			snapshot, err := r.getRecoverySnapshot(context.Background(), pcs, 0)
			require.NoError(t, err)
			assert.Equal(t, tc.wantReady, snapshot.available(pcs, 0, "epoch"))
		})
	}
}

func TestPatchGangRecoveryFencesStaleRequests(t *testing.T) {
	for _, tc := range []struct {
		name   string
		mutate func(*grovecorev1alpha1.PodCliqueSet)
	}{
		{name: "recreated PCS", mutate: func(pcs *grovecorev1alpha1.PodCliqueSet) { pcs.UID = "new-uid" }},
		{name: "scaled in replica", mutate: func(pcs *grovecorev1alpha1.PodCliqueSet) { pcs.Spec.Replicas = 0 }},
		{name: "new epoch", mutate: func(pcs *grovecorev1alpha1.PodCliqueSet) {
			require.NoError(t, componentutils.SetGangRecovery(pcs, 0, componentutils.GangRecovery{
				Epoch: "new", Phase: componentutils.GangRecoveryDraining,
			}))
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			pcs := testutils.NewPodCliqueSetBuilder("pcs", "default", "pcs-uid").WithReplicas(1).Build()
			old := componentutils.GangRecovery{Epoch: "old", Phase: componentutils.GangRecoveryDraining}
			require.NoError(t, componentutils.SetGangRecovery(pcs, 0, old))
			current := pcs.DeepCopy()
			tc.mutate(current)
			cl := testutils.NewTestClientBuilder().WithObjects(current).Build()
			r := _resource{client: cl}
			require.NoError(t, cl.Get(context.Background(), client.ObjectKeyFromObject(current), current))
			before := current.DeepCopy()
			require.NoError(t, r.patchGangRecovery(context.Background(), pcs, 0, old, componentutils.GangRecovery{
				Epoch: "old", Phase: componentutils.GangRecoveryRecreating,
			}))
			require.NoError(t, cl.Get(context.Background(), client.ObjectKeyFromObject(current), current))
			assert.Equal(t, before.ResourceVersion, current.ResourceVersion)
			assert.Equal(t, before.Annotations, current.Annotations)
		})
	}
}

func recoveryClique(pcs *grovecorev1alpha1.PodCliqueSet, name string, replicas int32) *grovecorev1alpha1.PodClique {
	return &grovecorev1alpha1.PodClique{
		ObjectMeta: metav1.ObjectMeta{
			Name: name, Namespace: pcs.Namespace, UID: types.UID(name + "-uid"),
			Labels: map[string]string{apicommon.LabelPartOfKey: pcs.Name, apicommon.LabelPodCliqueSetReplicaIndex: "0"},
			OwnerReferences: []metav1.OwnerReference{{
				APIVersion: grovecorev1alpha1.SchemeGroupVersion.String(), Kind: "PodCliqueSet",
				Name: pcs.Name, UID: pcs.UID, Controller: ptr.To(true),
			}},
		},
		Spec: grovecorev1alpha1.PodCliqueSpec{Replicas: replicas, MinAvailable: ptr.To(int32(1))},
		Status: grovecorev1alpha1.PodCliqueStatus{
			ReadyReplicas: replicas,
			Conditions:    []metav1.Condition{{Type: apiconstants.ConditionTypeMinAvailableBreached, Status: metav1.ConditionFalse}},
		},
	}
}

func recoveryPod(pclq *grovecorev1alpha1.PodClique, name, epoch string, ready bool) *corev1.Pod {
	status := corev1.ConditionFalse
	if ready {
		status = corev1.ConditionTrue
	}
	return &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name: name, Namespace: pclq.Namespace, UID: types.UID(name + "-uid"),
			Labels: pclq.Labels, Annotations: map[string]string{componentutils.AnnotationPodRecoveryEpoch: epoch},
			OwnerReferences: []metav1.OwnerReference{{
				APIVersion: grovecorev1alpha1.SchemeGroupVersion.String(), Kind: "PodClique",
				Name: pclq.Name, UID: pclq.UID, Controller: ptr.To(true),
			}},
		},
		Status: corev1.PodStatus{Conditions: []corev1.PodCondition{{Type: corev1.PodReady, Status: status}}},
	}
}
