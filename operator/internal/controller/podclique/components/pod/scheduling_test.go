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

package pod

import (
	"context"
	"errors"
	"fmt"
	"testing"

	apicommon "github.com/ai-dynamo/grove/operator/api/common"
	grovecorev1alpha1 "github.com/ai-dynamo/grove/operator/api/core/v1alpha1"
	groveerr "github.com/ai-dynamo/grove/operator/internal/errors"
	"github.com/ai-dynamo/grove/operator/internal/scheduler"
	testutils "github.com/ai-dynamo/grove/operator/test/utils"

	"github.com/go-logr/logr"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/utils/ptr"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

func TestPCSGMinimumScheduled(t *testing.T) {
	tests := []struct {
		name   string
		mutate func(*syncSnapshot, *grovecorev1alpha1.PodCliqueScalingGroup, []*grovecorev1alpha1.PodClique, []*corev1.Pod)
		want   bool
	}{
		{name: "exact minimum scheduled without waiting for Ready", want: true},
		{
			name: "extra scheduled replicas cannot replace minimum replica zero",
			mutate: func(_ *syncSnapshot, _ *grovecorev1alpha1.PodCliqueScalingGroup, _ []*grovecorev1alpha1.PodClique, pods []*corev1.Pod) {
				pods[0].Status.Conditions = nil
			},
		},
		{
			name: "every clique in every minimum replica must meet its floor",
			mutate: func(_ *syncSnapshot, _ *grovecorev1alpha1.PodCliqueScalingGroup, _ []*grovecorev1alpha1.PodClique, pods []*corev1.Pod) {
				pods[5].Status.Conditions = nil
			},
		},
		{
			name: "terminating pods do not prove quorum",
			mutate: func(_ *syncSnapshot, _ *grovecorev1alpha1.PodCliqueScalingGroup, _ []*grovecorev1alpha1.PodClique, pods []*corev1.Pod) {
				pods[0].DeletionTimestamp = ptr.To(metav1.Now())
				pods[0].Finalizers = []string{"test.grove.io/hold"}
			},
		},
		{
			name: "completed pods do not prove quorum",
			mutate: func(_ *syncSnapshot, _ *grovecorev1alpha1.PodCliqueScalingGroup, _ []*grovecorev1alpha1.PodClique, pods []*corev1.Pod) {
				pods[0].Status.Phase = corev1.PodSucceeded
			},
		},
		{
			name: "pods belonging to an old PodClique UID do not count",
			mutate: func(_ *syncSnapshot, _ *grovecorev1alpha1.PodCliqueScalingGroup, _ []*grovecorev1alpha1.PodClique, pods []*corev1.Pod) {
				pods[0].OwnerReferences[0].UID = "old-pclq"
			},
		},
		{
			name: "scheduled pods at a stale epoch do not count",
			mutate: func(_ *syncSnapshot, _ *grovecorev1alpha1.PodCliqueScalingGroup, _ []*grovecorev1alpha1.PodClique, pods []*corev1.Pod) {
				pods[0].Labels[apicommon.LabelPodGang] = "old-gang"
			},
		},
		{
			name: "stale PodClique status does not prove scheduling",
			mutate: func(_ *syncSnapshot, _ *grovecorev1alpha1.PodCliqueScalingGroup, pclqs []*grovecorev1alpha1.PodClique, pods []*corev1.Pod) {
				pclqs[0].Status.ScheduledReplicas = 99
				pods[0].Status.Conditions = nil
			},
		},
		{
			name: "stale PCSG ownership does not prove quorum",
			mutate: func(_ *syncSnapshot, pcsg *grovecorev1alpha1.PodCliqueScalingGroup, _ []*grovecorev1alpha1.PodClique, _ []*corev1.Pod) {
				pcsg.OwnerReferences[0].UID = "old-pcs"
			},
		},
		{
			name: "idle PCSG cannot release lingering scale-out pods",
			mutate: func(_ *syncSnapshot, pcsg *grovecorev1alpha1.PodCliqueScalingGroup, _ []*grovecorev1alpha1.PodClique, _ []*corev1.Pod) {
				pcsg.Spec.Replicas = 0
			},
		},
		{
			name: "idle child cannot satisfy the minimum",
			mutate: func(_ *syncSnapshot, _ *grovecorev1alpha1.PodCliqueScalingGroup, pclqs []*grovecorev1alpha1.PodClique, _ []*corev1.Pod) {
				pclqs[0].Spec.Replicas = ptr.To[int32](0)
			},
		},
		{
			name: "unplaced scale-out waits for the map",
			mutate: func(ss *syncSnapshot, _ *grovecorev1alpha1.PodCliqueScalingGroup, _ []*grovecorev1alpha1.PodClique, _ []*corev1.Pod) {
				ss.pgm.Spec.Entries[1].PCSGReplicaIndices = nil
			},
		},
		{
			name: "minimum replica does not wait on itself",
			mutate: func(ss *syncSnapshot, _ *grovecorev1alpha1.PodCliqueScalingGroup, pclqs []*grovecorev1alpha1.PodClique, pods []*corev1.Pod) {
				ss.pclq = pclqs[0]
				pods[0].Status.Conditions = nil
			},
			want: true,
		},
		{
			name: "coherent tail retains epoch ordering without quorum cycle",
			mutate: func(ss *syncSnapshot, _ *grovecorev1alpha1.PodCliqueScalingGroup, _ []*grovecorev1alpha1.PodClique, pods []*corev1.Pod) {
				ss.pgm.Spec.Entries[1].Role = grovecorev1alpha1.PodGangEntryRoleTail
				pods[0].Status.Conditions = nil
			},
			want: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ss, pcsg, pclqs, pods := minimumSchedulingFixture()
			if tt.mutate != nil {
				tt.mutate(ss, pcsg, pclqs, pods)
			}
			objects := []client.Object{ss.pcs, pcsg}
			for _, pclq := range pclqs {
				objects = append(objects, pclq)
			}
			for _, pod := range pods {
				objects = append(objects, pod)
			}
			r := _resource{client: testutils.SetupFakeClient(objects...)}
			scheduled, err := r.isPCSGMinimumScheduled(context.Background(), ss)
			require.NoError(t, err)
			assert.Equal(t, tt.want, scheduled)
		})
	}
}

func TestSchedulingGateBackendHandoff(t *testing.T) {
	for _, synced := range []bool{false, true} {
		t.Run(fmt.Sprintf("native policy synced=%t", synced), func(t *testing.T) {
			ctx := context.Background()
			gangName := podGangNameForEpoch(testAnchor0Epoch)
			pod := gatedPod("pod-a", gangName)
			gang := testutils.NewPodGangBuilder(gangName, testNamespace).
				WithLabels(map[string]string{apicommon.LabelEpoch: testAnchor0Epoch}).
				WithLastScheduled().
				WithPodGroupPods(testCliqueName, pod.Name).Build()
			cl := testutils.NewTestClientBuilder().WithObjects(pod, gang).Build()
			backend := &testutils.FakePodGangResourceBackend{
				Backend: testutils.NewFakeSchedulerBackend("native"), Synced: synced,
			}
			r := _resource{client: cl, schedRegistry: &testutils.FakeSchedulerRegistry{
				Backends: map[string]scheduler.Backend{"native": backend}, DefaultBackend: "native",
			}}
			ss := gateRemovalSnapshot([]*corev1.Pod{pod}, twoEpochPGM())
			skipped, err := r.checkAndRemovePodSchedulingGates(ctx, logr.Discard(), ss)
			require.NoError(t, err)
			assert.Len(t, skipped, map[bool]int{true: 0, false: 1}[synced])
			got := &corev1.Pod{}
			require.NoError(t, cl.Get(ctx, client.ObjectKeyFromObject(pod), got))
			assert.Equal(t, !synced, hasPodGangSchedulingGate(got))

			backend.Synced = true
			ss.existingPCLQPods = []*corev1.Pod{got}
			_, err = r.checkAndRemovePodSchedulingGates(ctx, logr.Discard(), ss)
			require.NoError(t, err)
			require.NoError(t, cl.Get(ctx, client.ObjectKeyFromObject(pod), got))
			require.False(t, hasPodGangSchedulingGate(got))

			// Regression after admission must not add a gate or block an already released pod.
			backend.Err = errors.New("backend unavailable")
			ss.existingPCLQPods = []*corev1.Pod{got}
			skipped, err = r.checkAndRemovePodSchedulingGates(ctx, logr.Discard(), ss)
			require.NoError(t, err)
			assert.Empty(t, skipped)
		})
	}
}

func TestSchedulingGateBackendFailure(t *testing.T) {
	backendErr := errors.New("native API unavailable")
	for _, tt := range []struct {
		name     string
		registry scheduler.Registry
		cause    error
	}{
		{name: "registry missing"},
		{name: "scheduler unresolved", registry: &testutils.FakeSchedulerRegistry{}},
		{
			name: "native policy lookup failed",
			registry: &testutils.FakeSchedulerRegistry{Backends: map[string]scheduler.Backend{
				"native": &testutils.FakePodGangResourceBackend{
					Backend: testutils.NewFakeSchedulerBackend("native"), Err: backendErr,
				},
			}},
			cause: backendErr,
		},
	} {
		t.Run(tt.name, func(t *testing.T) {
			ctx := context.Background()
			gangName := podGangNameForEpoch(testAnchor0Epoch)
			pod := gatedPod("pod-a", gangName)
			gang := testutils.NewPodGangBuilder(gangName, testNamespace).
				WithSchedulerName("native").
				WithLabels(map[string]string{apicommon.LabelEpoch: testAnchor0Epoch}).
				WithLastScheduled().WithPodGroupPods(testCliqueName, pod.Name).Build()
			cl := testutils.NewTestClientBuilder().WithObjects(pod, gang).Build()
			r := _resource{client: cl, schedRegistry: tt.registry}
			_, err := r.checkAndRemovePodSchedulingGates(ctx, logr.Discard(),
				gateRemovalSnapshot([]*corev1.Pod{pod}, twoEpochPGM()))
			var categorized *groveerr.GroveError
			require.ErrorAs(t, err, &categorized)
			assert.Equal(t, errCodeGetPodGang, categorized.Code)
			assert.Equal(t, tt.cause, categorized.Cause)
			got := &corev1.Pod{}
			require.NoError(t, cl.Get(ctx, client.ObjectKeyFromObject(pod), got))
			assert.True(t, hasPodGangSchedulingGate(got))
		})
	}
}

func minimumSchedulingFixture() (*syncSnapshot, *grovecorev1alpha1.PodCliqueScalingGroup, []*grovecorev1alpha1.PodClique, []*corev1.Pod) {
	pcs := testutils.NewPodCliqueSetBuilder("pcs", "default", "pcs-uid").Build()
	pcs.Spec.Template.PodCliqueScalingGroupConfigs = []grovecorev1alpha1.PodCliqueScalingGroupConfig{{
		Name: "sg", MinAvailable: ptr.To(int32(2)), CliqueNames: []string{"leader", "worker"},
	}}
	pcsg := &grovecorev1alpha1.PodCliqueScalingGroup{
		ObjectMeta: metav1.ObjectMeta{Name: "pcs-0-sg", Namespace: "default", UID: "pcsg-uid",
			OwnerReferences: []metav1.OwnerReference{*metav1.NewControllerRef(pcs, grovecorev1alpha1.SchemeGroupVersion.WithKind("PodCliqueSet"))}},
		Spec: grovecorev1alpha1.PodCliqueScalingGroupSpec{
			Replicas: 4, MinAvailable: ptr.To(int32(2)), CliqueNames: []string{"leader", "worker"},
		},
	}
	pgm := testutils.NewPodGangMapBuilder(pcs.Name, pcs.Namespace, pcs.UID, 0).
		WithEntries(testutils.NewAnchorEntry("gen", "anchor", 0, "sg", 0, 1),
			testutils.NewScaleOutEntry("gen", "scale", "sg", 2, 3)).Build()
	var pclqs []*grovecorev1alpha1.PodClique
	var pods []*corev1.Pod
	for replica := range 4 {
		for cliqueIndex, clique := range pcsg.Spec.CliqueNames {
			name := apicommon.GeneratePodCliqueName(apicommon.ResourceNameReplica{Name: pcsg.Name, Replica: replica}, clique)
			pclq := testutils.NewPCSGPodCliqueBuilder(name, pcs.Namespace, pcs.Name, pcsg.Name, 0, replica).
				WithOwnerReference("PodCliqueScalingGroup", pcsg.Name, pcsg.UID).Build()
			pclq.UID = types.UID(name)
			pclq.Spec.MinAvailable = ptr.To(int32(cliqueIndex + 1))
			pclq.Spec.Replicas = ptr.To[int32](*pclq.Spec.MinAvailable)
			pclqs = append(pclqs, pclq)
			gangName := apicommon.GenerateAnchorPodGangName(apicommon.ResourceNameReplica{Name: pcs.Name, Replica: 0}, "anchor")
			if replica >= 2 {
				gangName = apicommon.GenerateNonAnchorPodGangName(apicommon.ResourceNameReplica{Name: pcs.Name, Replica: 0}, "scale", "sg", int32(replica))
			}
			for podIndex := range ptr.Deref(pclq.Spec.Replicas, 1) {
				pods = append(pods, &corev1.Pod{
					ObjectMeta: metav1.ObjectMeta{
						Name: fmt.Sprintf("%s-%d", name, podIndex), Namespace: pcs.Namespace,
						Labels:          map[string]string{apicommon.LabelPodGang: gangName},
						OwnerReferences: []metav1.OwnerReference{*metav1.NewControllerRef(pclq, grovecorev1alpha1.SchemeGroupVersion.WithKind("PodClique"))},
					},
					Status: corev1.PodStatus{Phase: corev1.PodRunning, Conditions: []corev1.PodCondition{{Type: corev1.PodScheduled, Status: corev1.ConditionTrue}}},
				})
			}
		}
	}
	return &syncSnapshot{pcs: pcs, pclq: pclqs[4], cliqueName: "leader", pgm: pgm}, pcsg, pclqs, pods
}
