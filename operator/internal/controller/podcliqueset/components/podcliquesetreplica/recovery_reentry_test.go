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
	"testing"
	"time"

	apicommon "github.com/ai-dynamo/grove/operator/api/common"
	apiconstants "github.com/ai-dynamo/grove/operator/api/common/constants"
	grovecorev1alpha1 "github.com/ai-dynamo/grove/operator/api/core/v1alpha1"
	componentutils "github.com/ai-dynamo/grove/operator/internal/utils/component"
	testutils "github.com/ai-dynamo/grove/operator/test/utils"

	"github.com/go-logr/logr"
	"github.com/stretchr/testify/require"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/tools/record"
	"k8s.io/utils/ptr"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

func TestRecoveryReentryIgnoresStaleSiblingState(t *testing.T) {
	ctx := context.Background()
	pcs := testutils.NewPodCliqueSetBuilder("pcs", "default", "pcs-uid").WithReplicas(1).
		WithTerminationDelay(time.Minute).
		WithScalingGroupConfig("sg-a", []string{"a"}, 1, 1).
		WithScalingGroupConfig("sg-b", []string{"b"}, 1, 1).Build()
	previous := componentutils.GangRecovery{Epoch: "previous", Phase: componentutils.GangRecoveryComplete}
	require.NoError(t, componentutils.SetGangRecovery(pcs, 0, previous))
	objects := []client.Object{pcs}
	var groups []*grovecorev1alpha1.PodCliqueScalingGroup
	for _, name := range []string{"a", "b"} {
		group := &grovecorev1alpha1.PodCliqueScalingGroup{
			ObjectMeta: metav1.ObjectMeta{
				Name: "pcs-0-sg-" + name, Namespace: pcs.Namespace, UID: types.UID("group-" + name), Generation: 1,
				Labels:          apicommon.GetDefaultLabelsForPodCliqueSetManagedResources(pcs.Name),
				OwnerReferences: []metav1.OwnerReference{*metav1.NewControllerRef(pcs, grovecorev1alpha1.SchemeGroupVersion.WithKind("PodCliqueSet"))},
			},
			Spec: grovecorev1alpha1.PodCliqueScalingGroupSpec{Replicas: 2, MinAvailable: ptr.To(int32(1)), CliqueNames: []string{name}},
			Status: grovecorev1alpha1.PodCliqueScalingGroupStatus{Conditions: []metav1.Condition{{
				Type: apiconstants.ConditionTypeMinAvailableBreached, Status: metav1.ConditionTrue,
				Reason: apiconstants.ConditionReasonInsufficientAvailablePCSGReplicas, ObservedGeneration: 1,
				LastTransitionTime: metav1.NewTime(time.Now().Add(-time.Hour)),
			}}},
		}
		group.Labels[apicommon.LabelPodCliqueSetReplicaIndex] = "0"
		if name == "b" {
			// The sibling is still unarmed after its own restart. Its legacy
			// in-progress flag must not suppress a new regression in sg-a.
			group.Status.Conditions[0].Reason = apiconstants.ConditionReasonInitialScheduling
			group.Status.Conditions = append(group.Status.Conditions, metav1.Condition{
				Type: "GangTerminationInProgress", Status: metav1.ConditionTrue,
			})
		}
		groups = append(groups, group)
		objects = append(objects, group)
		clique := recoveryClique(pcs, group.Name+"-0-"+name, 1)
		clique.OwnerReferences = []metav1.OwnerReference{*metav1.NewControllerRef(group, grovecorev1alpha1.SchemeGroupVersion.WithKind("PodCliqueScalingGroup"))}
		objects = append(objects, clique, recoveryPod(clique, "old-"+name, previous.Epoch, true))
	}
	cl := testutils.NewTestClientBuilder().WithObjects(objects...).Build()
	for _, group := range groups {
		require.NoError(t, cl.Get(ctx, client.ObjectKeyFromObject(group), group))
	}
	r := _resource{client: cl, eventRecorder: record.NewFakeRecorder(10)}
	work, err := r.getPCSReplicaDeletionWork(ctx, logr.Discard(), pcs)
	require.NoError(t, err)
	require.Len(t, work.deletionTasks, 1)
	require.Equal(t, []int{0}, work.pcsIndicesToTerminate)
	require.NoError(t, work.deletionTasks[0].Fn(ctx))
	require.NoError(t, cl.Get(ctx, client.ObjectKeyFromObject(pcs), pcs))
	current, err := componentutils.GetGangRecovery(pcs, 0)
	require.NoError(t, err)
	require.Equal(t, componentutils.GangRecoveryDraining, current.Phase)
	require.NotEmpty(t, current.Epoch)
	require.NotEqual(t, previous.Epoch, current.Epoch)

	restarted := _resource{client: cl, eventRecorder: record.NewFakeRecorder(10)}
	work, err = restarted.getPCSReplicaDeletionWork(ctx, logr.Discard(), pcs)
	require.NoError(t, err)
	require.Empty(t, work.deletionTasks, "an active drain must not create another recovery episode")
	require.True(t, work.shouldRequeue())
	require.NoError(t, cl.Get(ctx, client.ObjectKeyFromObject(pcs), pcs))
	repeated, err := componentutils.GetGangRecovery(pcs, 0)
	require.NoError(t, err)
	require.Equal(t, current, repeated, "old-epoch Pods must still hold the drain barrier")
	for _, original := range groups {
		got := &grovecorev1alpha1.PodCliqueScalingGroup{}
		require.NoError(t, cl.Get(ctx, client.ObjectKeyFromObject(original), got))
		require.Equal(t, original.UID, got.UID)
		require.Equal(t, original.Spec.Replicas, got.Spec.Replicas)
		require.Equal(t, original.Status.Conditions, got.Status.Conditions)
	}
}
