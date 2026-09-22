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

package podclique

import (
	"testing"
	"time"

	"github.com/ai-dynamo/grove/operator/api/common/constants"
	grovecorev1alpha1 "github.com/ai-dynamo/grove/operator/api/core/v1alpha1"
	componentutils "github.com/ai-dynamo/grove/operator/internal/utils/component"

	"github.com/stretchr/testify/require"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/utils/ptr"
)

func TestPositiveScalePreservesBreachAndTerminationDelay(t *testing.T) {
	pclq := &grovecorev1alpha1.PodClique{
		ObjectMeta: metav1.ObjectMeta{Name: "worker", Generation: 1},
		Spec:       grovecorev1alpha1.PodCliqueSpec{Replicas: ptr.To[int32](3), MinAvailable: ptr.To(int32(2))},
		Status:     grovecorev1alpha1.PodCliqueStatus{ReadyReplicas: 3, ScheduledReplicas: 3},
	}
	mutateMinAvailableBreachedCondition(pclq, 0, 0)
	pclq.Status.ReadyReplicas, pclq.Status.ScheduledReplicas = 1, 1
	mutateMinAvailableBreachedCondition(pclq, 0, 0)
	breachedAt := metav1.NewTime(time.Unix(100, 0))
	meta.FindStatusCondition(pclq.Status.Conditions, constants.ConditionTypeMinAvailableBreached).LastTransitionTime = breachedAt

	for _, replicas := range []int32{4, 2} {
		pclq.Spec.Replicas = ptr.To[int32](replicas)
		pclq.Generation++
		candidates, _ := componentutils.GetMinAvailableBreachedPCLQInfo(
			[]grovecorev1alpha1.PodClique{*pclq}, time.Minute, breachedAt.Add(2*time.Minute))
		require.Empty(t, candidates, "the old generation cannot authorize recovery before status reconciliation")
		for range 2 {
			mutateMinAvailableBreachedCondition(pclq, 0, 0)
		}
		condition := meta.FindStatusCondition(pclq.Status.Conditions, constants.ConditionTypeMinAvailableBreached)
		require.Equal(t, constants.ConditionReasonScheduledReplicasBelowMinAvailable, condition.Reason)
		require.Equal(t, pclq.Generation, condition.ObservedGeneration)
		require.Equal(t, breachedAt, condition.LastTransitionTime)
		candidates, remaining := componentutils.GetMinAvailableBreachedPCLQInfo(
			[]grovecorev1alpha1.PodClique{*pclq}, time.Minute, breachedAt.Add(30*time.Second))
		require.Equal(t, []string{pclq.Name}, candidates)
		require.Equal(t, 30*time.Second, remaining, "scaling must not restart the termination delay")
	}
}

func TestHealthHistoryResetsOnlyOnObservedLifecycleTransitions(t *testing.T) {
	for _, transition := range []string{"idle", "update", "recovery"} {
		t.Run(transition, func(t *testing.T) {
			pclq := &grovecorev1alpha1.PodClique{
				ObjectMeta: metav1.ObjectMeta{Generation: 1},
				Spec:       grovecorev1alpha1.PodCliqueSpec{Replicas: ptr.To[int32](2), MinAvailable: ptr.To(int32(2))},
				Status:     grovecorev1alpha1.PodCliqueStatus{ReadyReplicas: 2, ScheduledReplicas: 2},
			}
			mutateMinAvailableBreachedCondition(pclq, 0, 0)
			pclq.Status.ReadyReplicas, pclq.Status.ScheduledReplicas = 0, 0
			pclq.Generation++
			switch transition {
			case "idle":
				pclq.Spec.Replicas = ptr.To[int32](0)
				mutateMinAvailableBreachedCondition(pclq, 0, 0)
				pclq.Spec.Replicas = ptr.To[int32](2)
			case "update":
				pclq.Status.UpdateProgress = &grovecorev1alpha1.PodCliqueUpdateProgress{}
				mutateMinAvailableBreachedCondition(pclq, 0, 0)
				pclq.Status.UpdateProgress = nil
			case "recovery":
				mutateMinAvailableBreachedCondition(pclq, 0, 0)
				componentutils.DisarmGangRecoveryBreach(&pclq.Status.Conditions)
			}
			pclq.Generation++
			for range 2 {
				mutateMinAvailableBreachedCondition(pclq, 0, 0)
				condition := meta.FindStatusCondition(pclq.Status.Conditions, constants.ConditionTypeMinAvailableBreached)
				require.Equal(t, constants.ConditionReasonInitialScheduling, condition.Reason)
				require.Equal(t, pclq.Generation, condition.ObservedGeneration)
				require.False(t, componentutils.HasObservedMinAvailable(pclq.Status.Conditions, pclq.Generation))
			}
			// Starting Pods alone must not re-arm the next recovery episode.
			pclq.Status.ScheduledReplicas = 2
			mutateMinAvailableBreachedCondition(pclq, 0, 0)
			require.False(t, componentutils.HasObservedMinAvailable(pclq.Status.Conditions, pclq.Generation))
			pclq.Status.ReadyReplicas = 2
			mutateMinAvailableBreachedCondition(pclq, 0, 0)
			require.True(t, componentutils.IsMinAvailableBreachArmed(pclq.Status.Conditions, pclq.Generation))
		})
	}
}
