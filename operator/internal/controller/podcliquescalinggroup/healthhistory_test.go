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

package podcliquescalinggroup

import (
	"testing"
	"time"

	"github.com/ai-dynamo/grove/operator/api/common/constants"
	grovecorev1alpha1 "github.com/ai-dynamo/grove/operator/api/core/v1alpha1"
	componentutils "github.com/ai-dynamo/grove/operator/internal/controller/common/component/utils"

	"github.com/go-logr/logr"
	"github.com/stretchr/testify/require"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/utils/ptr"
)

func TestPositiveScalePreservesBreachAndTransitionTime(t *testing.T) {
	pcsg := &grovecorev1alpha1.PodCliqueScalingGroup{
		ObjectMeta: metav1.ObjectMeta{Generation: 1},
		Spec:       grovecorev1alpha1.PodCliqueScalingGroupSpec{Replicas: 3, MinAvailable: ptr.To(int32(2))},
		Status:     grovecorev1alpha1.PodCliqueScalingGroupStatus{AvailableReplicas: 3},
	}
	mutateMinAvailableBreachedCondition(logr.Discard(), pcsg)
	pcsg.Status.AvailableReplicas = 1
	mutateMinAvailableBreachedCondition(logr.Discard(), pcsg)
	breachedAt := metav1.NewTime(time.Unix(100, 0))
	meta.FindStatusCondition(pcsg.Status.Conditions, constants.ConditionTypeMinAvailableBreached).LastTransitionTime = breachedAt
	for _, replicas := range []int32{4, 2} {
		pcsg.Spec.Replicas = replicas
		pcsg.Generation++
		require.False(t, componentutils.IsMinAvailableBreachArmed(pcsg.Status.Conditions, pcsg.Generation))
		for range 2 {
			mutateMinAvailableBreachedCondition(logr.Discard(), pcsg)
		}
		condition := meta.FindStatusCondition(pcsg.Status.Conditions, constants.ConditionTypeMinAvailableBreached)
		require.Equal(t, metav1.ConditionTrue, condition.Status)
		require.Equal(t, constants.ConditionReasonInsufficientAvailablePCSGReplicas, condition.Reason)
		require.Equal(t, pcsg.Generation, condition.ObservedGeneration)
		require.Equal(t, breachedAt, condition.LastTransitionTime)
		require.True(t, componentutils.IsMinAvailableBreachArmed(pcsg.Status.Conditions, pcsg.Generation))
	}
}

func TestHealthHistoryResetsOnlyOnObservedLifecycleTransitions(t *testing.T) {
	for _, transition := range []string{"idle", "update", "recovery"} {
		t.Run(transition, func(t *testing.T) {
			pcsg := &grovecorev1alpha1.PodCliqueScalingGroup{
				ObjectMeta: metav1.ObjectMeta{Generation: 1},
				Spec:       grovecorev1alpha1.PodCliqueScalingGroupSpec{Replicas: 2, MinAvailable: ptr.To(int32(2))},
				Status:     grovecorev1alpha1.PodCliqueScalingGroupStatus{AvailableReplicas: 2},
			}
			mutateMinAvailableBreachedCondition(logr.Discard(), pcsg)
			pcsg.Status.AvailableReplicas = 0
			pcsg.Generation++
			switch transition {
			case "idle":
				pcsg.Spec.Replicas = 0
				mutateMinAvailableBreachedCondition(logr.Discard(), pcsg)
				pcsg.Spec.Replicas = 2
			case "update":
				pcsg.Status.UpdateProgress = &grovecorev1alpha1.PodCliqueScalingGroupUpdateProgress{}
				mutateMinAvailableBreachedCondition(logr.Discard(), pcsg)
				pcsg.Status.UpdateProgress = nil
			case "recovery":
				mutateMinAvailableBreachedCondition(logr.Discard(), pcsg)
				componentutils.DisarmGangRecoveryBreach(&pcsg.Status.Conditions)
			}
			pcsg.Generation++
			for range 2 {
				mutateMinAvailableBreachedCondition(logr.Discard(), pcsg)
				condition := meta.FindStatusCondition(pcsg.Status.Conditions, constants.ConditionTypeMinAvailableBreached)
				require.Equal(t, constants.ConditionReasonInitialScheduling, condition.Reason)
				require.Equal(t, pcsg.Generation, condition.ObservedGeneration)
				require.False(t, componentutils.HasObservedMinAvailable(pcsg.Status.Conditions, pcsg.Generation))
			}
			pcsg.Status.AvailableReplicas = 2
			mutateMinAvailableBreachedCondition(logr.Discard(), pcsg)
			require.True(t, componentutils.IsMinAvailableBreachArmed(pcsg.Status.Conditions, pcsg.Generation))
		})
	}
}
