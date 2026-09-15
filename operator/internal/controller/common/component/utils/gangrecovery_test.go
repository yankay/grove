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

package utils

import (
	"testing"

	apiconstants "github.com/ai-dynamo/grove/operator/api/common/constants"
	grovecorev1alpha1 "github.com/ai-dynamo/grove/operator/api/core/v1alpha1"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

func TestGangRecoveryRejectsMalformedRecords(t *testing.T) {
	for _, value := range []string{`invalid`, `{}`, `{"epoch":"epoch","phase":"unknown"}`, `{"phase":"Draining"}`} {
		t.Run(value, func(t *testing.T) {
			pcs := &grovecorev1alpha1.PodCliqueSet{
				ObjectMeta: metav1.ObjectMeta{Annotations: map[string]string{AnnotationGangRecoveryPrefix + "0": value}},
			}
			_, err := GetGangRecovery(pcs, 0)
			require.Error(t, err, "invalid recovery is not equivalent to idle or completed recovery")
		})
	}
}

func TestDisarmRecoveryPreservesConditionContract(t *testing.T) {
	for _, status := range []metav1.ConditionStatus{metav1.ConditionTrue, metav1.ConditionFalse} {
		t.Run(string(status), func(t *testing.T) {
			condition := metav1.Condition{
				Type: apiconstants.ConditionTypeMinAvailableBreached, Status: status,
				Reason:             apiconstants.ConditionReasonScheduledReplicasBelowMinAvailable,
				ObservedGeneration: 7, LastTransitionTime: metav1.Now(),
			}
			conditions := []metav1.Condition{condition}
			DisarmGangRecoveryBreach(&conditions)
			assert.Equal(t, condition.Status, conditions[0].Status)
			assert.Equal(t, condition.ObservedGeneration, conditions[0].ObservedGeneration)
			assert.Equal(t, condition.LastTransitionTime, conditions[0].LastTransitionTime)
			if status == metav1.ConditionTrue {
				assert.Equal(t, apiconstants.ConditionReasonInitialScheduling, conditions[0].Reason)
			} else {
				assert.Equal(t, condition, conditions[0])
			}
		})
	}
}
