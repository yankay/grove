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

package component

import (
	"testing"

	"github.com/ai-dynamo/grove/operator/api/common/constants"

	"github.com/stretchr/testify/assert"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

func TestHasObservedMinAvailable(t *testing.T) {
	for _, tc := range []struct {
		name     string
		reason   string
		observed int64
		want     bool
	}{
		{name: "healthy previous generation", reason: constants.ConditionReasonSufficientReadyPods, observed: 1, want: true},
		{name: "regressed previous generation", reason: constants.ConditionReasonInsufficientReadyPods, observed: 1, want: true},
		{name: "healthy scaling group", reason: constants.ConditionReasonSufficientAvailablePCSGReplicas, observed: 1, want: true},
		{name: "regressed scaling group", reason: constants.ConditionReasonInsufficientAvailablePCSGReplicas, observed: 1, want: true},
		{name: "legacy condition", reason: constants.ConditionReasonSufficientReadyPods},
		{name: "future generation", reason: constants.ConditionReasonSufficientReadyPods, observed: 3},
		{name: "initial scheduling", reason: constants.ConditionReasonInitialScheduling, observed: 1},
		{name: "idle", reason: constants.ConditionReasonIdle, observed: 1},
		{name: "update", reason: constants.ConditionReasonUpdateInProgress, observed: 1},
	} {
		t.Run(tc.name, func(t *testing.T) {
			conditions := []metav1.Condition{{
				Type: constants.ConditionTypeMinAvailableBreached, Reason: tc.reason, ObservedGeneration: tc.observed,
			}}
			assert.Equal(t, tc.want, HasObservedMinAvailable(conditions, 2))
			assert.False(t, IsMinAvailableBreachArmed(conditions, 2), "history alone cannot authorize an action on a stale condition")
		})
	}
	assert.False(t, HasObservedMinAvailable(nil, 2))
}
