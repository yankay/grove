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
	"encoding/json"
	"fmt"
	"strconv"
	"strings"

	apicommon "github.com/ai-dynamo/grove/operator/api/common"
	apiconstants "github.com/ai-dynamo/grove/operator/api/common/constants"
	grovecorev1alpha1 "github.com/ai-dynamo/grove/operator/api/core/v1alpha1"

	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

const (
	// AnnotationGangRecoveryPrefix stores the latest recovery of each PCS replica.
	// Keeping the epoch after completion fences late pod creates from older reconciles.
	AnnotationGangRecoveryPrefix = "grove.io/gang-recovery-"
	// AnnotationPodRecoveryEpoch identifies the recovery that created a Pod.
	AnnotationPodRecoveryEpoch = "grove.io/recovery-epoch"
	// GangRecoveryDraining waits for the old pods to disappear before replacement.
	GangRecoveryDraining = "Draining"
	// GangRecoveryRecreating waits for the replacement gang to become available.
	GangRecoveryRecreating = "Recreating"
	// GangRecoveryComplete permits a later health regression to start another recovery.
	GangRecoveryComplete = "Complete"
)

// GangRecovery is an internal, durable recovery record. It never stores replica
// intent: the surviving scale targets remain the only authority for that intent.
type GangRecovery struct {
	Epoch string `json:"epoch"`
	Phase string `json:"phase"`
}

// Active reports whether recovery is holding gang termination disarmed.
func (r GangRecovery) Active() bool {
	return r.Phase == GangRecoveryDraining || r.Phase == GangRecoveryRecreating
}

// GangRecoveryAnnotationsChanged isolates recovery events from unrelated metadata.
func GangRecoveryAnnotationsChanged(oldAnnotations, newAnnotations map[string]string) bool {
	for key, value := range oldAnnotations {
		if strings.HasPrefix(key, AnnotationGangRecoveryPrefix) && newAnnotations[key] != value {
			return true
		}
	}
	for key, value := range newAnnotations {
		if strings.HasPrefix(key, AnnotationGangRecoveryPrefix) && oldAnnotations[key] != value {
			return true
		}
	}
	return false
}

// DisarmGangRecoveryBreach retains the condition's generation and transition time:
// recovery changes the reason, not its availability predicate.
func DisarmGangRecoveryBreach(conditions *[]metav1.Condition) {
	condition := meta.FindStatusCondition(*conditions, apiconstants.ConditionTypeMinAvailableBreached)
	if condition != nil && condition.Status == metav1.ConditionTrue {
		condition.Reason = apiconstants.ConditionReasonInitialScheduling
		condition.Message = "Waiting for gang recovery to reach minimum availability"
	}
}

// GetGangRecovery reads the record for a logical PCS replica.
func GetGangRecovery(pcs *grovecorev1alpha1.PodCliqueSet, replicaIndex int) (GangRecovery, error) {
	value := pcs.Annotations[AnnotationGangRecoveryPrefix+strconv.Itoa(replicaIndex)]
	if value == "" {
		return GangRecovery{}, nil
	}
	var recovery GangRecovery
	if err := json.Unmarshal([]byte(value), &recovery); err != nil {
		return recovery, fmt.Errorf("invalid gang recovery for PCS replica %d: %w", replicaIndex, err)
	}
	if recovery.Epoch == "" || (recovery.Phase != GangRecoveryDraining &&
		recovery.Phase != GangRecoveryRecreating && recovery.Phase != GangRecoveryComplete) {
		return recovery, fmt.Errorf("invalid gang recovery for PCS replica %d: %+v", replicaIndex, recovery)
	}
	return recovery, nil
}

// GetGangRecoveryForChild resolves the PCS replica through its managed child label.
func GetGangRecoveryForChild(pcs *grovecorev1alpha1.PodCliqueSet, child metav1.ObjectMeta) (GangRecovery, error) {
	index, err := strconv.Atoi(child.Labels[apicommon.LabelPodCliqueSetReplicaIndex])
	if err != nil {
		for key := range pcs.Annotations {
			if strings.HasPrefix(key, AnnotationGangRecoveryPrefix) {
				return GangRecovery{}, fmt.Errorf("get PCS replica index for %s: %w", child.Name, err)
			}
		}
		return GangRecovery{}, nil
	}
	return GetGangRecovery(pcs, index)
}

// SetGangRecovery mutates the annotation; the caller persists it with an optimistic lock.
func SetGangRecovery(pcs *grovecorev1alpha1.PodCliqueSet, replicaIndex int, recovery GangRecovery) error {
	value, err := json.Marshal(recovery)
	if err != nil {
		return err
	}
	if pcs.Annotations == nil {
		pcs.Annotations = make(map[string]string)
	}
	pcs.Annotations[AnnotationGangRecoveryPrefix+strconv.Itoa(replicaIndex)] = string(value)
	return nil
}
