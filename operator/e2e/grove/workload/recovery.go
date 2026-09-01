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

package workload

import (
	"context"
	"fmt"

	apicommon "github.com/ai-dynamo/grove/operator/api/common"
	grovecorev1alpha1 "github.com/ai-dynamo/grove/operator/api/core/v1alpha1"
	componentutils "github.com/ai-dynamo/grove/operator/internal/utils/component"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/sets"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

// VerifyNoGangRecovery checks both recovery history and Pod identity: recovery
// retains the scale target, so a stable PodClique UID alone is insufficient.
func (wm *WorkloadManager) VerifyNoGangRecovery(ctx context.Context, original *grovecorev1alpha1.PodClique, podUIDs sets.Set[types.UID]) error {
	if original.UID == "" || len(podUIDs) == 0 {
		return fmt.Errorf("recovery verification requires a PodClique UID and a nonempty Pod snapshot")
	}
	pclq := &grovecorev1alpha1.PodClique{}
	if err := wm.cl.Get(ctx, client.ObjectKeyFromObject(original), pclq); err != nil {
		return err
	}
	if pclq.UID != original.UID || !pclq.DeletionTimestamp.IsZero() {
		return fmt.Errorf("PodClique %s was replaced or is terminating", pclq.Name)
	}
	pcs, err := componentutils.GetPodCliqueSet(ctx, wm.cl, pclq.ObjectMeta)
	if err != nil {
		return err
	}
	recovery, err := componentutils.GetGangRecoveryForChild(pcs, pclq.ObjectMeta)
	if err != nil {
		return err
	}
	if recovery.Epoch != "" {
		return fmt.Errorf("PodClique %s entered gang recovery: %+v", pclq.Name, recovery)
	}
	pods := &corev1.PodList{}
	if err := wm.cl.List(ctx, pods, client.InNamespace(pclq.Namespace),
		client.MatchingLabels{apicommon.LabelPodClique: pclq.Name}); err != nil {
		return err
	}
	if len(pods.Items) != len(podUIDs) {
		return fmt.Errorf("PodClique %s has %d Pods, expected %d", pclq.Name, len(pods.Items), len(podUIDs))
	}
	for _, pod := range pods.Items {
		if !podUIDs.Has(pod.UID) || !pod.DeletionTimestamp.IsZero() || !metav1.IsControlledBy(&pod, pclq) {
			return fmt.Errorf("pod %s was replaced, is terminating, or has a different owner", pod.Name)
		}
	}
	return nil
}
