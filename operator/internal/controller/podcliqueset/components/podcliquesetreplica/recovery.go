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
	"strconv"
	"strings"

	apicommon "github.com/ai-dynamo/grove/operator/api/common"
	apiconstants "github.com/ai-dynamo/grove/operator/api/common/constants"
	grovecorev1alpha1 "github.com/ai-dynamo/grove/operator/api/core/v1alpha1"
	componentutils "github.com/ai-dynamo/grove/operator/internal/controller/common/component/utils"
	k8sutils "github.com/ai-dynamo/grove/operator/internal/utils/kubernetes"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/uuid"
	"k8s.io/client-go/util/retry"
	"k8s.io/utils/ptr"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

// pruneGangRecoveries removes records when their logical PCS replicas are scaled in.
func (r _resource) pruneGangRecoveries(ctx context.Context, pcs *grovecorev1alpha1.PodCliqueSet) error {
	before := pcs.DeepCopy()
	for key := range pcs.Annotations {
		if !strings.HasPrefix(key, componentutils.AnnotationGangRecoveryPrefix) {
			continue
		}
		index, err := strconv.Atoi(strings.TrimPrefix(key, componentutils.AnnotationGangRecoveryPrefix))
		if err != nil || index < 0 {
			return fmt.Errorf("invalid gang recovery annotation %q", key)
		}
		if index >= int(pcs.Spec.Replicas) {
			delete(pcs.Annotations, key)
		}
	}
	if len(before.Annotations) == len(pcs.Annotations) {
		return nil
	}
	return r.client.Patch(ctx, pcs, client.MergeFromWithOptions(before, client.MergeFromWithOptimisticLock{}))
}

func (r _resource) startGangRecovery(ctx context.Context, pcs *grovecorev1alpha1.PodCliqueSet, index int) error {
	previous, err := componentutils.GetGangRecovery(pcs, index)
	if err != nil || previous.Active() {
		return err
	}
	return r.patchGangRecovery(ctx, pcs, index, previous, componentutils.GangRecovery{
		Epoch: string(uuid.NewUUID()), Phase: componentutils.GangRecoveryDraining,
	})
}

// patchGangRecovery fences stale requests by PCS UID, replica index, recovery epoch,
// and resourceVersion. A retry can never rewind or restart an already completed drain.
func (r _resource) patchGangRecovery(ctx context.Context, pcs *grovecorev1alpha1.PodCliqueSet, index int, previous, next componentutils.GangRecovery) error {
	return retry.RetryOnConflict(retry.DefaultBackoff, func() error {
		latest := &grovecorev1alpha1.PodCliqueSet{}
		if err := r.client.Get(ctx, client.ObjectKeyFromObject(pcs), latest); err != nil {
			return client.IgnoreNotFound(err)
		}
		if latest.UID != pcs.UID || !latest.DeletionTimestamp.IsZero() || index >= int(latest.Spec.Replicas) {
			return nil
		}
		current, err := componentutils.GetGangRecovery(latest, index)
		if err != nil || current != previous {
			return err
		}
		patch := client.MergeFromWithOptions(latest.DeepCopy(), client.MergeFromWithOptimisticLock{})
		if err := componentutils.SetGangRecovery(latest, index, next); err != nil {
			return err
		}
		return r.client.Patch(ctx, latest, patch)
	})
}

func (r _resource) advanceGangRecovery(ctx context.Context, pcs *grovecorev1alpha1.PodCliqueSet, index int, recovery componentutils.GangRecovery) error {
	snapshot, err := r.getRecoverySnapshot(ctx, pcs, index)
	if err != nil {
		return err
	}
	// A deleting PodClique retains its finalizer until an uncached Pod list
	// confirms the drain. Its Pods and the clique can reach our caches separately.
	for _, pclq := range snapshot.pclqs {
		if !pclq.DeletionTimestamp.IsZero() {
			return nil
		}
	}
	for _, pod := range snapshot.pods {
		if pod.Annotations[componentutils.AnnotationPodRecoveryEpoch] != recovery.Epoch {
			return nil
		}
	}
	next := recovery
	switch recovery.Phase {
	case componentutils.GangRecoveryDraining:
		next.Phase = componentutils.GangRecoveryRecreating
	case componentutils.GangRecoveryRecreating:
		if !snapshot.available(pcs, index, recovery.Epoch) {
			return nil
		}
		next.Phase = componentutils.GangRecoveryComplete
	default:
		return nil
	}
	return r.patchGangRecovery(ctx, pcs, index, recovery, next)
}

type recoverySnapshot struct {
	pclqs map[string]*grovecorev1alpha1.PodClique
	pcsgs map[string]*grovecorev1alpha1.PodCliqueScalingGroup
	pods  []*corev1.Pod
}

// getRecoverySnapshot verifies the complete owner chain, not just matching labels.
func (r _resource) getRecoverySnapshot(ctx context.Context, pcs *grovecorev1alpha1.PodCliqueSet, index int) (*recoverySnapshot, error) {
	opts := []client.ListOption{client.InNamespace(pcs.Namespace), client.MatchingLabels{
		apicommon.LabelPartOfKey: pcs.Name, apicommon.LabelPodCliqueSetReplicaIndex: strconv.Itoa(index),
	}}
	pcsgList := &grovecorev1alpha1.PodCliqueScalingGroupList{}
	if err := r.client.List(ctx, pcsgList, opts...); err != nil {
		return nil, fmt.Errorf("list recovery scaling groups: %w", err)
	}
	snapshot := &recoverySnapshot{
		pclqs: make(map[string]*grovecorev1alpha1.PodClique),
		pcsgs: make(map[string]*grovecorev1alpha1.PodCliqueScalingGroup),
	}
	ownerUIDs := map[types.UID]bool{pcs.UID: true}
	for i := range pcsgList.Items {
		pcsg := &pcsgList.Items[i]
		if metav1.IsControlledBy(pcsg, pcs) {
			snapshot.pcsgs[pcsg.Name] = pcsg
			ownerUIDs[pcsg.UID] = true
		}
	}
	pclqList := &grovecorev1alpha1.PodCliqueList{}
	if err := r.client.List(ctx, pclqList, opts...); err != nil {
		return nil, fmt.Errorf("list recovery cliques: %w", err)
	}
	pclqUIDs := make(map[types.UID]bool)
	for i := range pclqList.Items {
		pclq := &pclqList.Items[i]
		owner := metav1.GetControllerOf(pclq)
		if owner != nil && ownerUIDs[owner.UID] {
			snapshot.pclqs[pclq.Name] = pclq
			pclqUIDs[pclq.UID] = true
		}
	}
	podList := &corev1.PodList{}
	if err := r.client.List(ctx, podList, opts...); err != nil {
		return nil, fmt.Errorf("list recovery pods: %w", err)
	}
	for i := range podList.Items {
		pod := &podList.Items[i]
		owner := metav1.GetControllerOf(pod)
		if owner != nil && pclqUIDs[owner.UID] {
			snapshot.pods = append(snapshot.pods, pod)
		}
	}
	return snapshot, nil
}

// available derives required components from the template, but reads their targets
// from the live scale objects. Missing objects are not interpreted as idle.
func (s *recoverySnapshot) available(pcs *grovecorev1alpha1.PodCliqueSet, index int, epoch string) bool {
	readyByClique := make(map[types.UID]int32)
	for _, pod := range s.pods {
		if pod.DeletionTimestamp.IsZero() && pod.Annotations[componentutils.AnnotationPodRecoveryEpoch] == epoch && k8sutils.IsPodReady(pod) {
			readyByClique[metav1.GetControllerOf(pod).UID]++
		}
	}
	ready := func(name string) bool {
		pclq := s.pclqs[name]
		if pclq == nil || !pclq.DeletionTimestamp.IsZero() {
			return false
		}
		if pclq.Spec.Replicas == 0 {
			return true
		}
		minimum := ptr.Deref(pclq.Spec.MinAvailable, 1)
		return readyByClique[pclq.UID] >= minimum && pclq.Status.ReadyReplicas >= minimum &&
			meta.IsStatusConditionFalse(pclq.Status.Conditions, apiconstants.ConditionTypeMinAvailableBreached)
	}
	pcsReplica := apicommon.ResourceNameReplica{Name: pcs.Name, Replica: index}
	for _, template := range pcs.Spec.Template.Cliques {
		if !isPCLQInPCSG(template.Name, pcs.Spec.Template.PodCliqueScalingGroupConfigs) &&
			!ready(apicommon.GeneratePodCliqueName(pcsReplica, template.Name)) {
			return false
		}
	}
	for _, config := range pcs.Spec.Template.PodCliqueScalingGroupConfigs {
		pcsg := s.pcsgs[apicommon.GeneratePodCliqueScalingGroupName(pcsReplica, config.Name)]
		if pcsg == nil || !pcsg.DeletionTimestamp.IsZero() {
			return false
		}
		if pcsg.Spec.Replicas == 0 {
			continue
		}
		var available int32
		for replica := range int(pcsg.Spec.Replicas) {
			allReady := true
			for _, clique := range pcsg.Spec.CliqueNames {
				if !ready(apicommon.GeneratePodCliqueName(apicommon.ResourceNameReplica{Name: pcsg.Name, Replica: replica}, clique)) {
					allReady = false
					break
				}
			}
			if allReady {
				available++
			}
		}
		minimum := ptr.Deref(pcsg.Spec.MinAvailable, 1)
		if available < minimum || pcsg.Status.AvailableReplicas < minimum ||
			!meta.IsStatusConditionFalse(pcsg.Status.Conditions, apiconstants.ConditionTypeMinAvailableBreached) {
			return false
		}
	}
	return true
}
