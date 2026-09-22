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
	"fmt"
	"strconv"

	apicommon "github.com/ai-dynamo/grove/operator/api/common"
	grovecorev1alpha1 "github.com/ai-dynamo/grove/operator/api/core/v1alpha1"
	"github.com/ai-dynamo/grove/operator/internal/controller/common/component"
	groveerr "github.com/ai-dynamo/grove/operator/internal/errors"
	"github.com/ai-dynamo/grove/operator/internal/scheduler"
	componentutils "github.com/ai-dynamo/grove/operator/internal/utils/component"
	k8sutils "github.com/ai-dynamo/grove/operator/internal/utils/kubernetes"

	groveschedulerv1alpha1 "github.com/ai-dynamo/grove/scheduler/api/core/v1alpha1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

type schedulingGateSnapshot struct {
	podGangByName              map[string]*groveschedulerv1alpha1.PodGang
	dependencySatisfiedByEpoch map[string]bool
	backendSyncedByPodGang     map[string]bool
	pcsgMinimumScheduled       bool
}

func (r _resource) prepareSchedulingGateSnapshot(ctx context.Context, ss *syncSnapshot, gatedPods []*corev1.Pod) (*schedulingGateSnapshot, error) {
	podGangs, err := r.fetchPodGangsForGatedPods(ctx, gatedPods, ss.pclq.Namespace)
	if err != nil {
		return nil, err
	}
	dependencies, err := r.resolveDependencySatisfiedByEpoch(ctx, ss)
	if err != nil {
		return nil, err
	}
	backendSynced, err := r.resolveBackendSynced(ctx, podGangs)
	if err != nil {
		return nil, err
	}
	quorumScheduled, err := r.isPCSGMinimumScheduled(ctx, ss)
	if err != nil {
		return nil, groveerr.WrapError(err, errCodeGetPodGang, component.OperationSync, "failed to resolve current PCSG scheduling quorum")
	}
	return &schedulingGateSnapshot{
		podGangByName:              podGangs,
		dependencySatisfiedByEpoch: dependencies,
		backendSyncedByPodGang:     backendSynced,
		pcsgMinimumScheduled:       quorumScheduled,
	}, nil
}

// resolveBackendSynced keeps new pods gated while a backend still has the old membership policy.
// Historical PodGang conditions cannot acknowledge a native PodGroup spec update.
func (r _resource) resolveBackendSynced(ctx context.Context, gangs map[string]*groveschedulerv1alpha1.PodGang) (map[string]bool, error) {
	synced := make(map[string]bool, len(gangs))
	for name, gang := range gangs {
		if gang == nil || !gang.DeletionTimestamp.IsZero() {
			continue
		}
		if r.schedRegistry == nil {
			return nil, groveerr.New(errCodeGetPodGang, component.OperationSync, "scheduler registry is not initialized")
		}
		backend := r.schedRegistry.GetOrDefault(gang.Labels[apicommon.LabelSchedulerName])
		if backend == nil {
			return nil, groveerr.New(errCodeGetPodGang, component.OperationSync, fmt.Sprintf("scheduler backend not found for PodGang %s", name))
		}
		synced[name] = true
		if resourceBackend, ok := backend.(scheduler.PodGangResourceBackend); ok {
			var err error
			synced[name], err = resourceBackend.IsPodGangSynced(ctx, gang)
			if err != nil {
				return nil, groveerr.WrapError(err, errCodeGetPodGang, component.OperationSync, fmt.Sprintf("failed to check backend policy for PodGang %s", name))
			}
		}
	}
	return synced, nil
}

// isPCSGMinimumScheduled gates only new scale-out pods. Minimum replicas and coherent-update
// tails retain their epoch ordering, avoiding a dependency cycle within an anchor or update step.
func (r _resource) isPCSGMinimumScheduled(ctx context.Context, ss *syncSnapshot) (bool, error) {
	config := componentutils.FindScalingGroupConfigForClique(ss.pcs.Spec.Template.PodCliqueScalingGroupConfigs, ss.cliqueName)
	if config == nil {
		return true, nil
	}
	replicaIndex, err := strconv.ParseInt(ss.pclq.Labels[apicommon.LabelPodCliqueScalingGroupReplicaIndex], 10, 32)
	if err != nil || replicaIndex < 0 {
		return false, fmt.Errorf("invalid PCSG replica index on PodClique %s: %q", ss.pclq.Name, ss.pclq.Labels[apicommon.LabelPodCliqueScalingGroupReplicaIndex])
	}
	entry, err := componentutils.FindPodGangEntryForPCSGReplica(ss.pgm.Spec.Entries, "", config.Name, int32(replicaIndex))
	if err != nil || entry == nil {
		return false, err
	}
	if entry.Role != grovecorev1alpha1.PodGangEntryRoleScaleOut {
		return true, nil
	}

	rnr := apicommon.ResourceNameReplica{Name: ss.pcs.Name, Replica: ss.pcsReplicaIndex}
	pcsg := &grovecorev1alpha1.PodCliqueScalingGroup{}
	if err := r.client.Get(ctx, client.ObjectKey{Namespace: ss.pcs.Namespace, Name: apicommon.GeneratePodCliqueScalingGroupName(rnr, config.Name)}, pcsg); err != nil {
		return false, client.IgnoreNotFound(err)
	}
	if !metav1.IsControlledBy(pcsg, ss.pcs) || !pcsg.DeletionTimestamp.IsZero() ||
		!metav1.IsControlledBy(ss.pclq, pcsg) || pcsg.Spec.MinAvailable == nil ||
		*pcsg.Spec.MinAvailable <= 0 || pcsg.Spec.Replicas < *pcsg.Spec.MinAvailable {
		return false, nil
	}
	if int32(replicaIndex) < *pcsg.Spec.MinAvailable {
		return true, nil
	}

	// Check the exact minimum replica indices, not an aggregate count or LastScheduled:
	// an anchor retains its history across sleep, and other PCSGs may still be serving in it.
	for index := range *pcsg.Spec.MinAvailable {
		gangName, err := componentutils.PodGangNameForPCSGReplica(ss.pgm, rnr, config.Name, index)
		if err != nil {
			return false, err
		}
		for _, cliqueName := range pcsg.Spec.CliqueNames {
			name := apicommon.GeneratePodCliqueName(apicommon.ResourceNameReplica{Name: pcsg.Name, Replica: int(index)}, cliqueName)
			pclq := &grovecorev1alpha1.PodClique{}
			if err := r.client.Get(ctx, client.ObjectKey{Namespace: pcsg.Namespace, Name: name}, pclq); err != nil {
				return false, client.IgnoreNotFound(err)
			}
			if !metav1.IsControlledBy(pclq, pcsg) || !pclq.DeletionTimestamp.IsZero() ||
				pclq.Spec.MinAvailable == nil || *pclq.Spec.MinAvailable <= 0 || pclq.Spec.Replicas < *pclq.Spec.MinAvailable {
				return false, nil
			}
			scheduled, err := r.scheduledPodsInGang(ctx, ss.pcs.Name, pclq, gangName)
			if err != nil || scheduled < *pclq.Spec.MinAvailable {
				return false, err
			}
		}
	}
	return true, nil
}

func (r _resource) scheduledPodsInGang(ctx context.Context, pcsName string, pclq *grovecorev1alpha1.PodClique, gangName string) (int32, error) {
	pods, err := componentutils.GetPCLQPods(ctx, r.client, pcsName, pclq)
	if err != nil {
		return 0, err
	}
	var scheduled int32
	for _, pod := range pods {
		if pod.Labels[apicommon.LabelPodGang] == gangName && k8sutils.IsPodActive(pod) &&
			k8sutils.IsPodScheduled(pod) && !hasPodGangSchedulingGate(pod) {
			scheduled++
		}
	}
	return scheduled, nil
}
