// Copyright 2025 The Grove Authors.
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
	"slices"
	"strconv"
	"time"

	apicommon "github.com/ai-dynamo/grove/operator/api/common"
	apiconstants "github.com/ai-dynamo/grove/operator/api/common/constants"
	grovecorev1alpha1 "github.com/ai-dynamo/grove/operator/api/core/v1alpha1"
	"github.com/ai-dynamo/grove/operator/internal/constants"
	componentutils "github.com/ai-dynamo/grove/operator/internal/controller/common/component/utils"
	"github.com/ai-dynamo/grove/operator/internal/utils"

	"github.com/go-logr/logr"
	"github.com/samber/lo"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

// deletionWork captures the PodCliqueSet replica deletion work.
type deletionWork struct {
	// deletionTasks are a slice of PodCliqueSet replica deletion tasks. These are the replicas where there is at least
	// one child resource whose MinAvailableBreached condition is set to true and TerminationDelay has expired.
	deletionTasks []utils.Task
	// pcsIndicesToTerminate are the PodCliqueSet replica indices for which one or more constituent standalone PCLQ or PCSG
	// have MinAvailableBreached condition set to true for a duration greater than TerminationDelay.
	pcsIndicesToTerminate []int
	// minAvailableBreachedConstituents is map of PCS replica index to PCLQ FQNs which have MinAvailableBreached condition
	// set to true but for these PCLQs TerminationDelay has not expired yet. If there is at least one such PCLQ then
	// a requeue should be done and the reconciler should re-check if the TerminationDelay for these PCLQs eventually expires
	// at which point the corresponding PCS replica should be deleted.
	minAvailableBreachedConstituents map[int][]string
}

// shouldRequeue returns true if there are constituents with MinAvailable breached but termination delay not expired.
func (d deletionWork) shouldRequeue() bool {
	return len(d.minAvailableBreachedConstituents) > 0
}

// hasPendingPCSReplicaDeletion returns true if there are replica deletion tasks ready to execute.
func (d deletionWork) hasPendingPCSReplicaDeletion() bool {
	return len(d.deletionTasks) > 0
}

// getPCSReplicaDeletionWork identifies PCS replicas that need termination due to MinAvailable breaches.
func (r _resource) getPCSReplicaDeletionWork(ctx context.Context, logger logr.Logger, pcs *grovecorev1alpha1.PodCliqueSet) (*deletionWork, error) {
	var (
		now              = time.Now()
		pcsObjectKey     = client.ObjectKeyFromObject(pcs)
		terminationDelay = pcs.Spec.Template.TerminationDelay.Duration
		deletionTasks    = make([]utils.Task, 0, pcs.Spec.Replicas)
		work             = &deletionWork{
			minAvailableBreachedConstituents: make(map[int][]string),
		}
	)

	for pcsReplicaIndex := range int(pcs.Spec.Replicas) {
		recovery, err := componentutils.GetGangRecovery(pcs, pcsReplicaIndex)
		if err != nil {
			return nil, err
		}
		if recovery.Active() {
			if err := r.advanceGangRecovery(ctx, pcs, pcsReplicaIndex, recovery); err != nil {
				return nil, err
			}
			work.minAvailableBreachedConstituents[pcsReplicaIndex] = []string{"gang recovery"}
			work.pcsIndicesToTerminate = append(work.pcsIndicesToTerminate, pcsReplicaIndex)
			continue
		}
		breachedPCSGNames, minPCSGWaitFor, err := r.getMinAvailableBreachedPCSGs(ctx, pcs, pcsReplicaIndex, terminationDelay, now)
		if err != nil {
			return nil, err
		}
		breachedPCLQNames, minPCLQWaitFor, skipPCSReplicaIndex, err := r.getMinAvailableBreachedPCLQsNotInPCSG(ctx, logger, pcs, pcsReplicaIndex, now)
		if err != nil {
			return nil, err
		}
		if skipPCSReplicaIndex {
			continue
		}
		if (len(breachedPCSGNames) > 0 && minPCSGWaitFor <= 0) ||
			(len(breachedPCLQNames) > 0 && minPCLQWaitFor <= 0) {
			reason := fmt.Sprintf("Recover PodCliqueSet %v replica %d after MinAvailable breached longer than TerminationDelay: %s", pcsObjectKey, pcsReplicaIndex, terminationDelay)
			pclqGangTerminationTask := r.createPCSReplicaRecoveryTask(logger, pcs, pcsReplicaIndex, reason)
			deletionTasks = append(deletionTasks, pclqGangTerminationTask)
			work.pcsIndicesToTerminate = append(work.pcsIndicesToTerminate, pcsReplicaIndex)
			work.minAvailableBreachedConstituents[pcsReplicaIndex] = []string{"gang recovery"}
		} else if len(breachedPCSGNames) > 0 || len(breachedPCLQNames) > 0 {
			work.minAvailableBreachedConstituents[pcsReplicaIndex] = append(work.minAvailableBreachedConstituents[pcsReplicaIndex], breachedPCLQNames...)
			work.minAvailableBreachedConstituents[pcsReplicaIndex] = append(work.minAvailableBreachedConstituents[pcsReplicaIndex], breachedPCSGNames...)
		}
	}
	work.deletionTasks = deletionTasks
	return work, nil
}

// getMinAvailableBreachedPCSGs retrieves PCSGs that have breached MinAvailable for a PCS replica.
func (r _resource) getMinAvailableBreachedPCSGs(ctx context.Context, pcs *grovecorev1alpha1.PodCliqueSet, pcsReplicaIndex int, terminationDelay time.Duration, since time.Time) ([]string, time.Duration, error) {
	pcsgList := &grovecorev1alpha1.PodCliqueScalingGroupList{}
	if err := r.client.List(ctx,
		pcsgList,
		client.InNamespace(pcs.Namespace),
		client.MatchingLabels(lo.Assign(
			apicommon.GetDefaultLabelsForPodCliqueSetManagedResources(pcs.Name),
			map[string]string{
				apicommon.LabelPodCliqueSetReplicaIndex: strconv.Itoa(pcsReplicaIndex),
			},
		)),
	); err != nil {
		return nil, 0, err
	}
	owned := lo.Filter(pcsgList.Items, func(pcsg grovecorev1alpha1.PodCliqueScalingGroup, _ int) bool {
		return metav1.IsControlledBy(&pcsg, pcs)
	})
	breachedPCSGNames, minWaitFor := getMinAvailableBreachedPCSGInfo(owned, terminationDelay, since)
	return breachedPCSGNames, minWaitFor, nil
}

// getMinAvailableBreachedPCLQsNotInPCSG retrieves standalone PCLQs that have breached MinAvailable.
func (r _resource) getMinAvailableBreachedPCLQsNotInPCSG(ctx context.Context, logger logr.Logger, pcs *grovecorev1alpha1.PodCliqueSet, pcsReplicaIndex int, since time.Time) (breachedPCLQNames []string, minWaitFor time.Duration, skipPCSReplica bool, err error) {
	pclqFQNsNotInPCSG := make([]string, 0, len(pcs.Spec.Template.Cliques))
	for _, pclqTemplateSpec := range pcs.Spec.Template.Cliques {
		if !isPCLQInPCSG(pclqTemplateSpec.Name, pcs.Spec.Template.PodCliqueScalingGroupConfigs) {
			pclqFQNsNotInPCSG = append(pclqFQNsNotInPCSG, apicommon.GeneratePodCliqueName(apicommon.ResourceNameReplica{Name: pcs.Name, Replica: pcsReplicaIndex}, pclqTemplateSpec.Name))
		}
	}
	var (
		pclqs            []grovecorev1alpha1.PodClique
		notFoundPCLQFQNs []string
	)
	pclqs, notFoundPCLQFQNs, err = r.getExistingPCLQsByNames(ctx, pcs.Namespace, pclqFQNsNotInPCSG)
	if err != nil {
		return
	}
	if len(notFoundPCLQFQNs) > 0 {
		logger.Info("PodClique(s) expected by PodCliqueSet replica not yet present; skipping MinAvailable evaluation for this replica index", "pcsName", pcs.Name, "replicaIndex", pcsReplicaIndex, "missingPodCliques", notFoundPCLQFQNs)
		skipPCSReplica = true
		return
	}
	for i := range pclqs {
		if !metav1.IsControlledBy(&pclqs[i], pcs) {
			return nil, 0, false, fmt.Errorf("PodClique %s is not controlled by PodCliqueSet %s", pclqs[i].Name, pcs.Name)
		}
	}
	breachedPCLQNames, minWaitFor = componentutils.GetMinAvailableBreachedPCLQInfo(pclqs, pcs.Spec.Template.TerminationDelay.Duration, since)
	return
}

// getExistingPCLQsByNames fetches PodClique objects. It returns the PCLQ objects that it found and a slice of PCLQ FQNs for which no PCLQ object exists. If there is an error it just returns the error.
func (r _resource) getExistingPCLQsByNames(ctx context.Context, namespace string, pclqFQNs []string) (pclqs []grovecorev1alpha1.PodClique, notFoundPCLQFQNs []string, err error) {
	for _, pclqFQN := range pclqFQNs {
		pclq := grovecorev1alpha1.PodClique{}
		if err = r.client.Get(ctx, client.ObjectKey{Name: pclqFQN, Namespace: namespace}, &pclq); err != nil {
			if apierrors.IsNotFound(err) {
				notFoundPCLQFQNs = append(notFoundPCLQFQNs, pclqFQN)
				continue
			}
			return nil, nil, err
		}
		pclqs = append(pclqs, pclq)
	}
	return pclqs, notFoundPCLQFQNs, nil
}

// getMinAvailableBreachedPCSGInfo returns actionable PCSG breaches and the shortest remaining delay.
func getMinAvailableBreachedPCSGInfo(pcsgs []grovecorev1alpha1.PodCliqueScalingGroup, terminationDelay time.Duration, since time.Time) ([]string, time.Duration) {
	pcsgCandidateNames := make([]string, 0, len(pcsgs))
	waitForDurations := make([]time.Duration, 0, len(pcsgs))
	for _, pcsg := range pcsgs {
		if pcsg.Spec.Replicas == 0 {
			continue
		}
		cond := meta.FindStatusCondition(pcsg.Status.Conditions, apiconstants.ConditionTypeMinAvailableBreached)
		if cond == nil {
			continue
		}
		if cond.Status != metav1.ConditionTrue ||
			!componentutils.IsMinAvailableBreachArmed(pcsg.Status.Conditions, pcsg.Generation) {
			continue
		}
		pcsgCandidateNames = append(pcsgCandidateNames, pcsg.Name)
		waitFor := terminationDelay - since.Sub(cond.LastTransitionTime.Time)
		waitForDurations = append(waitForDurations, waitFor)
	}
	if len(waitForDurations) == 0 {
		return pcsgCandidateNames, 0
	}
	slices.Sort(waitForDurations)
	return pcsgCandidateNames, waitForDurations[0]
}

// createPCSReplicaRecoveryTask persists the recovery before any pods are deleted.
// Scale targets survive, so concurrent accepted scale requests cannot be lost.
func (r _resource) createPCSReplicaRecoveryTask(logger logr.Logger, pcs *grovecorev1alpha1.PodCliqueSet, pcsReplicaIndex int, reason string) utils.Task {
	return utils.Task{
		Name: fmt.Sprintf("RecoverPCSReplica-%d", pcsReplicaIndex),
		Fn: func(ctx context.Context) error {
			if err := r.startGangRecovery(ctx, pcs, pcsReplicaIndex); err != nil {
				r.eventRecorder.Eventf(pcs, corev1.EventTypeWarning, constants.ReasonPodCliqueSetReplicaDeleteFailed, "Error starting recovery of PodCliqueSet replica %d: %v", pcsReplicaIndex, err)
				return err
			}
			logger.Info("Started PCS replica recovery", "pcsReplicaIndex", pcsReplicaIndex, "reason", reason)
			r.eventRecorder.Eventf(pcs, corev1.EventTypeNormal, constants.ReasonPodCliqueSetReplicaDeleteSuccessful, "PodCliqueSet replica %d recovery started", pcsReplicaIndex)
			return nil
		},
	}
}

// isPCLQInPCSG checks if a PodClique is part of any PCSG configuration.
func isPCLQInPCSG(pclqName string, pcsgConfigs []grovecorev1alpha1.PodCliqueScalingGroupConfig) bool {
	return lo.Reduce(pcsgConfigs, func(agg bool, pcsgConfig grovecorev1alpha1.PodCliqueScalingGroupConfig, _ int) bool {
		return agg || slices.Contains(pcsgConfig.CliqueNames, pclqName)
	}, false)
}
