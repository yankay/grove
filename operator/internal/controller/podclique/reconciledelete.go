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

package podclique

import (
	"context"
	"fmt"

	"github.com/ai-dynamo/grove/operator/api/common/constants"
	grovecorev1alpha1 "github.com/ai-dynamo/grove/operator/api/core/v1alpha1"
	ctrlconstants "github.com/ai-dynamo/grove/operator/internal/constants"
	ctrlcommon "github.com/ai-dynamo/grove/operator/internal/controller/common"
	componentutils "github.com/ai-dynamo/grove/operator/internal/controller/common/component/utils"
	ctrlutils "github.com/ai-dynamo/grove/operator/internal/controller/utils"

	"github.com/go-logr/logr"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
)

// triggerDeletionFlow keeps the owner chain observable until all owned Pods have
// drained. Recovery must not mistake an absent PodClique for absent descendants.
// ResourceClaims still use owner-reference garbage collection.
func (r *Reconciler) triggerDeletionFlow(ctx context.Context, logger logr.Logger, pclq *grovecorev1alpha1.PodClique) ctrlcommon.ReconcileStepResult {
	dLog := logger.WithValues("operation", "delete")
	deleteStepFns := []ctrlcommon.ReconcileStepFn[grovecorev1alpha1.PodClique]{
		r.clearPodCliqueExpectations,
		r.drainOwnedPods,
		r.removeFinalizer,
	}
	for _, fn := range deleteStepFns {
		if stepResult := fn(ctx, dLog, pclq); ctrlcommon.ShortCircuitReconcileFlow(stepResult) {
			return stepResult
		}
	}
	dLog.Info("PodClique Pods drained and finalizer removed")
	return ctrlcommon.DoNotRequeue()
}

func (r *Reconciler) drainOwnedPods(ctx context.Context, _ logr.Logger, pclq *grovecorev1alpha1.PodClique) ctrlcommon.ReconcileStepResult {
	// Explicit orphan deletion leaves descendant ownership changes to the GC.
	// It is outside the automatic-recovery drain contract.
	if controllerutil.ContainsFinalizer(pclq, metav1.FinalizerOrphanDependents) {
		return ctrlcommon.ContinueReconcile()
	}
	// Finalizer release is an absence check: bypass the cache and do not rely on
	// mutable labels to find descendants.
	pods := &corev1.PodList{}
	if err := r.apiReader.List(ctx, pods, client.InNamespace(pclq.Namespace)); err != nil {
		return ctrlcommon.ReconcileWithErrors("error listing deleting PodClique's Pods", err)
	}
	pending := false
	for i := range pods.Items {
		pod := &pods.Items[i]
		if !metav1.IsControlledBy(pod, pclq) {
			continue
		}
		pending = true
		if !pod.DeletionTimestamp.IsZero() {
			continue
		}
		if err := client.IgnoreNotFound(r.client.Delete(ctx, pod, client.Preconditions{UID: &pod.UID})); err != nil {
			return ctrlcommon.ReconcileWithErrors(fmt.Sprintf("error deleting Pod %s", pod.Name), err)
		}
	}
	if pending {
		return ctrlcommon.ReconcileAfter(ctrlconstants.ComponentSyncRetryInterval, "waiting for owned Pods to drain")
	}
	return ctrlcommon.ContinueReconcile()
}

// clearPodCliqueExpectations drops the in-memory expectations entries for this
// PodClique so the store does not retain stale UIDs after the object is gone.
func (r *Reconciler) clearPodCliqueExpectations(_ context.Context, logger logr.Logger, pclq *grovecorev1alpha1.PodClique) ctrlcommon.ReconcileStepResult {
	if err := componentutils.ClearPodCliqueExpectations(logger, r.expectationsStore, pclq.ObjectMeta); err != nil {
		return ctrlcommon.ReconcileWithErrors("error clearing expectations", err)
	}
	return ctrlcommon.ContinueReconcile()
}

// removeFinalizer removes the PodClique finalizer to allow Kubernetes to complete the deletion
func (r *Reconciler) removeFinalizer(ctx context.Context, logger logr.Logger, pclq *grovecorev1alpha1.PodClique) ctrlcommon.ReconcileStepResult {
	if !controllerutil.ContainsFinalizer(pclq, constants.FinalizerPodClique) {
		logger.Info("Finalizer not found", "PodClique", pclq)
		return ctrlcommon.DoNotRequeue()
	}
	logger.Info("Removing finalizer", "PodClique", pclq, "finalizerName", constants.FinalizerPodClique)
	if err := ctrlutils.RemoveAndPatchFinalizer(ctx, r.client, pclq, constants.FinalizerPodClique); err != nil {
		return ctrlcommon.ReconcileWithErrors("error removing finalizer", fmt.Errorf("failed to remove finalizer: %s from PodClique: %v: %w", constants.FinalizerPodClique, client.ObjectKeyFromObject(pclq), err))
	}
	return ctrlcommon.ContinueReconcile()
}
