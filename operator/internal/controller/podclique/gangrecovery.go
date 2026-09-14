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
	"context"
	"fmt"

	apicommon "github.com/ai-dynamo/grove/operator/api/common"
	grovecorev1alpha1 "github.com/ai-dynamo/grove/operator/api/core/v1alpha1"
	"github.com/ai-dynamo/grove/operator/internal/constants"
	ctrlcommon "github.com/ai-dynamo/grove/operator/internal/controller/common"
	componentutils "github.com/ai-dynamo/grove/operator/internal/controller/common/component/utils"

	"github.com/go-logr/logr"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/event"
	"sigs.k8s.io/controller-runtime/pkg/handler"
	"sigs.k8s.io/controller-runtime/pkg/log"
	"sigs.k8s.io/controller-runtime/pkg/predicate"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"
)

// reconcileGangRecovery preserves the scale target while draining old-epoch pods.
// UID preconditions make retries harmless even if a pod name is reused.
func (r *Reconciler) reconcileGangRecovery(ctx context.Context, logger logr.Logger, pcs *grovecorev1alpha1.PodCliqueSet, pclq *grovecorev1alpha1.PodClique) ctrlcommon.ReconcileStepResult {
	recovery, err := componentutils.GetGangRecoveryForChild(pcs, pclq.ObjectMeta)
	if err != nil {
		return ctrlcommon.ReconcileWithErrors("could not read gang recovery", err)
	}
	if recovery.Epoch == "" {
		return ctrlcommon.ContinueReconcile()
	}
	pods, err := componentutils.GetPCLQPods(ctx, r.client, pcs.Name, pclq)
	if err != nil {
		return ctrlcommon.ReconcileWithErrors("could not list recovery pods", err)
	}
	pending := false
	for _, pod := range pods {
		if !metav1.IsControlledBy(pod, pclq) || pod.Annotations[componentutils.AnnotationPodRecoveryEpoch] == recovery.Epoch {
			continue
		}
		pending = true
		if !pod.DeletionTimestamp.IsZero() {
			continue
		}
		if err := client.IgnoreNotFound(r.client.Delete(ctx, pod, client.Preconditions{UID: &pod.UID})); err != nil {
			return ctrlcommon.ReconcileWithErrors(fmt.Sprintf("could not delete old recovery pod %s", pod.Name), err)
		}
	}
	if pending || recovery.Phase == componentutils.GangRecoveryDraining {
		if err := componentutils.ClearPodCliqueExpectations(logger, r.expectationsStore, pclq.ObjectMeta); err != nil {
			return ctrlcommon.ReconcileWithErrors("could not clear recovery expectations", err)
		}
		return ctrlcommon.ReconcileAfter(constants.ComponentSyncRetryInterval, "waiting for gang recovery drain")
	}
	return ctrlcommon.ContinueReconcile()
}

func gangRecoveryPredicate() predicate.Predicate {
	return predicate.Funcs{
		CreateFunc: func(_ event.CreateEvent) bool { return false },
		DeleteFunc: func(_ event.DeleteEvent) bool { return false },
		UpdateFunc: func(e event.UpdateEvent) bool {
			return e.ObjectOld != nil && e.ObjectNew != nil &&
				componentutils.GangRecoveryAnnotationsChanged(e.ObjectOld.GetAnnotations(), e.ObjectNew.GetAnnotations())
		},
		GenericFunc: func(_ event.GenericEvent) bool { return false },
	}
}

// Recovery affects PCSG-owned cliques too, including replicas scaled above the template.
func (r *Reconciler) mapGangRecoveryToPCLQs() handler.MapFunc {
	return func(ctx context.Context, obj client.Object) []reconcile.Request {
		pcs, ok := obj.(*grovecorev1alpha1.PodCliqueSet)
		if !ok {
			return nil
		}
		cliques := &grovecorev1alpha1.PodCliqueList{}
		if err := r.client.List(ctx, cliques, client.InNamespace(pcs.Namespace),
			client.MatchingLabels(apicommon.GetDefaultLabelsForPodCliqueSetManagedResources(pcs.Name))); err != nil {
			log.FromContext(ctx).Error(err, "Failed to list recovery PodCliques")
			return nil
		}
		requests := make([]reconcile.Request, 0, len(cliques.Items))
		for i := range cliques.Items {
			requests = append(requests, reconcile.Request{NamespacedName: client.ObjectKeyFromObject(&cliques.Items[i])})
		}
		return requests
	}
}
