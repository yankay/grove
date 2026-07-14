// /*
// Copyright 2024 The Grove Authors.
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
// */

package podcliqueset

import (
	"context"
	"reflect"

	"github.com/ai-dynamo/grove/operator/api/common/constants"
	grovecorev1alpha1 "github.com/ai-dynamo/grove/operator/api/core/v1alpha1"
	componentutils "github.com/ai-dynamo/grove/operator/internal/controller/common/component/utils"
	grovectrlutils "github.com/ai-dynamo/grove/operator/internal/controller/utils"
	"github.com/ai-dynamo/grove/operator/internal/utils"

	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/builder"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller"
	"sigs.k8s.io/controller-runtime/pkg/event"
	"sigs.k8s.io/controller-runtime/pkg/handler"
	ctrllogger "sigs.k8s.io/controller-runtime/pkg/log"
	"sigs.k8s.io/controller-runtime/pkg/manager"
	"sigs.k8s.io/controller-runtime/pkg/predicate"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"
)

const (
	controllerName = "podcliqueset-controller"
)

// RegisterWithManager registers the PodCliqueSet Reconciler with the manager.
func (r *Reconciler) RegisterWithManager(mgr manager.Manager) error {
	return builder.ControllerManagedBy(mgr).
		Named(controllerName).
		WithOptions(controller.Options{
			MaxConcurrentReconciles: *r.config.ConcurrentSyncs,
		}).
		For(&grovecorev1alpha1.PodCliqueSet{}, builder.WithPredicates(podCliqueSetPredicate())).
		Watches(
			&grovecorev1alpha1.ClusterTopologyBinding{},
			handler.EnqueueRequestsFromMapFunc(mapClusterTopologyToPodCliqueSets(r.client)),
		).
		Watches(
			&grovecorev1alpha1.PodClique{},
			handler.EnqueueRequestsFromMapFunc(mapPodCliqueToPodCliqueSet()),
			builder.WithPredicates(podCliquePredicate()),
		).
		Watches(
			&grovecorev1alpha1.PodCliqueScalingGroup{},
			handler.EnqueueRequestsFromMapFunc(mapPodCliqueScaleGroupToPodCliqueSet()),
			builder.WithPredicates(podCliqueScalingGroupPredicate()),
		).
		Complete(r)
}

// podCliqueSetPredicate returns a predicate that allows spec changes and explicit no-op reconcile triggers.
func podCliqueSetPredicate() predicate.Predicate {
	return predicate.Funcs{
		CreateFunc: func(_ event.CreateEvent) bool { return true },
		DeleteFunc: func(_ event.DeleteEvent) bool { return true },
		UpdateFunc: func(updateEvent event.UpdateEvent) bool {
			if updateEvent.ObjectOld == nil || updateEvent.ObjectNew == nil {
				return false
			}
			specChanged := hasSpecChanged(updateEvent)
			reconcileTriggerChanged := hasAnnotationChanged(updateEvent.ObjectOld.GetAnnotations(), updateEvent.ObjectNew.GetAnnotations(), constants.AnnotationReconcileTrigger)
			enqueue := specChanged || reconcileTriggerChanged
			ctrllogger.Log.WithName(controllerName).Info("RU11_DIAG PCS primary watch observed update",
				"pcs", client.ObjectKeyFromObject(updateEvent.ObjectNew),
				"oldResourceVersion", updateEvent.ObjectOld.GetResourceVersion(),
				"newResourceVersion", updateEvent.ObjectNew.GetResourceVersion(),
				"oldGeneration", updateEvent.ObjectOld.GetGeneration(),
				"newGeneration", updateEvent.ObjectNew.GetGeneration(),
				"specChanged", specChanged,
				"oldReconcileTrigger", updateEvent.ObjectOld.GetAnnotations()[constants.AnnotationReconcileTrigger],
				"newReconcileTrigger", updateEvent.ObjectNew.GetAnnotations()[constants.AnnotationReconcileTrigger],
				"reconcileTriggerChanged", reconcileTriggerChanged,
				"enqueue", enqueue)
			return enqueue
		},
		GenericFunc: func(_ event.GenericEvent) bool { return true },
	}
}

// mapPodCliqueToPodCliqueSet returns a function that maps PodClique events to their parent PodCliqueSet.
func mapPodCliqueToPodCliqueSet() handler.MapFunc {
	return func(ctx context.Context, obj client.Object) []reconcile.Request {
		pclq, ok := obj.(*grovecorev1alpha1.PodClique)
		if !ok {
			return nil
		}
		pcsName := componentutils.GetPodCliqueSetName(pclq.ObjectMeta)
		requests := []reconcile.Request{{NamespacedName: types.NamespacedName{Name: pcsName, Namespace: pclq.Namespace}}}
		ctrllogger.FromContext(ctx).WithName(controllerName).Info("RU11_DIAG PCS watch mapped PCLQ event",
			"pclq", client.ObjectKeyFromObject(pclq),
			"pclqResourceVersion", pclq.ResourceVersion,
			"pclqGeneration", pclq.Generation,
			"pclqReplicas", pclq.Status.Replicas,
			"pclqReadyReplicas", pclq.Status.ReadyReplicas,
			"pclqUpdatedReplicas", pclq.Status.UpdatedReplicas,
			"pclqCurrentPodTemplateHash", watchStringPointerValue(pclq.Status.CurrentPodTemplateHash),
			"pclqCurrentGenerationHash", watchStringPointerValue(pclq.Status.CurrentPodCliqueSetGenerationHash),
			"pclqUpdateProgressPresent", pclq.Status.UpdateProgress != nil,
			"pclqUpdateTargetPodTemplateHash", watchPCLQUpdateTargetPodTemplateHash(pclq),
			"pclqUpdateTargetGenerationHash", watchPCLQUpdateTargetGenerationHash(pclq),
			"pclqUpdateEnded", pclq.Status.UpdateProgress != nil && pclq.Status.UpdateProgress.UpdateEndedAt != nil,
			"enqueuedPCS", diagnosticRequestNames(requests))
		return requests
	}
}

// mapPodCliqueScaleGroupToPodCliqueSet returns a function that maps PCSG events to their parent PodCliqueSet.
func mapPodCliqueScaleGroupToPodCliqueSet() handler.MapFunc {
	return func(ctx context.Context, obj client.Object) []reconcile.Request {
		pcsg, ok := obj.(*grovecorev1alpha1.PodCliqueScalingGroup)
		if !ok {
			return nil
		}
		pcsName := componentutils.GetPodCliqueSetName(pcsg.ObjectMeta)
		requests := []reconcile.Request{{NamespacedName: types.NamespacedName{Name: pcsName, Namespace: pcsg.Namespace}}}
		ctrllogger.FromContext(ctx).WithName(controllerName).Info("RU11_DIAG PCS watch mapped PCSG event",
			"pcsg", client.ObjectKeyFromObject(pcsg),
			"pcsgResourceVersion", pcsg.ResourceVersion,
			"pcsgGeneration", pcsg.Generation,
			"pcsgReplicas", pcsg.Status.Replicas,
			"pcsgAvailableReplicas", pcsg.Status.AvailableReplicas,
			"pcsgUpdatedReplicas", pcsg.Status.UpdatedReplicas,
			"pcsgCurrentGenerationHash", watchStringPointerValue(pcsg.Status.CurrentPodCliqueSetGenerationHash),
			"pcsgUpdateProgressPresent", pcsg.Status.UpdateProgress != nil,
			"pcsgUpdateTargetGenerationHash", watchPCSGUpdateTargetGenerationHash(pcsg),
			"pcsgUpdateEnded", pcsg.Status.UpdateProgress != nil && pcsg.Status.UpdateProgress.UpdateEndedAt != nil,
			"enqueuedPCS", diagnosticRequestNames(requests))
		return requests
	}
}

// mapClusterTopologyToPodCliqueSets returns a function that maps ClusterTopologyBinding events to PodCliqueSets
// whose explicit topology constraints resolve to this ClusterTopologyBinding.
func mapClusterTopologyToPodCliqueSets(cl client.Client) handler.MapFunc {
	return func(ctx context.Context, obj client.Object) []reconcile.Request {
		ct, ok := obj.(*grovecorev1alpha1.ClusterTopologyBinding)
		if !ok {
			return nil
		}

		pcsList := &grovecorev1alpha1.PodCliqueSetList{}
		if err := cl.List(ctx, pcsList); err != nil {
			return nil
		}

		requests := make([]reconcile.Request, 0, len(pcsList.Items))
		for i := range pcsList.Items {
			pcs := &pcsList.Items[i]
			topologyName, err := componentutils.FindExplicitTopologyNameForPodCliqueSet(pcs)
			if err != nil || topologyName != ct.Name {
				continue
			}
			requests = append(requests, reconcile.Request{
				NamespacedName: types.NamespacedName{Name: pcs.Name, Namespace: pcs.Namespace},
			})
		}
		return requests
	}
}

// podCliquePredicate returns a predicate that filters PodClique events based on ownership and changes.
func podCliquePredicate() predicate.Predicate {
	return predicate.Funcs{
		CreateFunc: func(_ event.CreateEvent) bool { return false },
		DeleteFunc: func(deleteEvent event.DeleteEvent) bool {
			return grovectrlutils.IsManagedPodClique(deleteEvent.Object, constants.KindPodCliqueSet)
		},
		UpdateFunc: func(updateEvent event.UpdateEvent) bool {
			oldPCLQ, okOld := updateEvent.ObjectOld.(*grovecorev1alpha1.PodClique)
			newPCLQ, okNew := updateEvent.ObjectNew.(*grovecorev1alpha1.PodClique)
			if !okOld || !okNew {
				return false
			}
			managed := grovectrlutils.IsManagedPodClique(oldPCLQ, constants.KindPodCliqueSet, constants.KindPodCliqueScalingGroup)
			specChanged := hasSpecChanged(updateEvent)
			replicasChanged := hasAnyStatusReplicasChanged(oldPCLQ.Status, newPCLQ.Status)
			hashesChanged := hasPodCliqueHashStatusChanged(oldPCLQ.Status, newPCLQ.Status)
			updateProgressChanged := hasUpdateStatusChanged(oldPCLQ.Status.UpdateProgress, newPCLQ.Status.UpdateProgress)
			minAvailableConditionChanged := hasMinAvailableBreachedConditionChanged(oldPCLQ.Status.Conditions, newPCLQ.Status.Conditions)
			statusChanged := replicasChanged || hashesChanged || updateProgressChanged || minAvailableConditionChanged
			enqueue := managed && (specChanged || statusChanged)
			ctrllogger.Log.WithName(controllerName).Info("RU11_DIAG PCS watch observed PCLQ update",
				"pclq", client.ObjectKeyFromObject(newPCLQ),
				"oldResourceVersion", oldPCLQ.ResourceVersion,
				"newResourceVersion", newPCLQ.ResourceVersion,
				"oldGeneration", oldPCLQ.Generation,
				"newGeneration", newPCLQ.Generation,
				"managed", managed,
				"specChanged", specChanged,
				"oldReplicas", oldPCLQ.Status.Replicas,
				"newReplicas", newPCLQ.Status.Replicas,
				"oldReadyReplicas", oldPCLQ.Status.ReadyReplicas,
				"newReadyReplicas", newPCLQ.Status.ReadyReplicas,
				"oldUpdatedReplicas", oldPCLQ.Status.UpdatedReplicas,
				"newUpdatedReplicas", newPCLQ.Status.UpdatedReplicas,
				"replicasChanged", replicasChanged,
				"oldCurrentPodTemplateHash", watchStringPointerValue(oldPCLQ.Status.CurrentPodTemplateHash),
				"newCurrentPodTemplateHash", watchStringPointerValue(newPCLQ.Status.CurrentPodTemplateHash),
				"oldCurrentGenerationHash", watchStringPointerValue(oldPCLQ.Status.CurrentPodCliqueSetGenerationHash),
				"newCurrentGenerationHash", watchStringPointerValue(newPCLQ.Status.CurrentPodCliqueSetGenerationHash),
				"hashesChanged", hashesChanged,
				"oldUpdateProgressPresent", oldPCLQ.Status.UpdateProgress != nil,
				"newUpdateProgressPresent", newPCLQ.Status.UpdateProgress != nil,
				"oldUpdateTargetGenerationHash", watchPCLQUpdateTargetGenerationHash(oldPCLQ),
				"newUpdateTargetGenerationHash", watchPCLQUpdateTargetGenerationHash(newPCLQ),
				"oldUpdateTargetPodTemplateHash", watchPCLQUpdateTargetPodTemplateHash(oldPCLQ),
				"newUpdateTargetPodTemplateHash", watchPCLQUpdateTargetPodTemplateHash(newPCLQ),
				"oldUpdateEnded", oldPCLQ.Status.UpdateProgress != nil && oldPCLQ.Status.UpdateProgress.UpdateEndedAt != nil,
				"newUpdateEnded", newPCLQ.Status.UpdateProgress != nil && newPCLQ.Status.UpdateProgress.UpdateEndedAt != nil,
				"updateProgressChanged", updateProgressChanged,
				"minAvailableConditionChanged", minAvailableConditionChanged,
				"statusChanged", statusChanged,
				"enqueue", enqueue)
			return enqueue
		},
		GenericFunc: func(_ event.GenericEvent) bool { return false },
	}
}

// podCliqueScalingGroupPredicate returns a predicate that filters PCSG events for relevant status changes.
func podCliqueScalingGroupPredicate() predicate.Predicate {
	return predicate.Funcs{
		CreateFunc: func(_ event.CreateEvent) bool { return false },
		DeleteFunc: func(_ event.DeleteEvent) bool { return false },
		UpdateFunc: func(updateEvent event.UpdateEvent) bool {
			oldPCSG, okOld := updateEvent.ObjectOld.(*grovecorev1alpha1.PodCliqueScalingGroup)
			newPCSG, okNew := updateEvent.ObjectNew.(*grovecorev1alpha1.PodCliqueScalingGroup)
			if !okOld || !okNew {
				return false
			}
			minAvailableConditionChanged := hasMinAvailableBreachedConditionChanged(oldPCSG.Status.Conditions, newPCSG.Status.Conditions)
			replicasChanged := oldPCSG.Status.AvailableReplicas != newPCSG.Status.AvailableReplicas ||
				oldPCSG.Status.UpdatedReplicas != newPCSG.Status.UpdatedReplicas
			generationHashChanged := !stringPointersEqual(oldPCSG.Status.CurrentPodCliqueSetGenerationHash, newPCSG.Status.CurrentPodCliqueSetGenerationHash)
			updateProgressChanged := hasUpdateStatusChanged(oldPCSG.Status.UpdateProgress, newPCSG.Status.UpdateProgress)
			statusChanged := replicasChanged || generationHashChanged || updateProgressChanged
			enqueue := minAvailableConditionChanged || statusChanged
			ctrllogger.Log.WithName(controllerName).Info("RU11_DIAG PCS watch observed PCSG update",
				"pcsg", client.ObjectKeyFromObject(newPCSG),
				"oldResourceVersion", oldPCSG.ResourceVersion,
				"newResourceVersion", newPCSG.ResourceVersion,
				"oldGeneration", oldPCSG.Generation,
				"newGeneration", newPCSG.Generation,
				"oldAvailableReplicas", oldPCSG.Status.AvailableReplicas,
				"newAvailableReplicas", newPCSG.Status.AvailableReplicas,
				"oldUpdatedReplicas", oldPCSG.Status.UpdatedReplicas,
				"newUpdatedReplicas", newPCSG.Status.UpdatedReplicas,
				"replicasChanged", replicasChanged,
				"oldCurrentGenerationHash", watchStringPointerValue(oldPCSG.Status.CurrentPodCliqueSetGenerationHash),
				"newCurrentGenerationHash", watchStringPointerValue(newPCSG.Status.CurrentPodCliqueSetGenerationHash),
				"generationHashChanged", generationHashChanged,
				"oldUpdateProgressPresent", oldPCSG.Status.UpdateProgress != nil,
				"newUpdateProgressPresent", newPCSG.Status.UpdateProgress != nil,
				"oldUpdateTargetGenerationHash", watchPCSGUpdateTargetGenerationHash(oldPCSG),
				"newUpdateTargetGenerationHash", watchPCSGUpdateTargetGenerationHash(newPCSG),
				"oldUpdateEnded", oldPCSG.Status.UpdateProgress != nil && oldPCSG.Status.UpdateProgress.UpdateEndedAt != nil,
				"newUpdateEnded", newPCSG.Status.UpdateProgress != nil && newPCSG.Status.UpdateProgress.UpdateEndedAt != nil,
				"updateProgressChanged", updateProgressChanged,
				"minAvailableConditionChanged", minAvailableConditionChanged,
				"statusChanged", statusChanged,
				"enqueue", enqueue)
			return enqueue
		},
		GenericFunc: func(_ event.TypedGenericEvent[client.Object]) bool { return false },
	}
}

// hasSpecChanged checks if the resource generation has changed.
func hasSpecChanged(updateEvent event.UpdateEvent) bool {
	return updateEvent.ObjectOld.GetGeneration() != updateEvent.ObjectNew.GetGeneration()
}

func hasAnnotationChanged(oldAnnotations, newAnnotations map[string]string, key string) bool {
	oldValue, oldOK := oldAnnotations[key]
	newValue, newOK := newAnnotations[key]
	return oldOK != newOK || oldValue != newValue
}

// hasAnyStatusReplicasChanged checks if any replica count fields have changed.
func hasAnyStatusReplicasChanged(oldPCLQStatus, newPCLQStatus grovecorev1alpha1.PodCliqueStatus) bool {
	return oldPCLQStatus.Replicas != newPCLQStatus.Replicas ||
		oldPCLQStatus.ReadyReplicas != newPCLQStatus.ReadyReplicas ||
		oldPCLQStatus.ScheduleGatedReplicas != newPCLQStatus.ScheduleGatedReplicas ||
		oldPCLQStatus.UpdatedReplicas != newPCLQStatus.UpdatedReplicas
}

func hasPodCliqueHashStatusChanged(oldPCLQStatus, newPCLQStatus grovecorev1alpha1.PodCliqueStatus) bool {
	return !stringPointersEqual(oldPCLQStatus.CurrentPodTemplateHash, newPCLQStatus.CurrentPodTemplateHash) ||
		!stringPointersEqual(oldPCLQStatus.CurrentPodCliqueSetGenerationHash, newPCLQStatus.CurrentPodCliqueSetGenerationHash)
}

// hasMinAvailableBreachedConditionChanged checks if the MinAvailableBreached condition has changed.
func hasMinAvailableBreachedConditionChanged(oldConditions, newConditions []metav1.Condition) bool {
	oldMinAvailableBreachedCond := meta.FindStatusCondition(oldConditions, constants.ConditionTypeMinAvailableBreached)
	newMinAvailableBreachedCond := meta.FindStatusCondition(newConditions, constants.ConditionTypeMinAvailableBreached)
	if utils.OnlyOneIsNil(oldMinAvailableBreachedCond, newMinAvailableBreachedCond) {
		return true
	}
	if oldMinAvailableBreachedCond != nil && newMinAvailableBreachedCond != nil {
		return oldMinAvailableBreachedCond.Status != newMinAvailableBreachedCond.Status
	}
	return false
}

// hasUpdateStatusChanged reports whether the update progress has changed between the old and new states.
func hasUpdateStatusChanged(oldProgress, newProgress any) bool {
	return !reflect.DeepEqual(oldProgress, newProgress)
}

// stringPointersEqual reports whether two *string values are equal, treating nil pointers as equal only to other nil pointers.
func stringPointersEqual(oldValue, newValue *string) bool {
	if oldValue == nil || newValue == nil {
		return oldValue == newValue
	}
	return *oldValue == *newValue
}

func watchStringPointerValue(value *string) string {
	if value == nil {
		return ""
	}
	return *value
}

func watchPCLQUpdateTargetGenerationHash(pclq *grovecorev1alpha1.PodClique) string {
	if pclq.Status.UpdateProgress == nil {
		return ""
	}
	return pclq.Status.UpdateProgress.PodCliqueSetGenerationHash
}

func watchPCLQUpdateTargetPodTemplateHash(pclq *grovecorev1alpha1.PodClique) string {
	if pclq.Status.UpdateProgress == nil {
		return ""
	}
	return pclq.Status.UpdateProgress.PodTemplateHash
}

func watchPCSGUpdateTargetGenerationHash(pcsg *grovecorev1alpha1.PodCliqueScalingGroup) string {
	if pcsg.Status.UpdateProgress == nil {
		return ""
	}
	return pcsg.Status.UpdateProgress.PodCliqueSetGenerationHash
}

func diagnosticRequestNames(requests []reconcile.Request) []string {
	names := make([]string, 0, len(requests))
	for _, request := range requests {
		names = append(names, request.String())
	}
	return names
}
