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

	apicommon "github.com/ai-dynamo/grove/operator/api/common"
	grovecorev1alpha1 "github.com/ai-dynamo/grove/operator/api/core/v1alpha1"
	grovectrlutils "github.com/ai-dynamo/grove/operator/internal/controller/utils"
	k8sutils "github.com/ai-dynamo/grove/operator/internal/utils/kubernetes"

	groveschedulerv1alpha1 "github.com/ai-dynamo/grove/scheduler/api/core/v1alpha1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/event"
	"sigs.k8s.io/controller-runtime/pkg/handler"
	"sigs.k8s.io/controller-runtime/pkg/log"
	"sigs.k8s.io/controller-runtime/pkg/predicate"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"
)

// mapSchedulingDependencyToPCLQs includes waiting siblings, not only the dependency's own pods.
// Pod events stay within their PCSG; gang policy and epoch changes affect the PCS replica.
func (r *Reconciler) mapSchedulingDependencyToPCLQs(direct handler.MapFunc) handler.MapFunc {
	return func(ctx context.Context, obj client.Object) []reconcile.Request {
		switch obj.(type) {
		case *corev1.Pod, *groveschedulerv1alpha1.PodGang, *grovecorev1alpha1.PodGangMap:
		default:
			owner := metav1.GetControllerOf(obj)
			if owner == nil || owner.Kind != "PodGang" || owner.APIVersion != groveschedulerv1alpha1.SchemeGroupVersion.String() {
				return nil
			}
			gang := &groveschedulerv1alpha1.PodGang{}
			if err := r.client.Get(ctx, client.ObjectKey{Namespace: obj.GetNamespace(), Name: owner.Name}, gang); err != nil {
				if client.IgnoreNotFound(err) != nil {
					log.FromContext(ctx).Error(err, "Failed to resolve backend resource owner")
				}
				return nil
			}
			if !metav1.IsControlledBy(obj, gang) {
				return nil
			}
			obj = gang
		}
		requests := make([]reconcile.Request, 0)
		if direct != nil {
			requests = direct(ctx, obj)
		}
		labels := obj.GetLabels()
		pcsName, replicaIndex := labels[apicommon.LabelPartOfKey], labels[apicommon.LabelPodCliqueSetReplicaIndex]
		if pcsName == "" || replicaIndex == "" || !grovectrlutils.IsManagedByGrove(labels) {
			return requests
		}
		selector := apicommon.GetDefaultLabelsForPodCliqueSetManagedResources(pcsName)
		selector[apicommon.LabelPodCliqueSetReplicaIndex] = replicaIndex
		if pod, ok := obj.(*corev1.Pod); ok {
			pcsgName := pod.Labels[apicommon.LabelPodCliqueScalingGroup]
			if pcsgName == "" {
				return requests
			}
			selector[apicommon.LabelPodCliqueScalingGroup] = pcsgName
		}
		pclqs := &grovecorev1alpha1.PodCliqueList{}
		if err := r.client.List(ctx, pclqs, client.InNamespace(obj.GetNamespace()), client.MatchingLabels(selector)); err != nil {
			log.FromContext(ctx).Error(err, "Failed to list PodCliques waiting for scheduling dependency")
			return requests
		}
		seen := make(map[client.ObjectKey]struct{}, len(requests)+len(pclqs.Items))
		for _, request := range requests {
			seen[request.NamespacedName] = struct{}{}
		}
		for i := range pclqs.Items {
			key := client.ObjectKeyFromObject(&pclqs.Items[i])
			if _, exists := seen[key]; !exists {
				requests = append(requests, reconcile.Request{NamespacedName: key})
				seen[key] = struct{}{}
			}
		}
		return requests
	}
}

func podSchedulingPredicate() predicate.Predicate {
	return predicate.Funcs{
		CreateFunc: func(e event.CreateEvent) bool {
			pod, ok := e.Object.(*corev1.Pod)
			return ok && isManagedPod(pod) && k8sutils.IsPodScheduled(pod)
		},
		DeleteFunc: func(e event.DeleteEvent) bool { return isManagedPod(e.Object) },
		UpdateFunc: func(e event.UpdateEvent) bool {
			oldPod, oldOK := e.ObjectOld.(*corev1.Pod)
			newPod, newOK := e.ObjectNew.(*corev1.Pod)
			return oldOK && newOK && isManagedPod(newPod) &&
				(hasConditionStatusChanged(oldPod.Status.Conditions, newPod.Status.Conditions, corev1.PodScheduled) ||
					oldPod.Spec.NodeName != newPod.Spec.NodeName || isMarkedForDeletion(e) ||
					k8sutils.IsPodActive(oldPod) != k8sutils.IsPodActive(newPod))
		},
		GenericFunc: func(_ event.GenericEvent) bool { return false },
	}
}
