// /*
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
// */

package utils

import (
	"context"

	k8sutils "github.com/ai-dynamo/grove/operator/internal/utils/kubernetes"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

// IsOwnerBeingDeleted reports whether the owner referenced by ownerKind in child is being
// deleted or already gone. ownerObj is an empty typed object of the owner's kind, e.g.
// &grovecorev1alpha1.PodCliqueSet{}. On transient errors it returns false to avoid
// swallowing real events.
func IsOwnerBeingDeleted(ctx context.Context, cl client.Client, child client.Object, ownerKind string, ownerObj client.Object) bool {
	ownerRef := k8sutils.FindOwnerRefByKind(child.GetOwnerReferences(), ownerKind)
	if ownerRef == nil {
		return false
	}
	key := client.ObjectKey{Namespace: child.GetNamespace(), Name: ownerRef.Name}
	if err := cl.Get(ctx, key, ownerObj); err != nil {
		return apierrors.IsNotFound(err)
	}
	// Same-name recreate: ownerRef points at an owner that no longer exists.
	if ownerObj.GetUID() != ownerRef.UID {
		return true
	}
	return !ownerObj.GetDeletionTimestamp().IsZero()
}
