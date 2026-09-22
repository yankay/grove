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

package defaulting

import (
	"context"
	"fmt"

	grovecorev1alpha1 "github.com/ai-dynamo/grove/operator/api/core/v1alpha1"

	admissionv1 "k8s.io/api/admission/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/utils/ptr"
	"sigs.k8s.io/controller-runtime/pkg/webhook/admission"
)

// Handler defaults the minimum for newly created PodCliques.
type Handler struct{}

// NewHandler creates a PodClique defaulting handler.
func NewHandler() *Handler {
	return &Handler{}
}

// Default initializes the quorum without changing replica intent.
func (h *Handler) Default(ctx context.Context, obj runtime.Object) error {
	pclq, ok := obj.(*grovecorev1alpha1.PodClique)
	if !ok {
		return fmt.Errorf("expected a PodClique object but got %T", obj)
	}
	req, err := admission.RequestFromContext(ctx)
	if err != nil {
		return err
	}
	if req.SubResource == "" && pclq.Spec.Replicas == nil {
		pclq.Spec.Replicas = ptr.To(int32(1))
	}
	// Updating a legacy object must not silently establish a new immutable quorum.
	if req.Operation == admissionv1.Create && req.SubResource == "" && pclq.Spec.MinAvailable == nil {
		pclq.Spec.MinAvailable = ptr.To(max(int32(1), ptr.Deref(pclq.Spec.Replicas, 1)))
	}
	return nil
}
