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

package validation

import (
	"context"
	"testing"

	apicommonconstants "github.com/ai-dynamo/grove/operator/api/common/constants"
	grovecorev1alpha1 "github.com/ai-dynamo/grove/operator/api/core/v1alpha1"
	"github.com/ai-dynamo/grove/operator/internal/constants"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/utils/ptr"
)

func TestValidateInjectedPodSpecNames(t *testing.T) {
	t.Run("container claim colliding with injected resource sharing claim is rejected", func(t *testing.T) {
		pcs := createTestPodCliqueSet("a")
		pcs.Spec.Replicas = 43
		pcs.Spec.Template.ResourceSharing = []grovecorev1alpha1.PCSResourceSharingSpec{{
			ResourceSharingSpec: grovecorev1alpha1.ResourceSharingSpec{
				Name:  "gpu",
				Scope: grovecorev1alpha1.ResourceSharingScopePerReplica,
			},
		}}
		pcs.Spec.Template.Cliques[0].Spec.PodSpec.Containers[0].Resources.Claims = []corev1.ResourceClaim{{
			Name: "a-42-gpu",
		}}

		errs := newGeneratedNameTestValidator(pcs).validateInjectedPodSpecNames()

		require.Len(t, errs, 1)
		assert.Equal(t, "spec.template.cliques[0].spec.podSpec.containers[0].resources.claims[0].name", errs[0].Field)
		assert.Contains(t, errs[0].Detail, "PodCliqueSet resourceSharing")
	})

	t.Run("resource sharing filter excluding clique avoids container claim collision", func(t *testing.T) {
		pcs := createTestPodCliqueSet("a")
		pcs.Spec.Template.ResourceSharing = []grovecorev1alpha1.PCSResourceSharingSpec{{
			ResourceSharingSpec: grovecorev1alpha1.ResourceSharingSpec{
				Name:  "gpu",
				Scope: grovecorev1alpha1.ResourceSharingScopeAllReplicas,
			},
			Filter: &grovecorev1alpha1.PCSResourceSharingFilter{
				ChildCliqueNames: []string{"other"},
			},
		}}
		pcs.Spec.Template.Cliques[0].Spec.PodSpec.Containers[0].Resources.Claims = []corev1.ResourceClaim{{
			Name: "a-all-gpu",
		}}

		assert.Empty(t, newGeneratedNameTestValidator(pcs).validateInjectedPodSpecNames())
	})

	t.Run("overlapping user container claim references are rejected", func(t *testing.T) {
		pcs := createTestPodCliqueSet("a")
		pcs.Spec.Template.Cliques[0].Spec.PodSpec.Containers[0].Resources.Claims = []corev1.ResourceClaim{
			{Name: "gpu", Request: "memory"},
			{Name: "gpu"},
		}

		errs := newGeneratedNameTestValidator(pcs).validateInjectedPodSpecNames()

		require.Len(t, errs, 1)
		assert.Equal(t, "spec.template.cliques[0].spec.podSpec.containers[0].resources.claims[1]", errs[0].Field)
		assert.Contains(t, errs[0].Detail, "entries overlap")
	})

	t.Run("distinct requests from one claim are allowed", func(t *testing.T) {
		pcs := createTestPodCliqueSet("a")
		pcs.Spec.Template.Cliques[0].Spec.PodSpec.Containers[0].Resources.Claims = []corev1.ResourceClaim{
			{Name: "gpu", Request: "memory"},
			{Name: "gpu", Request: "compute"},
		}

		assert.Empty(t, newGeneratedNameTestValidator(pcs).validateInjectedPodSpecNames())
	})

	t.Run("base Grove environment variable is reserved", func(t *testing.T) {
		pcs := createTestPodCliqueSet("a")
		pcs.Spec.Template.Cliques[0].Spec.PodSpec.Containers[0].Env = []corev1.EnvVar{{
			Name:  apicommonconstants.EnvVarPodCliqueSetName,
			Value: "custom",
		}}

		errs := newGeneratedNameTestValidator(pcs).validateInjectedPodSpecNames()

		require.Len(t, errs, 1)
		assert.Equal(t, "spec.template.cliques[0].spec.podSpec.containers[0].env[0].name", errs[0].Field)
		assert.Contains(t, errs[0].Detail, "reserved")
	})

	t.Run("PCSG environment variable is reserved only for grouped clique", func(t *testing.T) {
		pcs := createTestPodCliqueSet("a")
		pcs.Spec.Template.Cliques[0].Name = "worker"
		pcs.Spec.Template.Cliques[0].Spec.PodSpec.Containers[0].Env = []corev1.EnvVar{{
			Name:  apicommonconstants.EnvVarPodCliqueScalingGroupName,
			Value: "custom",
		}}

		assert.Empty(t, newGeneratedNameTestValidator(pcs).validateInjectedPodSpecNames())

		pcs.Spec.Template.PodCliqueScalingGroupConfigs = []grovecorev1alpha1.PodCliqueScalingGroupConfig{{
			Name:         "group",
			CliqueNames:  []string{"worker"},
			Replicas:     ptr.To(int32(1)),
			MinAvailable: ptr.To(int32(1)),
		}}
		errs := newGeneratedNameTestValidator(pcs).validateInjectedPodSpecNames()
		require.Len(t, errs, 1)
		assert.Contains(t, errs[0].Detail, "reserved")
	})

	t.Run("startup init container and volume names are reserved when injection is active", func(t *testing.T) {
		pcs := createTestPodCliqueSet("a")
		pcs.Spec.Template.StartupType = ptr.To(grovecorev1alpha1.CliqueStartupTypeInOrder)
		second := createDummyPodCliqueTemplate("second")
		second.Spec.PodSpec.InitContainers = []corev1.Container{{
			Name: constants.StartupInitContainerName,
		}}
		second.Spec.PodSpec.Volumes = []corev1.Volume{{
			Name: constants.StartupPodInfoVolumeName,
		}}
		pcs.Spec.Template.Cliques = append(pcs.Spec.Template.Cliques, second)

		errs := newGeneratedNameTestValidator(pcs).validateInjectedPodSpecNames()

		require.Len(t, errs, 2)
		assert.Contains(t, errs[0].Detail, "reserved for Grove startup-order injection")
		assert.Contains(t, errs[1].Detail, "reserved for Grove startup-order injection")
	})

	t.Run("startup names are allowed when no startup init container is injected", func(t *testing.T) {
		pcs := createTestPodCliqueSet("a")
		pcs.Spec.Template.StartupType = ptr.To(grovecorev1alpha1.CliqueStartupTypeAnyOrder)
		pcs.Spec.Template.Cliques[0].Spec.PodSpec.InitContainers = []corev1.Container{{
			Name: constants.StartupInitContainerName,
		}}
		pcs.Spec.Template.Cliques[0].Spec.PodSpec.Volumes = []corev1.Volume{{
			Name: constants.StartupPodInfoVolumeName,
		}}

		assert.Empty(t, newGeneratedNameTestValidator(pcs).validateInjectedPodSpecNames())
	})
}

func TestValidateInjectedPodSpecNamesOnUpdate(t *testing.T) {
	t.Run("unchanged legacy reserved environment variable is allowed", func(t *testing.T) {
		oldPCS := createTestPodCliqueSet("a")
		oldPCS.Spec.Template.Cliques[0].Spec.PodSpec.Containers[0].Env = []corev1.EnvVar{{
			Name:  apicommonconstants.EnvVarPodCliqueSetName,
			Value: "custom",
		}}
		newPCS := oldPCS.DeepCopy()
		newPCS.Labels = map[string]string{"updated": "true"}

		assert.Empty(t, newGeneratedNameTestValidator(newPCS).
			validateInjectedPodSpecNamesOnUpdate(oldPCS))
	})

	t.Run("reordered legacy conflicts are allowed", func(t *testing.T) {
		oldPCS := createTestPodCliqueSet("a")
		oldPCS.Spec.Template.Cliques[0].Spec.PodSpec.Containers[0].Env = []corev1.EnvVar{
			{Name: "USER_ENV", Value: "value"},
			{Name: apicommonconstants.EnvVarPodCliqueSetName, Value: "custom"},
		}
		newPCS := oldPCS.DeepCopy()
		newPCS.Spec.Template.Cliques[0].Spec.PodSpec.Containers[0].Env[0],
			newPCS.Spec.Template.Cliques[0].Spec.PodSpec.Containers[0].Env[1] =
			newPCS.Spec.Template.Cliques[0].Spec.PodSpec.Containers[0].Env[1],
			newPCS.Spec.Template.Cliques[0].Spec.PodSpec.Containers[0].Env[0]

		assert.Empty(t, newGeneratedNameTestValidator(newPCS).
			validateInjectedPodSpecNamesOnUpdate(oldPCS))
	})

	t.Run("additional legacy conflict with the same identity is rejected", func(t *testing.T) {
		oldPCS := createTestPodCliqueSet("a")
		oldPCS.Spec.Template.Cliques[0].Spec.PodSpec.Containers[0].Env = []corev1.EnvVar{{
			Name:  apicommonconstants.EnvVarPodCliqueSetName,
			Value: "custom",
		}}
		newPCS := oldPCS.DeepCopy()
		newPCS.Spec.Template.Cliques[0].Spec.PodSpec.Containers[0].Env = append(
			newPCS.Spec.Template.Cliques[0].Spec.PodSpec.Containers[0].Env,
			corev1.EnvVar{
				Name:  apicommonconstants.EnvVarPodCliqueSetName,
				Value: "second",
			},
		)

		errs := newGeneratedNameTestValidator(newPCS).
			validateInjectedPodSpecNamesOnUpdate(oldPCS)

		require.Len(t, errs, 1)
		assert.Equal(t, "spec.template.cliques[0].spec.podSpec.containers[0].env[1].name", errs[0].Field)
	})

	t.Run("scale out introducing container claim collision is rejected", func(t *testing.T) {
		oldPCS := createTestPodCliqueSet("a")
		oldPCS.Spec.Replicas = 42
		oldPCS.Spec.Template.ResourceSharing = []grovecorev1alpha1.PCSResourceSharingSpec{{
			ResourceSharingSpec: grovecorev1alpha1.ResourceSharingSpec{
				Name:  "gpu",
				Scope: grovecorev1alpha1.ResourceSharingScopePerReplica,
			},
		}}
		oldPCS.Spec.Template.Cliques[0].Spec.PodSpec.Containers[0].Resources.Claims = []corev1.ResourceClaim{{
			Name: "a-42-gpu",
		}}
		newPCS := oldPCS.DeepCopy()
		newPCS.Spec.Replicas = 43

		errs := newGeneratedNameTestValidator(newPCS).validateInjectedPodSpecNamesOnUpdate(oldPCS)

		require.Len(t, errs, 1)
		assert.Equal(t, "a-42-gpu", errs[0].BadValue)
	})
}

func TestHandlerValidatesInjectedPodSpecNames(t *testing.T) {
	pcs := createTestPodCliqueSet("a")
	pcs.Spec.Template.Cliques[0].Spec.PodSpec.Containers[0].Env = []corev1.EnvVar{{
		Name:  apicommonconstants.EnvVarPodCliqueSetName,
		Value: "custom",
	}}

	_, err := newGeneratedNameTestHandler(false).ValidateCreate(context.Background(), pcs)

	require.Error(t, err)
	assert.Contains(t, err.Error(), "reserved because Grove injects it")
}
