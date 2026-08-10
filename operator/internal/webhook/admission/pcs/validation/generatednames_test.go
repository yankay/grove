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
	"strconv"
	"strings"
	"testing"

	groveconfigv1alpha1 "github.com/ai-dynamo/grove/operator/api/config/v1alpha1"
	grovecorev1alpha1 "github.com/ai-dynamo/grove/operator/api/core/v1alpha1"
	"github.com/ai-dynamo/grove/operator/internal/scheduler"
	testutils "github.com/ai-dynamo/grove/operator/test/utils"

	"github.com/go-logr/logr"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	admissionv1 "k8s.io/api/admission/v1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/utils/ptr"
)

func TestBuildGeneratedResourceClaimNameChecks(t *testing.T) {
	pcs := createTestPodCliqueSet("root")
	pcs.Spec.Replicas = 2
	pcs.Spec.Template.ResourceSharing = []grovecorev1alpha1.PCSResourceSharingSpec{
		{ResourceSharingSpec: grovecorev1alpha1.ResourceSharingSpec{
			Name:  "pcs-all",
			Scope: grovecorev1alpha1.ResourceSharingScopeAllReplicas,
		}},
		{ResourceSharingSpec: grovecorev1alpha1.ResourceSharingSpec{
			Name:  "pcs-per",
			Scope: grovecorev1alpha1.ResourceSharingScopePerReplica,
		}},
	}

	standalone := pcs.Spec.Template.Cliques[0]
	standalone.Name = "solo"
	standalone.Spec.Replicas = 2
	standalone.ResourceSharing = []grovecorev1alpha1.ResourceSharingSpec{
		{Name: "standalone-all", Scope: grovecorev1alpha1.ResourceSharingScopeAllReplicas},
		{Name: "standalone-per", Scope: grovecorev1alpha1.ResourceSharingScopePerReplica},
	}

	grouped := createDummyPodCliqueTemplate("worker")
	grouped.Spec.Replicas = 3
	grouped.ResourceSharing = []grovecorev1alpha1.ResourceSharingSpec{
		{Name: "grouped-all", Scope: grovecorev1alpha1.ResourceSharingScopeAllReplicas},
		{Name: "grouped-per", Scope: grovecorev1alpha1.ResourceSharingScopePerReplica},
	}
	pcs.Spec.Template.Cliques = append(pcs.Spec.Template.Cliques, grouped)
	pcs.Spec.Template.PodCliqueScalingGroupConfigs = []grovecorev1alpha1.PodCliqueScalingGroupConfig{{
		Name:         "sg",
		CliqueNames:  []string{"worker"},
		Replicas:     ptr.To(int32(2)),
		MinAvailable: ptr.To(int32(1)),
		ResourceSharing: []grovecorev1alpha1.PCSGResourceSharingSpec{
			{ResourceSharingSpec: grovecorev1alpha1.ResourceSharingSpec{
				Name:  "pcsg-all",
				Scope: grovecorev1alpha1.ResourceSharingScopeAllReplicas,
			}},
			{ResourceSharingSpec: grovecorev1alpha1.ResourceSharingSpec{
				Name:  "pcsg-per",
				Scope: grovecorev1alpha1.ResourceSharingScopePerReplica,
			}},
		},
	}}

	checks := indexGeneratedResourceClaimNameChecks(buildGeneratedResourceClaimNameChecks(pcs))
	require.Len(t, checks, 8)
	assert.Equal(t, "root-all-pcs-all", checks["pcs/pcs-all/AllReplicas"].name)
	assert.Equal(t, "root-1-pcs-per", checks["pcs/pcs-per/PerReplica"].name)
	assert.Equal(t, "root-1-sg-all-pcsg-all", checks["pcsg/sg/pcsg-all/AllReplicas"].name)
	assert.Equal(t, "root-1-sg-1-pcsg-per", checks["pcsg/sg/pcsg-per/PerReplica"].name)
	assert.Equal(t, "root-1-solo-all-standalone-all", checks["pclq/solo/standalone/standalone-all/AllReplicas"].name)
	assert.Equal(t, "root-1-solo-1-standalone-per", checks["pclq/solo/standalone/standalone-per/PerReplica"].name)
	assert.Equal(t, "root-1-sg-1-worker-all-grouped-all", checks["pclq/worker/pcsg/sg/grouped-all/AllReplicas"].name)
	assert.Equal(t, "root-1-sg-1-worker-2-grouped-per", checks["pclq/worker/pcsg/sg/grouped-per/PerReplica"].name)
}

func TestValidateGeneratedResourceClaimNames(t *testing.T) {
	tests := []struct {
		name          string
		pcsName       string
		refName       string
		expectError   bool
		errorContains string
	}{
		{
			name:        "63-character generated name is valid",
			pcsName:     "p",
			refName:     strings.Repeat("a", 57),
			expectError: false,
		},
		{
			name:          "64-character generated name is rejected",
			pcsName:       "p",
			refName:       strings.Repeat("a", 58),
			expectError:   true,
			errorContains: "must be no more than 63 characters",
		},
		{
			name:          "dot in generated claim alias is rejected",
			pcsName:       "p",
			refName:       "gpu.pool",
			expectError:   true,
			errorContains: "must not contain dots",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			pcs := createTestPodCliqueSet(tc.pcsName)
			pcs.Spec.Template.ResourceSharing = []grovecorev1alpha1.PCSResourceSharingSpec{{
				ResourceSharingSpec: grovecorev1alpha1.ResourceSharingSpec{
					Name:  tc.refName,
					Scope: grovecorev1alpha1.ResourceSharingScopeAllReplicas,
				},
			}}
			validator := newGeneratedNameTestValidator(pcs)

			errs := validator.validateGeneratedResourceClaimNames()
			if tc.expectError {
				require.Len(t, errs, 1)
				assert.Contains(t, errs[0].Detail, tc.errorContains)
				assert.Equal(t, "spec.template.resourceSharing[0].name", errs[0].Field)
				return
			}
			assert.Empty(t, errs)
		})
	}
}

func TestValidateGeneratedResourceClaimNamesUsesAutoscalingMaximum(t *testing.T) {
	tests := []struct {
		name              string
		validMaxReplicas  int32
		invalidMaxReplica int32
		refNameLength     int
		expectedIndex     string
	}{
		{
			name:              "index 9 to 10",
			validMaxReplicas:  10,
			invalidMaxReplica: 11,
			refNameLength:     17,
			expectedIndex:     "-10-",
		},
		{
			name:              "index 99 to 100",
			validMaxReplicas:  100,
			invalidMaxReplica: 101,
			refNameLength:     16,
			expectedIndex:     "-100-",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			pcs := createTestPodCliqueSet(strings.Repeat("p", 30))
			clique := pcs.Spec.Template.Cliques[0]
			clique.Name = strings.Repeat("c", 10)
			clique.ResourceSharing = []grovecorev1alpha1.ResourceSharingSpec{{
				Name:  strings.Repeat("r", tc.refNameLength),
				Scope: grovecorev1alpha1.ResourceSharingScopePerReplica,
			}}
			clique.Spec.ScaleConfig = &grovecorev1alpha1.AutoScalingConfig{
				MinReplicas: ptr.To(int32(1)),
				MaxReplicas: tc.validMaxReplicas,
			}

			validator := newGeneratedNameTestValidator(pcs)
			assert.Empty(t, validator.validateGeneratedResourceClaimNames())

			clique.Spec.ScaleConfig.MaxReplicas = tc.invalidMaxReplica
			errs := validator.validateGeneratedResourceClaimNames()
			require.Len(t, errs, 1)
			assert.Contains(t, errs[0].BadValue.(string), tc.expectedIndex)
			assert.Contains(t, errs[0].Detail, "must be no more than 63 characters")
		})
	}
}

func TestValidateGeneratedResourceClaimNameCollisions(t *testing.T) {
	t.Run("PCS and standalone PodClique names collide", func(t *testing.T) {
		pcs := createTestPodCliqueSet("a")
		pcs.Spec.Template.ResourceSharing = []grovecorev1alpha1.PCSResourceSharingSpec{{
			ResourceSharingSpec: grovecorev1alpha1.ResourceSharingSpec{
				Name:  "b-all-c",
				Scope: grovecorev1alpha1.ResourceSharingScopePerReplica,
			},
		}}
		clique := pcs.Spec.Template.Cliques[0]
		clique.Name = "b"
		clique.ResourceSharing = []grovecorev1alpha1.ResourceSharingSpec{{
			Name:  "c",
			Scope: grovecorev1alpha1.ResourceSharingScopeAllReplicas,
		}}

		errs := newGeneratedNameTestValidator(pcs).validateGeneratedResourceClaimNames()

		require.Len(t, errs, 1)
		assert.Equal(t, "spec.template.cliques[0].resourceSharing[0].name", errs[0].Field)
		assert.Equal(t, "a-0-b-all-c", errs[0].BadValue)
		assert.Contains(t, errs[0].Detail, "collides between PodCliqueSet and standalone PodClique")
	})

	t.Run("collision outside replica bound is allowed", func(t *testing.T) {
		pcs := createPCSGResourceClaimCollisionTestPCS(42)

		assert.Empty(t, newGeneratedNameTestValidator(pcs).validateGeneratedResourceClaimNames())
	})

	t.Run("collision at replica bound is rejected", func(t *testing.T) {
		pcs := createPCSGResourceClaimCollisionTestPCS(43)

		errs := newGeneratedNameTestValidator(pcs).validateGeneratedResourceClaimNames()

		require.Len(t, errs, 1)
		assert.Equal(t, "spec.template.cliques[0].resourceSharing[0].name", errs[0].Field)
		assert.Equal(t, "a-0-g-42-x-all-y", errs[0].BadValue)
		assert.Contains(t, errs[0].Detail, `PodCliqueScalingGroup "g"`)
		assert.Contains(t, errs[0].Detail, `standalone PodClique "g-42-x"`)
	})
}

func TestValidatePodResourceClaimAliasCollisions(t *testing.T) {
	t.Run("duplicate user-defined aliases are rejected", func(t *testing.T) {
		pcs := createTestPodCliqueSet("a")
		pcs.Spec.Template.Cliques[0].Spec.PodSpec.ResourceClaims = []corev1.PodResourceClaim{
			{Name: "shared", ResourceClaimName: ptr.To("first")},
			{Name: "shared", ResourceClaimName: ptr.To("second")},
		}

		errs := newGeneratedNameTestValidator(pcs).validatePodResourceClaimAliasCollisions(false)

		require.Len(t, errs, 1)
		assert.Equal(t, "spec.template.cliques[0].spec.podSpec.resourceClaims[1].name", errs[0].Field)
		assert.Equal(t, "shared", errs[0].BadValue)
	})

	t.Run("user-defined alias colliding with injected PCS claim is rejected", func(t *testing.T) {
		pcs := createTestPodCliqueSet("a")
		pcs.Spec.Template.ResourceSharing = []grovecorev1alpha1.PCSResourceSharingSpec{{
			ResourceSharingSpec: grovecorev1alpha1.ResourceSharingSpec{
				Name:  "gpu",
				Scope: grovecorev1alpha1.ResourceSharingScopeAllReplicas,
			},
		}}
		pcs.Spec.Template.Cliques[0].Spec.PodSpec.ResourceClaims = []corev1.PodResourceClaim{{
			Name:              "a-all-gpu",
			ResourceClaimName: ptr.To("user-claim"),
		}}

		errs := newGeneratedNameTestValidator(pcs).validatePodResourceClaimAliasCollisions(false)

		require.Len(t, errs, 1)
		assert.Equal(t, "spec.template.cliques[0].spec.podSpec.resourceClaims[0].name", errs[0].Field)
		assert.Equal(t, "a-all-gpu", errs[0].BadValue)
		assert.Contains(t, errs[0].Detail, "PodCliqueSet resourceSharing")
	})

	t.Run("PCS filter excluding clique avoids alias collision", func(t *testing.T) {
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
		pcs.Spec.Template.Cliques[0].Spec.PodSpec.ResourceClaims = []corev1.PodResourceClaim{{
			Name:              "a-all-gpu",
			ResourceClaimName: ptr.To("user-claim"),
		}}

		assert.Empty(t, newGeneratedNameTestValidator(pcs).validatePodResourceClaimAliasCollisions(false))
	})

	t.Run("PCSG filter is applied per clique", func(t *testing.T) {
		pcs := createTestPodCliqueSet("a")
		worker := pcs.Spec.Template.Cliques[0]
		worker.Name = "worker"
		worker.Spec.PodSpec.ResourceClaims = []corev1.PodResourceClaim{{
			Name:              "a-0-group-all-gpu",
			ResourceClaimName: ptr.To("user-claim"),
		}}
		sidecar := createDummyPodCliqueTemplate("sidecar")
		sidecar.Spec.PodSpec.ResourceClaims = []corev1.PodResourceClaim{{
			Name:              "a-0-group-all-gpu",
			ResourceClaimName: ptr.To("user-claim"),
		}}
		pcs.Spec.Template.Cliques = append(pcs.Spec.Template.Cliques, sidecar)
		pcs.Spec.Template.PodCliqueScalingGroupConfigs = []grovecorev1alpha1.PodCliqueScalingGroupConfig{{
			Name:         "group",
			CliqueNames:  []string{"worker", "sidecar"},
			Replicas:     ptr.To(int32(1)),
			MinAvailable: ptr.To(int32(1)),
			ResourceSharing: []grovecorev1alpha1.PCSGResourceSharingSpec{{
				ResourceSharingSpec: grovecorev1alpha1.ResourceSharingSpec{
					Name:  "gpu",
					Scope: grovecorev1alpha1.ResourceSharingScopeAllReplicas,
				},
				Filter: &grovecorev1alpha1.PCSGResourceSharingFilter{
					ChildCliqueNames: []string{"worker"},
				},
			}},
		}}

		errs := newGeneratedNameTestValidator(pcs).validatePodResourceClaimAliasCollisions(false)

		require.Len(t, errs, 1)
		assert.Equal(t, "spec.template.cliques[0].spec.podSpec.resourceClaims[0].name", errs[0].Field)
		assert.Contains(t, errs[0].Detail, `PodClique "worker"`)
	})

	t.Run("Auto-MNNVL reserved alias is rejected for enrolled GPU clique", func(t *testing.T) {
		pcs := createValidPCSWithGPU(map[string]string{
			"grove.io/mnnvl-group": "fabric",
		})
		pcs.Spec.Template.Cliques[0].Spec.PodSpec.ResourceClaims = []corev1.PodResourceClaim{{
			Name:              "mnnvl-claim",
			ResourceClaimName: ptr.To("user-claim"),
		}}

		errs := newGeneratedNameTestValidator(pcs).validatePodResourceClaimAliasCollisions(true)

		require.Len(t, errs, 1)
		assert.Equal(t, "spec.template.cliques[0].spec.podSpec.resourceClaims[0].name", errs[0].Field)
		assert.Contains(t, errs[0].Detail, "Auto-MNNVL injected claim")
	})

	t.Run("Auto-MNNVL alias is allowed when feature is disabled", func(t *testing.T) {
		pcs := createValidPCSWithGPU(map[string]string{
			"grove.io/mnnvl-group": "fabric",
		})
		pcs.Spec.Template.Cliques[0].Spec.PodSpec.ResourceClaims = []corev1.PodResourceClaim{{
			Name:              "mnnvl-claim",
			ResourceClaimName: ptr.To("user-claim"),
		}}

		assert.Empty(t, newGeneratedNameTestValidator(pcs).validatePodResourceClaimAliasCollisions(false))
	})
}

func TestValidatePodResourceClaimAliasCollisionsOnUpdate(t *testing.T) {
	t.Run("unchanged legacy collision is allowed", func(t *testing.T) {
		oldPCS := createTestPodCliqueSet("a")
		oldPCS.Spec.Template.ResourceSharing = []grovecorev1alpha1.PCSResourceSharingSpec{{
			ResourceSharingSpec: grovecorev1alpha1.ResourceSharingSpec{
				Name:  "gpu",
				Scope: grovecorev1alpha1.ResourceSharingScopeAllReplicas,
			},
		}}
		oldPCS.Spec.Template.Cliques[0].Spec.PodSpec.ResourceClaims = []corev1.PodResourceClaim{{
			Name:              "a-all-gpu",
			ResourceClaimName: ptr.To("user-claim"),
		}}
		newPCS := oldPCS.DeepCopy()
		newPCS.Labels = map[string]string{"updated": "true"}

		assert.Empty(t, newGeneratedNameTestValidator(newPCS).
			validatePodResourceClaimAliasCollisionsOnUpdate(oldPCS, false))
	})

	t.Run("reordered legacy collisions are allowed", func(t *testing.T) {
		oldPCS := createTestPodCliqueSet("a")
		oldPCS.Spec.Template.Cliques[0].Spec.PodSpec.ResourceClaims = []corev1.PodResourceClaim{
			{Name: "shared", ResourceClaimName: ptr.To("first")},
			{Name: "other", ResourceClaimName: ptr.To("other")},
			{Name: "shared", ResourceClaimName: ptr.To("second")},
		}
		newPCS := oldPCS.DeepCopy()
		claims := newPCS.Spec.Template.Cliques[0].Spec.PodSpec.ResourceClaims
		claims[0], claims[1], claims[2] = claims[2], claims[0], claims[1]

		assert.Empty(t, newGeneratedNameTestValidator(newPCS).
			validatePodResourceClaimAliasCollisionsOnUpdate(oldPCS, false))
	})

	t.Run("additional legacy collision with the same identity is rejected", func(t *testing.T) {
		oldPCS := createTestPodCliqueSet("a")
		oldPCS.Spec.Template.Cliques[0].Spec.PodSpec.ResourceClaims = []corev1.PodResourceClaim{
			{Name: "shared", ResourceClaimName: ptr.To("first")},
			{Name: "shared", ResourceClaimName: ptr.To("second")},
		}
		newPCS := oldPCS.DeepCopy()
		newPCS.Spec.Template.Cliques[0].Spec.PodSpec.ResourceClaims = append(
			newPCS.Spec.Template.Cliques[0].Spec.PodSpec.ResourceClaims,
			corev1.PodResourceClaim{
				Name:              "shared",
				ResourceClaimName: ptr.To("third"),
			},
		)

		errs := newGeneratedNameTestValidator(newPCS).
			validatePodResourceClaimAliasCollisionsOnUpdate(oldPCS, false)

		require.NotEmpty(t, errs)
		assert.Equal(t, "spec.template.cliques[0].spec.podSpec.resourceClaims[2].name", errs[0].Field)
	})

	t.Run("scale out introducing alias collision is rejected", func(t *testing.T) {
		oldPCS := createTestPodCliqueSet("a")
		oldPCS.Spec.Replicas = 42
		oldPCS.Spec.Template.ResourceSharing = []grovecorev1alpha1.PCSResourceSharingSpec{{
			ResourceSharingSpec: grovecorev1alpha1.ResourceSharingSpec{
				Name:  "gpu",
				Scope: grovecorev1alpha1.ResourceSharingScopePerReplica,
			},
		}}
		oldPCS.Spec.Template.Cliques[0].Spec.PodSpec.ResourceClaims = []corev1.PodResourceClaim{{
			Name:              "a-42-gpu",
			ResourceClaimName: ptr.To("user-claim"),
		}}
		newPCS := oldPCS.DeepCopy()
		newPCS.Spec.Replicas = 43

		errs := newGeneratedNameTestValidator(newPCS).
			validatePodResourceClaimAliasCollisionsOnUpdate(oldPCS, false)

		require.Len(t, errs, 1)
		assert.Equal(t, "a-42-gpu", errs[0].BadValue)
	})

	t.Run("new Auto-MNNVL legacy collision is rejected", func(t *testing.T) {
		oldPCS := createValidPCSWithGPU(map[string]string{
			"grove.io/mnnvl-group": "fabric",
		})
		newPCS := oldPCS.DeepCopy()
		newPCS.Spec.Template.Cliques[0].Spec.PodSpec.ResourceClaims = []corev1.PodResourceClaim{{
			Name:              "mnnvl-claim",
			ResourceClaimName: ptr.To("user-claim"),
		}}

		errs := newGeneratedNameTestValidator(newPCS).
			validatePodResourceClaimAliasCollisionsOnUpdate(oldPCS, true)

		require.Len(t, errs, 1)
		assert.Contains(t, errs[0].Detail, "Auto-MNNVL")
	})
}

func TestValidateGeneratedObjectNameCollisions(t *testing.T) {
	t.Run("grouped PodClique label uses maximum replica indices", func(t *testing.T) {
		pcs := createTestPodCliqueSet(strings.Repeat("p", 20))
		pcs.Spec.Replicas = 1_000_000_000
		pcs.Spec.Template.Cliques[0].Name = strings.Repeat("c", 15)
		pcs.Spec.Template.PodCliqueScalingGroupConfigs = []grovecorev1alpha1.PodCliqueScalingGroupConfig{{
			Name:         strings.Repeat("g", 10),
			CliqueNames:  []string{strings.Repeat("c", 15)},
			Replicas:     ptr.To(int32(1_000_000_000)),
			MinAvailable: ptr.To(int32(1)),
		}}

		errs := newGeneratedNameTestValidator(pcs).validateGeneratedObjectNameCollisions()

		require.Len(t, errs, 2)
		assert.Equal(t, "spec.template.podCliqueScalingGroups[0].cliqueNames[0]", errs[0].Field)
		assert.Len(t, errs[0].BadValue, 67)
		assert.Contains(t, errs[0].Detail, "generated PodClique name")
		assert.Contains(t, errs[0].Detail, "valid label value")
		assert.Contains(t, errs[1].Detail, "generated Pod hostname")
	})

	t.Run("scaled PodGang label uses maximum replica indices", func(t *testing.T) {
		pcs := createTestPodCliqueSet(strings.Repeat("p", 22))
		pcs.Spec.Replicas = 1_000_000_000
		pcs.Spec.Template.Cliques[0].Name = "c"
		pcs.Spec.Template.PodCliqueScalingGroupConfigs = []grovecorev1alpha1.PodCliqueScalingGroupConfig{{
			Name:         strings.Repeat("g", 22),
			CliqueNames:  []string{"c"},
			Replicas:     ptr.To(int32(1_000_000_000)),
			MinAvailable: ptr.To(int32(1)),
		}}

		errs := newGeneratedNameTestValidator(pcs).validateGeneratedObjectNameCollisions()

		require.Len(t, errs, 3)
		assert.Contains(t, errs[0].Detail, "generated PodGang name")
		assert.Len(t, errs[0].BadValue, 65)
	})

	t.Run("empty PCSG still validates generated label value", func(t *testing.T) {
		pcs := createTestPodCliqueSet(strings.Repeat("p", 44))
		pcs.Spec.Template.Cliques[0].Name = "c"
		pcs.Spec.Template.PodCliqueScalingGroupConfigs = []grovecorev1alpha1.PodCliqueScalingGroupConfig{{
			Name:         strings.Repeat("g", 63),
			Replicas:     ptr.To(int32(1)),
			MinAvailable: ptr.To(int32(1)),
		}}

		errs := newGeneratedNameTestValidator(pcs).validateGeneratedObjectNameCollisions()

		require.Len(t, errs, 1)
		assert.Equal(t, "spec.template.podCliqueScalingGroups[0].name", errs[0].Field)
		assert.Contains(t, errs[0].Detail, "generated PodCliqueScalingGroup name")
	})

	t.Run("dot in generated Pod hostname is rejected", func(t *testing.T) {
		pcs := createTestPodCliqueSet("a")
		pcs.Spec.Template.Cliques[0].Name = "worker.v2"

		errs := newGeneratedNameTestValidator(pcs).validateGeneratedObjectNameCollisions()

		require.Len(t, errs, 1)
		assert.Equal(t, "spec.template.cliques[0].name", errs[0].Field)
		assert.Contains(t, errs[0].Detail, "pod.spec.hostname")
		assert.Contains(t, errs[0].Detail, "must not contain dots")
	})

	t.Run("standalone and grouped PodClique names collide", func(t *testing.T) {
		pcs := createGeneratedPodCliqueCollisionTestPCS(1, 0)

		errs := newGeneratedNameTestValidator(pcs).validateGeneratedObjectNameCollisions()

		require.Len(t, errs, 1)
		assert.Equal(t, "spec.template.podCliqueScalingGroups[0].cliqueNames[0]", errs[0].Field)
		assert.Equal(t, "a-0-g-0-worker", errs[0].BadValue)
		assert.Contains(t, errs[0].Detail, "generated PodClique name collides")
	})

	t.Run("collision outside PCSG replica bound is allowed", func(t *testing.T) {
		pcs := createGeneratedPodCliqueCollisionTestPCS(42, 42)

		assert.Empty(t, newGeneratedNameTestValidator(pcs).validateGeneratedObjectNameCollisions())
	})

	t.Run("collision at PCSG replica bound is rejected", func(t *testing.T) {
		pcs := createGeneratedPodCliqueCollisionTestPCS(43, 42)

		errs := newGeneratedNameTestValidator(pcs).validateGeneratedObjectNameCollisions()

		require.Len(t, errs, 1)
		assert.Equal(t, "a-0-g-42-worker", errs[0].BadValue)
	})

	t.Run("standalone PodClique and PCSG HPA names collide", func(t *testing.T) {
		pcs := createTestPodCliqueSet("a")
		standalone := pcs.Spec.Template.Cliques[0]
		standalone.Name = "shared"
		standalone.Spec.ScaleConfig = &grovecorev1alpha1.AutoScalingConfig{
			MinReplicas: ptr.To(int32(1)),
			MaxReplicas: 2,
		}
		pcs.Spec.Template.Cliques = append(pcs.Spec.Template.Cliques, createDummyPodCliqueTemplate("worker"))
		pcs.Spec.Template.PodCliqueScalingGroupConfigs = []grovecorev1alpha1.PodCliqueScalingGroupConfig{{
			Name:         "shared",
			CliqueNames:  []string{"worker"},
			Replicas:     ptr.To(int32(1)),
			MinAvailable: ptr.To(int32(1)),
			ScaleConfig: &grovecorev1alpha1.AutoScalingConfig{
				MinReplicas: ptr.To(int32(1)),
				MaxReplicas: 2,
			},
		}}

		errs := newGeneratedNameTestValidator(pcs).validateGeneratedObjectNameCollisions()

		require.Len(t, errs, 1)
		assert.Equal(t, "a-0-shared", errs[0].BadValue)
		assert.Contains(t, errs[0].Detail, "generated HorizontalPodAutoscaler name collides")
	})

	t.Run("topology parent and standalone scheduler subgroup names collide", func(t *testing.T) {
		pcs := createTestPodCliqueSet("a")
		pcs.Spec.Template.Cliques[0].Name = "g-0"
		pcs.Spec.Template.Cliques = append(pcs.Spec.Template.Cliques, createDummyPodCliqueTemplate("worker"))
		pcs.Spec.Template.PodCliqueScalingGroupConfigs = []grovecorev1alpha1.PodCliqueScalingGroupConfig{{
			Name:               "g",
			CliqueNames:        []string{"worker"},
			Replicas:           ptr.To(int32(1)),
			MinAvailable:       ptr.To(int32(1)),
			TopologyConstraint: &grovecorev1alpha1.TopologyConstraint{},
		}}

		validator := newGeneratedNameTestValidator(pcs)
		validator.tasEnabled = true
		validator.schedRegistry = &testutils.FakeSchedulerRegistry{
			Backends: map[string]scheduler.Backend{
				string(groveconfigv1alpha1.SchedulerNameKai): testutils.NewFakeSchedulerBackend(
					string(groveconfigv1alpha1.SchedulerNameKai),
				),
			},
			DefaultBackend: string(groveconfigv1alpha1.SchedulerNameKai),
		}
		errs := validator.validateGeneratedObjectNameCollisions()

		require.Len(t, errs, 1)
		assert.Equal(t, "a-0-g-0", errs[0].BadValue)
		assert.Contains(t, errs[0].Detail, "generated scheduler subgroup name collides")
	})

	t.Run("topology subgroup collision is KAI-specific", func(t *testing.T) {
		pcs := createTestPodCliqueSet("a")
		pcs.Spec.Template.Cliques[0].Name = "g-0"
		pcs.Spec.Template.Cliques = append(pcs.Spec.Template.Cliques, createDummyPodCliqueTemplate("worker"))
		pcs.Spec.Template.PodCliqueScalingGroupConfigs = []grovecorev1alpha1.PodCliqueScalingGroupConfig{{
			Name:               "g",
			CliqueNames:        []string{"worker"},
			Replicas:           ptr.To(int32(1)),
			MinAvailable:       ptr.To(int32(1)),
			TopologyConstraint: &grovecorev1alpha1.TopologyConstraint{},
		}}
		validator := newGeneratedNameTestValidator(pcs)
		validator.tasEnabled = true

		assert.Empty(t, validator.validateGeneratedObjectNameCollisions())
	})

	t.Run("KAI subgroup name must be a DNS label", func(t *testing.T) {
		pcs := createTestPodCliqueSet("a")
		pcs.Spec.Template.Cliques[0].Name = "worker.v2"
		validator := newGeneratedNameTestValidator(pcs)
		validator.tasEnabled = true
		validator.schedRegistry = &testutils.FakeSchedulerRegistry{
			Backends: map[string]scheduler.Backend{
				string(groveconfigv1alpha1.SchedulerNameKai): testutils.NewFakeSchedulerBackend(
					string(groveconfigv1alpha1.SchedulerNameKai),
				),
			},
			DefaultBackend: string(groveconfigv1alpha1.SchedulerNameKai),
		}

		errs := validator.validateGeneratedObjectNameCollisions()

		require.Len(t, errs, 2)
		assert.Contains(t, errs[1].Detail, "generated KAI scheduler subgroup name")
		assert.Contains(t, errs[1].Detail, "must not contain dots")
	})
}

func TestValidateGeneratedResourceClaimNamesOnUpdateGrandfathersLegacyViolations(t *testing.T) {
	oldPCS := createTestPodCliqueSet("p")
	oldPCS.Spec.Template.ResourceSharing = []grovecorev1alpha1.PCSResourceSharingSpec{{
		ResourceSharingSpec: grovecorev1alpha1.ResourceSharingSpec{
			Name:  strings.Repeat("r", 58),
			Scope: grovecorev1alpha1.ResourceSharingScopeAllReplicas,
		},
	}}

	t.Run("unchanged invalid name is allowed", func(t *testing.T) {
		newPCS := oldPCS.DeepCopy()
		validator := newGeneratedNameTestValidator(newPCS)
		assert.Empty(t, validator.validateGeneratedResourceClaimNamesOnUpdate(oldPCS))
	})

	t.Run("all-replicas violation allows unrelated scale out", func(t *testing.T) {
		newPCS := oldPCS.DeepCopy()
		newPCS.Spec.Replicas++
		validator := newGeneratedNameTestValidator(newPCS)
		assert.Empty(t, validator.validateGeneratedResourceClaimNamesOnUpdate(oldPCS))
	})

	t.Run("scale in with invalid name is allowed", func(t *testing.T) {
		scaledOldPCS := oldPCS.DeepCopy()
		scaledOldPCS.Spec.Replicas = 2
		newPCS := scaledOldPCS.DeepCopy()
		newPCS.Spec.Replicas = 1
		validator := newGeneratedNameTestValidator(newPCS)
		assert.Empty(t, validator.validateGeneratedResourceClaimNamesOnUpdate(scaledOldPCS))
	})

	t.Run("per-replica violation rejects scale out", func(t *testing.T) {
		perReplicaOldPCS := oldPCS.DeepCopy()
		perReplicaOldPCS.Spec.Template.ResourceSharing[0].Scope = grovecorev1alpha1.ResourceSharingScopePerReplica
		perReplicaOldPCS.Spec.Template.ResourceSharing[0].Name = strings.Repeat("r", 60)
		newPCS := perReplicaOldPCS.DeepCopy()
		newPCS.Spec.Replicas++
		validator := newGeneratedNameTestValidator(newPCS)
		errs := validator.validateGeneratedResourceClaimNamesOnUpdate(perReplicaOldPCS)
		require.Len(t, errs, 1)
		assert.Equal(t, "spec.template.resourceSharing[0].name", errs[0].Field)
	})

	t.Run("per-replica violation rejects autoscaling maximum increase", func(t *testing.T) {
		scaledOldPCS := createTestPodCliqueSet(strings.Repeat("p", 30))
		clique := scaledOldPCS.Spec.Template.Cliques[0]
		clique.Name = strings.Repeat("c", 10)
		clique.ResourceSharing = []grovecorev1alpha1.ResourceSharingSpec{{
			Name:  strings.Repeat("r", 18),
			Scope: grovecorev1alpha1.ResourceSharingScopePerReplica,
		}}
		clique.Spec.ScaleConfig = &grovecorev1alpha1.AutoScalingConfig{
			MinReplicas: ptr.To(int32(1)),
			MaxReplicas: 10,
		}
		newPCS := scaledOldPCS.DeepCopy()
		newPCS.Spec.Template.Cliques[0].Spec.ScaleConfig.MaxReplicas = 11
		validator := newGeneratedNameTestValidator(newPCS)
		errs := validator.validateGeneratedResourceClaimNamesOnUpdate(scaledOldPCS)
		require.Len(t, errs, 1)
		assert.Equal(t, "spec.template.cliques[0].resourceSharing[0].name", errs[0].Field)
	})
}

func TestValidateGeneratedResourceClaimNameCollisionsOnUpdate(t *testing.T) {
	t.Run("scale out introducing collision is rejected", func(t *testing.T) {
		oldPCS := createPCSGResourceClaimCollisionTestPCS(42)
		newPCS := oldPCS.DeepCopy()
		newPCS.Spec.Template.PodCliqueScalingGroupConfigs[0].Replicas = ptr.To(int32(43))

		errs := newGeneratedNameTestValidator(newPCS).validateGeneratedResourceClaimNamesOnUpdate(oldPCS)

		require.Len(t, errs, 1)
		assert.Equal(t, "a-0-g-42-x-all-y", errs[0].BadValue)
	})

	t.Run("scale in removing collision is allowed", func(t *testing.T) {
		oldPCS := createPCSGResourceClaimCollisionTestPCS(43)
		newPCS := oldPCS.DeepCopy()
		newPCS.Spec.Template.PodCliqueScalingGroupConfigs[0].Replicas = ptr.To(int32(42))

		assert.Empty(t, newGeneratedNameTestValidator(newPCS).validateGeneratedResourceClaimNamesOnUpdate(oldPCS))
	})

	t.Run("unchanged legacy collision is allowed", func(t *testing.T) {
		oldPCS := createPCSGResourceClaimCollisionTestPCS(43)
		newPCS := oldPCS.DeepCopy()
		newPCS.Labels = map[string]string{"updated": "true"}

		assert.Empty(t, newGeneratedNameTestValidator(newPCS).validateGeneratedResourceClaimNamesOnUpdate(oldPCS))
	})
}

func TestValidateGeneratedObjectNameCollisionsOnUpdate(t *testing.T) {
	t.Run("replica increase introducing invalid PodClique label is rejected", func(t *testing.T) {
		oldPCS := createTestPodCliqueSet(strings.Repeat("p", 20))
		oldPCS.Spec.Replicas = 10
		oldPCS.Spec.Template.Cliques[0].Name = strings.Repeat("c", 15)
		oldPCS.Spec.Template.PodCliqueScalingGroupConfigs = []grovecorev1alpha1.PodCliqueScalingGroupConfig{{
			Name:         strings.Repeat("g", 10),
			CliqueNames:  []string{strings.Repeat("c", 15)},
			Replicas:     ptr.To(int32(10)),
			MinAvailable: ptr.To(int32(1)),
		}}
		newPCS := oldPCS.DeepCopy()
		newPCS.Spec.Replicas = 1_000_000_000
		newPCS.Spec.Template.PodCliqueScalingGroupConfigs[0].Replicas = ptr.To(int32(1_000_000_000))

		errs := newGeneratedNameTestValidator(newPCS).validateGeneratedObjectNameCollisionsOnUpdate(oldPCS)

		require.Len(t, errs, 2)
		assert.Contains(t, errs[0].Detail, "generated PodClique name")
		assert.Contains(t, errs[1].Detail, "generated Pod hostname")
	})

	t.Run("unchanged legacy invalid PodClique label is allowed", func(t *testing.T) {
		oldPCS := createTestPodCliqueSet(strings.Repeat("p", 20))
		oldPCS.Spec.Replicas = 1_000_000_000
		oldPCS.Spec.Template.Cliques[0].Name = strings.Repeat("c", 15)
		oldPCS.Spec.Template.PodCliqueScalingGroupConfigs = []grovecorev1alpha1.PodCliqueScalingGroupConfig{{
			Name:         strings.Repeat("g", 10),
			CliqueNames:  []string{strings.Repeat("c", 15)},
			Replicas:     ptr.To(int32(1_000_000_000)),
			MinAvailable: ptr.To(int32(1)),
		}}
		newPCS := oldPCS.DeepCopy()
		newPCS.Labels = map[string]string{"updated": "true"}

		assert.Empty(t, newGeneratedNameTestValidator(newPCS).
			validateGeneratedObjectNameCollisionsOnUpdate(oldPCS))
	})

	t.Run("scale out introducing PodClique collision is rejected", func(t *testing.T) {
		oldPCS := createGeneratedPodCliqueCollisionTestPCS(42, 42)
		newPCS := oldPCS.DeepCopy()
		newPCS.Spec.Template.PodCliqueScalingGroupConfigs[0].Replicas = ptr.To(int32(43))

		errs := newGeneratedNameTestValidator(newPCS).validateGeneratedObjectNameCollisionsOnUpdate(oldPCS)

		require.Len(t, errs, 1)
		assert.Equal(t, "a-0-g-42-worker", errs[0].BadValue)
	})

	t.Run("scale in removing PodClique collision is allowed", func(t *testing.T) {
		oldPCS := createGeneratedPodCliqueCollisionTestPCS(43, 42)
		newPCS := oldPCS.DeepCopy()
		newPCS.Spec.Template.PodCliqueScalingGroupConfigs[0].Replicas = ptr.To(int32(42))

		assert.Empty(t, newGeneratedNameTestValidator(newPCS).validateGeneratedObjectNameCollisionsOnUpdate(oldPCS))
	})

	t.Run("unchanged legacy PodClique collision is allowed", func(t *testing.T) {
		oldPCS := createGeneratedPodCliqueCollisionTestPCS(43, 42)
		newPCS := oldPCS.DeepCopy()
		newPCS.Labels = map[string]string{"updated": "true"}

		assert.Empty(t, newGeneratedNameTestValidator(newPCS).validateGeneratedObjectNameCollisionsOnUpdate(oldPCS))
	})

	t.Run("adding autoscaling that creates HPA collision is rejected", func(t *testing.T) {
		oldPCS := createTestPodCliqueSet("a")
		oldPCS.Spec.Template.Cliques[0].Name = "shared"
		oldPCS.Spec.Template.Cliques = append(oldPCS.Spec.Template.Cliques, createDummyPodCliqueTemplate("worker"))
		oldPCS.Spec.Template.PodCliqueScalingGroupConfigs = []grovecorev1alpha1.PodCliqueScalingGroupConfig{{
			Name:         "shared",
			CliqueNames:  []string{"worker"},
			Replicas:     ptr.To(int32(1)),
			MinAvailable: ptr.To(int32(1)),
			ScaleConfig: &grovecorev1alpha1.AutoScalingConfig{
				MinReplicas: ptr.To(int32(1)),
				MaxReplicas: 2,
			},
		}}
		newPCS := oldPCS.DeepCopy()
		newPCS.Spec.Template.Cliques[0].Spec.ScaleConfig = &grovecorev1alpha1.AutoScalingConfig{
			MinReplicas: ptr.To(int32(1)),
			MaxReplicas: 2,
		}

		errs := newGeneratedNameTestValidator(newPCS).validateGeneratedObjectNameCollisionsOnUpdate(oldPCS)

		require.Len(t, errs, 1)
		assert.Contains(t, errs[0].Detail, "generated HorizontalPodAutoscaler name collides")
	})
}

func TestHandlerValidatesGeneratedNames(t *testing.T) {
	t.Run("create rejects invalid ResourceClaim alias", func(t *testing.T) {
		pcs := createTestPodCliqueSet("p")
		pcs.Spec.Template.ResourceSharing = []grovecorev1alpha1.PCSResourceSharingSpec{{
			ResourceSharingSpec: grovecorev1alpha1.ResourceSharingSpec{
				Name:  strings.Repeat("r", 58),
				Scope: grovecorev1alpha1.ResourceSharingScopeAllReplicas,
			},
		}}

		_, err := newGeneratedNameTestHandler(false).ValidateCreate(context.Background(), pcs)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "pod.spec.resourceClaims[].name")
	})

	t.Run("update allows unchanged legacy ResourceClaim violation", func(t *testing.T) {
		oldPCS := createTestPodCliqueSet("p")
		oldPCS.Spec.Template.ResourceSharing = []grovecorev1alpha1.PCSResourceSharingSpec{{
			ResourceSharingSpec: grovecorev1alpha1.ResourceSharingSpec{
				Name:  strings.Repeat("r", 58),
				Scope: grovecorev1alpha1.ResourceSharingScopeAllReplicas,
			},
		}}
		newPCS := oldPCS.DeepCopy()

		_, err := newGeneratedNameTestHandler(false).ValidateUpdate(context.Background(), oldPCS, newPCS)
		require.NoError(t, err)
	})

	t.Run("create rejects generated PodClique collision", func(t *testing.T) {
		pcs := createGeneratedPodCliqueCollisionTestPCS(1, 0)

		_, err := newGeneratedNameTestHandler(false).ValidateCreate(context.Background(), pcs)

		require.Error(t, err)
		assert.Contains(t, err.Error(), "generated PodClique name collides")
	})

	t.Run("create rejects Pod resource claim alias collision", func(t *testing.T) {
		pcs := createTestPodCliqueSet("a")
		pcs.Spec.Template.ResourceSharing = []grovecorev1alpha1.PCSResourceSharingSpec{{
			ResourceSharingSpec: grovecorev1alpha1.ResourceSharingSpec{
				Name:  "gpu",
				Scope: grovecorev1alpha1.ResourceSharingScopeAllReplicas,
			},
		}}
		pcs.Spec.Template.Cliques[0].Spec.PodSpec.ResourceClaims = []corev1.PodResourceClaim{{
			Name:              "a-all-gpu",
			ResourceClaimName: ptr.To("user-claim"),
		}}

		_, err := newGeneratedNameTestHandler(false).ValidateCreate(context.Background(), pcs)

		require.Error(t, err)
		assert.Contains(t, err.Error(), "final pod.spec.resourceClaims[].name values must be unique")
	})

	t.Run("create rejects reserved Auto-MNNVL claim alias", func(t *testing.T) {
		pcs := createValidPCSWithGPU(map[string]string{
			"grove.io/mnnvl-group": "fabric",
		})
		pcs.Spec.Template.Cliques[0].Spec.PodSpec.ResourceClaims = []corev1.PodResourceClaim{{
			Name:              "mnnvl-claim",
			ResourceClaimName: ptr.To("user-claim"),
		}}

		_, err := newGeneratedNameTestHandler(true).ValidateCreate(context.Background(), pcs)

		require.Error(t, err)
		assert.Contains(t, err.Error(), "Auto-MNNVL injected claim")
	})

	t.Run("update allows unchanged legacy PodClique collision", func(t *testing.T) {
		oldPCS := createGeneratedPodCliqueCollisionTestPCS(1, 0)
		newPCS := oldPCS.DeepCopy()

		_, err := newGeneratedNameTestHandler(false).ValidateUpdate(context.Background(), oldPCS, newPCS)

		require.NoError(t, err)
	})

	t.Run("create rejects invalid ComputeDomain label", func(t *testing.T) {
		pcs := createValidPCSWithGPU(map[string]string{
			"grove.io/mnnvl-group": strings.Repeat("g", 22),
		})
		pcs.Name = strings.Repeat("p", 39)

		_, err := newGeneratedNameTestHandler(true).ValidateCreate(context.Background(), pcs)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "app.kubernetes.io/name")
	})
}

func newGeneratedNameTestValidator(pcs *grovecorev1alpha1.PodCliqueSet) *pcsValidator {
	return newPCSValidator(
		pcs,
		admissionv1.Create,
		defaultTASConfig(),
		groveconfigv1alpha1.SchedulerConfiguration{
			Profiles:           []groveconfigv1alpha1.SchedulerProfile{{Name: groveconfigv1alpha1.SchedulerNameKube}},
			DefaultProfileName: string(groveconfigv1alpha1.SchedulerNameKube),
		},
		nil,
		testutils.NewDefaultFakeRegistry(),
	)
}

func newGeneratedNameTestHandler(autoMNNVLEnabled bool) *Handler {
	cl := testutils.NewTestClientBuilder().Build()
	mgr := &testutils.FakeManager{
		Client: cl,
		Scheme: cl.Scheme(),
		Logger: logr.Discard(),
	}
	cfg := groveconfigv1alpha1.OperatorConfiguration{
		TopologyAwareScheduling: getDefaultTASConfig(),
		Network: groveconfigv1alpha1.NetworkAcceleration{
			AutoMNNVLEnabled: autoMNNVLEnabled,
		},
		Scheduler: groveconfigv1alpha1.SchedulerConfiguration{
			Profiles:           []groveconfigv1alpha1.SchedulerProfile{{Name: groveconfigv1alpha1.SchedulerNameKube}},
			DefaultProfileName: string(groveconfigv1alpha1.SchedulerNameKube),
		},
	}
	return NewHandler(mgr, &cfg, testutils.NewDefaultFakeRegistry())
}

func createPCSGResourceClaimCollisionTestPCS(pcsgReplicas int32) *grovecorev1alpha1.PodCliqueSet {
	pcs := createTestPodCliqueSet("a")
	standalone := pcs.Spec.Template.Cliques[0]
	standalone.Name = "g-42-x"
	standalone.ResourceSharing = []grovecorev1alpha1.ResourceSharingSpec{{
		Name:  "y",
		Scope: grovecorev1alpha1.ResourceSharingScopeAllReplicas,
	}}
	pcs.Spec.Template.Cliques = append(pcs.Spec.Template.Cliques, createDummyPodCliqueTemplate("worker"))
	pcs.Spec.Template.PodCliqueScalingGroupConfigs = []grovecorev1alpha1.PodCliqueScalingGroupConfig{{
		Name:         "g",
		CliqueNames:  []string{"worker"},
		Replicas:     ptr.To(pcsgReplicas),
		MinAvailable: ptr.To(int32(1)),
		ResourceSharing: []grovecorev1alpha1.PCSGResourceSharingSpec{{
			ResourceSharingSpec: grovecorev1alpha1.ResourceSharingSpec{
				Name:  "x-all-y",
				Scope: grovecorev1alpha1.ResourceSharingScopePerReplica,
			},
		}},
	}}
	return pcs
}

func createGeneratedPodCliqueCollisionTestPCS(pcsgReplicas int32, collisionReplica int) *grovecorev1alpha1.PodCliqueSet {
	pcs := createTestPodCliqueSet("a")
	pcs.Spec.Template.Cliques[0].Name = "g-" + strconv.Itoa(collisionReplica) + "-worker"
	pcs.Spec.Template.Cliques = append(pcs.Spec.Template.Cliques, createDummyPodCliqueTemplate("worker"))
	pcs.Spec.Template.PodCliqueScalingGroupConfigs = []grovecorev1alpha1.PodCliqueScalingGroupConfig{{
		Name:         "g",
		CliqueNames:  []string{"worker"},
		Replicas:     ptr.To(pcsgReplicas),
		MinAvailable: ptr.To(int32(1)),
	}}
	return pcs
}
