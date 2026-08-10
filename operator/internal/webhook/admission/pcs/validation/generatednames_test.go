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
	"strings"
	"testing"

	groveconfigv1alpha1 "github.com/ai-dynamo/grove/operator/api/config/v1alpha1"
	grovecorev1alpha1 "github.com/ai-dynamo/grove/operator/api/core/v1alpha1"
	testutils "github.com/ai-dynamo/grove/operator/test/utils"

	"github.com/go-logr/logr"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	admissionv1 "k8s.io/api/admission/v1"
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
