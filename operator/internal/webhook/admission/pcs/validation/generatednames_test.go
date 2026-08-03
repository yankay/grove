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
	"k8s.io/apimachinery/pkg/util/validation/field"
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
		name        string
		refName     string
		expectError bool
	}{
		{
			name:        "63-character generated name is valid",
			refName:     strings.Repeat("a", 57),
			expectError: false,
		},
		{
			name:        "64-character generated name is rejected",
			refName:     strings.Repeat("a", 58),
			expectError: true,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			pcs := createTestPodCliqueSet("p")
			pcs.Spec.Template.ResourceSharing = []grovecorev1alpha1.PCSResourceSharingSpec{{
				ResourceSharingSpec: grovecorev1alpha1.ResourceSharingSpec{
					Name:  tc.refName,
					Scope: grovecorev1alpha1.ResourceSharingScopeAllReplicas,
				},
			}}

			errs := newGeneratedNameTestValidator(pcs).validateGeneratedResourceClaimNames()
			if !tc.expectError {
				assert.Empty(t, errs)
				return
			}
			require.Len(t, errs, 1)
			assert.Equal(t, "spec.template.resourceSharing[0].name", errs[0].Field)
			assert.Equal(t, field.ErrorTypeInvalid, errs[0].Type)
			assert.Contains(t, errs[0].Detail, "pod.spec.resourceClaims[].name")
		})
	}
}

func TestValidateGeneratedResourceClaimNamesRejectsIssue660Example(t *testing.T) {
	pcs := createTestPodCliqueSet(strings.Repeat("a", 40))
	clique := pcs.Spec.Template.Cliques[0]
	clique.Name = "workr"
	clique.ResourceSharing = []grovecorev1alpha1.ResourceSharingSpec{{
		Name:  "shared-gpus",
		Scope: grovecorev1alpha1.ResourceSharingScopeAllReplicas,
	}}

	errs := newGeneratedNameTestValidator(pcs).validateGeneratedResourceClaimNames()
	require.Len(t, errs, 1)
	assert.Equal(t, "spec.template.cliques[0].resourceSharing[0].name", errs[0].Field)
	assert.Equal(t, strings.Repeat("a", 40)+"-0-workr-all-shared-gpus", errs[0].BadValue)
}

func TestValidateGeneratedResourceClaimNamesUsesAutoscalingMaximum(t *testing.T) {
	pcs := createTestPodCliqueSet(strings.Repeat("p", 30))
	clique := pcs.Spec.Template.Cliques[0]
	clique.Name = strings.Repeat("c", 10)
	clique.ResourceSharing = []grovecorev1alpha1.ResourceSharingSpec{{
		Name:  strings.Repeat("r", 17),
		Scope: grovecorev1alpha1.ResourceSharingScopePerReplica,
	}}
	clique.Spec.ScaleConfig = &grovecorev1alpha1.AutoScalingConfig{
		MinReplicas: ptr.To(int32(1)),
		MaxReplicas: 10,
	}

	validator := newGeneratedNameTestValidator(pcs)
	assert.Empty(t, validator.validateGeneratedResourceClaimNames())

	clique.Spec.ScaleConfig.MaxReplicas = 11
	errs := validator.validateGeneratedResourceClaimNames()
	require.Len(t, errs, 1)
	assert.Contains(t, errs[0].BadValue.(string), "-10-")
}

func TestValidateGeneratedResourceClaimNamesOnUpdate(t *testing.T) {
	oldPCS := createTestPodCliqueSet("p")
	oldPCS.Spec.Template.ResourceSharing = []grovecorev1alpha1.PCSResourceSharingSpec{{
		ResourceSharingSpec: grovecorev1alpha1.ResourceSharingSpec{
			Name:  strings.Repeat("r", 58),
			Scope: grovecorev1alpha1.ResourceSharingScopeAllReplicas,
		},
	}}

	t.Run("unchanged legacy violation is allowed", func(t *testing.T) {
		newPCS := oldPCS.DeepCopy()
		assert.Empty(t, newGeneratedNameTestValidator(newPCS).validateGeneratedResourceClaimNamesOnUpdate(oldPCS))
	})

	t.Run("all-replicas violation allows scale out", func(t *testing.T) {
		newPCS := oldPCS.DeepCopy()
		newPCS.Spec.Replicas++
		assert.Empty(t, newGeneratedNameTestValidator(newPCS).validateGeneratedResourceClaimNamesOnUpdate(oldPCS))
	})

	t.Run("per-replica violation allows scale in", func(t *testing.T) {
		perReplicaOldPCS := oldPCS.DeepCopy()
		perReplicaOldPCS.Spec.Replicas = 2
		perReplicaOldPCS.Spec.Template.ResourceSharing[0].Scope = grovecorev1alpha1.ResourceSharingScopePerReplica
		perReplicaOldPCS.Spec.Template.ResourceSharing[0].Name = strings.Repeat("r", 60)
		newPCS := perReplicaOldPCS.DeepCopy()
		newPCS.Spec.Replicas--

		assert.Empty(t, newGeneratedNameTestValidator(newPCS).validateGeneratedResourceClaimNamesOnUpdate(perReplicaOldPCS))
	})

	t.Run("per-replica violation rejects scale out", func(t *testing.T) {
		perReplicaOldPCS := oldPCS.DeepCopy()
		perReplicaOldPCS.Spec.Template.ResourceSharing[0].Scope = grovecorev1alpha1.ResourceSharingScopePerReplica
		perReplicaOldPCS.Spec.Template.ResourceSharing[0].Name = strings.Repeat("r", 60)
		newPCS := perReplicaOldPCS.DeepCopy()
		newPCS.Spec.Replicas++

		errs := newGeneratedNameTestValidator(newPCS).validateGeneratedResourceClaimNamesOnUpdate(perReplicaOldPCS)
		require.Len(t, errs, 1)
		assert.Equal(t, "spec.template.resourceSharing[0].name", errs[0].Field)
	})
}

func TestHandlerValidatesGeneratedResourceClaimNames(t *testing.T) {
	pcs := createTestPodCliqueSet("p")
	pcs.Spec.Template.ResourceSharing = []grovecorev1alpha1.PCSResourceSharingSpec{{
		ResourceSharingSpec: grovecorev1alpha1.ResourceSharingSpec{
			Name:  strings.Repeat("r", 58),
			Scope: grovecorev1alpha1.ResourceSharingScopeAllReplicas,
		},
	}}

	_, err := newGeneratedNameTestHandler().ValidateCreate(context.Background(), pcs)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "pod.spec.resourceClaims[].name")

	_, err = newGeneratedNameTestHandler().ValidateUpdate(context.Background(), pcs, pcs.DeepCopy())
	require.NoError(t, err)
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

func newGeneratedNameTestHandler() *Handler {
	cl := testutils.NewTestClientBuilder().Build()
	mgr := &testutils.FakeManager{
		Client: cl,
		Scheme: cl.Scheme(),
		Logger: logr.Discard(),
	}
	cfg := groveconfigv1alpha1.OperatorConfiguration{
		TopologyAwareScheduling: getDefaultTASConfig(),
		Network:                 getDefaultNetworkConfig(),
		Scheduler: groveconfigv1alpha1.SchedulerConfiguration{
			Profiles:           []groveconfigv1alpha1.SchedulerProfile{{Name: groveconfigv1alpha1.SchedulerNameKube}},
			DefaultProfileName: string(groveconfigv1alpha1.SchedulerNameKube),
		},
	}
	return NewHandler(mgr, &cfg, testutils.NewDefaultFakeRegistry())
}
