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
	"fmt"
	"strings"

	apicommon "github.com/ai-dynamo/grove/operator/api/common"
	grovecorev1alpha1 "github.com/ai-dynamo/grove/operator/api/core/v1alpha1"
	"github.com/ai-dynamo/grove/operator/internal/resourceclaim"

	k8svalidation "k8s.io/apimachinery/pkg/util/validation"
	"k8s.io/apimachinery/pkg/util/validation/field"
)

type generatedResourceClaimNameCheck struct {
	key           string
	fieldPath     *field.Path
	name          string
	context       string
	replicaBounds []int32
}

func (v *pcsValidator) validateGeneratedResourceClaimNames() field.ErrorList {
	return generatedResourceClaimNameErrors(buildGeneratedResourceClaimNameChecks(v.pcs))
}

func (v *pcsValidator) validateGeneratedResourceClaimNamesOnUpdate(oldPCS *grovecorev1alpha1.PodCliqueSet) field.ErrorList {
	oldChecks := indexGeneratedResourceClaimNameChecks(buildGeneratedResourceClaimNameChecks(oldPCS))

	var allErrs field.ErrorList
	for _, check := range buildGeneratedResourceClaimNameChecks(v.pcs) {
		if len(k8svalidation.IsDNS1123Label(check.name)) == 0 {
			continue
		}
		oldCheck, exists := oldChecks[check.key]
		if exists && len(k8svalidation.IsDNS1123Label(oldCheck.name)) > 0 &&
			!replicaBoundsIncreased(oldCheck.replicaBounds, check.replicaBounds) {
			continue
		}
		allErrs = append(allErrs, generatedResourceClaimNameError(check))
	}
	return allErrs
}

func buildGeneratedResourceClaimNameChecks(pcs *grovecorev1alpha1.PodCliqueSet) []generatedResourceClaimNameCheck {
	var checks []generatedResourceClaimNameCheck
	templatePath := field.NewPath("spec", "template")
	pcsReplicaIndex := maxReplicaIndex(pcs.Spec.Replicas)

	for i := range pcs.Spec.Template.ResourceSharing {
		ref := &pcs.Spec.Template.ResourceSharing[i].ResourceSharingSpec
		checks = appendGeneratedResourceClaimNameCheck(
			checks,
			"pcs/"+ref.Name+"/"+string(ref.Scope),
			templatePath.Child("resourceSharing").Index(i).Child("name"),
			pcs.Name,
			pcsReplicaIndex,
			ref,
			"PodCliqueSet",
			nil,
			pcs.Spec.Replicas,
		)
	}

	pcsgIndexesByClique := make(map[string]int)
	for i := range pcs.Spec.Template.PodCliqueScalingGroupConfigs {
		cfg := &pcs.Spec.Template.PodCliqueScalingGroupConfigs[i]
		pcsgName := apicommon.GeneratePodCliqueScalingGroupName(
			apicommon.ResourceNameReplica{Name: pcs.Name, Replica: pcsReplicaIndex},
			cfg.Name,
		)
		pcsgReplicaIndex := maxReplicaIndex(maxConfiguredReplicas(cfg.Replicas, cfg.ScaleConfig))
		for j := range cfg.ResourceSharing {
			ref := &cfg.ResourceSharing[j].ResourceSharingSpec
			checks = appendGeneratedResourceClaimNameCheck(
				checks,
				fmt.Sprintf("pcsg/%s/%s/%s", cfg.Name, ref.Name, ref.Scope),
				templatePath.Child("podCliqueScalingGroups").Index(i).Child("resourceSharing").Index(j).Child("name"),
				pcsgName,
				pcsgReplicaIndex,
				ref,
				fmt.Sprintf("PodCliqueScalingGroup %q", cfg.Name),
				[]int32{pcs.Spec.Replicas},
				maxConfiguredReplicas(cfg.Replicas, cfg.ScaleConfig),
			)
		}
		for _, cliqueName := range cfg.CliqueNames {
			pcsgIndexesByClique[cliqueName] = i
		}
	}

	for i, clique := range pcs.Spec.Template.Cliques {
		if clique == nil {
			continue
		}

		ownerName := apicommon.GeneratePodCliqueName(
			apicommon.ResourceNameReplica{Name: pcs.Name, Replica: pcsReplicaIndex},
			clique.Name,
		)
		keyPrefix := "pclq/" + clique.Name + "/standalone"
		context := fmt.Sprintf("standalone PodClique %q", clique.Name)
		ownerReplicaBounds := []int32{pcs.Spec.Replicas}
		if pcsgIndex, exists := pcsgIndexesByClique[clique.Name]; exists {
			cfg := &pcs.Spec.Template.PodCliqueScalingGroupConfigs[pcsgIndex]
			pcsgReplicas := maxConfiguredReplicas(cfg.Replicas, cfg.ScaleConfig)
			pcsgName := apicommon.GeneratePodCliqueScalingGroupName(
				apicommon.ResourceNameReplica{Name: pcs.Name, Replica: pcsReplicaIndex},
				cfg.Name,
			)
			ownerName = apicommon.GeneratePodCliqueName(
				apicommon.ResourceNameReplica{
					Name:    pcsgName,
					Replica: maxReplicaIndex(pcsgReplicas),
				},
				clique.Name,
			)
			keyPrefix = fmt.Sprintf("pclq/%s/pcsg/%s", clique.Name, cfg.Name)
			context = fmt.Sprintf("PodClique %q in PodCliqueScalingGroup %q", clique.Name, cfg.Name)
			ownerReplicaBounds = append(ownerReplicaBounds, pcsgReplicas)
		}

		cliqueReplicas := maxConfiguredReplicas(&clique.Spec.Replicas, clique.Spec.ScaleConfig)
		replicaIndex := maxReplicaIndex(cliqueReplicas)
		for j := range clique.ResourceSharing {
			ref := &clique.ResourceSharing[j]
			checks = appendGeneratedResourceClaimNameCheck(
				checks,
				fmt.Sprintf("%s/%s/%s", keyPrefix, ref.Name, ref.Scope),
				templatePath.Child("cliques").Index(i).Child("resourceSharing").Index(j).Child("name"),
				ownerName,
				replicaIndex,
				ref,
				context,
				ownerReplicaBounds,
				cliqueReplicas,
			)
		}
	}

	return checks
}

func appendGeneratedResourceClaimNameCheck(
	checks []generatedResourceClaimNameCheck,
	key string,
	fldPath *field.Path,
	ownerName string,
	replicaIndex int,
	ref *grovecorev1alpha1.ResourceSharingSpec,
	context string,
	ownerReplicaBounds []int32,
	localReplicaBound int32,
) []generatedResourceClaimNameCheck {
	if ref.Name == "" {
		return checks
	}

	var name string
	replicaBounds := append([]int32(nil), ownerReplicaBounds...)
	switch ref.Scope {
	case grovecorev1alpha1.ResourceSharingScopeAllReplicas:
		name = resourceclaim.AllReplicasRCName(ownerName, ref.Name)
	case grovecorev1alpha1.ResourceSharingScopePerReplica:
		name = resourceclaim.PerReplicaRCName(ownerName, replicaIndex, ref.Name)
		replicaBounds = append(replicaBounds, localReplicaBound)
	default:
		return checks
	}

	return append(checks, generatedResourceClaimNameCheck{
		key:           key,
		fieldPath:     fldPath,
		name:          name,
		context:       context,
		replicaBounds: replicaBounds,
	})
}

func generatedResourceClaimNameErrors(checks []generatedResourceClaimNameCheck) field.ErrorList {
	var allErrs field.ErrorList
	for _, check := range checks {
		if len(k8svalidation.IsDNS1123Label(check.name)) > 0 {
			allErrs = append(allErrs, generatedResourceClaimNameError(check))
		}
	}
	return allErrs
}

func generatedResourceClaimNameError(check generatedResourceClaimNameCheck) *field.Error {
	return field.Invalid(
		check.fieldPath,
		check.name,
		fmt.Sprintf(
			"generated ResourceClaim name for %s must be a valid DNS label because it is used as pod.spec.resourceClaims[].name: %s",
			check.context,
			strings.Join(k8svalidation.IsDNS1123Label(check.name), "; "),
		),
	)
}

func indexGeneratedResourceClaimNameChecks(checks []generatedResourceClaimNameCheck) map[string]generatedResourceClaimNameCheck {
	result := make(map[string]generatedResourceClaimNameCheck, len(checks))
	for _, check := range checks {
		result[check.key] = check
	}
	return result
}

func replicaBoundsIncreased(oldBounds, newBounds []int32) bool {
	if len(oldBounds) != len(newBounds) {
		return true
	}
	for i := range newBounds {
		if newBounds[i] > oldBounds[i] {
			return true
		}
	}
	return false
}

func maxConfiguredReplicas(replicas *int32, scaleConfig *grovecorev1alpha1.AutoScalingConfig) int32 {
	var result int32
	if replicas != nil {
		result = *replicas
	}
	if scaleConfig != nil && scaleConfig.MaxReplicas > result {
		result = scaleConfig.MaxReplicas
	}
	return result
}

func maxReplicaIndex(replicas int32) int {
	if replicas <= 1 {
		return 0
	}
	return int(replicas - 1)
}
