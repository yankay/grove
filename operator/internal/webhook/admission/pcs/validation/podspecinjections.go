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

	apicommonconstants "github.com/ai-dynamo/grove/operator/api/common/constants"
	grovecorev1alpha1 "github.com/ai-dynamo/grove/operator/api/core/v1alpha1"
	"github.com/ai-dynamo/grove/operator/internal/constants"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/util/validation/field"
)

type injectedPodSpecNameConflict struct {
	key              string
	fieldPath        *field.Path
	value            string
	detail           string
	replicaBoundKeys []string
}

func (v *pcsValidator) validateInjectedPodSpecNames() field.ErrorList {
	return injectedPodSpecNameConflictErrors(
		buildInjectedPodSpecNameConflicts(v.pcs, generatedNameReplicaBounds(v.pcs)),
	)
}

func (v *pcsValidator) validateInjectedPodSpecNamesOnUpdate(
	oldPCS *grovecorev1alpha1.PodCliqueSet,
) field.ErrorList {
	oldBounds := generatedNameReplicaBounds(oldPCS)
	newBounds := generatedNameReplicaBounds(v.pcs)
	oldConflicts := indexInjectedPodSpecNameConflicts(
		buildInjectedPodSpecNameConflicts(oldPCS, oldBounds),
	)

	var allErrs field.ErrorList
	for _, conflict := range buildInjectedPodSpecNameConflicts(v.pcs, newBounds) {
		if oldConflicts[conflict.key] > 0 &&
			!replicaBoundsIncreasedForKeys(oldBounds, newBounds, conflict.replicaBoundKeys) {
			oldConflicts[conflict.key]--
			continue
		}
		allErrs = append(allErrs, injectedPodSpecNameConflictError(conflict))
	}
	return allErrs
}

func buildInjectedPodSpecNameConflicts(
	pcs *grovecorev1alpha1.PodCliqueSet,
	replicaBounds map[string]int32,
) []injectedPodSpecNameConflict {
	var conflicts []injectedPodSpecNameConflict
	contexts := buildPodResourceClaimAliasContexts(pcs, false)
	for _, podContext := range contexts {
		clique := pcs.Spec.Template.Cliques[podContext.cliqueIndex]
		cliquePath := field.NewPath("spec", "template", "cliques").Index(podContext.cliqueIndex)
		conflicts = append(
			conflicts,
			buildContainerResourceClaimConflicts(clique, cliquePath, podContext, replicaBounds)...,
		)
	}

	for cliqueIndex, clique := range pcs.Spec.Template.Cliques {
		if clique == nil {
			continue
		}
		cliquePath := field.NewPath("spec", "template", "cliques").Index(cliqueIndex)
		conflicts = append(conflicts, buildReservedEnvConflicts(pcs, clique, cliquePath)...)
		if cliqueUsesStartupInitContainer(pcs, cliqueIndex) {
			conflicts = append(conflicts, buildStartupInjectionConflicts(clique, cliquePath)...)
		}
		conflicts = append(conflicts, buildUserContainerResourceClaimConflicts(clique, cliquePath)...)
	}
	return conflicts
}

func buildContainerResourceClaimConflicts(
	clique *grovecorev1alpha1.PodCliqueTemplateSpec,
	cliquePath *field.Path,
	podContext podResourceClaimAliasContext,
	replicaBounds map[string]int32,
) []injectedPodSpecNameConflict {
	var conflicts []injectedPodSpecNameConflict
	generatedChecks := make([]podResourceClaimAliasCheck, 0, len(podContext.checks))
	for _, check := range podContext.checks {
		if check.origin == podResourceClaimAliasOriginGenerated {
			generatedChecks = append(generatedChecks, check)
		}
	}

	containerSets := []struct {
		kind       string
		containers []corev1.Container
	}{
		{kind: "containers", containers: clique.Spec.PodSpec.Containers},
		{kind: "initContainers", containers: clique.Spec.PodSpec.InitContainers},
	}
	for _, containerSet := range containerSets {
		for containerIndex, container := range containerSet.containers {
			for claimIndex, claim := range container.Resources.Claims {
				for _, generatedCheck := range generatedChecks {
					name, collides := generatedNamePatternsCollide(
						literalGeneratedNamePattern(claim.Name),
						generatedCheck.pattern,
						replicaBounds,
					)
					if !collides {
						continue
					}
					conflicts = append(conflicts, injectedPodSpecNameConflict{
						key: fmt.Sprintf(
							"container-claim/%s/%s/%s/%s/%s/%s",
							podContext.key,
							containerSet.kind,
							container.Name,
							claim.Name,
							claim.Request,
							generatedCheck.key,
						),
						fieldPath: cliquePath.
							Child("spec", "podSpec", containerSet.kind).
							Index(containerIndex).
							Child("resources", "claims").
							Index(claimIndex).
							Child("name"),
						value: name,
						detail: fmt.Sprintf(
							"container resource claim conflicts with %s in %s; final resources.claims entries must be unique",
							generatedCheck.context,
							podContext.description,
						),
						replicaBoundKeys: generatedCheck.replicaBoundKeys,
					})
				}
			}
		}
	}
	return conflicts
}

func buildUserContainerResourceClaimConflicts(
	clique *grovecorev1alpha1.PodCliqueTemplateSpec,
	cliquePath *field.Path,
) []injectedPodSpecNameConflict {
	var conflicts []injectedPodSpecNameConflict
	containerSets := []struct {
		kind       string
		containers []corev1.Container
	}{
		{kind: "containers", containers: clique.Spec.PodSpec.Containers},
		{kind: "initContainers", containers: clique.Spec.PodSpec.InitContainers},
	}
	for _, containerSet := range containerSets {
		for containerIndex, container := range containerSet.containers {
			for i := range container.Resources.Claims {
				for j := i + 1; j < len(container.Resources.Claims); j++ {
					left, right := container.Resources.Claims[i], container.Resources.Claims[j]
					if !containerResourceClaimsOverlap(left, right) {
						continue
					}
					leftRequest, rightRequest := left.Request, right.Request
					if leftRequest > rightRequest {
						leftRequest, rightRequest = rightRequest, leftRequest
					}
					conflicts = append(conflicts, injectedPodSpecNameConflict{
						key: fmt.Sprintf(
							"duplicate-container-claim/%s/%s/%s/%s/%s/%s",
							clique.Name,
							containerSet.kind,
							container.Name,
							right.Name,
							leftRequest,
							rightRequest,
						),
						fieldPath: cliquePath.
							Child("spec", "podSpec", containerSet.kind).
							Index(containerIndex).
							Child("resources", "claims").
							Index(j),
						value:  right.Name,
						detail: "container resources.claims entries overlap; a claim may be listed once without a request or once per distinct request",
					})
				}
			}
		}
	}
	return conflicts
}

func containerResourceClaimsOverlap(left, right corev1.ResourceClaim) bool {
	return left.Name == right.Name &&
		(left.Request == "" || right.Request == "" || left.Request == right.Request)
}

func buildReservedEnvConflicts(
	pcs *grovecorev1alpha1.PodCliqueSet,
	clique *grovecorev1alpha1.PodCliqueTemplateSpec,
	cliquePath *field.Path,
) []injectedPodSpecNameConflict {
	reservedNames := map[string]struct{}{
		apicommonconstants.EnvVarPodCliqueSetName:  {},
		apicommonconstants.EnvVarPodCliqueSetIndex: {},
		apicommonconstants.EnvVarPodCliqueName:     {},
		apicommonconstants.EnvVarHeadlessService:   {},
		apicommonconstants.EnvVarPodIndex:          {},
	}
	for i := range pcs.Spec.Template.PodCliqueScalingGroupConfigs {
		cfg := &pcs.Spec.Template.PodCliqueScalingGroupConfigs[i]
		for _, cliqueName := range cfg.CliqueNames {
			if cliqueName != clique.Name {
				continue
			}
			reservedNames[apicommonconstants.EnvVarPodCliqueScalingGroupName] = struct{}{}
			reservedNames[apicommonconstants.EnvVarPodCliqueScalingGroupIndex] = struct{}{}
			reservedNames[apicommonconstants.EnvVarPodCliqueScalingGroupTemplateNumPods] = struct{}{}
			break
		}
	}

	var conflicts []injectedPodSpecNameConflict
	containerSets := []struct {
		kind       string
		containers []corev1.Container
	}{
		{kind: "containers", containers: clique.Spec.PodSpec.Containers},
		{kind: "initContainers", containers: clique.Spec.PodSpec.InitContainers},
	}
	for _, containerSet := range containerSets {
		for containerIndex, container := range containerSet.containers {
			for envIndex, env := range container.Env {
				if _, reserved := reservedNames[env.Name]; !reserved {
					continue
				}
				conflicts = append(conflicts, injectedPodSpecNameConflict{
					key: fmt.Sprintf(
						"reserved-env/%s/%s/%s/%s",
						clique.Name,
						containerSet.kind,
						container.Name,
						env.Name,
					),
					fieldPath: cliquePath.
						Child("spec", "podSpec", containerSet.kind).
						Index(containerIndex).
						Child("env").
						Index(envIndex).
						Child("name"),
					value:  env.Name,
					detail: "environment variable name is reserved because Grove injects it into this PodClique",
				})
			}
		}
	}
	return conflicts
}

func cliqueUsesStartupInitContainer(pcs *grovecorev1alpha1.PodCliqueSet, cliqueIndex int) bool {
	if pcs.Spec.Template.StartupType == nil {
		return false
	}
	clique := pcs.Spec.Template.Cliques[cliqueIndex]
	switch *pcs.Spec.Template.StartupType {
	case grovecorev1alpha1.CliqueStartupTypeInOrder:
		return cliqueIndex > 0
	case grovecorev1alpha1.CliqueStartupTypeExplicit:
		return len(clique.Spec.StartsAfter) > 0
	default:
		return false
	}
}

func buildStartupInjectionConflicts(
	clique *grovecorev1alpha1.PodCliqueTemplateSpec,
	cliquePath *field.Path,
) []injectedPodSpecNameConflict {
	var conflicts []injectedPodSpecNameConflict
	for i, container := range clique.Spec.PodSpec.InitContainers {
		if container.Name != constants.StartupInitContainerName {
			continue
		}
		conflicts = append(conflicts, injectedPodSpecNameConflict{
			key:       "startup-init-container/" + clique.Name,
			fieldPath: cliquePath.Child("spec", "podSpec", "initContainers").Index(i).Child("name"),
			value:     container.Name,
			detail:    "init container name is reserved for Grove startup-order injection",
		})
	}

	reservedVolumes := map[string]struct{}{
		constants.StartupServiceAccountTokenVolumeName: {},
		constants.StartupPodInfoVolumeName:             {},
	}
	for i, volume := range clique.Spec.PodSpec.Volumes {
		if _, reserved := reservedVolumes[volume.Name]; !reserved {
			continue
		}
		conflicts = append(conflicts, injectedPodSpecNameConflict{
			key:       "startup-volume/" + clique.Name + "/" + volume.Name,
			fieldPath: cliquePath.Child("spec", "podSpec", "volumes").Index(i).Child("name"),
			value:     volume.Name,
			detail:    "volume name is reserved for Grove startup-order injection",
		})
	}
	return conflicts
}

func injectedPodSpecNameConflictErrors(conflicts []injectedPodSpecNameConflict) field.ErrorList {
	allErrs := make(field.ErrorList, 0, len(conflicts))
	for _, conflict := range conflicts {
		allErrs = append(allErrs, injectedPodSpecNameConflictError(conflict))
	}
	return allErrs
}

func injectedPodSpecNameConflictError(conflict injectedPodSpecNameConflict) *field.Error {
	return field.Invalid(conflict.fieldPath, conflict.value, conflict.detail)
}

func indexInjectedPodSpecNameConflicts(
	conflicts []injectedPodSpecNameConflict,
) map[string]int {
	result := make(map[string]int, len(conflicts))
	for _, conflict := range conflicts {
		result[conflict.key]++
	}
	return result
}
