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
	"strconv"
	"strings"

	apicommon "github.com/ai-dynamo/grove/operator/api/common"
	groveconfigv1alpha1 "github.com/ai-dynamo/grove/operator/api/config/v1alpha1"
	grovecorev1alpha1 "github.com/ai-dynamo/grove/operator/api/core/v1alpha1"
	"github.com/ai-dynamo/grove/operator/internal/mnnvl"
	"github.com/ai-dynamo/grove/operator/internal/resourceclaim"

	k8svalidation "k8s.io/apimachinery/pkg/util/validation"
	"k8s.io/apimachinery/pkg/util/validation/field"
)

type generatedResourceClaimNameCheck struct {
	key              string
	fieldPath        *field.Path
	name             string
	context          string
	pattern          generatedNamePattern
	replicaBoundKeys []string
	errors           []string
}

type generatedNameToken struct {
	literal         string
	replicaBoundKey string
}

type generatedNamePattern []generatedNameToken

type generatedResourceClaimNameCollision struct {
	key              string
	fieldPath        *field.Path
	name             string
	contexts         [2]string
	replicaBoundKeys []string
}

type generatedObjectNameCheck struct {
	domain           string
	key              string
	fieldPath        *field.Path
	context          string
	pattern          generatedNamePattern
	replicaBoundKeys []string
}

type generatedObjectNameCollision struct {
	key              string
	domain           string
	fieldPath        *field.Path
	name             string
	contexts         [2]string
	replicaBoundKeys []string
}

type podCliqueScalingGroupCliqueRef struct {
	configIndex int
	cliqueIndex int
}

type podResourceClaimAliasOrigin string

const (
	podResourceClaimAliasOriginUser      podResourceClaimAliasOrigin = "user"
	podResourceClaimAliasOriginGenerated podResourceClaimAliasOrigin = "generated"
	podResourceClaimAliasOriginMNNVL     podResourceClaimAliasOrigin = "mnnvl"
)

type podResourceClaimAliasCheck struct {
	key              string
	fieldPath        *field.Path
	context          string
	pattern          generatedNamePattern
	replicaBoundKeys []string
	origin           podResourceClaimAliasOrigin
}

type podResourceClaimAliasContext struct {
	key         string
	description string
	cliqueIndex int
	checks      []podResourceClaimAliasCheck
}

type podResourceClaimAliasCollision struct {
	key              string
	fieldPath        *field.Path
	name             string
	podContext       string
	contexts         [2]string
	replicaBoundKeys []string
}

func (v *pcsValidator) validateGeneratedResourceClaimNames() field.ErrorList {
	checks := buildGeneratedResourceClaimNameChecks(v.pcs)
	return generatedResourceClaimNameErrors(checks, generatedNameReplicaBounds(v.pcs))
}

func (v *pcsValidator) validatePodResourceClaimAliasCollisions(autoMNNVLEnabled bool) field.ErrorList {
	return podResourceClaimAliasCollisionErrors(
		buildPodResourceClaimAliasContexts(v.pcs, autoMNNVLEnabled),
		generatedNameReplicaBounds(v.pcs),
	)
}

func (v *pcsValidator) validatePodResourceClaimAliasCollisionsOnUpdate(
	oldPCS *grovecorev1alpha1.PodCliqueSet,
	autoMNNVLEnabled bool,
) field.ErrorList {
	oldBounds := generatedNameReplicaBounds(oldPCS)
	newBounds := generatedNameReplicaBounds(v.pcs)
	oldCollisions := indexPodResourceClaimAliasCollisions(
		buildPodResourceClaimAliasCollisions(
			buildPodResourceClaimAliasContexts(oldPCS, autoMNNVLEnabled),
			oldBounds,
		),
	)

	var allErrs field.ErrorList
	for _, collision := range buildPodResourceClaimAliasCollisions(
		buildPodResourceClaimAliasContexts(v.pcs, autoMNNVLEnabled),
		newBounds,
	) {
		if oldCollisions[collision.key] > 0 &&
			!replicaBoundsIncreasedForKeys(oldBounds, newBounds, collision.replicaBoundKeys) {
			oldCollisions[collision.key]--
			continue
		}
		allErrs = append(allErrs, podResourceClaimAliasCollisionError(collision))
	}
	return allErrs
}

func (v *pcsValidator) validateGeneratedObjectNameCollisions() field.ErrorList {
	checks := buildGeneratedObjectNameChecks(v.pcs, v.validateSchedulerSubgroupNames())
	bounds := generatedNameReplicaBounds(v.pcs)
	allErrs := generatedObjectNameValidationErrors(checks, bounds)
	return append(allErrs, generatedObjectNameCollisionErrors(checks, bounds)...)
}

func (v *pcsValidator) validateGeneratedResourceClaimNamesOnUpdate(oldPCS *grovecorev1alpha1.PodCliqueSet) field.ErrorList {
	oldChecksList := buildGeneratedResourceClaimNameChecks(oldPCS)
	oldChecks := indexGeneratedResourceClaimNameChecks(oldChecksList)
	newChecks := buildGeneratedResourceClaimNameChecks(v.pcs)
	oldBounds := generatedNameReplicaBounds(oldPCS)
	newBounds := generatedNameReplicaBounds(v.pcs)

	var allErrs field.ErrorList
	for _, check := range newChecks {
		if len(check.errors) == 0 {
			continue
		}
		oldCheck, exists := oldChecks[check.key]
		if exists && len(oldCheck.errors) > 0 &&
			!generatedNameReplicaBoundsIncreasedForCheck(oldPCS, v.pcs, check) {
			continue
		}
		allErrs = append(allErrs, generatedResourceClaimNameError(check))
	}
	oldCollisions := indexGeneratedResourceClaimNameCollisions(
		buildGeneratedResourceClaimNameCollisions(oldChecksList, oldBounds),
	)
	for _, collision := range buildGeneratedResourceClaimNameCollisions(newChecks, newBounds) {
		if oldCollisions[collision.key] > 0 &&
			!replicaBoundsIncreasedForKeys(oldBounds, newBounds, collision.replicaBoundKeys) {
			oldCollisions[collision.key]--
			continue
		}
		allErrs = append(allErrs, generatedResourceClaimNameCollisionError(collision))
	}
	return allErrs
}

func (v *pcsValidator) validateGeneratedObjectNameCollisionsOnUpdate(oldPCS *grovecorev1alpha1.PodCliqueSet) field.ErrorList {
	oldBounds := generatedNameReplicaBounds(oldPCS)
	newBounds := generatedNameReplicaBounds(v.pcs)
	oldChecksList := buildGeneratedObjectNameChecks(oldPCS, v.validateSchedulerSubgroupNames())
	oldChecks := indexGeneratedObjectNameChecks(oldChecksList)
	newChecks := buildGeneratedObjectNameChecks(v.pcs, v.validateSchedulerSubgroupNames())
	oldCollisions := indexGeneratedObjectNameCollisions(
		buildGeneratedObjectNameCollisions(
			oldChecksList,
			oldBounds,
		),
	)

	var allErrs field.ErrorList
	for _, check := range newChecks {
		name, errors := generatedObjectNameValidation(check, newBounds)
		if len(errors) == 0 {
			continue
		}
		oldCheck, exists := oldChecks[check.key]
		if exists {
			_, oldErrors := generatedObjectNameValidation(oldCheck, oldBounds)
			if len(oldErrors) > 0 &&
				!replicaBoundsIncreasedForKeys(oldBounds, newBounds, check.replicaBoundKeys) {
				continue
			}
		}
		allErrs = append(allErrs, generatedObjectNameError(check, name, errors))
	}
	for _, collision := range buildGeneratedObjectNameCollisions(
		newChecks,
		newBounds,
	) {
		if oldCollisions[collision.key] > 0 &&
			!replicaBoundsIncreasedForKeys(oldBounds, newBounds, collision.replicaBoundKeys) {
			oldCollisions[collision.key]--
			continue
		}
		allErrs = append(allErrs, generatedObjectNameCollisionError(collision))
	}
	return allErrs
}

func (v *pcsValidator) validateSchedulerSubgroupNames() bool {
	if !v.tasEnabled || v.schedRegistry == nil || len(v.pcs.Spec.Template.Cliques) == 0 ||
		v.pcs.Spec.Template.Cliques[0] == nil {
		return false
	}
	schedulerName := v.pcs.Spec.Template.Cliques[0].Spec.PodSpec.SchedulerName
	backend := v.schedRegistry.GetOrDefault(schedulerName)
	return backend != nil && backend.Name() == string(groveconfigv1alpha1.SchedulerNameKai)
}

func buildGeneratedObjectNameChecks(pcs *grovecorev1alpha1.PodCliqueSet, validateSchedulerSubgroups bool) []generatedObjectNameCheck {
	var checks []generatedObjectNameCheck
	templatePath := field.NewPath("spec", "template")
	pcsPattern := appendReplicaToken(literalGeneratedNamePattern(pcs.Name), "pcs")
	checks = appendGeneratedObjectNameCheck(
		checks,
		"PodGang",
		"podgang/base",
		field.NewPath("metadata", "name"),
		"base PodGang",
		pcsPattern,
	)

	pcsgRefsByClique := make(map[string][]podCliqueScalingGroupCliqueRef)
	for i := range pcs.Spec.Template.PodCliqueScalingGroupConfigs {
		cfg := &pcs.Spec.Template.PodCliqueScalingGroupConfigs[i]
		for j, cliqueName := range cfg.CliqueNames {
			pcsgRefsByClique[cliqueName] = append(
				pcsgRefsByClique[cliqueName],
				podCliqueScalingGroupCliqueRef{configIndex: i, cliqueIndex: j},
			)
		}

		pcsgPattern := appendLiteralTokens(pcsPattern, cfg.Name)
		checks = appendGeneratedObjectNameCheck(
			checks,
			"PodCliqueScalingGroup",
			"pcsg/"+cfg.Name,
			templatePath.Child("podCliqueScalingGroups").Index(i).Child("name"),
			fmt.Sprintf("PodCliqueScalingGroup %q", cfg.Name),
			pcsgPattern,
		)
		if maxPCSGConfiguredReplicas(cfg) > minAvailableReplicas(cfg) {
			scaledPodGangPattern := appendReplicaToken(pcsgPattern, "pcsg-scaled/"+cfg.Name)
			checks = appendGeneratedObjectNameCheck(
				checks,
				"PodGang",
				"podgang/scaled/"+cfg.Name,
				templatePath.Child("podCliqueScalingGroups").Index(i).Child("name"),
				fmt.Sprintf("scaled PodGang for PodCliqueScalingGroup %q", cfg.Name),
				scaledPodGangPattern,
			)
		}
		if cfg.ScaleConfig != nil {
			checks = appendGeneratedObjectNameCheck(
				checks,
				"HorizontalPodAutoscaler",
				"hpa/pcsg/"+cfg.Name,
				templatePath.Child("podCliqueScalingGroups").Index(i).Child("name"),
				fmt.Sprintf("PodCliqueScalingGroup %q", cfg.Name),
				pcsgPattern,
			)
		}

		if validateSchedulerSubgroups && cfg.TopologyConstraint != nil {
			parentPattern := appendReplicaToken(pcsgPattern, "pcsg-base/"+cfg.Name)
			checks = appendGeneratedObjectNameCheck(
				checks,
				"scheduler subgroup",
				"scheduler-parent/"+cfg.Name,
				templatePath.Child("podCliqueScalingGroups").Index(i).Child("name"),
				fmt.Sprintf("topology group for PodCliqueScalingGroup %q", cfg.Name),
				parentPattern,
			)
		}
	}

	for i, clique := range pcs.Spec.Template.Cliques {
		if clique == nil {
			continue
		}
		cliquePath := templatePath.Child("cliques").Index(i).Child("name")
		pcsgRefs := pcsgRefsByClique[clique.Name]
		if len(pcsgRefs) == 0 {
			pattern := appendLiteralTokens(pcsPattern, clique.Name)
			checks = appendGeneratedObjectNameCheck(
				checks,
				"PodClique",
				"pclq/standalone/"+clique.Name,
				cliquePath,
				fmt.Sprintf("standalone PodClique %q", clique.Name),
				pattern,
			)
			checks = appendGeneratedObjectNameCheck(
				checks,
				"Pod hostname",
				"pod-hostname/standalone/"+clique.Name,
				cliquePath,
				fmt.Sprintf("standalone PodClique %q", clique.Name),
				appendReplicaToken(pattern, "pclq/"+clique.Name),
			)
			if validateSchedulerSubgroups {
				checks = appendGeneratedObjectNameCheck(
					checks,
					"scheduler subgroup",
					"scheduler-leaf/standalone/"+clique.Name,
					cliquePath,
					fmt.Sprintf("standalone PodClique %q", clique.Name),
					pattern,
				)
			}
			if clique.Spec.ScaleConfig != nil {
				checks = appendGeneratedObjectNameCheck(
					checks,
					"HorizontalPodAutoscaler",
					"hpa/pclq/"+clique.Name,
					cliquePath,
					fmt.Sprintf("standalone PodClique %q", clique.Name),
					pattern,
				)
			}
			continue
		}

		for _, pcsgRef := range pcsgRefs {
			cfg := &pcs.Spec.Template.PodCliqueScalingGroupConfigs[pcsgRef.configIndex]
			pcsgPattern := appendLiteralTokens(pcsPattern, cfg.Name)
			pclqPattern := appendReplicaToken(pcsgPattern, "pcsg/"+cfg.Name)
			pclqPattern = appendLiteralTokens(pclqPattern, clique.Name)
			pcsgCliquePath := templatePath.Child("podCliqueScalingGroups").
				Index(pcsgRef.configIndex).
				Child("cliqueNames").
				Index(pcsgRef.cliqueIndex)
			checks = appendGeneratedObjectNameCheck(
				checks,
				"PodClique",
				fmt.Sprintf("pclq/pcsg/%s/%s", cfg.Name, clique.Name),
				pcsgCliquePath,
				fmt.Sprintf("PodClique %q in PodCliqueScalingGroup %q", clique.Name, cfg.Name),
				pclqPattern,
			)
			checks = appendGeneratedObjectNameCheck(
				checks,
				"Pod hostname",
				fmt.Sprintf("pod-hostname/pcsg/%s/%s", cfg.Name, clique.Name),
				pcsgCliquePath,
				fmt.Sprintf("PodClique %q in PodCliqueScalingGroup %q", clique.Name, cfg.Name),
				appendReplicaToken(pclqPattern, "pclq/"+clique.Name),
			)

			if validateSchedulerSubgroups {
				basePCLQPattern := appendReplicaToken(pcsgPattern, "pcsg-base/"+cfg.Name)
				basePCLQPattern = appendLiteralTokens(basePCLQPattern, clique.Name)
				checks = appendGeneratedObjectNameCheck(
					checks,
					"scheduler subgroup",
					fmt.Sprintf("scheduler-leaf/pcsg/%s/%s", cfg.Name, clique.Name),
					pcsgCliquePath,
					fmt.Sprintf("PodClique %q in PodCliqueScalingGroup %q", clique.Name, cfg.Name),
					basePCLQPattern,
				)
			}
		}
	}

	return checks
}

func appendGeneratedObjectNameCheck(
	checks []generatedObjectNameCheck,
	domain, key string,
	fldPath *field.Path,
	context string,
	pattern generatedNamePattern,
) []generatedObjectNameCheck {
	return append(checks, generatedObjectNameCheck{
		domain:           domain,
		key:              key,
		fieldPath:        fldPath,
		context:          context,
		pattern:          pattern,
		replicaBoundKeys: replicaBoundKeysForPattern(pattern),
	})
}

func generatedObjectNameCollisionErrors(
	checks []generatedObjectNameCheck,
	replicaBounds map[string]int32,
) field.ErrorList {
	var allErrs field.ErrorList
	for _, collision := range buildGeneratedObjectNameCollisions(checks, replicaBounds) {
		allErrs = append(allErrs, generatedObjectNameCollisionError(collision))
	}
	return allErrs
}

func generatedObjectNameValidationErrors(
	checks []generatedObjectNameCheck,
	replicaBounds map[string]int32,
) field.ErrorList {
	var allErrs field.ErrorList
	for _, check := range checks {
		name, errors := generatedObjectNameValidation(check, replicaBounds)
		if len(errors) > 0 {
			allErrs = append(allErrs, generatedObjectNameError(check, name, errors))
		}
	}
	return allErrs
}

func generatedObjectNameValidation(
	check generatedObjectNameCheck,
	replicaBounds map[string]int32,
) (string, []string) {
	name, exists := renderGeneratedNamePatternAtMaximum(check.pattern, replicaBounds)
	if !exists {
		return "", nil
	}
	if check.domain == "Pod hostname" || check.domain == "scheduler subgroup" {
		return name, k8svalidation.IsDNS1123Label(name)
	}
	return name, k8svalidation.IsValidLabelValue(name)
}

func generatedObjectNameError(
	check generatedObjectNameCheck,
	name string,
	errors []string,
) *field.Error {
	return field.Invalid(
		check.fieldPath,
		name,
		generatedObjectNameValidationDetail(check, errors),
	)
}

func generatedObjectNameValidationDetail(
	check generatedObjectNameCheck,
	errors []string,
) string {
	if check.domain == "Pod hostname" {
		return fmt.Sprintf(
			"generated Pod hostname for %s must be a valid DNS_LABEL because it is used as pod.spec.hostname: %s",
			check.context,
			strings.Join(errors, "; "),
		)
	}
	if check.domain == "scheduler subgroup" {
		return fmt.Sprintf(
			"generated KAI scheduler subgroup name for %s must be a valid DNS_LABEL: %s",
			check.context,
			strings.Join(errors, "; "),
		)
	}
	return fmt.Sprintf(
		"generated %s name for %s must be a valid label value because it is copied into Kubernetes labels: %s",
		check.domain,
		check.context,
		strings.Join(errors, "; "),
	)
}

func buildGeneratedObjectNameCollisions(
	checks []generatedObjectNameCheck,
	replicaBounds map[string]int32,
) []generatedObjectNameCollision {
	var collisions []generatedObjectNameCollision
	for i := range checks {
		for j := i + 1; j < len(checks); j++ {
			if checks[i].domain != checks[j].domain {
				continue
			}
			if checks[i].domain == "Pod hostname" {
				continue
			}
			if checks[i].domain == "scheduler subgroup" &&
				strings.HasPrefix(checks[i].key, "scheduler-parent/") ==
					strings.HasPrefix(checks[j].key, "scheduler-parent/") {
				continue
			}
			name, collides := generatedNamePatternsCollide(checks[i].pattern, checks[j].pattern, replicaBounds)
			if !collides {
				continue
			}
			collisions = append(collisions, generatedObjectNameCollision{
				key:       generatedObjectNameCollisionKey(checks[i], checks[j]),
				domain:    checks[i].domain,
				fieldPath: checks[j].fieldPath,
				name:      name,
				contexts:  [2]string{checks[i].context, checks[j].context},
				replicaBoundKeys: mergeReplicaBoundKeys(
					checks[i].replicaBoundKeys,
					checks[j].replicaBoundKeys,
				),
			})
		}
	}
	return collisions
}

func generatedObjectNameCollisionError(collision generatedObjectNameCollision) *field.Error {
	return field.Invalid(
		collision.fieldPath,
		collision.name,
		fmt.Sprintf(
			"generated %s name collides between %s and %s; generated %s names must be unique within a PodCliqueSet",
			collision.domain,
			collision.contexts[0],
			collision.contexts[1],
			collision.domain,
		),
	)
}

func generatedObjectNameCollisionKey(left, right generatedObjectNameCheck) string {
	return left.domain + "\x00" + generatedResourceClaimNameCollisionKey(left.key, right.key)
}

func indexGeneratedObjectNameCollisions(
	collisions []generatedObjectNameCollision,
) map[string]int {
	result := make(map[string]int, len(collisions))
	for _, collision := range collisions {
		result[collision.key]++
	}
	return result
}

func indexGeneratedObjectNameChecks(checks []generatedObjectNameCheck) map[string]generatedObjectNameCheck {
	result := make(map[string]generatedObjectNameCheck, len(checks))
	for _, check := range checks {
		result[check.key] = check
	}
	return result
}

func buildPodResourceClaimAliasContexts(
	pcs *grovecorev1alpha1.PodCliqueSet,
	autoMNNVLEnabled bool,
) []podResourceClaimAliasContext {
	pcsgIndexesByClique := make(map[string][]int)
	for i := range pcs.Spec.Template.PodCliqueScalingGroupConfigs {
		for _, cliqueName := range pcs.Spec.Template.PodCliqueScalingGroupConfigs[i].CliqueNames {
			pcsgIndexesByClique[cliqueName] = append(pcsgIndexesByClique[cliqueName], i)
		}
	}

	var contexts []podResourceClaimAliasContext
	for cliqueIndex, clique := range pcs.Spec.Template.Cliques {
		if clique == nil {
			continue
		}
		pcsgIndexes := pcsgIndexesByClique[clique.Name]
		if len(pcsgIndexes) == 0 {
			contexts = append(contexts, buildPodResourceClaimAliasContext(
				pcs,
				cliqueIndex,
				nil,
				autoMNNVLEnabled,
			))
			continue
		}
		for _, pcsgIndex := range pcsgIndexes {
			contexts = append(contexts, buildPodResourceClaimAliasContext(
				pcs,
				cliqueIndex,
				&pcsgIndex,
				autoMNNVLEnabled,
			))
		}
	}
	return contexts
}

func buildPodResourceClaimAliasContext(
	pcs *grovecorev1alpha1.PodCliqueSet,
	cliqueIndex int,
	pcsgIndex *int,
	autoMNNVLEnabled bool,
) podResourceClaimAliasContext {
	clique := pcs.Spec.Template.Cliques[cliqueIndex]
	cliquePath := field.NewPath("spec", "template", "cliques").Index(cliqueIndex)
	context := podResourceClaimAliasContext{
		key:         "standalone/" + clique.Name,
		description: fmt.Sprintf("standalone PodClique %q", clique.Name),
		cliqueIndex: cliqueIndex,
	}
	matchNames := []string{clique.Name}

	var pcsg *grovecorev1alpha1.PodCliqueScalingGroupConfig
	if pcsgIndex != nil {
		pcsg = &pcs.Spec.Template.PodCliqueScalingGroupConfigs[*pcsgIndex]
		context.key = fmt.Sprintf("pcsg/%s/%s", pcsg.Name, clique.Name)
		context.description = fmt.Sprintf("PodClique %q in PodCliqueScalingGroup %q", clique.Name, pcsg.Name)
		matchNames = append(matchNames, pcsg.Name)
	}

	for i, claim := range clique.Spec.PodSpec.ResourceClaims {
		if claim.Name == "" {
			continue
		}
		context.checks = append(context.checks, podResourceClaimAliasCheck{
			key:       "user/" + claim.Name,
			fieldPath: cliquePath.Child("spec", "podSpec", "resourceClaims").Index(i).Child("name"),
			context:   fmt.Sprintf("user-defined pod resource claim %q", claim.Name),
			pattern:   literalGeneratedNamePattern(claim.Name),
			origin:    podResourceClaimAliasOriginUser,
		})
	}

	pcsOwnerPattern := literalGeneratedNamePattern(pcs.Name)
	for i := range pcs.Spec.Template.ResourceSharing {
		ref := &pcs.Spec.Template.ResourceSharing[i]
		if !ref.FilterMatches(matchNames...) {
			continue
		}
		context.checks = appendGeneratedPodResourceClaimAliasCheck(
			context.checks,
			fmt.Sprintf("generated/pcs/%d", i),
			field.NewPath("spec", "template", "resourceSharing").Index(i).Child("name"),
			"PodCliqueSet resourceSharing",
			pcsOwnerPattern,
			&ref.ResourceSharingSpec,
			"pcs",
		)
	}

	pclqOwnerPattern := appendReplicaToken(pcsOwnerPattern, "pcs")
	if pcsg != nil {
		pcsgOwnerPattern := appendLiteralTokens(pclqOwnerPattern, pcsg.Name)
		for i := range pcsg.ResourceSharing {
			ref := &pcsg.ResourceSharing[i]
			if !ref.FilterMatches(clique.Name) {
				continue
			}
			context.checks = appendGeneratedPodResourceClaimAliasCheck(
				context.checks,
				fmt.Sprintf("generated/pcsg/%s/%d", pcsg.Name, i),
				field.NewPath("spec", "template", "podCliqueScalingGroups").
					Index(*pcsgIndex).
					Child("resourceSharing").
					Index(i).
					Child("name"),
				fmt.Sprintf("PodCliqueScalingGroup %q resourceSharing", pcsg.Name),
				pcsgOwnerPattern,
				&ref.ResourceSharingSpec,
				"pcsg/"+pcsg.Name,
			)
		}
		pclqOwnerPattern = appendReplicaToken(pcsgOwnerPattern, "pcsg/"+pcsg.Name)
	}
	pclqOwnerPattern = appendLiteralTokens(pclqOwnerPattern, clique.Name)
	for i := range clique.ResourceSharing {
		context.checks = appendGeneratedPodResourceClaimAliasCheck(
			context.checks,
			fmt.Sprintf("generated/pclq/%s/%d", clique.Name, i),
			cliquePath.Child("resourceSharing").Index(i).Child("name"),
			fmt.Sprintf("PodClique %q resourceSharing", clique.Name),
			pclqOwnerPattern,
			&clique.ResourceSharing[i],
			"pclq/"+clique.Name,
		)
	}

	annotationLayers := []map[string]string{clique.Annotations}
	if pcsg != nil {
		annotationLayers = append(annotationLayers, pcsg.Annotations)
	}
	annotationLayers = append(annotationLayers, pcs.Annotations)
	if _, enabled := mnnvl.ResolveGroupNameHierarchically(annotationLayers...); autoMNNVLEnabled &&
		enabled &&
		mnnvl.HasGPUInPodSpec(&clique.Spec.PodSpec) {
		context.checks = append(context.checks, podResourceClaimAliasCheck{
			key:       "mnnvl",
			fieldPath: cliquePath.Child("spec", "podSpec", "resourceClaims"),
			context:   "Auto-MNNVL injected claim",
			pattern:   literalGeneratedNamePattern(mnnvl.MNNVLClaimName),
			origin:    podResourceClaimAliasOriginMNNVL,
		})
	}

	return context
}

func appendGeneratedPodResourceClaimAliasCheck(
	checks []podResourceClaimAliasCheck,
	key string,
	fldPath *field.Path,
	context string,
	ownerPattern generatedNamePattern,
	ref *grovecorev1alpha1.ResourceSharingSpec,
	localReplicaBoundKey string,
) []podResourceClaimAliasCheck {
	if ref.Name == "" {
		return checks
	}

	pattern := append(generatedNamePattern(nil), ownerPattern...)
	switch ref.Scope {
	case grovecorev1alpha1.ResourceSharingScopeAllReplicas:
		pattern = appendLiteralTokens(pattern, "all")
	case grovecorev1alpha1.ResourceSharingScopePerReplica:
		pattern = appendReplicaToken(pattern, localReplicaBoundKey)
	default:
		return checks
	}
	pattern = appendLiteralTokens(pattern, ref.Name)

	return append(checks, podResourceClaimAliasCheck{
		key:              key,
		fieldPath:        fldPath,
		context:          fmt.Sprintf("%s %q", context, ref.Name),
		pattern:          pattern,
		replicaBoundKeys: replicaBoundKeysForPattern(pattern),
		origin:           podResourceClaimAliasOriginGenerated,
	})
}

func podResourceClaimAliasCollisionErrors(
	contexts []podResourceClaimAliasContext,
	replicaBounds map[string]int32,
) field.ErrorList {
	var allErrs field.ErrorList
	for _, collision := range buildPodResourceClaimAliasCollisions(contexts, replicaBounds) {
		allErrs = append(allErrs, podResourceClaimAliasCollisionError(collision))
	}
	return allErrs
}

func buildPodResourceClaimAliasCollisions(
	contexts []podResourceClaimAliasContext,
	replicaBounds map[string]int32,
) []podResourceClaimAliasCollision {
	var collisions []podResourceClaimAliasCollision
	for _, podContext := range contexts {
		for i := range podContext.checks {
			for j := i + 1; j < len(podContext.checks); j++ {
				left, right := podContext.checks[i], podContext.checks[j]
				if left.origin == podResourceClaimAliasOriginGenerated &&
					right.origin == podResourceClaimAliasOriginGenerated {
					continue
				}
				name, collides := generatedNamePatternsCollide(left.pattern, right.pattern, replicaBounds)
				if !collides {
					continue
				}
				reportedCheck := preferredPodResourceClaimAliasCheck(left, right)
				collisions = append(collisions, podResourceClaimAliasCollision{
					key:        podContext.key + "\x00" + generatedResourceClaimNameCollisionKey(left.key, right.key),
					fieldPath:  reportedCheck.fieldPath,
					name:       name,
					podContext: podContext.description,
					contexts:   [2]string{left.context, right.context},
					replicaBoundKeys: mergeReplicaBoundKeys(
						left.replicaBoundKeys,
						right.replicaBoundKeys,
					),
				})
			}
		}
	}
	return collisions
}

func preferredPodResourceClaimAliasCheck(
	left, right podResourceClaimAliasCheck,
) podResourceClaimAliasCheck {
	priority := map[podResourceClaimAliasOrigin]int{
		podResourceClaimAliasOriginUser:      0,
		podResourceClaimAliasOriginGenerated: 1,
		podResourceClaimAliasOriginMNNVL:     2,
	}
	if priority[left.origin] < priority[right.origin] {
		return left
	}
	return right
}

func podResourceClaimAliasCollisionError(collision podResourceClaimAliasCollision) *field.Error {
	return field.Invalid(
		collision.fieldPath,
		collision.name,
		fmt.Sprintf(
			"pod resource claim alias collides between %s and %s in %s; final pod.spec.resourceClaims[].name values must be unique",
			collision.contexts[0],
			collision.contexts[1],
			collision.podContext,
		),
	)
}

func indexPodResourceClaimAliasCollisions(
	collisions []podResourceClaimAliasCollision,
) map[string]int {
	result := make(map[string]int, len(collisions))
	for _, collision := range collisions {
		result[collision.key]++
	}
	return result
}

func buildGeneratedResourceClaimNameChecks(pcs *grovecorev1alpha1.PodCliqueSet) []generatedResourceClaimNameCheck {
	var checks []generatedResourceClaimNameCheck
	templatePath := field.NewPath("spec", "template")
	pcsReplicaIndex := maxReplicaIndex(pcs.Spec.Replicas)
	pcsOwnerPattern := literalGeneratedNamePattern(pcs.Name)

	for i := range pcs.Spec.Template.ResourceSharing {
		ref := &pcs.Spec.Template.ResourceSharing[i].ResourceSharingSpec
		checks = appendResourceClaimNameCheck(
			checks,
			fmt.Sprintf("pcs/%s/%s", ref.Name, ref.Scope),
			templatePath.Child("resourceSharing").Index(i).Child("name"),
			pcs.Name,
			pcsOwnerPattern,
			ref,
			pcsReplicaIndex,
			"PodCliqueSet",
			"pcs",
		)
	}

	pcsgConfigsByClique := make(map[string][]int)
	for i := range pcs.Spec.Template.PodCliqueScalingGroupConfigs {
		cfg := &pcs.Spec.Template.PodCliqueScalingGroupConfigs[i]
		pcsgName := apicommon.GeneratePodCliqueScalingGroupName(
			apicommon.ResourceNameReplica{Name: pcs.Name, Replica: pcsReplicaIndex},
			cfg.Name,
		)
		pcsgOwnerPattern := appendReplicaToken(pcsOwnerPattern, "pcs")
		pcsgOwnerPattern = appendLiteralTokens(pcsgOwnerPattern, cfg.Name)
		pcsgReplicaIndex := maxReplicaIndex(maxPCSGConfiguredReplicas(cfg))
		for j := range cfg.ResourceSharing {
			ref := &cfg.ResourceSharing[j].ResourceSharingSpec
			checks = appendResourceClaimNameCheck(
				checks,
				fmt.Sprintf("pcsg/%s/%s/%s", cfg.Name, ref.Name, ref.Scope),
				templatePath.Child("podCliqueScalingGroups").Index(i).Child("resourceSharing").Index(j).Child("name"),
				pcsgName,
				pcsgOwnerPattern,
				ref,
				pcsgReplicaIndex,
				fmt.Sprintf("PodCliqueScalingGroup %q", cfg.Name),
				"pcsg/"+cfg.Name,
			)
		}
		for _, cliqueName := range cfg.CliqueNames {
			pcsgConfigsByClique[cliqueName] = append(pcsgConfigsByClique[cliqueName], i)
		}
	}

	for i, clique := range pcs.Spec.Template.Cliques {
		if clique == nil {
			continue
		}
		cliquePath := templatePath.Child("cliques").Index(i).Child("resourceSharing")
		pclqReplicaIndex := maxReplicaIndex(maxConfiguredReplicas(&clique.Spec.Replicas, clique.Spec.ScaleConfig))
		pcsgIndexes := pcsgConfigsByClique[clique.Name]
		if len(pcsgIndexes) == 0 {
			pclqName := apicommon.GeneratePodCliqueName(
				apicommon.ResourceNameReplica{Name: pcs.Name, Replica: pcsReplicaIndex},
				clique.Name,
			)
			pclqOwnerPattern := appendReplicaToken(pcsOwnerPattern, "pcs")
			pclqOwnerPattern = appendLiteralTokens(pclqOwnerPattern, clique.Name)
			for j := range clique.ResourceSharing {
				ref := &clique.ResourceSharing[j]
				checks = appendResourceClaimNameCheck(
					checks,
					fmt.Sprintf("pclq/%s/standalone/%s/%s", clique.Name, ref.Name, ref.Scope),
					cliquePath.Index(j).Child("name"),
					pclqName,
					pclqOwnerPattern,
					ref,
					pclqReplicaIndex,
					fmt.Sprintf("standalone PodClique %q", clique.Name),
					"pclq/"+clique.Name,
				)
			}
			continue
		}

		for _, pcsgIndex := range pcsgIndexes {
			cfg := &pcs.Spec.Template.PodCliqueScalingGroupConfigs[pcsgIndex]
			pcsgName := apicommon.GeneratePodCliqueScalingGroupName(
				apicommon.ResourceNameReplica{Name: pcs.Name, Replica: pcsReplicaIndex},
				cfg.Name,
			)
			pclqName := apicommon.GeneratePodCliqueName(
				apicommon.ResourceNameReplica{
					Name:    pcsgName,
					Replica: maxReplicaIndex(maxPCSGConfiguredReplicas(cfg)),
				},
				clique.Name,
			)
			pclqOwnerPattern := appendReplicaToken(pcsOwnerPattern, "pcs")
			pclqOwnerPattern = appendLiteralTokens(pclqOwnerPattern, cfg.Name)
			pclqOwnerPattern = appendReplicaToken(pclqOwnerPattern, "pcsg/"+cfg.Name)
			pclqOwnerPattern = appendLiteralTokens(pclqOwnerPattern, clique.Name)
			for j := range clique.ResourceSharing {
				ref := &clique.ResourceSharing[j]
				checks = appendResourceClaimNameCheck(
					checks,
					fmt.Sprintf("pclq/%s/pcsg/%s/%s/%s", clique.Name, cfg.Name, ref.Name, ref.Scope),
					cliquePath.Index(j).Child("name"),
					pclqName,
					pclqOwnerPattern,
					ref,
					pclqReplicaIndex,
					fmt.Sprintf("PodClique %q in PodCliqueScalingGroup %q", clique.Name, cfg.Name),
					"pclq/"+clique.Name,
				)
			}
		}
	}

	return checks
}

func appendResourceClaimNameCheck(
	checks []generatedResourceClaimNameCheck,
	key string,
	fldPath *field.Path,
	ownerName string,
	ownerPattern generatedNamePattern,
	ref *grovecorev1alpha1.ResourceSharingSpec,
	replicaIndex int,
	context string,
	localReplicaBoundKey string,
) []generatedResourceClaimNameCheck {
	if ref.Name == "" {
		return checks
	}

	var generatedName string
	pattern := append(generatedNamePattern(nil), ownerPattern...)
	switch ref.Scope {
	case grovecorev1alpha1.ResourceSharingScopeAllReplicas:
		generatedName = resourceclaim.AllReplicasRCName(ownerName, ref.Name)
		pattern = appendLiteralTokens(pattern, "all")
	case grovecorev1alpha1.ResourceSharingScopePerReplica:
		generatedName = resourceclaim.PerReplicaRCName(ownerName, replicaIndex, ref.Name)
		pattern = appendReplicaToken(pattern, localReplicaBoundKey)
	default:
		return checks
	}
	pattern = appendLiteralTokens(pattern, ref.Name)

	return append(checks, generatedResourceClaimNameCheck{
		key:              key,
		fieldPath:        fldPath,
		name:             generatedName,
		context:          context,
		pattern:          pattern,
		replicaBoundKeys: replicaBoundKeysForPattern(pattern),
		errors:           k8svalidation.IsDNS1123Label(generatedName),
	})
}

func generatedResourceClaimNameErrors(
	checks []generatedResourceClaimNameCheck,
	replicaBounds map[string]int32,
) field.ErrorList {
	var allErrs field.ErrorList
	for _, check := range checks {
		if len(check.errors) > 0 {
			allErrs = append(allErrs, generatedResourceClaimNameError(check))
		}
	}
	for _, collision := range buildGeneratedResourceClaimNameCollisions(checks, replicaBounds) {
		allErrs = append(allErrs, generatedResourceClaimNameCollisionError(collision))
	}
	return allErrs
}

func generatedResourceClaimNameError(check generatedResourceClaimNameCheck) *field.Error {
	return field.Invalid(
		check.fieldPath,
		check.name,
		fmt.Sprintf(
			"generated ResourceClaim name for %s must be a valid DNS_LABEL because it is used as pod.spec.resourceClaims[].name: %s",
			check.context,
			strings.Join(check.errors, "; "),
		),
	)
}

func generatedResourceClaimNameCollisionError(collision generatedResourceClaimNameCollision) *field.Error {
	return field.Invalid(
		collision.fieldPath,
		collision.name,
		fmt.Sprintf(
			"generated ResourceClaim name collides between %s and %s; names generated by one PodCliqueSet must be unique and must not duplicate pod.spec.resourceClaims entries",
			collision.contexts[0],
			collision.contexts[1],
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

func buildGeneratedResourceClaimNameCollisions(
	checks []generatedResourceClaimNameCheck,
	replicaBounds map[string]int32,
) []generatedResourceClaimNameCollision {
	var collisions []generatedResourceClaimNameCollision
	for i := range checks {
		if len(checks[i].errors) > 0 {
			continue
		}
		for j := i + 1; j < len(checks); j++ {
			if len(checks[j].errors) > 0 {
				continue
			}
			name, collides := generatedNamePatternsCollide(checks[i].pattern, checks[j].pattern, replicaBounds)
			if !collides {
				continue
			}
			collisions = append(collisions, generatedResourceClaimNameCollision{
				key:       generatedResourceClaimNameCollisionKey(checks[i].key, checks[j].key),
				fieldPath: checks[j].fieldPath,
				name:      name,
				contexts:  [2]string{checks[i].context, checks[j].context},
				replicaBoundKeys: mergeReplicaBoundKeys(
					checks[i].replicaBoundKeys,
					checks[j].replicaBoundKeys,
				),
			})
		}
	}
	return collisions
}

func indexGeneratedResourceClaimNameCollisions(
	collisions []generatedResourceClaimNameCollision,
) map[string]int {
	result := make(map[string]int, len(collisions))
	for _, collision := range collisions {
		result[collision.key]++
	}
	return result
}

func generatedResourceClaimNameCollisionKey(left, right string) string {
	if left > right {
		left, right = right, left
	}
	return left + "\x00" + right
}

func generatedNamePatternsCollide(
	left, right generatedNamePattern,
	replicaBounds map[string]int32,
) (string, bool) {
	if len(left) != len(right) {
		return "", false
	}

	parts := make([]string, len(left))
	for i := range left {
		part, compatible := generatedNameTokensIntersect(left[i], right[i], replicaBounds)
		if !compatible {
			return "", false
		}
		parts[i] = part
	}
	return strings.Join(parts, "-"), true
}

func renderGeneratedNamePatternAtMaximum(
	pattern generatedNamePattern,
	replicaBounds map[string]int32,
) (string, bool) {
	parts := make([]string, len(pattern))
	for i, token := range pattern {
		if token.replicaBoundKey == "" {
			parts[i] = token.literal
			continue
		}
		bound := replicaBounds[token.replicaBoundKey]
		if bound <= 0 {
			return "", false
		}
		parts[i] = strconv.FormatInt(int64(bound-1), 10)
	}
	return strings.Join(parts, "-"), true
}

func generatedNameTokensIntersect(
	left, right generatedNameToken,
	replicaBounds map[string]int32,
) (string, bool) {
	switch {
	case left.replicaBoundKey == "" && right.replicaBoundKey == "":
		return left.literal, left.literal == right.literal
	case left.replicaBoundKey != "" && right.replicaBoundKey != "":
		if replicaBounds[left.replicaBoundKey] <= 0 || replicaBounds[right.replicaBoundKey] <= 0 {
			return "", false
		}
		return "0", true
	case left.replicaBoundKey != "":
		return replicaTokenIntersectsLiteral(left.replicaBoundKey, right.literal, replicaBounds)
	default:
		return replicaTokenIntersectsLiteral(right.replicaBoundKey, left.literal, replicaBounds)
	}
}

func replicaTokenIntersectsLiteral(
	replicaBoundKey, literal string,
	replicaBounds map[string]int32,
) (string, bool) {
	if literal == "" || (len(literal) > 1 && literal[0] == '0') {
		return "", false
	}
	value, err := strconv.ParseInt(literal, 10, 32)
	if err != nil || value < 0 || value >= int64(replicaBounds[replicaBoundKey]) {
		return "", false
	}
	return literal, true
}

func literalGeneratedNamePattern(value string) generatedNamePattern {
	return appendLiteralTokens(nil, value)
}

func appendLiteralTokens(pattern generatedNamePattern, value string) generatedNamePattern {
	result := append(generatedNamePattern(nil), pattern...)
	for _, part := range strings.Split(value, "-") {
		result = append(result, generatedNameToken{literal: part})
	}
	return result
}

func appendReplicaToken(pattern generatedNamePattern, replicaBoundKey string) generatedNamePattern {
	return append(
		append(generatedNamePattern(nil), pattern...),
		generatedNameToken{replicaBoundKey: replicaBoundKey},
	)
}

func replicaBoundKeysForPattern(pattern generatedNamePattern) []string {
	var result []string
	seen := make(map[string]struct{})
	for _, token := range pattern {
		if token.replicaBoundKey == "" {
			continue
		}
		if _, exists := seen[token.replicaBoundKey]; exists {
			continue
		}
		seen[token.replicaBoundKey] = struct{}{}
		result = append(result, token.replicaBoundKey)
	}
	return result
}

func mergeReplicaBoundKeys(left, right []string) []string {
	result := append([]string(nil), left...)
	seen := make(map[string]struct{}, len(left)+len(right))
	for _, key := range left {
		seen[key] = struct{}{}
	}
	for _, key := range right {
		if _, exists := seen[key]; exists {
			continue
		}
		seen[key] = struct{}{}
		result = append(result, key)
	}
	return result
}

func generatedNameReplicaBoundsIncreasedForCheck(
	oldPCS, newPCS *grovecorev1alpha1.PodCliqueSet,
	check generatedResourceClaimNameCheck,
) bool {
	oldBounds := generatedNameReplicaBounds(oldPCS)
	newBounds := generatedNameReplicaBounds(newPCS)
	return replicaBoundsIncreasedForKeys(oldBounds, newBounds, check.replicaBoundKeys)
}

func replicaBoundsIncreasedForKeys(oldBounds, newBounds map[string]int32, keys []string) bool {
	for _, key := range keys {
		newBound := newBounds[key]
		if newBound > oldBounds[key] {
			return true
		}
	}
	return false
}

func generatedNameReplicaBounds(pcs *grovecorev1alpha1.PodCliqueSet) map[string]int32 {
	bounds := map[string]int32{"pcs": pcs.Spec.Replicas}
	for _, clique := range pcs.Spec.Template.Cliques {
		if clique == nil {
			continue
		}
		bounds["pclq/"+clique.Name] = maxConfiguredReplicas(&clique.Spec.Replicas, clique.Spec.ScaleConfig)
	}
	for i := range pcs.Spec.Template.PodCliqueScalingGroupConfigs {
		cfg := &pcs.Spec.Template.PodCliqueScalingGroupConfigs[i]
		bounds["pcsg/"+cfg.Name] = maxPCSGConfiguredReplicas(cfg)
		bounds["pcsg-base/"+cfg.Name] = minAvailableReplicas(cfg)
		bounds["pcsg-scaled/"+cfg.Name] = max(
			0,
			maxPCSGConfiguredReplicas(cfg)-minAvailableReplicas(cfg),
		)
	}
	return bounds
}

func minAvailableReplicas(cfg *grovecorev1alpha1.PodCliqueScalingGroupConfig) int32 {
	if cfg.MinAvailable == nil {
		return 0
	}
	return *cfg.MinAvailable
}

func maxPCSGConfiguredReplicas(cfg *grovecorev1alpha1.PodCliqueScalingGroupConfig) int32 {
	replicas := int32(1)
	if cfg.Replicas != nil {
		replicas = *cfg.Replicas
	}
	return maxConfiguredReplicas(&replicas, cfg.ScaleConfig)
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
