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
	grovecorev1alpha1 "github.com/ai-dynamo/grove/operator/api/core/v1alpha1"
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

func (v *pcsValidator) validateGeneratedResourceClaimNames() field.ErrorList {
	checks := buildGeneratedResourceClaimNameChecks(v.pcs)
	return generatedResourceClaimNameErrors(checks, generatedNameReplicaBounds(v.pcs))
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
		if _, exists := oldCollisions[collision.key]; exists &&
			!replicaBoundsIncreasedForKeys(oldBounds, newBounds, collision.replicaBoundKeys) {
			continue
		}
		allErrs = append(allErrs, generatedResourceClaimNameCollisionError(collision))
	}
	return allErrs
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
			"generated ResourceClaim name collides between %s and %s; generated names must be unique within the namespace and pod.spec.resourceClaims",
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
) map[string]generatedResourceClaimNameCollision {
	result := make(map[string]generatedResourceClaimNameCollision, len(collisions))
	for _, collision := range collisions {
		result[collision.key] = collision
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
	}
	return bounds
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
