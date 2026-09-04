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

package kube

import (
	"context"
	"crypto/sha256"
	"fmt"
	"maps"

	apicommon "github.com/ai-dynamo/grove/operator/api/common"

	groveschedulerv1alpha1 "github.com/ai-dynamo/grove/scheduler/api/core/v1alpha1"
	schedulingv1alpha3 "k8s.io/api/scheduling/v1alpha3"
	schedulingv1beta1 "k8s.io/api/scheduling/v1beta1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/sets"
	workloadbuilder "k8s.io/component-helpers/scheduling/schedulingv1/workloadbuilder"
	"k8s.io/utils/ptr"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

// Template and runtime object naming for the generated hierarchy:
//
//	Workload (name: <podgang>)
//	└─ CompositePodGroupTemplate "root"          -> CompositePodGroup <podgang>
//	   ├─ CompositePodGroupTemplate group-<hash> -> CompositePodGroup <podgang>-<tcg-name>
//	   │  └─ PodGroupTemplate leaf-<hash>        -> PodGroup <grove-podgroup>
//	   └─ PodGroupTemplate leaf-<hash>           -> PodGroup <grove-podgroup>
//
// Runtime leaf PodGroups keep the Grove PodGroup name (the PodClique FQN),
// which is also the value of the pod label grove.io/podclique. PreparePod uses
// that label to point pod.spec.schedulingGroup.podGroupName at the leaf.
const (
	rootTemplateName      = "root"
	templateNameHashBytes = 10
)

// workloadTemplateName returns a stable DNS-label-safe identifier. Workload
// template names are DNS labels, while Grove runtime names are DNS subdomains
// and can contain dots or exceed 63 characters.
func workloadTemplateName(prefix, groveName string) string {
	sum := sha256.Sum256([]byte(groveName))
	return fmt.Sprintf("%s-%x", prefix, sum[:templateNameHashBytes])
}

func leafTemplateName(podGroupName string) string {
	return workloadTemplateName("leaf", podGroupName)
}

func topologyGroupTemplateName(groupName string) string {
	return workloadTemplateName("group", groupName)
}

// buildWorkloadForPodGang compiles a Grove PodGang into an upstream Workload
// using the workloadbuilder helper. Unsupported mappings fail closed.
// fallbackMinCounts carries the last persisted positive gang minCount per leaf
// template, used when Grove releases MinReplicas to zero after initial
// placement (the upstream gang policy requires minCount >= 1).
func buildWorkloadForPodGang(podGang *groveschedulerv1alpha1.PodGang, fallbackMinCounts map[string]int32) (*schedulingv1beta1.Workload, error) {
	root, err := workloadItemTreeForPodGang(podGang, fallbackMinCounts)
	if err != nil {
		return nil, err
	}

	builder := workloadbuilder.NewBuilder(root, workloadbuilder.BuildOptions{
		Name:      podGang.Name,
		Namespace: podGang.Namespace,
		Owner:     podGangOwnerReference(podGang),
		AllowedPolicies: []workloadbuilder.SchedulingPolicyOption{
			workloadbuilder.GangPolicy,
		},
		AllowedDisruptionModes: []workloadbuilder.DisruptionModeOption{
			workloadbuilder.SingleMode, workloadbuilder.AllMode,
		},
	})
	workload, err := builder.BuildWorkload()
	if err != nil {
		return nil, fmt.Errorf("failed to compile Workload for PodGang %s/%s: %w", podGang.Namespace, podGang.Name, err)
	}
	workload.Labels = workloadHierarchyLabels(podGang, workload.Labels)
	return workload, nil
}

// workloadItemTreeForPodGang translates the PodGang layout into a
// workloadbuilder item tree:
//   - the PodGang becomes the root composite node (gang over all direct children),
//   - each TopologyConstraintGroupConfig becomes a child composite node,
//   - each Grove PodGroup becomes a leaf node.
func workloadItemTreeForPodGang(podGang *groveschedulerv1alpha1.PodGang, fallbackMinCounts map[string]int32) (*workloadbuilder.WorkloadItem, error) {
	if len(podGang.Spec.PodGroups) == 0 {
		return nil, fmt.Errorf("PodGang %s/%s has no PodGroups", podGang.Namespace, podGang.Name)
	}

	rootConstraints, err := toUpstreamTopologyConstraints(podGang.Spec.TopologyConstraint)
	if err != nil {
		return nil, fmt.Errorf("PodGang %s/%s topology constraint: %w", podGang.Namespace, podGang.Name, err)
	}

	leafItems := make(map[string]*workloadbuilder.WorkloadItem, len(podGang.Spec.PodGroups))
	for _, podGroup := range podGang.Spec.PodGroups {
		leafConstraints, leafErr := toUpstreamTopologyConstraints(podGroup.TopologyConstraint)
		if leafErr != nil {
			return nil, fmt.Errorf("PodGang %s/%s PodGroup %q topology constraint: %w", podGang.Namespace, podGang.Name, podGroup.Name, leafErr)
		}
		minCount := podGroup.MinReplicas
		if minCount < 1 {
			// The upstream gang policy requires minCount >= 1. When Grove releases
			// MinReplicas to zero after initial placement, retain the last positive
			// persisted minCount; fail closed when there is none.
			fallback, found := fallbackMinCounts[podGroup.Name]
			if !found || fallback < 1 {
				return nil, fmt.Errorf("PodGang %s/%s PodGroup %q has minReplicas %d; the %s API requires a gang minCount >= 1", podGang.Namespace, podGang.Name, podGroup.Name, podGroup.MinReplicas, schedulingv1beta1.SchemeGroupVersion.String())
			}
			minCount = fallback
		}
		leafItems[podGroup.Name] = &workloadbuilder.WorkloadItem{
			Name: leafTemplateName(podGroup.Name),
			DefaultConfig: &workloadbuilder.SchedulingConfig{
				Policy: &workloadbuilder.SchedulingPolicy{
					Gang: &workloadbuilder.GangSchedulingPolicy{MinCount: ptr.To(minCount)},
				},
				Constraints:       leafConstraints,
				PriorityClassName: podGang.Spec.PriorityClassName,
			},
		}
	}

	rootChildren := make([]*workloadbuilder.WorkloadItem, 0, len(podGang.Spec.TopologyConstraintGroupConfigs)+len(podGang.Spec.PodGroups))
	grouped := sets.New[string]()
	for _, groupConfig := range podGang.Spec.TopologyConstraintGroupConfigs {
		if len(groupConfig.PodGroupNames) == 0 {
			continue
		}
		groupConstraints, groupErr := toUpstreamTopologyConstraints(groupConfig.TopologyConstraint)
		if groupErr != nil {
			return nil, fmt.Errorf("PodGang %s/%s topology constraint group %q: %w", podGang.Namespace, podGang.Name, groupConfig.Name, groupErr)
		}
		children := make([]*workloadbuilder.WorkloadItem, 0, len(groupConfig.PodGroupNames))
		for _, podGroupName := range groupConfig.PodGroupNames {
			leaf, found := leafItems[podGroupName]
			if !found {
				return nil, fmt.Errorf("PodGang %s/%s topology constraint group %q references unknown PodGroup %q", podGang.Namespace, podGang.Name, groupConfig.Name, podGroupName)
			}
			if grouped.Has(podGroupName) {
				return nil, fmt.Errorf("PodGang %s/%s PodGroup %q is referenced by multiple topology constraint groups", podGang.Namespace, podGang.Name, podGroupName)
			}
			grouped.Insert(podGroupName)
			children = append(children, leaf)
		}
		rootChildren = append(rootChildren, &workloadbuilder.WorkloadItem{
			Name: topologyGroupTemplateName(groupConfig.Name),
			DefaultConfig: &workloadbuilder.SchedulingConfig{
				Policy: &workloadbuilder.SchedulingPolicy{
					Gang: &workloadbuilder.GangSchedulingPolicy{MinCount: ptr.To(int32(len(children)))},
				},
				Constraints: groupConstraints,
			},
			Children: children,
		})
	}
	// Group configs cover a strict subset of PodGroups. Unmatched PodGroups
	// intentionally remain direct children of the root composite node.
	for _, podGroup := range podGang.Spec.PodGroups {
		if !grouped.Has(podGroup.Name) {
			rootChildren = append(rootChildren, leafItems[podGroup.Name])
		}
	}

	root := &workloadbuilder.WorkloadItem{
		Name: rootTemplateName,
		DefaultConfig: &workloadbuilder.SchedulingConfig{
			Policy: &workloadbuilder.SchedulingPolicy{
				Gang: &workloadbuilder.GangSchedulingPolicy{MinCount: ptr.To(int32(len(rootChildren)))},
			},
			Constraints:       rootConstraints,
			PriorityClassName: podGang.Spec.PriorityClassName,
		},
		Children: rootChildren,
	}
	if err := validateWorkloadLimits(podGang, root, 1); err != nil {
		return nil, err
	}
	return root, nil
}

// validateWorkloadLimits fails closed when the generated tree exceeds the
// upstream Workload API limits: at most WorkloadMaxPodGroupTemplates entries
// per template list, and at most WorkloadMaxTreeDepth levels deep. Grove's own
// translation keeps the tree at most 3 levels (root, topology group, leaf), but
// the depth check is enforced explicitly so future mappings still fail closed
// rather than producing a Workload the API server rejects. depth is the 1-based
// level of item within the tree (the root is at depth 1).
func validateWorkloadLimits(podGang *groveschedulerv1alpha1.PodGang, item *workloadbuilder.WorkloadItem, depth int) error {
	if depth > schedulingv1beta1.WorkloadMaxTreeDepth {
		return fmt.Errorf("PodGang %s/%s maps to a template tree deeper than %d levels at group %q; the %s Workload API supports at most %d levels", podGang.Namespace, podGang.Name, schedulingv1beta1.WorkloadMaxTreeDepth, item.Name, schedulingv1beta1.SchemeGroupVersion.String(), schedulingv1beta1.WorkloadMaxTreeDepth)
	}
	if len(item.Children) > schedulingv1beta1.WorkloadMaxPodGroupTemplates {
		return fmt.Errorf("PodGang %s/%s group %q maps to %d child templates; the %s Workload API supports at most %d templates per group", podGang.Namespace, podGang.Name, item.Name, len(item.Children), schedulingv1beta1.SchemeGroupVersion.String(), schedulingv1beta1.WorkloadMaxPodGroupTemplates)
	}
	for _, child := range item.Children {
		if err := validateWorkloadLimits(podGang, child, depth+1); err != nil {
			return err
		}
	}
	return nil
}

// toUpstreamTopologyConstraints converts a Grove topology constraint into
// upstream scheduling constraints. Preferred constraints are not supported by
// Workload-Aware Scheduling and fail closed.
func toUpstreamTopologyConstraints(topologyConstraint *groveschedulerv1alpha1.TopologyConstraint) (*workloadbuilder.SchedulingConstraints, error) {
	if topologyConstraint == nil || topologyConstraint.PackConstraint == nil {
		return nil, nil
	}
	packConstraint := topologyConstraint.PackConstraint
	if packConstraint.Preferred != nil {
		return nil, fmt.Errorf("preferred topology constraint %q is not supported by the %s API; only required constraints are supported", *packConstraint.Preferred, schedulingv1beta1.SchemeGroupVersion.String())
	}
	if packConstraint.Required == nil {
		return nil, nil
	}
	return &workloadbuilder.SchedulingConstraints{
		Topology: []schedulingv1beta1.TopologyConstraint{{Key: *packConstraint.Required}},
	}, nil
}

// podGangOwnerReference returns the controller owner reference pointing at the
// PodGang for all generated scheduling objects, so that deleting the PodGang
// garbage-collects the entire generated hierarchy.
func podGangOwnerReference(podGang *groveschedulerv1alpha1.PodGang) *metav1.OwnerReference {
	return &metav1.OwnerReference{
		APIVersion:         groveschedulerv1alpha1.SchemeGroupVersion.String(),
		Kind:               "PodGang",
		Name:               podGang.Name,
		UID:                podGang.UID,
		Controller:         ptr.To(true),
		BlockOwnerDeletion: ptr.To(true),
	}
}

// workloadHierarchyLabels returns the labels stamped on every generated
// scheduling object, merging the given base labels.
func workloadHierarchyLabels(podGang *groveschedulerv1alpha1.PodGang, base map[string]string) map[string]string {
	result := maps.Clone(podGang.Labels)
	if result == nil {
		result = map[string]string{}
	}
	maps.Copy(result, base)
	result[apicommon.LabelPodGang] = podGang.Name
	return result
}

// syncWorkloadHierarchy creates or updates the Workload and the runtime
// CompositePodGroup / PodGroup objects for the PodGang.
//
// Workload templates cannot be added or removed after creation. When the
// desired structure differs from the persisted one, the whole generated
// hierarchy is deleted and recreated; pods stay gated until the hierarchy for
// the current PodGang generation is ready.
func (b *schedulerBackend) syncWorkloadHierarchy(ctx context.Context, podGang *groveschedulerv1alpha1.PodGang) error {
	fallbackMinCounts, err := b.persistedLeafMinCounts(ctx, podGang)
	if err != nil {
		return err
	}
	desired, err := buildWorkloadForPodGang(podGang, fallbackMinCounts)
	if err != nil {
		return err
	}

	persisted, err := b.syncWorkload(ctx, podGang, desired)
	if err != nil {
		return err
	}

	materializer := workloadbuilder.NewBuilderFromExistingWorkload(persisted, workloadbuilder.BuildOptions{
		Owner: podGangOwnerReference(podGang),
	})
	if err := b.syncCompositePodGroups(ctx, podGang, materializer); err != nil {
		return err
	}
	return b.syncLeafPodGroups(ctx, podGang, persisted)
}

// persistedLeafMinCounts returns the last positive gang minCount for every
// Grove PodGroup. It reads Workload templates first and supplements them from
// surviving runtime PodGroups so reconciliation can recover if the Workload
// was deleted after Grove released MinReplicas to zero.
func (b *schedulerBackend) persistedLeafMinCounts(ctx context.Context, podGang *groveschedulerv1alpha1.PodGang) (map[string]int32, error) {
	templateMinCounts := map[string]int32{}
	existing := &schedulingv1beta1.Workload{}
	err := b.client.Get(ctx, client.ObjectKey{Namespace: podGang.Namespace, Name: podGang.Name}, existing)
	if err != nil {
		if !apierrors.IsNotFound(err) {
			return nil, fmt.Errorf("failed to get Workload %s/%s: %w", podGang.Namespace, podGang.Name, err)
		}
	} else {
		if err := requirePodGangOwnership(existing, podGang, "Workload"); err != nil {
			return nil, err
		}
		collectLeafMinCounts(existing.Spec.CompositePodGroupTemplates, existing.Spec.PodGroupTemplates, templateMinCounts)
	}

	minCounts := make(map[string]int32, len(podGang.Spec.PodGroups))
	for _, podGroup := range podGang.Spec.PodGroups {
		if minCount, found := templateMinCounts[leafTemplateName(podGroup.Name)]; found && minCount > 0 {
			minCounts[podGroup.Name] = minCount
		}
	}

	runtimeGroups := &schedulingv1beta1.PodGroupList{}
	if err := b.client.List(ctx, runtimeGroups,
		client.InNamespace(podGang.Namespace),
		client.MatchingLabels{apicommon.LabelPodGang: podGang.Name},
	); err != nil {
		return nil, fmt.Errorf("failed to list PodGroups for PodGang %s/%s: %w", podGang.Namespace, podGang.Name, err)
	}
	for i := range runtimeGroups.Items {
		runtimeGroup := &runtimeGroups.Items[i]
		if !metav1.IsControlledBy(runtimeGroup, podGang) {
			continue
		}
		if _, found := minCounts[runtimeGroup.Name]; found {
			continue
		}
		if gang := runtimeGroup.Spec.SchedulingPolicy.Gang; gang != nil && gang.MinCount > 0 {
			minCounts[runtimeGroup.Name] = gang.MinCount
		}
	}
	return minCounts, nil
}

// materializeLeafPodGroup builds a runtime PodGroup from the named leaf
// template anywhere in the persisted Workload's composite template tree.
// Builder.NewPodGroup only indexes top-level PodGroupTemplates, but Grove's
// leaf templates always live under the root CompositePodGroupTemplate.
func materializeLeafPodGroup(podGang *groveschedulerv1alpha1.PodGang, workload *schedulingv1beta1.Workload, name, templateName string) (*schedulingv1beta1.PodGroup, error) {
	tmpl := findLeafTemplate(workload.Spec.CompositePodGroupTemplates, workload.Spec.PodGroupTemplates, templateName)
	if tmpl == nil {
		return nil, fmt.Errorf("podGroupTemplate %q not found in Workload %q", templateName, workload.Name)
	}
	spec := schedulingv1beta1.PodGroupSpec{
		WorkloadRef: &schedulingv1beta1.WorkloadReference{
			WorkloadName: workload.Name,
			TemplateName: tmpl.Name,
		},
		SchedulingPolicy:      *tmpl.SchedulingPolicy.DeepCopy(),
		SchedulingConstraints: tmpl.SchedulingConstraints.DeepCopy(),
		DisruptionMode:        tmpl.DisruptionMode.DeepCopy(),
		PriorityClassName:     tmpl.PriorityClassName,
	}
	return &schedulingv1beta1.PodGroup{
		ObjectMeta: metav1.ObjectMeta{
			Name:            name,
			Namespace:       workload.Namespace,
			OwnerReferences: []metav1.OwnerReference{*podGangOwnerReference(podGang)},
		},
		Spec: spec,
	}, nil
}

// findLeafTemplate returns the named leaf PodGroupTemplate from the template
// tree, or nil when absent.
func findLeafTemplate(composites []schedulingv1beta1.CompositePodGroupTemplate, leaves []schedulingv1beta1.PodGroupTemplate, name string) *schedulingv1beta1.PodGroupTemplate {
	for i := range leaves {
		if leaves[i].Name == name {
			return &leaves[i]
		}
	}
	for i := range composites {
		if tmpl := findLeafTemplate(composites[i].CompositePodGroupTemplates, composites[i].PodGroupTemplates, name); tmpl != nil {
			return tmpl
		}
	}
	return nil
}

// syncWorkload creates the Workload, or replaces it when its immutable
// template structure no longer matches the desired one. It returns the
// persisted Workload runtime objects must be materialized from.
func (b *schedulerBackend) syncWorkload(ctx context.Context, podGang *groveschedulerv1alpha1.PodGang, desired *schedulingv1beta1.Workload) (*schedulingv1beta1.Workload, error) {
	existing := &schedulingv1beta1.Workload{}
	err := b.client.Get(ctx, client.ObjectKeyFromObject(desired), existing)
	if err != nil {
		if !apierrors.IsNotFound(err) {
			return nil, fmt.Errorf("failed to get Workload %s/%s: %w", desired.Namespace, desired.Name, err)
		}
		if createErr := b.client.Create(ctx, desired); createErr != nil {
			return nil, fmt.Errorf("failed to create Workload %s/%s: %w", desired.Namespace, desired.Name, createErr)
		}
		return desired, nil
	}

	if err := requirePodGangOwnership(existing, podGang, "Workload"); err != nil {
		return nil, err
	}

	if workloadStructureEqual(existing, desired) {
		if err := b.updateMutableWorkloadFields(ctx, existing, desired); err != nil {
			return nil, err
		}
		return existing, nil
	}

	// Structure changed: templates cannot be added or removed in place, so
	// rebuild the hierarchy before pods of the new generation are ungated.
	if err := b.client.Delete(ctx, existing); err != nil && !apierrors.IsNotFound(err) {
		return nil, fmt.Errorf("failed to delete outdated Workload %s/%s: %w", existing.Namespace, existing.Name, err)
	}
	if err := b.deleteGeneratedGroups(ctx, podGang); err != nil {
		return nil, err
	}
	if err := b.client.Create(ctx, desired); err != nil {
		return nil, fmt.Errorf("failed to recreate Workload %s/%s: %w", desired.Namespace, desired.Name, err)
	}
	return desired, nil
}

// updateMutableWorkloadFields patches the only mutable scheduling field of a
// persisted Workload: the gang minCount of each leaf PodGroupTemplate.
func (b *schedulerBackend) updateMutableWorkloadFields(ctx context.Context, existing, desired *schedulingv1beta1.Workload) error {
	before := existing.DeepCopy()
	changed := false
	desiredMinCounts := map[string]int32{}
	collectLeafMinCounts(desired.Spec.CompositePodGroupTemplates, desired.Spec.PodGroupTemplates, desiredMinCounts)
	changed = applyLeafMinCounts(existing.Spec.CompositePodGroupTemplates, existing.Spec.PodGroupTemplates, desiredMinCounts)
	if !changed {
		return nil
	}
	if err := b.client.Patch(ctx, existing, client.MergeFrom(before)); err != nil {
		return fmt.Errorf("failed to patch Workload %s/%s gang minCount: %w", existing.Namespace, existing.Name, err)
	}
	return nil
}

// collectLeafMinCounts records the gang minCount of every leaf template by name.
func collectLeafMinCounts(composites []schedulingv1beta1.CompositePodGroupTemplate, leaves []schedulingv1beta1.PodGroupTemplate, out map[string]int32) {
	for i := range leaves {
		if gang := leaves[i].SchedulingPolicy.Gang; gang != nil {
			out[leaves[i].Name] = gang.MinCount
		}
	}
	for i := range composites {
		collectLeafMinCounts(composites[i].CompositePodGroupTemplates, composites[i].PodGroupTemplates, out)
	}
}

// applyLeafMinCounts writes desired gang minCounts into matching leaf
// templates and reports whether anything changed.
func applyLeafMinCounts(composites []schedulingv1beta1.CompositePodGroupTemplate, leaves []schedulingv1beta1.PodGroupTemplate, desired map[string]int32) bool {
	changed := false
	for i := range leaves {
		gang := leaves[i].SchedulingPolicy.Gang
		if gang == nil {
			continue
		}
		if desiredMinCount, found := desired[leaves[i].Name]; found && gang.MinCount != desiredMinCount {
			gang.MinCount = desiredMinCount
			changed = true
		}
	}
	for i := range composites {
		if applyLeafMinCounts(composites[i].CompositePodGroupTemplates, composites[i].PodGroupTemplates, desired) {
			changed = true
		}
	}
	return changed
}

// workloadStructureEqual reports whether the persisted Workload has the same
// immutable template structure (tree shape, names, constraints, policies
// modulo the mutable leaf gang minCount) as the desired one.
func workloadStructureEqual(existing, desired *schedulingv1beta1.Workload) bool {
	return compositeTemplatesStructureEqual(existing.Spec.CompositePodGroupTemplates, desired.Spec.CompositePodGroupTemplates) &&
		leafTemplatesStructureEqual(existing.Spec.PodGroupTemplates, desired.Spec.PodGroupTemplates)
}

func compositeTemplatesStructureEqual(existing, desired []schedulingv1beta1.CompositePodGroupTemplate) bool {
	if len(existing) != len(desired) {
		return false
	}
	desiredByName := make(map[string]*schedulingv1beta1.CompositePodGroupTemplate, len(desired))
	for i := range desired {
		desiredByName[desired[i].Name] = &desired[i]
	}
	for i := range existing {
		desiredTemplate, found := desiredByName[existing[i].Name]
		if !found {
			return false
		}
		if !compositePoliciesEqual(&existing[i], desiredTemplate) {
			return false
		}
		if !compositeTemplatesStructureEqual(existing[i].CompositePodGroupTemplates, desiredTemplate.CompositePodGroupTemplates) {
			return false
		}
		if !leafTemplatesStructureEqual(existing[i].PodGroupTemplates, desiredTemplate.PodGroupTemplates) {
			return false
		}
	}
	return true
}

func compositePoliciesEqual(existing, desired *schedulingv1beta1.CompositePodGroupTemplate) bool {
	if existing.PriorityClassName != desired.PriorityClassName {
		return false
	}
	existingGang, desiredGang := existing.SchedulingPolicy.Gang, desired.SchedulingPolicy.Gang
	if (existingGang == nil) != (desiredGang == nil) {
		return false
	}
	if existingGang != nil && existingGang.MinGroupCount != desiredGang.MinGroupCount {
		return false
	}
	return topologyConstraintsEqual(compositeTopology(existing.SchedulingConstraints), compositeTopology(desired.SchedulingConstraints))
}

func leafTemplatesStructureEqual(existing, desired []schedulingv1beta1.PodGroupTemplate) bool {
	if len(existing) != len(desired) {
		return false
	}
	desiredByName := make(map[string]*schedulingv1beta1.PodGroupTemplate, len(desired))
	for i := range desired {
		desiredByName[desired[i].Name] = &desired[i]
	}
	for i := range existing {
		desiredTemplate, found := desiredByName[existing[i].Name]
		if !found {
			return false
		}
		if (existing[i].SchedulingPolicy.Gang == nil) != (desiredTemplate.SchedulingPolicy.Gang == nil) {
			return false
		}
		if existing[i].PriorityClassName != desiredTemplate.PriorityClassName {
			return false
		}
		if !topologyConstraintsEqual(leafTopology(existing[i].SchedulingConstraints), leafTopology(desiredTemplate.SchedulingConstraints)) {
			return false
		}
	}
	return true
}

func compositeTopology(constraints *schedulingv1beta1.CompositePodGroupSchedulingConstraints) []schedulingv1beta1.TopologyConstraint {
	if constraints == nil {
		return nil
	}
	return constraints.Topology
}

func leafTopology(constraints *schedulingv1beta1.PodGroupSchedulingConstraints) []schedulingv1beta1.TopologyConstraint {
	if constraints == nil {
		return nil
	}
	return constraints.Topology
}

func topologyConstraintsEqual(existing, desired []schedulingv1beta1.TopologyConstraint) bool {
	if len(existing) != len(desired) {
		return false
	}
	for i := range existing {
		if existing[i].Key != desired[i].Key {
			return false
		}
	}
	return true
}

// syncCompositePodGroups materializes the runtime CompositePodGroup objects:
// one root for the PodGang plus one per topology constraint group, with
// parent linkage. CompositePodGroup specs are immutable, so existing objects
// are left untouched.
func (b *schedulerBackend) syncCompositePodGroups(ctx context.Context, podGang *groveschedulerv1alpha1.PodGang, materializer *workloadbuilder.Builder) error {
	rootName := runtimeRootCompositePodGroupName(podGang)
	if err := b.ensureCompositePodGroup(ctx, podGang, materializer, rootName, rootTemplateName, nil); err != nil {
		return err
	}
	for _, groupConfig := range podGang.Spec.TopologyConstraintGroupConfigs {
		if len(groupConfig.PodGroupNames) == 0 {
			continue
		}
		childName := runtimeChildCompositePodGroupName(podGang, groupConfig.Name)
		if err := b.ensureCompositePodGroup(ctx, podGang, materializer, childName, topologyGroupTemplateName(groupConfig.Name), ptr.To(rootName)); err != nil {
			return err
		}
	}
	return nil
}

// ensureCompositePodGroup creates the named CompositePodGroup from its
// template if it does not exist yet.
func (b *schedulerBackend) ensureCompositePodGroup(ctx context.Context, podGang *groveschedulerv1alpha1.PodGang, materializer *workloadbuilder.Builder, name, templateName string, parentName *string) error {
	existing := &schedulingv1alpha3.CompositePodGroup{}
	err := b.client.Get(ctx, client.ObjectKey{Namespace: podGang.Namespace, Name: name}, existing)
	if err == nil {
		if err := requirePodGangOwnership(existing, podGang, "CompositePodGroup"); err != nil {
			return err
		}
		return nil
	}
	if !apierrors.IsNotFound(err) {
		return fmt.Errorf("failed to get CompositePodGroup %s/%s: %w", podGang.Namespace, name, err)
	}
	compositePodGroup, err := materializer.NewCompositePodGroup(name, templateName)
	if err != nil {
		return fmt.Errorf("failed to materialize CompositePodGroup %s/%s: %w", podGang.Namespace, name, err)
	}
	compositePodGroup.Spec.ParentCompositePodGroupName = parentName
	compositePodGroup.Labels = workloadHierarchyLabels(podGang, compositePodGroup.Labels)
	if err := b.client.Create(ctx, compositePodGroup); err != nil && !apierrors.IsAlreadyExists(err) {
		return fmt.Errorf("failed to create CompositePodGroup %s/%s: %w", podGang.Namespace, name, err)
	}
	return nil
}

// syncLeafPodGroups materializes one runtime PodGroup per Grove PodGroup and
// keeps the mutable gang minCount in sync. When Grove releases MinReplicas to
// zero after initial placement, the last positive persisted minCount is
// retained because the upstream API requires minCount >= 1.
func (b *schedulerBackend) syncLeafPodGroups(ctx context.Context, podGang *groveschedulerv1alpha1.PodGang, persistedWorkload *schedulingv1beta1.Workload) error {
	for _, groveGroup := range podGang.Spec.PodGroups {
		parentName := runtimeParentCompositePodGroupNameFor(podGang, groveGroup.Name)
		existing := &schedulingv1beta1.PodGroup{}
		err := b.client.Get(ctx, client.ObjectKey{Namespace: podGang.Namespace, Name: groveGroup.Name}, existing)
		if err != nil {
			if !apierrors.IsNotFound(err) {
				return fmt.Errorf("failed to get PodGroup %s/%s: %w", podGang.Namespace, groveGroup.Name, err)
			}
			podGroup, materializeErr := materializeLeafPodGroup(podGang, persistedWorkload, groveGroup.Name, leafTemplateName(groveGroup.Name))
			if materializeErr != nil {
				return fmt.Errorf("failed to materialize PodGroup %s/%s: %w", podGang.Namespace, groveGroup.Name, materializeErr)
			}
			podGroup.Spec.ParentCompositePodGroupName = ptr.To(parentName)
			podGroup.Labels = workloadHierarchyLabels(podGang, podGroup.Labels)
			if createErr := b.client.Create(ctx, podGroup); createErr != nil && !apierrors.IsAlreadyExists(createErr) {
				return fmt.Errorf("failed to create PodGroup %s/%s: %w", podGang.Namespace, groveGroup.Name, createErr)
			}
			continue
		}

		if err := requirePodGangOwnership(existing, podGang, "PodGroup"); err != nil {
			return err
		}

		gang := existing.Spec.SchedulingPolicy.Gang
		if gang == nil || groveGroup.MinReplicas < 1 || gang.MinCount == groveGroup.MinReplicas {
			continue
		}
		before := existing.DeepCopy()
		gang.MinCount = groveGroup.MinReplicas
		if err := b.client.Patch(ctx, existing, client.MergeFrom(before)); err != nil {
			return fmt.Errorf("failed to patch PodGroup %s/%s gang minCount: %w", podGang.Namespace, groveGroup.Name, err)
		}
	}
	return nil
}

// deleteGeneratedGroups deletes runtime groups generated for the PodGang.
// Labels select candidates, and controller ownership prevents deleting foreign
// objects that happen to carry the same label.
func (b *schedulerBackend) deleteGeneratedGroups(ctx context.Context, podGang *groveschedulerv1alpha1.PodGang) error {
	selector := client.MatchingLabels{apicommon.LabelPodGang: podGang.Name}
	podGroups := &schedulingv1beta1.PodGroupList{}
	if err := b.client.List(ctx, podGroups, client.InNamespace(podGang.Namespace), selector); err != nil {
		return fmt.Errorf("failed to list generated PodGroups for PodGang %s/%s: %w", podGang.Namespace, podGang.Name, err)
	}
	for i := range podGroups.Items {
		if !metav1.IsControlledBy(&podGroups.Items[i], podGang) {
			continue
		}
		if err := b.client.Delete(ctx, &podGroups.Items[i]); err != nil && !apierrors.IsNotFound(err) {
			return fmt.Errorf("failed to delete generated PodGroup %s/%s: %w", podGroups.Items[i].Namespace, podGroups.Items[i].Name, err)
		}
	}

	compositePodGroups := &schedulingv1alpha3.CompositePodGroupList{}
	if err := b.client.List(ctx, compositePodGroups, client.InNamespace(podGang.Namespace), selector); err != nil {
		return fmt.Errorf("failed to list generated CompositePodGroups for PodGang %s/%s: %w", podGang.Namespace, podGang.Name, err)
	}
	for i := range compositePodGroups.Items {
		if !metav1.IsControlledBy(&compositePodGroups.Items[i], podGang) {
			continue
		}
		if err := b.client.Delete(ctx, &compositePodGroups.Items[i]); err != nil && !apierrors.IsNotFound(err) {
			return fmt.Errorf("failed to delete generated CompositePodGroup %s/%s: %w", compositePodGroups.Items[i].Namespace, compositePodGroups.Items[i].Name, err)
		}
	}
	return nil
}

func requirePodGangOwnership(obj metav1.Object, podGang *groveschedulerv1alpha1.PodGang, kind string) error {
	if metav1.IsControlledBy(obj, podGang) {
		return nil
	}
	return fmt.Errorf("%s %s/%s is not controlled by PodGang %s/%s", kind, obj.GetNamespace(), obj.GetName(), podGang.Namespace, podGang.Name)
}

// runtimeRootCompositePodGroupName returns the name of the root runtime
// CompositePodGroup for the PodGang.
func runtimeRootCompositePodGroupName(podGang *groveschedulerv1alpha1.PodGang) string {
	return podGang.Name
}

// runtimeChildCompositePodGroupName returns the name of the runtime
// CompositePodGroup generated for a topology constraint group.
func runtimeChildCompositePodGroupName(podGang *groveschedulerv1alpha1.PodGang, groupName string) string {
	return fmt.Sprintf("%s-%s", podGang.Name, groupName)
}

// runtimeParentCompositePodGroupNameFor returns the runtime parent
// CompositePodGroup name for the given Grove PodGroup: the topology constraint
// group's composite when the PodGroup belongs to one, the root otherwise.
func runtimeParentCompositePodGroupNameFor(podGang *groveschedulerv1alpha1.PodGang, podGroupName string) string {
	for _, groupConfig := range podGang.Spec.TopologyConstraintGroupConfigs {
		for _, name := range groupConfig.PodGroupNames {
			if name == podGroupName {
				return runtimeChildCompositePodGroupName(podGang, groupConfig.Name)
			}
		}
	}
	return runtimeRootCompositePodGroupName(podGang)
}
