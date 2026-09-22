//go:build e2e

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

package tests

import (
	"context"
	"fmt"
	"os"
	"reflect"
	"slices"
	"strconv"
	"testing"
	"time"

	apicommon "github.com/ai-dynamo/grove/operator/api/common"
	apiconstants "github.com/ai-dynamo/grove/operator/api/common/constants"
	grovecorev1alpha1 "github.com/ai-dynamo/grove/operator/api/core/v1alpha1"
	"github.com/ai-dynamo/grove/operator/e2e/grove/podgang"
	"github.com/ai-dynamo/grove/operator/e2e/grove/podgangmap"
	"github.com/ai-dynamo/grove/operator/e2e/grove/podgroup"
	"github.com/ai-dynamo/grove/operator/e2e/grove/workload"
	"github.com/ai-dynamo/grove/operator/e2e/k8s/pods"
	"github.com/ai-dynamo/grove/operator/e2e/setup"
	"github.com/ai-dynamo/grove/operator/e2e/testctx"
	internalconstants "github.com/ai-dynamo/grove/operator/internal/constants"
	componentutils "github.com/ai-dynamo/grove/operator/internal/utils/component"
	k8sutils "github.com/ai-dynamo/grove/operator/internal/utils/kubernetes"

	groveschedulerv1alpha1 "github.com/ai-dynamo/grove/scheduler/api/core/v1alpha1"
	autoscalingv1 "k8s.io/api/autoscaling/v1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/sets"
	"k8s.io/apimachinery/pkg/util/wait"
	"k8s.io/client-go/util/retry"
	"k8s.io/utils/ptr"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/yaml"
)

const (
	zeroReplicaObservationWindow     = 5 * time.Second
	zeroReplicaWakeObservationWindow = 15 * time.Second
	zeroReplicaGangGate              = "grove.io/podgang-pending-creation"
)

func Test_ZR1_AllIdleBootstrapAndStaleBreach(t *testing.T) {
	const pcsName = "zero-bootstrap"
	ctx := context.Background()
	tc, cleanup := prepareIdleWorkload(t, ctx, 1, pcsName, 0, nil)
	defer cleanup()

	workerName := pcsName + "-0-worker"
	pcsgName := pcsName + "-0-workers"
	worker := waitForPCLQ(t, ctx, tc, workerName)
	pcsg, err := workload.NewWorkloadManager(tc.Client, Logger).WaitForPCSG(ctx, tc.Namespace, pcsgName, tc.Timeout, tc.Interval)
	if err != nil {
		t.Fatalf("Failed to wait for idle PodCliqueScalingGroup: %v", err)
	}
	waitForAllIdleBootstrap(t, ctx, tc, pcsName)

	beforePCLQEvents := countEvents(t, ctx, tc, worker.UID, internalconstants.ReasonAllScheduledReplicasLost)
	beforePCSGEvents := countEvents(t, ctx, tc, pcsg.UID, internalconstants.ReasonAllScheduledReplicasLost)
	setStaleBreachConditions(t, ctx, tc, workerName, pcsgName)
	restartOperator(t, ctx, tc)

	waitForPCLQConditionReason(t, ctx, tc, workerName, apiconstants.ConditionReasonIdle)
	waitForPCSGConditionReason(t, ctx, tc, pcsgName, apiconstants.ConditionReasonIdle)
	assertStableFor(t, ctx, zeroReplicaObservationWindow, func(ctx context.Context) error {
		currentWorker := &grovecorev1alpha1.PodClique{}
		if err := tc.Client.Get(ctx, client.ObjectKey{Namespace: tc.Namespace, Name: workerName}, currentWorker); err != nil {
			return err
		}
		if currentWorker.UID != worker.UID {
			return fmt.Errorf("idle PodClique UID changed: %s -> %s", worker.UID, currentWorker.UID)
		}
		currentPCSG := &grovecorev1alpha1.PodCliqueScalingGroup{}
		if err := tc.Client.Get(ctx, client.ObjectKey{Namespace: tc.Namespace, Name: pcsgName}, currentPCSG); err != nil {
			return err
		}
		if currentPCSG.UID != pcsg.UID {
			return fmt.Errorf("idle PodCliqueScalingGroup UID changed: %s -> %s", pcsg.UID, currentPCSG.UID)
		}
		if got := countEvents(t, ctx, tc, worker.UID, internalconstants.ReasonAllScheduledReplicasLost); got != beforePCLQEvents {
			return fmt.Errorf("PodClique AllScheduledReplicasLost event count changed: %d -> %d", beforePCLQEvents, got)
		}
		if got := countEvents(t, ctx, tc, pcsg.UID, internalconstants.ReasonAllScheduledReplicasLost); got != beforePCSGEvents {
			return fmt.Errorf("PodCliqueScalingGroup AllScheduledReplicasLost event count changed: %d -> %d", beforePCSGEvents, got)
		}
		return nil
	})
}

func Test_ZR2_StandaloneLifecycle(t *testing.T) {
	const pcsName = "zero-standalone"
	ctx := context.Background()
	tc, cleanup := prepareIdleWorkload(t, ctx, 3, pcsName, 3, nil)
	defer cleanup()

	workerName := pcsName + "-0-worker"
	if err := tc.WaitForReadyPods(3); err != nil {
		t.Fatalf("Initial standalone pods did not become ready: %v", err)
	}
	waitForStandaloneMembership(t, ctx, tc, pcsName, "worker", 3)
	initialPods := podsForClique(t, tc, workerName)
	initialLocations := podUIDLocations(initialPods)
	initialPodGangs := podGangNameSet(ctx, t, tc, pcsName)
	initialEpoch := maxPGMEpoch(t, ctx, tc, pcsName)

	updateScale(t, ctx, tc, &grovecorev1alpha1.PodClique{
		ObjectMeta: metav1.ObjectMeta{Name: workerName, Namespace: tc.Namespace},
	}, 1)
	waitForPodCountAndReady(t, tc, 1)
	waitForStandaloneMembership(t, ctx, tc, pcsName, "worker", 1)
	assertRetainedPodLocation(t, podsForClique(t, tc, workerName), initialLocations)

	updateScale(t, ctx, tc, &grovecorev1alpha1.PodClique{
		ObjectMeta: metav1.ObjectMeta{Name: workerName, Namespace: tc.Namespace},
	}, 0)
	waitForAllIdle(t, ctx, tc, pcsName, pcsName+"-0-workers-0-prefill", pcsName+"-0-workers-0-decode")
	waitForAllIdleBootstrap(t, ctx, tc, pcsName)

	updateScale(t, ctx, tc, &grovecorev1alpha1.PodClique{
		ObjectMeta: metav1.ObjectMeta{Name: workerName, Namespace: tc.Namespace},
	}, 1)
	waitForPodCountAndReady(t, tc, 1)
	waitForStandaloneMembership(t, ctx, tc, pcsName, "worker", 1)
	if wakeEpoch := maxPGMEpoch(t, ctx, tc, pcsName); wakeEpoch != initialEpoch {
		t.Fatalf("standalone wake changed anchor epoch: %d -> %d", initialEpoch, wakeEpoch)
	}
	if names := podGangNameSet(ctx, t, tc, pcsName); !initialPodGangs.Equal(names) {
		t.Fatalf("standalone wake changed anchor names: %v -> %v", initialPodGangs, names)
	}
	for _, pod := range podsForClique(t, tc, workerName) {
		if _, existed := initialLocations[pod.UID]; existed {
			t.Fatalf("standalone wake retained an old pod %s", pod.Name)
		}
	}

	updateScale(t, ctx, tc, &grovecorev1alpha1.PodClique{
		ObjectMeta: metav1.ObjectMeta{Name: workerName, Namespace: tc.Namespace},
	}, 3)
	waitForPodCountAndReady(t, tc, 3)
	waitForStandaloneMembership(t, ctx, tc, pcsName, "worker", 3)
	assertScaleStatus(t, ctx, tc, &grovecorev1alpha1.PodClique{
		ObjectMeta: metav1.ObjectMeta{Name: workerName, Namespace: tc.Namespace},
	}, 3, true)
}

func Test_ZR3_PodCliqueScalingGroupLifecycle(t *testing.T) {
	const pcsName = "zero-pcsg"
	ctx := context.Background()
	tc, cleanup := prepareIdleWorkload(t, ctx, 6, pcsName, 0, func(pcs *grovecorev1alpha1.PodCliqueSet) {
		config := idlePCSGConfig(t, pcs)
		config.Replicas = ptr.To(int32(3))
		config.MinAvailable = ptr.To(int32(2))
	})
	defer cleanup()

	pcsgName := pcsName + "-0-workers"
	if err := tc.WaitForReadyPods(6); err != nil {
		t.Fatalf("Initial scaling-group pods did not become ready: %v", err)
	}
	waitForPCSGMembership(t, ctx, tc, pcsName, "workers", []int32{0, 1, 2})
	retainedPCLQUIDs := pclqUIDsForPCSGIndex(t, ctx, tc, pcsgName, "0")
	retainedPodLocations := podUIDLocations(podsForPCSGIndex(t, tc, "0"))
	initialPodGangs := podGangNameSet(ctx, t, tc, pcsName)

	updateScale(t, ctx, tc, &grovecorev1alpha1.PodCliqueScalingGroup{
		ObjectMeta: metav1.ObjectMeta{Name: pcsgName, Namespace: tc.Namespace},
	}, 2)
	waitForPodCountAndReady(t, tc, 4)
	waitForPCSGMembership(t, ctx, tc, pcsName, "workers", []int32{0, 1})
	waitForPCSGChildrenAbsent(t, ctx, tc, pcsgName, 2)
	assertPCLQUIDs(t, ctx, tc, retainedPCLQUIDs)
	assertRetainedPodLocation(t, podsForPCSGIndex(t, tc, "0"), retainedPodLocations)

	updateScale(t, ctx, tc, &grovecorev1alpha1.PodCliqueScalingGroup{
		ObjectMeta: metav1.ObjectMeta{Name: pcsgName, Namespace: tc.Namespace},
	}, 0)
	waitForAllIdle(t, ctx, tc, pcsName, pcsgName+"-0-prefill", pcsgName+"-0-decode")
	waitForAllIdleBootstrap(t, ctx, tc, pcsName)

	updateScale(t, ctx, tc, &grovecorev1alpha1.PodCliqueScalingGroup{
		ObjectMeta: metav1.ObjectMeta{Name: pcsgName, Namespace: tc.Namespace},
	}, 3)
	waitForPodCountAndReady(t, tc, 6)
	waitForPCSGMembership(t, ctx, tc, pcsName, "workers", []int32{0, 1, 2})
	pgm := getPGM(t, ctx, tc, pcsName, 0)
	var anchorEpoch, scaleOutEpoch string
	for _, entry := range pgm.Spec.Entries {
		if entry.Role == grovecorev1alpha1.PodGangEntryRoleAnchor {
			if !slices.Equal(entry.PCSGReplicaIndices["workers"], []int32{0, 1}) {
				t.Fatalf("PCSG wake did not restore its minimum replicas to the anchor: %+v", entry)
			}
			anchorEpoch = entry.Epoch
		}
		if entry.Role == grovecorev1alpha1.PodGangEntryRoleScaleOut {
			if !slices.Equal(entry.PCSGReplicaIndices["workers"], []int32{2}) {
				t.Fatalf("unexpected PCSG-only wake entry: %+v", entry)
			}
			scaleOutEpoch = entry.Epoch
		}
	}
	if scaleOutEpoch == "" || anchorEpoch == "" {
		t.Fatal("PCSG wake requires both an anchor and a ScaleOut entry")
	}
	wokenNames := podGangNameSet(ctx, t, tc, pcsName)
	if len(wokenNames) != 2 {
		t.Fatalf("PCSG wake created %d PodGangs, want one anchor and one scaled gang", len(wokenNames))
	}
	rnr := apicommon.ResourceNameReplica{Name: pcsName, Replica: 0}
	anchorName := apicommon.GenerateAnchorPodGangName(rnr, anchorEpoch)
	if !initialPodGangs.Has(anchorName) || !wokenNames.Has(anchorName) {
		t.Fatalf("PCSG wake did not reuse its anchor name %s", anchorName)
	}
	scaledName := apicommon.GenerateNonAnchorPodGangName(rnr, scaleOutEpoch, "workers", 2)
	if !wokenNames.Has(scaledName) || initialPodGangs.Has(scaledName) {
		t.Fatalf("PCSG wake must give its extra replica a fresh scaled gang: %s", scaledName)
	}
}

func Test_ZR4_AdmissionValidationAndRetry(t *testing.T) {
	const pcsName = "zero-admission"
	ctx := context.Background()
	tc, cleanup := prepareIdleWorkload(t, ctx, 4, pcsName, 0, func(pcs *grovecorev1alpha1.PodCliqueSet) {
		guarded := idleClique(t, pcs, "guarded")
		guarded.Spec.MinAvailable = ptr.To(int32(3))
	})
	defer cleanup()

	template := idleClique(t, loadIdlePCS(t, "unused"), "worker").Spec
	invalidPCLQ := &grovecorev1alpha1.PodClique{
		ObjectMeta: metav1.ObjectMeta{Name: "invalid-pclq", Namespace: tc.Namespace},
		Spec:       template,
	}
	invalidPCLQ.Spec.Replicas = ptr.To[int32](1)
	invalidPCLQ.Spec.MinAvailable = ptr.To(int32(2))
	requireInvalidCause(t, tc.Client.Create(ctx, invalidPCLQ), "spec")
	assertObjectNotFound(t, ctx, tc, client.ObjectKeyFromObject(invalidPCLQ), &grovecorev1alpha1.PodClique{})

	invalidPCSG := &grovecorev1alpha1.PodCliqueScalingGroup{
		ObjectMeta: metav1.ObjectMeta{Name: "invalid-pcsg", Namespace: tc.Namespace},
		Spec: grovecorev1alpha1.PodCliqueScalingGroupSpec{
			Replicas:     1,
			MinAvailable: ptr.To(int32(2)),
			CliqueNames:  []string{"worker"},
		},
	}
	requireInvalidCause(t, tc.Client.Create(ctx, invalidPCSG), "spec")
	assertObjectNotFound(t, ctx, tc, client.ObjectKeyFromObject(invalidPCSG), &grovecorev1alpha1.PodCliqueScalingGroup{})

	invalidPCS := loadIdlePCS(t, "invalid-pcs-template")
	invalidTemplate := idleClique(t, invalidPCS, "worker")
	invalidTemplate.Spec.Replicas = ptr.To[int32](1)
	invalidTemplate.Spec.MinAvailable = ptr.To(int32(2))
	requireInvalidCause(t, tc.Client.Create(ctx, invalidPCS), "spec.template.cliques[1].spec")
	assertObjectNotFound(t, ctx, tc, client.ObjectKeyFromObject(invalidPCS), &grovecorev1alpha1.PodCliqueSet{})

	guardedName := pcsName + "-0-guarded"
	waitForPCLQ(t, ctx, tc, guardedName)
	assertRejectedReplicaUpdate(t, ctx, tc, guardedName, 1)

	updateScale(t, ctx, tc, &grovecorev1alpha1.PodClique{
		ObjectMeta: metav1.ObjectMeta{Name: guardedName, Namespace: tc.Namespace},
	}, 4)
	waitForPodCountAndReady(t, tc, 4)
	assertRejectedReplicaUpdate(t, ctx, tc, guardedName, 2)

	beforeEvents := countEvents(t, ctx, tc, waitForPCLQ(t, ctx, tc, guardedName).UID, "")
	for range 3 {
		assertRejectedScaleUpdate(t, ctx, tc, guardedName, 2)
	}
	guarded := waitForPCLQ(t, ctx, tc, guardedName)
	if ptr.Deref(guarded.Spec.Replicas, 1) != 4 {
		t.Fatalf("stored replicas = %d after rejected retries, want 4", ptr.Deref(guarded.Spec.Replicas, 1))
	}
	if got := countEvents(t, ctx, tc, guarded.UID, ""); got != beforeEvents {
		t.Fatalf("warning event count changed after rejected scale retries: %d -> %d", beforeEvents, got)
	}

	updateScale(t, ctx, tc, &grovecorev1alpha1.PodClique{
		ObjectMeta: metav1.ObjectMeta{Name: guardedName, Namespace: tc.Namespace},
	}, 0)
	waitForAllIdle(t, ctx, tc, pcsName, pcsName+"-0-workers-0-prefill", pcsName+"-0-workers-0-decode")
	assertScaleStatus(t, ctx, tc, &grovecorev1alpha1.PodClique{
		ObjectMeta: metav1.ObjectMeta{Name: guardedName, Namespace: tc.Namespace},
	}, 0, true)
}

func Test_ZR5_PodCliqueSetReplicaIsolation(t *testing.T) {
	const pcsName = "zero-isolation"
	ctx := context.Background()
	tc, cleanup := prepareIdleWorkload(t, ctx, 2, pcsName, 1, func(pcs *grovecorev1alpha1.PodCliqueSet) {
		pcs.Spec.Replicas = 2
	})
	defer cleanup()

	if err := tc.WaitForReadyPods(2); err != nil {
		t.Fatalf("Initial replica-isolation pods did not become ready: %v", err)
	}
	replicaOnePCLQ := pcsName + "-1-worker"
	replicaOnePods := podsForClique(t, tc, replicaOnePCLQ)
	replicaOneLocations := podUIDLocations(replicaOnePods)
	replicaOnePGM := getPGM(t, ctx, tc, pcsName, 1).DeepCopy()
	replicaOnePodGangs := podGangUIDsForPCSReplica(t, ctx, tc, pcsName, "1")

	updateScale(t, ctx, tc, &grovecorev1alpha1.PodClique{
		ObjectMeta: metav1.ObjectMeta{Name: pcsName + "-0-worker", Namespace: tc.Namespace},
	}, 0)
	waitForPodCountAndReady(t, tc, 1)
	assertRetainedPodLocation(t, podsForClique(t, tc, replicaOnePCLQ), replicaOneLocations)
	if current := getPGM(t, ctx, tc, pcsName, 1); !reflect.DeepEqual(replicaOnePGM.Spec.Entries, current.Spec.Entries) {
		t.Fatalf("PodGangMap for untouched PCS replica changed")
	}
	assertPodGangUIDs(t, ctx, tc, replicaOnePodGangs)
}

func Test_ZR6_StandaloneWakePreservesRunningAnchor(t *testing.T) {
	const (
		pcsName  = "zero-running-anchor"
		wakeGate = "test.grove.io/hold-wake"
	)
	ctx := context.Background()
	tc, cleanup := prepareIdleWorkload(t, ctx, 6, pcsName, 1, func(pcs *grovecorev1alpha1.PodCliqueSet) {
		guarded := idleClique(t, pcs, "guarded")
		guarded.Spec.MinAvailable = ptr.To(int32(2))
		guarded.Spec.PodSpec.SchedulingGates = []corev1.PodSchedulingGate{{Name: wakeGate}}
	})
	defer cleanup()
	waitForPodCountAndReady(t, tc, 1)
	routerName := pcsName + "-0-worker"
	guardedName := pcsName + "-0-guarded"
	pcsgName := pcsName + "-0-workers"
	routerLocations := podUIDLocations(podsForClique(t, tc, routerName))
	initialGangs := podGangUIDsForPCSReplica(t, ctx, tc, pcsName, "0")
	pgm := getPGM(t, ctx, tc, pcsName, 0)
	if len(pgm.Spec.Entries) != 2 {
		t.Fatalf("unexpected initial PodGangMap entries: %v", pgm.Spec.Entries)
	}
	var anchorEpoch string
	for _, entry := range pgm.Spec.Entries {
		if entry.Role == grovecorev1alpha1.PodGangEntryRoleAnchor {
			anchorEpoch = entry.Epoch
		}
	}
	anchorKey := client.ObjectKey{Namespace: tc.Namespace, Name: apicommon.GenerateAnchorPodGangName(
		apicommon.ResourceNameReplica{Name: pcsName, Replica: 0}, anchorEpoch)}
	anchor := &groveschedulerv1alpha1.PodGang{}
	if err := wait.PollUntilContextTimeout(ctx, tc.Interval, tc.Timeout, true, func(ctx context.Context) (bool, error) {
		if err := tc.Client.Get(ctx, anchorKey, anchor); err != nil {
			return false, err
		}
		return anchor.Status.LastScheduled != nil, nil
	}); err != nil {
		t.Fatalf("running anchor did not record scheduling: %v", err)
	}
	lastScheduled := anchor.Status.LastScheduled.DeepCopy()

	scaleIdleComponents(t, ctx, tc, guardedName, pcsgName, 2)
	if _, err := tc.WaitForPodCount(7); err != nil {
		t.Fatal(err)
	}
	if err := wait.PollUntilContextTimeout(ctx, tc.Interval, tc.Timeout, true, func(ctx context.Context) (bool, error) {
		if err := tc.Client.Get(ctx, anchorKey, anchor); err != nil {
			return false, err
		}
		pcsg := &grovecorev1alpha1.PodCliqueScalingGroup{}
		if err := tc.Client.Get(ctx, client.ObjectKey{Namespace: tc.Namespace, Name: pcsgName}, pcsg); err != nil {
			return false, err
		}
		native, err := podgroup.GetNativePodGroup(ctx, tc.Client, anchor)
		if err != nil || podgroup.VerifyFlatMembership(anchor, native) != nil {
			return false, err
		}
		pods, err := tc.ListPods()
		if err != nil {
			return false, err
		}
		for _, pod := range pods.Items {
			gated := slices.ContainsFunc(pod.Spec.SchedulingGates, func(gate corev1.PodSchedulingGate) bool {
				return gate.Name == zeroReplicaGangGate
			})
			isExtra := pod.Labels[apicommon.LabelPodCliqueScalingGroupReplicaIndex] == "1"
			if gated != isExtra {
				return false, nil
			}
		}
		for _, group := range anchor.Spec.PodGroups {
			if group.Name == guardedName && group.MinReplicas == 2 && len(group.PodReferences) == 2 {
				return pcsg.Status.AvailableReplicas == 0 &&
					meta.IsStatusConditionFalse(anchor.Status.Conditions, string(groveschedulerv1alpha1.PodGangConditionTypeScheduled)), nil
			}
		}
		return false, nil
	}); err != nil {
		t.Fatalf("joint wake did not restore the anchor policy: %v", err)
	}
	if !lastScheduled.Equal(anchor.Status.LastScheduled) {
		t.Fatal("pending standalone wake changed the anchor scheduling history")
	}
	assertPodGangUIDs(t, ctx, tc, initialGangs)
	assertRetainedPodLocation(t, podsForClique(t, tc, routerName), routerLocations)
	for _, pod := range podsForClique(t, tc, routerName) {
		if !k8sutils.IsPodReady(&pod) {
			t.Fatalf("running anchor pod %s became unready", pod.Name)
		}
	}
	pending := podsForClique(t, tc, guardedName)
	if len(pending) != 2 {
		t.Fatalf("restored standalone has %d pods, want 2", len(pending))
	}
	releaseWakeGate := func(pod *corev1.Pod) {
		if err := tc.Client.Get(ctx, client.ObjectKeyFromObject(pod), pod); err != nil {
			t.Fatal(err)
		}
		patch := client.MergeFrom(pod.DeepCopy())
		pod.Spec.SchedulingGates = slices.DeleteFunc(pod.Spec.SchedulingGates, func(gate corev1.PodSchedulingGate) bool {
			return gate.Name == wakeGate
		})
		if err := tc.Client.Patch(ctx, pod, patch); err != nil {
			t.Fatalf("failed to release wake gate: %v", err)
		}
	}
	// One waking member cannot satisfy the restored standalone floor or admit the PCSG minimum.
	releaseWakeGate(&pending[0])
	assertStableFor(t, ctx, zeroReplicaWakeObservationWindow, func(ctx context.Context) error {
		pods, err := tc.ListPods()
		if err != nil {
			return err
		}
		if len(pods.Items) != 7 {
			return fmt.Errorf("joint wake has %d pods, expected 7", len(pods.Items))
		}
		for _, pod := range pods.Items {
			if node, survivor := routerLocations[pod.UID]; survivor {
				if pod.Spec.NodeName != node || !k8sutils.IsPodReady(&pod) {
					return fmt.Errorf("running anchor member %s was disrupted", pod.Name)
				}
			} else if pod.Spec.NodeName != "" {
				return fmt.Errorf("pod %s scheduled below the joint wake quorum", pod.Name)
			}
		}
		return nil
	})
	releaseWakeGate(&pending[1])
	waitForPodCountAndReady(t, tc, 7)
	waitForStandaloneMembership(t, ctx, tc, pcsName, "guarded", 2)
	assertRetainedPodLocation(t, podsForClique(t, tc, routerName), routerLocations)
}

func Test_ZR7_PCSGWakeWaitsForLiveQuorum(t *testing.T) {
	const pcsName = "zero-live-quorum"
	ctx := context.Background()
	tc, cleanup := prepareIdleWorkload(t, ctx, 6, pcsName, 1, func(pcs *grovecorev1alpha1.PodCliqueSet) {
		config := idlePCSGConfig(t, pcs)
		config.Replicas = ptr.To(int32(3))
		config.MinAvailable = ptr.To(int32(2))
		observer := idleClique(t, pcs, "worker").DeepCopy()
		observer.Name = "observer"
		observer.Spec.RoleName = "observer"
		pcs.Spec.Template.Cliques = append(pcs.Spec.Template.Cliques, observer)
		pcs.Spec.Template.PodCliqueScalingGroupConfigs = append(pcs.Spec.Template.PodCliqueScalingGroupConfigs,
			grovecorev1alpha1.PodCliqueScalingGroupConfig{
				Name: "observers", Replicas: ptr.To(int32(1)), MinAvailable: ptr.To(int32(1)), CliqueNames: []string{"observer"},
			})
	})
	defer cleanup()
	waitForPodCountAndReady(t, tc, 8)
	survivors := append(podsForClique(t, tc, pcsName+"-0-worker"), podsForClique(t, tc, pcsName+"-0-observers-0-observer")...)
	survivorLocations := podUIDLocations(survivors)
	if len(survivorLocations) != 2 {
		t.Fatalf("expected standalone and second-PCSG survivors, got %d", len(survivorLocations))
	}
	pcsg := &grovecorev1alpha1.PodCliqueScalingGroup{ObjectMeta: metav1.ObjectMeta{Name: pcsName + "-0-workers", Namespace: tc.Namespace}}
	updateScale(t, ctx, tc, pcsg, 0)
	waitForPodCountAndReady(t, tc, 2)
	waitForPCSGMembership(t, ctx, tc, pcsName, "workers", nil)

	pgm := getPGM(t, ctx, tc, pcsName, 0)
	var anchorEpoch string
	for _, entry := range pgm.Spec.Entries {
		if entry.Role == grovecorev1alpha1.PodGangEntryRoleAnchor {
			anchorEpoch = entry.Epoch
		}
	}
	anchor := &groveschedulerv1alpha1.PodGang{}
	anchorKey := client.ObjectKey{Namespace: tc.Namespace, Name: apicommon.GenerateAnchorPodGangName(
		apicommon.ResourceNameReplica{Name: pcsName, Replica: 0}, anchorEpoch)}
	if err := tc.Client.Get(ctx, anchorKey, anchor); err != nil || anchor.Status.LastScheduled == nil {
		t.Fatalf("surviving anchor must retain scheduling history: %v", err)
	}
	anchorUID, lastScheduled := anchor.UID, anchor.Status.LastScheduled.DeepCopy()
	nodes, err := tc.GetWorkerNodes()
	if err != nil {
		t.Fatal(err)
	}
	tc.CordonNodes(nodes)
	defer tc.UncordonNodes(nodes)
	updateScale(t, ctx, tc, pcsg, 3)
	if _, err := tc.WaitForPodCount(8); err != nil {
		t.Fatal(err)
	}
	waitForPCSGMembership(t, ctx, tc, pcsName, "workers", []int32{0, 1, 2})
	checkBlockedWake := func(ctx context.Context) error {
		list, err := tc.ListPods()
		if err != nil {
			return err
		}
		var minimum, extra int
		for i := range list.Items {
			pod := &list.Items[i]
			if _, survivor := survivorLocations[pod.UID]; survivor {
				if !isPodReady(pod) || survivorLocations[pod.UID] != pod.Spec.NodeName {
					return fmt.Errorf("surviving pod %s was disrupted", pod.Name)
				}
				continue
			}
			if pod.Labels[apicommon.LabelPodCliqueScalingGroup] != pcsg.Name {
				return fmt.Errorf("unexpected pod %s replaced a survivor", pod.Name)
			}
			gated := slices.ContainsFunc(pod.Spec.SchedulingGates, func(gate corev1.PodSchedulingGate) bool {
				return gate.Name == zeroReplicaGangGate
			})
			if pod.Labels[apicommon.LabelPodCliqueScalingGroupReplicaIndex] == "2" {
				extra++
				if !gated || pod.Spec.NodeName != "" {
					return fmt.Errorf("extra replica %s escaped before the minimum scheduled", pod.Name)
				}
			} else {
				minimum++
				if gated || pod.Spec.NodeName != "" {
					return fmt.Errorf("minimum pod %s must be admitted but capacity-blocked", pod.Name)
				}
			}
		}
		if minimum != 4 || extra != 2 || len(list.Items) != 8 {
			return fmt.Errorf("wake pod counts: minimum=%d extra=%d total=%d", minimum, extra, len(list.Items))
		}
		if err := tc.Client.Get(ctx, anchorKey, anchor); err != nil {
			return err
		}
		if anchor.UID != anchorUID || !lastScheduled.Equal(anchor.Status.LastScheduled) {
			return fmt.Errorf("wake changed anchor identity or historical scheduling marker")
		}
		if err := tc.Client.Get(ctx, client.ObjectKeyFromObject(pcsg), pcsg); err != nil {
			return err
		}
		breach := meta.FindStatusCondition(pcsg.Status.Conditions, apiconstants.ConditionTypeMinAvailableBreached)
		if breach == nil || breach.Reason != apiconstants.ConditionReasonInitialScheduling ||
			breach.ObservedGeneration != pcsg.Generation {
			return fmt.Errorf("pending wake must not arm gang termination: %+v", breach)
		}
		native, err := podgroup.GetNativePodGroup(ctx, tc.Client, anchor)
		if err != nil {
			return err
		}
		return podgroup.VerifyFlatMembership(anchor, native)
	}
	if err := wait.PollUntilContextTimeout(ctx, tc.Interval, tc.Timeout, true, func(ctx context.Context) (bool, error) {
		return checkBlockedWake(ctx) == nil, nil
	}); err != nil {
		t.Fatalf("wake did not reach its gated handoff state: %v (last check: %v)", err, checkBlockedWake(ctx))
	}
	restartOperator(t, ctx, tc)
	assertStableFor(t, ctx, zeroReplicaWakeObservationWindow, checkBlockedWake)
	native, err := podgroup.GetNativePodGroup(ctx, tc.Client, anchor)
	if err != nil {
		t.Fatal(err)
	}
	nativeUID := native.GetUID()
	if err := tc.Client.Delete(ctx, native); err != nil {
		t.Fatal(err)
	}
	if err := wait.PollUntilContextTimeout(ctx, tc.Interval, tc.Timeout, true, func(ctx context.Context) (bool, error) {
		current, err := podgroup.GetNativePodGroup(ctx, tc.Client, anchor)
		return err == nil && current.GetUID() != nativeUID && podgroup.VerifyFlatMembership(anchor, current) == nil, nil
	}); err != nil {
		t.Fatalf("native PodGroup was not reconstructed: %v", err)
	}
	assertStableFor(t, ctx, zeroReplicaWakeObservationWindow, checkBlockedWake)
	tc.UncordonNodes(nodes)
	waitForPodCountAndReady(t, tc, 8)
	assertRetainedPodLocation(t, append(podsForClique(t, tc, pcsName+"-0-worker"),
		podsForClique(t, tc, pcsName+"-0-observers-0-observer")...), survivorLocations)
}

func Test_ZR8_RecoveryPreservesLatestScaleTargets(t *testing.T) {
	const pcsName = "zero-recovery"
	const holdFinalizer = "e2e.grove.io/hold-recovery"
	ctx := context.Background()
	tc, cleanup := prepareIdleWorkload(t, ctx, 6, pcsName, 0, func(pcs *grovecorev1alpha1.PodCliqueSet) {
		idleClique(t, pcs, "guarded").Spec.Replicas = ptr.To[int32](2)
		idlePCSGConfig(t, pcs).Replicas = ptr.To(int32(1))
	})
	defer cleanup()
	waitForPodCountAndReady(t, tc, 4)
	worker := waitForPCLQ(t, ctx, tc, pcsName+"-0-worker")
	guarded := waitForPCLQ(t, ctx, tc, pcsName+"-0-guarded")
	group := &grovecorev1alpha1.PodCliqueScalingGroup{}
	if err := tc.Client.Get(ctx, client.ObjectKey{Namespace: tc.Namespace, Name: pcsName + "-0-workers"}, group); err != nil {
		t.Fatal(err)
	}
	updateScale(t, ctx, tc, guarded, 0)
	updateScale(t, ctx, tc, worker, 3)
	updateScale(t, ctx, tc, group, 2)
	waitForPodCountAndReady(t, tc, 7)
	waitForPCLQConditionReason(t, ctx, tc, worker.Name, apiconstants.ConditionReasonSufficientReadyPods)
	oldPods, err := tc.ListPods()
	if err != nil {
		t.Fatal(err)
	}
	oldUIDs := sets.New[types.UID]()
	for _, pod := range oldPods.Items {
		oldUIDs.Insert(pod.UID)
	}
	held := podsForPCSGIndex(t, tc, "0")[0].DeepCopy()
	if err := tc.Client.Patch(ctx, held, client.RawPatch(types.MergePatchType, []byte(
		`{"metadata":{"finalizers":["`+holdFinalizer+`"]}}`))); err != nil {
		t.Fatal(err)
	}
	defer func() {
		_ = client.IgnoreNotFound(tc.Client.Patch(ctx, held,
			client.RawPatch(types.MergePatchType, []byte(`{"metadata":{"finalizers":[]}}`))))
	}()
	nodes := tc.SetupAndCordonNodes(6)
	defer tc.UncordonNodes(nodes)
	for _, pod := range podsForClique(t, tc, worker.Name) {
		if err := tc.Client.Delete(ctx, &pod); err != nil {
			t.Fatal(err)
		}
	}
	draining := waitForGangRecovery(ctx, t, tc, pcsName, componentutils.GangRecoveryDraining)
	if err := wait.PollUntilContextTimeout(ctx, tc.Interval, tc.Timeout, true, func(ctx context.Context) (bool, error) {
		current := &corev1.Pod{}
		err := tc.Client.Get(ctx, client.ObjectKeyFromObject(held), current)
		return err == nil && !current.DeletionTimestamp.IsZero(), err
	}); err != nil {
		t.Fatalf("recovery did not reach the held deletion: %v", err)
	}
	restartOperator(t, ctx, tc)
	updateScale(t, ctx, tc, worker, 4)
	assertScaleStatus(t, ctx, tc, worker, 4, false)
	if err := tc.Client.Patch(ctx, held, client.RawPatch(types.MergePatchType, []byte(`{"metadata":{"finalizers":[]}}`))); err != nil {
		t.Fatal(err)
	}
	recreating := waitForGangRecovery(ctx, t, tc, pcsName, componentutils.GangRecoveryRecreating)
	if recreating.Epoch != draining.Epoch {
		t.Fatal("restart changed the recovery epoch")
	}
	tc.UncordonNodes(nodes)
	waitForPodCountAndReady(t, tc, 8)
	waitForGangRecovery(ctx, t, tc, pcsName, componentutils.GangRecoveryComplete)
	assertScaleStatus(t, ctx, tc, worker, 4, true)
	assertScaleStatus(t, ctx, tc, guarded, 0, true)
	assertScaleStatus(t, ctx, tc, group, 2, true)
	for _, target := range []client.Object{worker, guarded, group} {
		current := target.DeepCopyObject().(client.Object)
		if err := tc.Client.Get(ctx, client.ObjectKeyFromObject(target), current); err != nil {
			t.Fatal(err)
		}
		if current.GetUID() != target.GetUID() {
			t.Fatalf("recovery replaced scale target %s", target.GetName())
		}
	}
	pods, err := tc.ListPods()
	if err != nil {
		t.Fatal(err)
	}
	for _, pod := range pods.Items {
		if oldUIDs.Has(pod.UID) || pod.Annotations[componentutils.AnnotationPodRecoveryEpoch] != draining.Epoch || !isPodReady(&pod) {
			t.Fatalf("pod %s is not a Ready replacement from the current recovery", pod.Name)
		}
	}

	// Re-arm after real health, then preserve an idle PCSG through another restart.
	updateScale(t, ctx, tc, group, 0)
	waitForPodCountAndReady(t, tc, 4)
	nodes = tc.SetupAndCordonNodes(6)
	for _, pod := range podsForClique(t, tc, worker.Name) {
		if err := tc.Client.Delete(ctx, &pod); err != nil {
			t.Fatal(err)
		}
	}
	second := waitForGangRecovery(ctx, t, tc, pcsName, componentutils.GangRecoveryRecreating)
	if second.Epoch == draining.Epoch {
		t.Fatal("a new regression did not start a new recovery")
	}
	restartOperator(t, ctx, tc)
	assertScaleStatus(t, ctx, tc, group, 0, true)
	assertScaleStatus(t, ctx, tc, worker, 4, false)
	tc.UncordonNodes(nodes)
	waitForPodCountAndReady(t, tc, 4)
	waitForGangRecovery(ctx, t, tc, pcsName, componentutils.GangRecoveryComplete)
}

func Test_ZR9_ScaleInDuringRecoveryDrain(t *testing.T) {
	const pcsName = "zero-recovery-scale-in"
	const holdFinalizer = "e2e.grove.io/hold-scale-in"
	ctx := context.Background()
	tc, cleanup := prepareIdleWorkload(t, ctx, 6, pcsName, 1, func(pcs *grovecorev1alpha1.PodCliqueSet) {
		idlePCSGConfig(t, pcs).Replicas = ptr.To(int32(1))
	})
	defer cleanup()
	waitForPodCountAndReady(t, tc, 3)
	worker := waitForPCLQ(t, ctx, tc, pcsName+"-0-worker")
	group := &grovecorev1alpha1.PodCliqueScalingGroup{}
	if err := tc.Client.Get(ctx, client.ObjectKey{Namespace: tc.Namespace, Name: pcsName + "-0-workers"}, group); err != nil {
		t.Fatal(err)
	}
	waitForPCLQConditionReason(t, ctx, tc, worker.Name, apiconstants.ConditionReasonSufficientReadyPods)
	held := podsForPCSGIndex(t, tc, "0")[0].DeepCopy()
	owner := metav1.GetControllerOf(held)
	if owner == nil {
		t.Fatal("group Pod has no controller owner")
	}
	memberKey := client.ObjectKey{Namespace: tc.Namespace, Name: owner.Name}
	if err := tc.Client.Patch(ctx, held, client.RawPatch(types.MergePatchType, []byte(
		`{"metadata":{"finalizers":["`+holdFinalizer+`"]}}`))); err != nil {
		t.Fatal(err)
	}
	defer func() {
		_ = client.IgnoreNotFound(tc.Client.Patch(ctx, held,
			client.RawPatch(types.MergePatchType, []byte(`{"metadata":{"finalizers":[]}}`))))
	}()
	nodes := tc.SetupAndCordonNodes(6)
	defer tc.UncordonNodes(nodes)
	for _, pod := range podsForClique(t, tc, worker.Name) {
		if err := tc.Client.Delete(ctx, &pod); err != nil {
			t.Fatal(err)
		}
	}
	draining := waitForGangRecovery(ctx, t, tc, pcsName, componentutils.GangRecoveryDraining)
	updateScale(t, ctx, tc, group, 0)
	if err := wait.PollUntilContextTimeout(ctx, tc.Interval, tc.Timeout, true, func(ctx context.Context) (bool, error) {
		member := &grovecorev1alpha1.PodClique{}
		err := tc.Client.Get(ctx, memberKey, member)
		return err == nil && !member.DeletionTimestamp.IsZero(), err
	}); err != nil {
		t.Fatalf("scale-in did not start member deletion: %v", err)
	}
	tc.UncordonNodes(nodes)
	restartOperator(t, ctx, tc)
	assertStableFor(t, ctx, zeroReplicaObservationWindow, func(ctx context.Context) error {
		pcs := &grovecorev1alpha1.PodCliqueSet{}
		if err := tc.Client.Get(ctx, client.ObjectKey{Namespace: tc.Namespace, Name: pcsName}, pcs); err != nil {
			return err
		}
		current, err := componentutils.GetGangRecovery(pcs, 0)
		if err != nil || current != draining {
			return fmt.Errorf("recovery advanced before scale-in Pods drained: %+v, %v", current, err)
		}
		member := &grovecorev1alpha1.PodClique{}
		if err := tc.Client.Get(ctx, memberKey, member); err != nil {
			return err
		}
		if !slices.Contains(member.Finalizers, apiconstants.FinalizerPodClique) {
			return fmt.Errorf("member finalizer released before its Pod disappeared")
		}
		pod := &corev1.Pod{}
		if err := tc.Client.Get(ctx, client.ObjectKeyFromObject(held), pod); err != nil {
			return err
		}
		pods, err := tc.ListPods()
		if err != nil {
			return err
		}
		for _, pod := range pods.Items {
			if pod.Annotations[componentutils.AnnotationPodRecoveryEpoch] == draining.Epoch {
				return fmt.Errorf("replacement %s created before the old gang drained", pod.Name)
			}
		}
		return nil
	})
	if err := tc.Client.Patch(ctx, held, client.RawPatch(types.MergePatchType, []byte(`{"metadata":{"finalizers":[]}}`))); err != nil {
		t.Fatal(err)
	}
	waitForPodCountAndReady(t, tc, 1)
	waitForGangRecovery(ctx, t, tc, pcsName, componentutils.GangRecoveryComplete)
	assertScaleStatus(t, ctx, tc, group, 0, true)
	current := &grovecorev1alpha1.PodCliqueScalingGroup{}
	if err := tc.Client.Get(ctx, client.ObjectKeyFromObject(group), current); err != nil {
		t.Fatal(err)
	}
	if current.UID != group.UID {
		t.Fatal("scale-in during recovery replaced its scale target")
	}
}

func Test_ZR10_FreshScaleOutEpochWithRetainedMembers(t *testing.T) {
	const pcsName = "zero-retained-scale-out"
	ctx := context.Background()
	tc, cleanup := prepareIdleWorkload(t, ctx, 6, pcsName, 0, func(pcs *grovecorev1alpha1.PodCliqueSet) {
		config := idlePCSGConfig(t, pcs)
		config.Replicas, config.MinAvailable = ptr.To(int32(2)), ptr.To(int32(2))
	})
	defer cleanup()
	waitForPodCountAndReady(t, tc, 4)
	group := &grovecorev1alpha1.PodCliqueScalingGroup{ObjectMeta: metav1.ObjectMeta{
		Name: pcsName + "-0-workers", Namespace: tc.Namespace,
	}}
	updateScale(t, ctx, tc, group, 3)
	waitForPodCountAndReady(t, tc, 6)
	waitForPCSGMembership(t, ctx, tc, pcsName, "workers", []int32{0, 1, 2})
	anchorPods := append(podsForPCSGIndex(t, tc, "0"), podsForPCSGIndex(t, tc, "1")...)
	anchorLocations := podUIDLocations(anchorPods)
	oldPods := podUIDLocations(podsForPCSGIndex(t, tc, "2"))
	oldCliques := pclqUIDsForPCSGIndex(t, ctx, tc, group.Name, "2")
	pgm := getPGM(t, ctx, tc, pcsName, 0)
	oldEpoch := ""
	// Deterministically model the inter-controller gap in 3 -> 2 -> 3:
	// PCS has emptied ScaleOut while PCSG's old index-2 members still exist.
	for i := range pgm.Spec.Entries {
		entry := &pgm.Spec.Entries[i]
		if entry.Role == grovecorev1alpha1.PodGangEntryRoleScaleOut {
			oldEpoch = entry.Epoch
			delete(entry.PCSGReplicaIndices, "workers")
		}
	}
	if oldEpoch == "" {
		t.Fatal("runtime scale-out did not create a ScaleOut entry")
	}
	if err := tc.Client.Update(ctx, pgm); err != nil {
		t.Fatal(err)
	}
	// PGM writes do not enqueue PCS; model the scale event that starts the next pass.
	if err := workload.NewWorkloadManager(tc.Client, Logger).TriggerPCSReconcile(ctx, tc.Namespace, pcsName, "retained-scale-out"); err != nil {
		t.Fatal(err)
	}
	var newGang string
	if err := wait.PollUntilContextTimeout(ctx, tc.Interval, tc.Timeout, true, func(ctx context.Context) (bool, error) {
		current := &grovecorev1alpha1.PodGangMap{}
		if err := tc.Client.Get(ctx, client.ObjectKeyFromObject(pgm), current); err != nil {
			return false, err
		}
		for _, entry := range current.Spec.Entries {
			if entry.Role == grovecorev1alpha1.PodGangEntryRoleScaleOut &&
				entry.Epoch != oldEpoch && slices.Equal(entry.PCSGReplicaIndices["workers"], []int32{2}) {
				newGang = apicommon.GenerateNonAnchorPodGangName(apicommon.ResourceNameReplica{Name: pcsName, Replica: 0}, entry.Epoch, "workers", 2)
				return true, nil
			}
		}
		return false, nil
	}); err != nil {
		t.Fatalf("ScaleOut did not acquire a fresh epoch: %v", err)
	}
	if err := wait.PollUntilContextTimeout(ctx, tc.Interval, tc.Timeout, true, func(ctx context.Context) (bool, error) {
		for name, uid := range oldCliques {
			pclq := &grovecorev1alpha1.PodClique{}
			if err := tc.Client.Get(ctx, client.ObjectKey{Namespace: tc.Namespace, Name: name}, pclq); err != nil {
				return false, client.IgnoreNotFound(err)
			}
			if pclq.UID == uid || pclq.Labels[apicommon.LabelPodGang] != newGang {
				return false, nil
			}
		}
		return true, nil
	}); err != nil {
		t.Fatalf("retained members were not reconstructed for the new epoch: %v", err)
	}
	waitForPodCountAndReady(t, tc, 6)
	for _, pod := range podsForPCSGIndex(t, tc, "2") {
		if _, retained := oldPods[pod.UID]; retained || pod.Labels[apicommon.LabelPodGang] != newGang {
			t.Fatalf("Pod %s did not get fresh scheduler membership", pod.Name)
		}
	}
	assertRetainedPodLocation(t, append(podsForPCSGIndex(t, tc, "0"), podsForPCSGIndex(t, tc, "1")...), anchorLocations)
	gang := &groveschedulerv1alpha1.PodGang{}
	if err := tc.Client.Get(ctx, client.ObjectKey{Namespace: tc.Namespace, Name: newGang}, gang); err != nil {
		t.Fatal(err)
	}
	native, err := podgroup.GetNativePodGroup(ctx, tc.Client, gang)
	if err != nil {
		t.Fatal(err)
	}
	if err := podgroup.VerifyFlatMembership(gang, native); err != nil {
		t.Fatal(err)
	}
}

func waitForGangRecovery(ctx context.Context, t *testing.T, tc *testctx.TestContext, pcsName, phase string) componentutils.GangRecovery {
	t.Helper()
	var recovery componentutils.GangRecovery
	if err := wait.PollUntilContextTimeout(ctx, tc.Interval, tc.Timeout, true, func(ctx context.Context) (bool, error) {
		pcs := &grovecorev1alpha1.PodCliqueSet{}
		if err := tc.Client.Get(ctx, client.ObjectKey{Namespace: tc.Namespace, Name: pcsName}, pcs); err != nil {
			return false, err
		}
		var err error
		recovery, err = componentutils.GetGangRecovery(pcs, 0)
		return err == nil && recovery.Phase == phase, err
	}); err != nil {
		t.Fatalf("gang recovery did not reach %s: %v (last state: %+v)", phase, err, recovery)
	}
	return recovery
}

func prepareIdleWorkload(
	t *testing.T,
	ctx context.Context,
	workerNodes int,
	name string,
	workerReplicas int32,
	mutate func(*grovecorev1alpha1.PodCliqueSet),
) (*testctx.TestContext, func()) {
	t.Helper()
	tc, cleanup := testctx.PrepareTest(ctx, t, workerNodes,
		testctx.WithWorkload(&testctx.WorkloadConfig{
			Name:         name,
			Namespace:    "default",
			ExpectedPods: int(workerReplicas),
		}),
	)
	pcs := loadIdlePCS(t, name)
	idleClique(t, pcs, "worker").Spec.Replicas = ptr.To[int32](workerReplicas)
	if mutate != nil {
		mutate(pcs)
	}
	if err := tc.Client.Create(ctx, pcs); err != nil {
		cleanup()
		t.Fatalf("Failed to create PodCliqueSet %s: %v", name, err)
	}
	return tc, cleanup
}

func loadIdlePCS(t *testing.T, name string) *grovecorev1alpha1.PodCliqueSet {
	t.Helper()
	data, err := os.ReadFile("../yaml/workload-idle-wake.yaml")
	if err != nil {
		t.Fatalf("Failed to read idle workload fixture: %v", err)
	}
	pcs := &grovecorev1alpha1.PodCliqueSet{}
	if err := yaml.Unmarshal(data, pcs); err != nil {
		t.Fatalf("Failed to decode idle workload fixture: %v", err)
	}
	pcs.Name = name
	pcs.Namespace = "default"
	pcs.ResourceVersion = ""
	pcs.UID = ""
	pcs.Labels["app"] = name
	for _, clique := range pcs.Spec.Template.Cliques {
		if schedulerName := os.Getenv("GROVE_E2E_SCHEDULER"); schedulerName != "" {
			clique.Spec.PodSpec.SchedulerName = schedulerName
		}
		if image := os.Getenv("GROVE_E2E_WORKLOAD_IMAGE"); image != "" {
			for i := range clique.Spec.PodSpec.Containers {
				clique.Spec.PodSpec.Containers[i].Image = image
			}
		}
	}
	return pcs
}

func idleClique(t *testing.T, pcs *grovecorev1alpha1.PodCliqueSet, name string) *grovecorev1alpha1.PodCliqueTemplateSpec {
	t.Helper()
	for i := range pcs.Spec.Template.Cliques {
		if pcs.Spec.Template.Cliques[i].Name == name {
			return pcs.Spec.Template.Cliques[i]
		}
	}
	t.Fatalf("PodClique template %s not found", name)
	return nil
}

func idlePCSGConfig(t *testing.T, pcs *grovecorev1alpha1.PodCliqueSet) *grovecorev1alpha1.PodCliqueScalingGroupConfig {
	t.Helper()
	for i := range pcs.Spec.Template.PodCliqueScalingGroupConfigs {
		if pcs.Spec.Template.PodCliqueScalingGroupConfigs[i].Name == "workers" {
			return &pcs.Spec.Template.PodCliqueScalingGroupConfigs[i]
		}
	}
	t.Fatal("PodCliqueScalingGroup config workers not found")
	return nil
}

func waitForAllIdleBootstrap(t *testing.T, ctx context.Context, tc *testctx.TestContext, pcsName string) {
	t.Helper()
	if err := wait.PollUntilContextTimeout(ctx, tc.Interval, tc.Timeout, true, func(ctx context.Context) (bool, error) {
		pods, err := tc.ListPods()
		if err != nil || len(pods.Items) != 0 {
			return false, err
		}
		if len(listPodGangs(t, ctx, tc, pcsName).Items) != 0 {
			return false, nil
		}
		pclqs := &grovecorev1alpha1.PodCliqueList{}
		if err := tc.Client.List(ctx, pclqs,
			client.InNamespace(tc.Namespace),
			client.MatchingLabels{apicommon.LabelPartOfKey: pcsName},
		); err != nil {
			return false, err
		}
		for i := range pclqs.Items {
			owner := metav1.GetControllerOfNoCopy(&pclqs.Items[i])
			if owner != nil && owner.Kind == apiconstants.KindPodCliqueScalingGroup {
				return false, nil
			}
		}
		pgm := getPGM(t, ctx, tc, pcsName, 0)
		var baseAnchor, scaleOut bool
		for i := range pgm.Spec.Entries {
			entry := &pgm.Spec.Entries[i]
			if len(entry.PodCliques) != 0 || hasPCSGIndices(entry.PCSGReplicaIndices) {
				return false, nil
			}
			if entry.Role == grovecorev1alpha1.PodGangEntryRoleAnchor &&
				entry.AnchorIndex != nil && *entry.AnchorIndex == 0 {
				baseAnchor = true
			}
			if entry.Role == grovecorev1alpha1.PodGangEntryRoleScaleOut {
				scaleOut = true
			}
		}
		pcs := &grovecorev1alpha1.PodCliqueSet{}
		if err := tc.Client.Get(ctx, client.ObjectKey{Namespace: tc.Namespace, Name: pcsName}, pcs); err != nil {
			return false, err
		}
		return baseAnchor && scaleOut && pcs.Status.AvailableReplicas == pcs.Spec.Replicas, nil
	}); err != nil {
		t.Fatalf("All-idle bootstrap did not converge: %v", err)
	}
}

func hasPCSGIndices(indices map[string][]int32) bool {
	for _, values := range indices {
		if len(values) != 0 {
			return true
		}
	}
	return false
}

func setStaleBreachConditions(t *testing.T, ctx context.Context, tc *testctx.TestContext, pclqName, pcsgName string) {
	t.Helper()
	if err := retry.RetryOnConflict(retry.DefaultRetry, func() error {
		pclq := &grovecorev1alpha1.PodClique{}
		if err := tc.Client.Get(ctx, client.ObjectKey{Namespace: tc.Namespace, Name: pclqName}, pclq); err != nil {
			return err
		}
		meta.SetStatusCondition(&pclq.Status.Conditions, metav1.Condition{
			Type:               apiconstants.ConditionTypeMinAvailableBreached,
			Status:             metav1.ConditionTrue,
			Reason:             apiconstants.ConditionReasonInsufficientScheduledPods,
			ObservedGeneration: pclq.Generation,
		})
		return tc.Client.Status().Update(ctx, pclq)
	}); err != nil {
		t.Fatalf("Failed to seed stale PodClique breach: %v", err)
	}
	if err := retry.RetryOnConflict(retry.DefaultRetry, func() error {
		pcsg := &grovecorev1alpha1.PodCliqueScalingGroup{}
		if err := tc.Client.Get(ctx, client.ObjectKey{Namespace: tc.Namespace, Name: pcsgName}, pcsg); err != nil {
			return err
		}
		meta.SetStatusCondition(&pcsg.Status.Conditions, metav1.Condition{
			Type:               apiconstants.ConditionTypeMinAvailableBreached,
			Status:             metav1.ConditionTrue,
			Reason:             apiconstants.ConditionReasonInsufficientAvailablePCSGReplicas,
			ObservedGeneration: pcsg.Generation,
		})
		return tc.Client.Status().Update(ctx, pcsg)
	}); err != nil {
		t.Fatalf("Failed to seed stale PodCliqueScalingGroup breach: %v", err)
	}
}

func waitForPCSGConditionReason(t *testing.T, ctx context.Context, tc *testctx.TestContext, name, reason string) {
	t.Helper()
	if err := wait.PollUntilContextTimeout(ctx, tc.Interval, tc.Timeout, true, func(ctx context.Context) (bool, error) {
		pcsg := &grovecorev1alpha1.PodCliqueScalingGroup{}
		if err := tc.Client.Get(ctx, client.ObjectKey{Namespace: tc.Namespace, Name: name}, pcsg); err != nil {
			return false, client.IgnoreNotFound(err)
		}
		condition := meta.FindStatusCondition(pcsg.Status.Conditions, apiconstants.ConditionTypeMinAvailableBreached)
		return condition != nil && condition.Status == metav1.ConditionFalse &&
			condition.Reason == reason && condition.ObservedGeneration == pcsg.Generation, nil
	}); err != nil {
		t.Fatalf("PodCliqueScalingGroup %s condition did not reach reason %s: %v", name, reason, err)
	}
}

func countEvents(t *testing.T, ctx context.Context, tc *testctx.TestContext, uid types.UID, reason string) int {
	t.Helper()
	events := &corev1.EventList{}
	if err := tc.Client.List(ctx, events, client.InNamespace(tc.Namespace)); err != nil {
		t.Fatalf("Failed to list events: %v", err)
	}
	count := 0
	for i := range events.Items {
		event := &events.Items[i]
		if event.InvolvedObject.UID == uid && (reason == "" || event.Reason == reason) &&
			(reason != "" || event.Type == corev1.EventTypeWarning) {
			count++
		}
	}
	return count
}

func assertStableFor(t *testing.T, ctx context.Context, duration time.Duration, check func(context.Context) error) {
	t.Helper()
	deadline := time.Now().Add(duration)
	for {
		if err := check(ctx); err != nil {
			t.Fatal(err)
		}
		if time.Now().After(deadline) {
			return
		}
		select {
		case <-ctx.Done():
			t.Fatal(ctx.Err())
		case <-time.After(500 * time.Millisecond):
		}
	}
}

func updateScale(t *testing.T, ctx context.Context, tc *testctx.TestContext, obj client.Object, replicas int32) {
	t.Helper()
	scale := &autoscalingv1.Scale{
		ObjectMeta: metav1.ObjectMeta{Name: obj.GetName(), Namespace: obj.GetNamespace()},
		Spec:       autoscalingv1.ScaleSpec{Replicas: replicas},
	}
	if err := tc.Client.SubResource("scale").Update(ctx, obj, client.WithSubResourceBody(scale)); err != nil {
		t.Fatalf("Failed to scale %T %s to %d: %v", obj, obj.GetName(), replicas, err)
	}
}

func assertScaleStatus(t *testing.T, ctx context.Context, tc *testctx.TestContext, obj client.Object, replicas int32, wantSelector bool) {
	t.Helper()
	scale := &autoscalingv1.Scale{}
	if err := tc.Client.SubResource("scale").Get(ctx, obj, scale); err != nil {
		t.Fatalf("Failed to get scale for %T %s: %v", obj, obj.GetName(), err)
	}
	if scale.Spec.Replicas != replicas {
		t.Fatalf("scale spec replicas = %d, want %d", scale.Spec.Replicas, replicas)
	}
	if wantSelector && scale.Status.Selector == "" {
		t.Fatal("scale status selector is empty")
	}
}

func waitForPodCountAndReady(t *testing.T, tc *testctx.TestContext, count int) {
	t.Helper()
	if _, err := tc.WaitForPodCount(count); err != nil {
		t.Fatalf("Pod count did not converge to %d: %v", count, err)
	}
	if err := tc.WaitForReadyPods(count); err != nil {
		t.Fatalf("Ready pod count did not converge to %d: %v", count, err)
	}
}

func waitForStandaloneMembership(t *testing.T, ctx context.Context, tc *testctx.TestContext, pcsName, cliqueName string, replicas int32) {
	t.Helper()
	if err := wait.PollUntilContextTimeout(ctx, tc.Interval, tc.Timeout, true, func(ctx context.Context) (bool, error) {
		pgm := getPGM(t, ctx, tc, pcsName, 0)
		total := int32(0)
		for i := range pgm.Spec.Entries {
			total += pgm.Spec.Entries[i].PodCliques[cliqueName]
		}
		if total != replicas {
			return false, nil
		}
		podGangs := listPodGangs(t, ctx, tc, pcsName)
		podGroups := 0
		references := 0
		for i := range podGangs.Items {
			for _, group := range podGangs.Items[i].Spec.PodGroups {
				if group.Name == pcsName+"-0-"+cliqueName {
					podGroups++
					references += len(group.PodReferences)
				}
			}
		}
		return podGroups == 1 && references == int(replicas), nil
	}); err != nil {
		t.Fatalf("Standalone membership did not converge to %d: %v", replicas, err)
	}
}

func waitForPCSGMembership(t *testing.T, ctx context.Context, tc *testctx.TestContext, pcsName, pcsgName string, want []int32) {
	t.Helper()
	if err := podgangmap.WaitUntilVerified(ctx, podgangmap.NewVerifier(tc.Client, Logger),
		client.ObjectKey{Namespace: tc.Namespace, Name: pcsName}, 0, tc.Timeout, tc.Interval,
		podgangmap.PCSGReplicaIndicesCheckFn(pcsgName, want)); err != nil {
		t.Fatalf("PCSG membership did not converge to %v: %v", want, err)
	}
}

func getPGM(t *testing.T, ctx context.Context, tc *testctx.TestContext, pcsName string, replica int) *grovecorev1alpha1.PodGangMap {
	t.Helper()
	pgm, err := podgangmap.NewVerifier(tc.Client, Logger).Get(ctx,
		client.ObjectKey{Namespace: tc.Namespace, Name: pcsName}, replica)
	if err != nil {
		t.Fatalf("Failed to get PodGangMap for %s replica %d: %v", pcsName, replica, err)
	}
	return pgm
}

func maxPGMEpoch(t *testing.T, ctx context.Context, tc *testctx.TestContext, pcsName string) int64 {
	t.Helper()
	var maxEpoch int64
	for _, entry := range getPGM(t, ctx, tc, pcsName, 0).Spec.Entries {
		epoch, err := strconv.ParseInt(entry.Epoch, 10, 64)
		if err != nil {
			t.Fatalf("Invalid epoch %q: %v", entry.Epoch, err)
		}
		maxEpoch = max(maxEpoch, epoch)
	}
	return maxEpoch
}

func podGangNameSet(ctx context.Context, t *testing.T, tc *testctx.TestContext, pcsName string) sets.Set[string] {
	t.Helper()
	result := sets.New[string]()
	for _, podGang := range listPodGangs(t, ctx, tc, pcsName).Items {
		result[podGang.Name] = struct{}{}
	}
	return result
}

func podsForClique(t *testing.T, tc *testctx.TestContext, pclqName string) []corev1.Pod {
	t.Helper()
	pods, err := tc.ListPods()
	if err != nil {
		t.Fatalf("Failed to list pods: %v", err)
	}
	var result []corev1.Pod
	for _, pod := range pods.Items {
		if pod.Labels[apicommon.LabelPodClique] == pclqName {
			result = append(result, pod)
		}
	}
	return result
}

func podsForPCSGIndex(t *testing.T, tc *testctx.TestContext, index string) []corev1.Pod {
	t.Helper()
	pods, err := tc.ListPods()
	if err != nil {
		t.Fatalf("Failed to list pods: %v", err)
	}
	var result []corev1.Pod
	for _, pod := range pods.Items {
		if pod.Labels[apicommon.LabelPodCliqueScalingGroupReplicaIndex] == index {
			result = append(result, pod)
		}
	}
	return result
}

func podUIDLocations(pods []corev1.Pod) map[types.UID]string {
	result := make(map[types.UID]string, len(pods))
	for _, pod := range pods {
		result[pod.UID] = pod.Spec.NodeName
	}
	return result
}

func assertRetainedPodLocation(t *testing.T, pods []corev1.Pod, original map[types.UID]string) {
	t.Helper()
	if len(pods) == 0 {
		t.Fatal("no retained pods found")
	}
	for _, pod := range pods {
		node, ok := original[pod.UID]
		if !ok {
			t.Fatalf("pod %s has new UID %s", pod.Name, pod.UID)
		}
		if pod.Spec.NodeName != node {
			t.Fatalf("pod %s moved from node %s to %s", pod.Name, node, pod.Spec.NodeName)
		}
	}
}

func waitForPCSGChildrenAbsent(t *testing.T, ctx context.Context, tc *testctx.TestContext, pcsgName string, indices ...int) {
	t.Helper()
	if err := wait.PollUntilContextTimeout(ctx, tc.Interval, tc.Timeout, true, func(ctx context.Context) (bool, error) {
		for _, index := range indices {
			for _, clique := range []string{"prefill", "decode"} {
				name := fmt.Sprintf("%s-%d-%s", pcsgName, index, clique)
				err := tc.Client.Get(ctx, client.ObjectKey{Namespace: tc.Namespace, Name: name}, &grovecorev1alpha1.PodClique{})
				if err == nil {
					return false, nil
				}
				if !apierrors.IsNotFound(err) {
					return false, err
				}
			}
		}
		return true, nil
	}); err != nil {
		t.Fatalf("Scaled-in PCSG children remained: %v", err)
	}
}

func pclqUIDsForPCSGIndex(t *testing.T, ctx context.Context, tc *testctx.TestContext, pcsgName, index string) map[string]types.UID {
	t.Helper()
	result := map[string]types.UID{}
	for _, clique := range []string{"prefill", "decode"} {
		name := pcsgName + "-" + index + "-" + clique
		pclq := waitForPCLQ(t, ctx, tc, name)
		result[name] = pclq.UID
	}
	return result
}

func assertPCLQUIDs(t *testing.T, ctx context.Context, tc *testctx.TestContext, want map[string]types.UID) {
	t.Helper()
	for name, uid := range want {
		pclq := &grovecorev1alpha1.PodClique{}
		if err := tc.Client.Get(ctx, client.ObjectKey{Namespace: tc.Namespace, Name: name}, pclq); err != nil {
			t.Fatalf("Failed to get retained PodClique %s: %v", name, err)
		}
		if pclq.UID != uid {
			t.Fatalf("PodClique %s UID changed: %s -> %s", name, uid, pclq.UID)
		}
	}
}

func requireInvalidCause(t *testing.T, err error, field string) {
	t.Helper()
	if !apierrors.IsInvalid(err) {
		t.Fatalf("error = %v, want StatusReasonInvalid", err)
	}
	statusErr, ok := err.(apierrors.APIStatus)
	if !ok {
		t.Fatalf("error type %T does not implement APIStatus", err)
	}
	status := statusErr.Status()
	if status.Reason != metav1.StatusReasonInvalid || status.Details == nil {
		t.Fatalf("status = %#v, want Invalid with details", status)
	}
	for _, cause := range status.Details.Causes {
		if cause.Field == field {
			return
		}
	}
	t.Fatalf("field causes = %#v, want exact field %q", status.Details.Causes, field)
}

func assertObjectNotFound(t *testing.T, ctx context.Context, tc *testctx.TestContext, key client.ObjectKey, obj client.Object) {
	t.Helper()
	if err := tc.Client.Get(ctx, key, obj); !apierrors.IsNotFound(err) {
		t.Fatalf("Get %T %s after rejected create = %v, want NotFound", obj, key, err)
	}
}

func assertRejectedReplicaUpdate(t *testing.T, ctx context.Context, tc *testctx.TestContext, name string, replicas int32) {
	t.Helper()
	before := waitForPCLQ(t, ctx, tc, name)
	updated := before.DeepCopy()
	updated.Spec.Replicas = ptr.To[int32](replicas)
	requireInvalidCause(t, tc.Client.Update(ctx, updated), "spec")
	after := waitForPCLQ(t, ctx, tc, name)
	if ptr.Deref(after.Spec.Replicas, 1) != ptr.Deref(before.Spec.Replicas, 1) || after.ResourceVersion != before.ResourceVersion {
		t.Fatalf("rejected update changed stored object: replicas %d -> %d, resourceVersion %s -> %s", ptr.Deref(before.Spec.Replicas, 1), ptr.Deref(after.Spec.Replicas, 1), before.ResourceVersion, after.ResourceVersion)
	}
	assertRejectedScaleUpdate(t, ctx, tc, name, replicas)
}

func assertRejectedScaleUpdate(t *testing.T, ctx context.Context, tc *testctx.TestContext, name string, replicas int32) {
	t.Helper()
	before := waitForPCLQ(t, ctx, tc, name)
	scale := &autoscalingv1.Scale{
		ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: tc.Namespace},
		Spec:       autoscalingv1.ScaleSpec{Replicas: replicas},
	}
	requireInvalidCause(t, tc.Client.SubResource("scale").Update(ctx, before, client.WithSubResourceBody(scale)), "spec")
	after := waitForPCLQ(t, ctx, tc, name)
	if ptr.Deref(after.Spec.Replicas, 1) != ptr.Deref(before.Spec.Replicas, 1) {
		t.Fatalf("rejected scale changed stored replicas: %d -> %d", ptr.Deref(before.Spec.Replicas, 1), ptr.Deref(after.Spec.Replicas, 1))
	}
}

func podGangUIDsForPCSReplica(t *testing.T, ctx context.Context, tc *testctx.TestContext, pcsName, replica string) map[string]types.UID {
	t.Helper()
	result := map[string]types.UID{}
	for _, podGang := range listPodGangs(t, ctx, tc, pcsName).Items {
		if podGang.Labels[apicommon.LabelPodCliqueSetReplicaIndex] == replica {
			result[podGang.Name] = podGang.UID
		}
	}
	if len(result) == 0 {
		t.Fatalf("no PodGangs found for PCS replica %s", replica)
	}
	return result
}

func assertPodGangUIDs(t *testing.T, ctx context.Context, tc *testctx.TestContext, want map[string]types.UID) {
	t.Helper()
	verifier := podgang.NewVerifier(tc.Client, Logger)
	for name, uid := range want {
		if err := verifier.VerifyByName(ctx, tc.Namespace, name, podgang.SameUIDCheckFn(uid)); err != nil {
			t.Fatalf("Retained PodGang changed: %v", err)
		}
	}
}

func restartOperator(t *testing.T, ctx context.Context, tc *testctx.TestContext) {
	t.Helper()
	if err := pods.NewPodManager(tc.Client, Logger).RestartAndWait(ctx, setup.OperatorNamespace,
		labels.SelectorFromSet(labels.Set(setup.OperatorPodLabels)).String(), tc.Timeout, tc.Interval); err != nil {
		t.Fatalf("Operator did not recover after restart: %v", err)
	}
}
