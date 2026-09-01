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

package k8s

import (
	"context"
	"errors"
	"testing"

	"github.com/stretchr/testify/require"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/controller-runtime/pkg/client/interceptor"
)

func TestCountWarningEventOccurrencesAggregation(t *testing.T) {
	for _, tt := range []struct {
		name   string
		count  int32
		series *corev1.EventSeries
		want   int32
	}{
		{name: "single event without counters", want: 1},
		{name: "legacy counter", count: 3, want: 3},
		{name: "series counter", series: &corev1.EventSeries{Count: 4}, want: 4},
		{name: "overlapping counters use series", count: 2, series: &corev1.EventSeries{Count: 5}, want: 5},
		{name: "overlapping counters use legacy", count: 6, series: &corev1.EventSeries{Count: 3}, want: 6},
	} {
		t.Run(tt.name, func(t *testing.T) {
			target := &corev1.Pod{ObjectMeta: metav1.ObjectMeta{Name: "target", Namespace: "test", UID: "target-uid"}}
			event := warningEventFor(target, "warning")
			event.Count, event.Series = tt.count, tt.series
			scheme := runtime.NewScheme()
			require.NoError(t, corev1.AddToScheme(scheme))
			cl := fake.NewClientBuilder().WithScheme(scheme).WithObjects(event).Build()
			count, err := CountWarningEventOccurrences(context.Background(), cl, target, "FailedRescale")
			require.NoError(t, err)
			require.Equal(t, tt.want, count)
		})
	}
}

func TestCountWarningEventOccurrencesFiltersAndSums(t *testing.T) {
	target := &corev1.Pod{ObjectMeta: metav1.ObjectMeta{Name: "target", Namespace: "test", UID: "target-uid"}}
	first := warningEventFor(target, "first")
	first.Count = 2
	second := warningEventFor(target, "second")
	second.Series = &corev1.EventSeries{Count: 3}
	oldTarget := warningEventFor(target, "old-target")
	oldTarget.InvolvedObject.UID = "old-uid"
	normal := warningEventFor(target, "normal")
	normal.Type = corev1.EventTypeNormal
	otherReason := warningEventFor(target, "other-reason")
	otherReason.Reason = "FailedGetScale"
	otherNamespace := warningEventFor(target, "other-namespace")
	otherNamespace.Namespace = "another"

	scheme := runtime.NewScheme()
	require.NoError(t, corev1.AddToScheme(scheme))
	cl := fake.NewClientBuilder().WithScheme(scheme).
		WithObjects(first, second, oldTarget, normal, otherReason, otherNamespace).Build()
	count, err := CountWarningEventOccurrences(context.Background(), cl, target, "FailedRescale")
	require.NoError(t, err)
	require.EqualValues(t, 5, count)

	count, err = CountWarningEventOccurrences(context.Background(), cl, target, "MissingReason")
	require.NoError(t, err)
	require.Zero(t, count)
}

func TestCountWarningEventOccurrencesErrors(t *testing.T) {
	t.Run("missing UID does not list", func(t *testing.T) {
		target := &corev1.Pod{ObjectMeta: metav1.ObjectMeta{Name: "target", Namespace: "test"}}
		count, err := CountWarningEventOccurrences(context.Background(), nil, target, "FailedRescale")
		require.EqualError(t, err, "cannot count warning events for test/target without a UID")
		require.Zero(t, count)
	})
	t.Run("list failure is preserved", func(t *testing.T) {
		target := &corev1.Pod{ObjectMeta: metav1.ObjectMeta{Name: "target", Namespace: "test", UID: "target-uid"}}
		listErr := errors.New("list failed")
		cl := fake.NewClientBuilder().WithScheme(runtime.NewScheme()).WithInterceptorFuncs(interceptor.Funcs{
			List: func(context.Context, client.WithWatch, client.ObjectList, ...client.ListOption) error {
				return listErr
			},
		}).Build()
		count, err := CountWarningEventOccurrences(context.Background(), cl, target, "FailedRescale")
		require.ErrorIs(t, err, listErr)
		require.Zero(t, count)
	})
}

func warningEventFor(target client.Object, name string) *corev1.Event {
	return &corev1.Event{
		ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: target.GetNamespace()},
		InvolvedObject: corev1.ObjectReference{
			Name: target.GetName(), Namespace: target.GetNamespace(), UID: target.GetUID(),
		},
		Type: corev1.EventTypeWarning, Reason: "FailedRescale",
	}
}
