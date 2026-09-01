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

package podgangmap

import (
	"testing"

	grovecorev1alpha1 "github.com/ai-dynamo/grove/operator/api/core/v1alpha1"
	"github.com/stretchr/testify/require"
)

func TestPCSGReplicaIndicesCheckFn(t *testing.T) {
	tests := []struct {
		name    string
		entries []map[string][]int32
		want    []int32
		wantErr bool
	}{
		{
			name: "idle membership",
			entries: []map[string][]int32{
				{"other": {0}},
			},
		},
		{
			name: "membership across entries ignores order and other groups",
			entries: []map[string][]int32{
				{"workers": {2, 0}},
				{"workers": {1}, "other": {3}},
			},
			want: []int32{1, 2, 0},
		},
		{
			name: "duplicate ownership is rejected",
			entries: []map[string][]int32{
				{"workers": {0}},
				{"workers": {0, 1}},
			},
			want:    []int32{0, 1},
			wantErr: true,
		},
		{
			name: "missing replica is rejected",
			entries: []map[string][]int32{
				{"workers": {0}},
			},
			want:    []int32{0, 1},
			wantErr: true,
		},
		{
			name: "unexpected replica is rejected",
			entries: []map[string][]int32{
				{"workers": {0}},
			},
			wantErr: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			pgm := &grovecorev1alpha1.PodGangMap{}
			for _, indices := range tt.entries {
				pgm.Spec.Entries = append(pgm.Spec.Entries, grovecorev1alpha1.PodGangEntry{
					PCSGReplicaIndices: indices,
				})
			}
			err := PCSGReplicaIndicesCheckFn("workers", tt.want)(pgm)
			if tt.wantErr {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
			}
		})
	}
}

func TestPCSGReplicaIndicesCheckFnPreservesInputs(t *testing.T) {
	want := []int32{1, 0}
	check := PCSGReplicaIndicesCheckFn("workers", want)
	require.Equal(t, []int32{1, 0}, want)
	want[0] = 2
	pgm := &grovecorev1alpha1.PodGangMap{
		Spec: grovecorev1alpha1.PodGangMapSpec{
			Entries: []grovecorev1alpha1.PodGangEntry{{
				PCSGReplicaIndices: map[string][]int32{"workers": {1, 0}},
			}},
		},
	}
	require.NoError(t, check(pgm))
	require.Equal(t, []int32{1, 0}, pgm.Spec.Entries[0].PCSGReplicaIndices["workers"])
}
