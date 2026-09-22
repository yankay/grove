# Copyright 2026 The Grove Authors.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import unittest
from types import SimpleNamespace
from unittest.mock import Mock

from kai_only import (
    GROUP_ANNOTATION,
    QUEUE_LABEL,
    SKIP_ANNOTATION,
    SUBGROUP_LABEL,
    KaiLab,
    policy,
)
from volcano_fault_proxy import route_scheduler
from volcano_only import MEMBER_LABEL, WORKERS


class KaiOnlyTest(unittest.TestCase):
    def setUp(self):
        self.lab = object.__new__(KaiLab)
        self.lab.args = SimpleNamespace(
            scheduler="kai-scheduler",
            queue="test",
            image="test/workload",
        )
        self.lab.namespace = "test"
        self.lab.members = ["router", *WORKERS]
        self.lab.sizes = {}

    def test_policy_shrink_and_restore(self):
        full = self.lab.expected_policy()
        idle = self.lab.expected_policy(["router"])
        self.assertEqual(full["minMember"], 5)
        self.assertEqual(full["stalenessGracePeriod"], "-1s")
        self.assertEqual(
            idle,
            {
                "minMember": 1,
                "queue": "test",
                "stalenessGracePeriod": "-1s",
                "subGroups": [{"name": "router", "minMember": 1}],
            },
        )
        self.assertEqual(
            full["subGroups"],
            [{"name": name, "minMember": 1} for name in ["router", *WORKERS]],
        )

    def test_multi_member_policy(self):
        spec = policy(["leader", "worker"], "test", {"worker": 2})
        self.assertEqual(spec["minMember"], 3)
        self.assertEqual(
            spec["subGroups"],
            [
                {"name": "leader", "minMember": 1},
                {"name": "worker", "minMember": 2},
            ],
        )

    def test_pod_association_and_default_scheduler_probe(self):
        pod = self.lab.make_pod("leader-0", "leader-0")
        self.assertEqual(pod["spec"]["schedulerName"], "kai-scheduler")
        self.assertEqual(
            pod["metadata"]["annotations"],
            {
                GROUP_ANNOTATION: "wake",
                SKIP_ANNOTATION: "true",
            },
        )
        self.assertEqual(
            pod["metadata"]["labels"],
            {
                MEMBER_LABEL: "leader-0",
                SUBGROUP_LABEL: "leader-0",
                QUEUE_LABEL: "test",
            },
        )
        probe = self.lab.make_pod("probe")
        self.assertEqual(probe["spec"]["schedulerName"], "default-scheduler")
        self.assertEqual(probe["metadata"]["annotations"], {})
        self.assertEqual(probe["metadata"]["labels"], {})

    def test_pending_oracle_rejects_wrong_subgroup_queue_or_podgrouper(self):
        for category, field, value in (
            ("labels", SUBGROUP_LABEL, "other"),
            ("labels", QUEUE_LABEL, "other"),
            ("annotations", SKIP_ANNOTATION, "false"),
        ):
            with self.subTest(field=field):
                pod = self.lab.make_pod("leader-0", "leader-0")
                self.assertTrue(self.lab.assert_pending([pod], ["leader-0"]))
                pod["metadata"][category][field] = value
                with self.assertRaises(AssertionError):
                    self.lab.assert_pending([pod], ["leader-0"])

    def test_pending_oracle_rejects_binding_and_gate(self):
        for field, value in (
            ("nodeName", "worker"),
            ("schedulingGates", [{"name": "test/hold"}]),
        ):
            with self.subTest(field=field):
                pod = self.lab.make_pod("leader-0", "leader-0")
                pod["spec"][field] = value
                with self.assertRaises(AssertionError):
                    self.lab.assert_pending([pod], ["leader-0"])

    def test_patch_changes_full_policy_together(self):
        self.lab.native = "podgroups.scheduling.run.ai"
        spec = self.lab.expected_policy(["router"])
        self.lab.get = Mock(
            side_effect=[
                {"metadata": {"resourceVersion": "42"}},
                {"metadata": {"uid": "same"}, "spec": spec},
            ]
        )
        self.lab.write = Mock()
        self.assertEqual(self.lab.set_policy(["router"]), "same")
        self.lab.write.assert_called_once_with(
            "patch",
            {
                "metadata": {"resourceVersion": "42"},
                "spec": spec,
            },
        )

    def test_canary_is_separate_kai_gang(self):
        pod = self.lab.canary_pod()
        self.assertEqual(pod["spec"]["schedulerName"], "kai-scheduler")
        self.assertEqual(
            pod["metadata"]["annotations"][GROUP_ANNOTATION], "fault-canary"
        )
        self.assertEqual(pod["metadata"]["annotations"][SKIP_ANNOTATION], "true")
        self.assertEqual(pod["metadata"]["labels"], {QUEUE_LABEL: "test"})
        self.assertEqual(
            pod["spec"]["nodeSelector"]["validation.grove.io/role"], "control"
        )

    def test_proxy_routes_kai_without_changing_scheduler_flags(self):
        container = {
            "args": ["--test"],
            "env": [
                {"name": "KEEP", "value": "unchanged"},
                {"name": "KUBECONFIG", "value": "old"},
            ],
        }
        route_scheduler(container, "kai-scheduler")
        self.assertEqual(container["args"], ["--test"])
        self.assertEqual(
            container["env"],
            [
                {"name": "KEEP", "value": "unchanged"},
                {"name": "KUBECONFIG", "value": "/fault/kubeconfig"},
            ],
        )
        self.assertEqual(container["volumeMounts"][0]["mountPath"], "/fault")


if __name__ == "__main__":
    unittest.main()
