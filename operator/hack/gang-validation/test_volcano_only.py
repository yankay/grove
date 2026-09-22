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
from unittest.mock import Mock

from volcano_only import (
    GROUP_ANNOTATION,
    MEMBER_LABEL,
    PODGROUP,
    WORKERS,
    VolcanoLab,
    assert_pending,
    assert_router_history,
    policy,
)


class VolcanoOnlyTest(unittest.TestCase):
    def test_pending_members(self):
        self.assertTrue(assert_pending(pending_pods(), WORKERS))

    def test_missing_member_is_not_evidence(self):
        self.assertFalse(assert_pending(pending_pods()[:-1], WORKERS))

    def test_bound_member_fails(self):
        pods = pending_pods()
        pods[0]["spec"]["nodeName"] = "node"
        with self.assertRaisesRegex(AssertionError, "partial gang"):
            assert_pending(pods, WORKERS)

    def test_gated_member_fails(self):
        pods = pending_pods()
        pods[0]["spec"]["schedulingGates"] = [{"name": "test/gate"}]
        with self.assertRaisesRegex(AssertionError, "Scheduling-gated"):
            assert_pending(pods, WORKERS)

    def test_wrong_scheduler_fails(self):
        pods = pending_pods()
        pods[0]["spec"]["schedulerName"] = "default-scheduler"
        with self.assertRaisesRegex(AssertionError, "Wrong scheduler"):
            assert_pending(pods, WORKERS)

    def test_unassociated_member_fails(self):
        pods = pending_pods()
        pods[0]["metadata"]["annotations"] = {}
        with self.assertRaisesRegex(AssertionError, "Wrong PodGroup"):
            assert_pending(pods, WORKERS)

    def test_shrink_and_restore_whole_policy(self):
        full = policy(["router", *WORKERS], "default")
        idle = policy(["router"], "default")
        self.assertEqual(full["minMember"], 5)
        self.assertEqual(len(full["subGroupPolicy"]), 5)
        self.assertEqual(idle["minMember"], 1)
        self.assertEqual(idle["subGroupPolicy"], [full["subGroupPolicy"][0]])
        for item in full["subGroupPolicy"]:
            self.assertEqual(
                item["labelSelector"]["matchLabels"], {MEMBER_LABEL: item["name"]}
            )
            self.assertEqual(item["subGroupSize"], 1)
            self.assertEqual(item["minSubGroups"], 1)

    def test_policy_update_is_one_versioned_patch(self):
        lab = object.__new__(VolcanoLab)
        lab.args = Mock(queue="default")
        lab.sizes = {}
        lab.native = "podgroups.scheduling.volcano.sh"
        before = {"metadata": {"resourceVersion": "42"}}
        after = {"metadata": {"uid": "same"}, "spec": policy(["router"], "default")}
        lab.get = Mock(side_effect=[before, after])
        lab.write = Mock()
        self.assertEqual(lab.set_policy(["router"]), "same")
        lab.write.assert_called_once_with(
            "patch",
            {
                "metadata": {"resourceVersion": "42"},
                "spec": policy(["router"], "default"),
            },
        )

    def test_total_count_does_not_replace_missing_subgroup(self):
        lab = object.__new__(VolcanoLab)
        lab.args = Mock(scenario="singleton")
        lab.worker_names = WORKERS
        lab.make_pod = Mock(side_effect=lambda name, member: (name, member))
        lab.write = Mock()
        names = lab.create_workers(missing_subgroup=True)
        self.assertEqual(len(names), 4)
        self.assertNotIn("worker-1", names)
        lab.make_pod.assert_any_call("decoy", "leader-0")

    def test_two_member_worker_subgroup(self):
        spec = policy(["leader", "worker"], "default", {"worker": 2})
        self.assertEqual(spec["minMember"], 3)
        leader, worker = spec["subGroupPolicy"]
        self.assertEqual(leader["subGroupSize"], 1)
        self.assertEqual(worker["subGroupSize"], 2)
        self.assertEqual(worker["minSubGroups"], 1)
        self.assertEqual(
            worker["labelSelector"]["matchLabels"], {MEMBER_LABEL: "worker"}
        )
        self.assertEqual(worker["matchLabelKeys"], [MEMBER_LABEL])

    def test_fault_oracle_requires_held_policy_and_forwarded_current_pods(self):
        lab = object.__new__(VolcanoLab)
        lab.waking_uids = {"leader-0": "current"}
        old = policy(["router"], "default")
        state = {
            "active": True,
            "expired": False,
            "held": [{"resourceVersion": "2"}],
            "lastGroup": {"spec": old},
            "frozenSpec": old,
            "pods": {"leader-0": {"uid": "current"}},
        }
        lab.fault = Mock(return_value=state)
        self.assertEqual(lab.assert_fault_active(), state)
        state["pods"]["leader-0"]["uid"] = "previous"
        with self.assertRaisesRegex(AssertionError, "every waking Pod"):
            lab.assert_fault_active()
        state["pods"]["leader-0"]["uid"] = "current"
        state["lastGroup"] = {"spec": policy(["router", *WORKERS], "default")}
        with self.assertRaisesRegex(AssertionError, "leaked"):
            lab.assert_fault_active()

    def test_expired_fault_never_passes(self):
        lab = object.__new__(VolcanoLab)
        lab.fault = Mock(return_value={"active": True, "expired": True, "held": [1]})
        with self.assertRaisesRegex(AssertionError, "continuously active"):
            lab.assert_fault_active()

    def test_terminating_member_fails(self):
        pods = pending_pods()
        pods[0]["metadata"]["deletionTimestamp"] = "2026-09-22T00:00:00Z"
        with self.assertRaisesRegex(AssertionError, "terminating"):
            assert_pending(pods, WORKERS)

    def test_watch_ignores_old_uid_but_rejects_new_binding(self):
        lab = object.__new__(VolcanoLab)
        lab.watch = Mock()
        lab.watch.poll.return_value = None
        lab.watch_thread = Mock()
        lab.watch_thread.is_alive.return_value = True
        lab.survivor = None
        lab.waking_uids = {"leader-0": "new"}
        event = {
            "type": "MODIFIED",
            "monotonic": 2,
            "object": {
                "metadata": {"name": "leader-0", "uid": "old"},
                "spec": {"nodeName": "worker"},
            },
        }
        lab.events = [event]
        lab.check_watch(1, ["leader-0"])
        event["object"]["metadata"]["uid"] = "new"
        with self.assertRaisesRegex(AssertionError, "forbidden binding"):
            lab.check_watch(1, ["leader-0"])

    def test_router_replay_catches_transient_readiness_loss(self):
        events = [
            router_event("False"),
            router_event("True"),
            router_event("False"),
            router_event("True"),
        ]
        with self.assertRaisesRegex(AssertionError, "continuity"):
            assert_router_history(events, ("router-uid", "control"))

    def test_router_replay_requires_positive_evidence(self):
        with self.assertRaisesRegex(AssertionError, "never observed"):
            assert_router_history([], ("router-uid", "control"))
        assert_router_history(
            [router_event("False"), router_event("True")], ("router-uid", "control")
        )


def pending_pods():
    return [
        {
            "metadata": {
                "name": name,
                "annotations": {GROUP_ANNOTATION: PODGROUP},
            },
            "spec": {"schedulerName": "volcano"},
        }
        for name in WORKERS
    ]


def router_event(status):
    return {
        "type": "MODIFIED",
        "object": {
            "metadata": {"name": "router", "uid": "router-uid"},
            "spec": {"nodeName": "control"},
            "status": {"conditions": [{"type": "Ready", "status": status}]},
        },
    }


if __name__ == "__main__":
    unittest.main()
