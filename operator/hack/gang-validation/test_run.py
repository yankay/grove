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

"""Check that the negative scheduling oracle cannot pass on resource existence."""

import unittest
from unittest.mock import Mock

from run import GANG_GATE, GROUP_LABEL, INDEX_LABEL, Lab, ready


def pod(index, role, *, bound=False, gated=False):
    return {
        "metadata": {
            "name": f"worker-{index}-{role}",
            "uid": f"uid-{index}-{role}",
            "labels": {GROUP_LABEL: "wake-0-workers", INDEX_LABEL: str(index)},
        },
        "spec": {
            **({"nodeName": "worker"} if bound else {}),
            **({"schedulingGates": [{"name": GANG_GATE}]} if gated else {}),
        },
    }


class SchedulingOracleTest(unittest.TestCase):
    def setUp(self):
        self.lab = object.__new__(Lab)
        self.lab.verify_survivors = Mock()
        self.lab.verify_native = Mock(return_value=True)
        self.items = [
            pod(index, role, gated=index == 2)
            for index in range(3)
            for role in ("leader", "worker")
        ]
        self.lab.pods = lambda: self.items

    def test_valid_blocked_gang(self):
        self.assertTrue(self.lab.blocked({}))

    def test_grove_gated_minimum_is_not_scheduler_evidence(self):
        self.items[0]["spec"]["schedulingGates"] = [{"name": GANG_GATE}]
        self.assertFalse(self.lab.blocked({}))

    def test_partial_binding_fails(self):
        self.items[0]["spec"]["nodeName"] = "worker"
        with self.assertRaisesRegex(AssertionError, "Partial gang bound"):
            self.lab.blocked({})

    def test_ungated_extra_fails(self):
        self.items[-1]["spec"].pop("schedulingGates")
        with self.assertRaisesRegex(AssertionError, "Scale-out escaped"):
            self.lab.blocked({})

    def test_missing_pod_does_not_pass(self):
        self.items.pop()
        self.assertFalse(self.lab.blocked({}))

    def test_stale_policy_does_not_pass(self):
        self.lab.verify_native.return_value = False
        self.assertFalse(self.lab.blocked({}))

    def test_ready_requires_positive_condition(self):
        value = pod(0, "leader", bound=True)
        self.assertFalse(ready(value))
        value["status"] = {"conditions": [{"type": "Ready", "status": "True"}]}
        self.assertTrue(ready(value))
        value["metadata"]["deletionTimestamp"] = "2026-09-22T00:00:00Z"
        self.assertFalse(ready(value))

    def test_remaining_capacity_is_proved_after_persistent_probes(self):
        self.lab.namespace = "test"
        self.lab.apply = Mock()
        self.lab.auxiliary_pod = Mock(return_value={"kind": "Pod"})
        self.lab.get = Mock(
            return_value={
                "metadata": {},
                "status": {"conditions": [{"type": "Ready", "status": "True"}]},
            }
        )
        self.lab.wait = Mock(side_effect=lambda _name, predicate: predicate())
        self.lab.command = Mock()
        self.lab.check = Mock()

        self.lab.prove_remaining_capacity()

        self.lab.auxiliary_pod.assert_called_once_with("remaining-capacity-probe", "1")
        self.lab.apply.assert_called_once_with({"kind": "Pod"})
        self.lab.command.assert_called_once_with(
            "delete",
            "pod",
            "remaining-capacity-probe",
            "-n",
            "test",
            "--wait=true",
            "--timeout=30s",
        )


if __name__ == "__main__":
    unittest.main()
