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

import threading
import time
import unittest

from volcano_fault_proxy import Delay

TEST_WAIT_SECONDS = 2


class DelayTest(unittest.TestCase):
    def setUp(self):
        self.delay = Delay()
        self.old = group(1, "1")
        self.new = group(5, "2")
        self.delay.forwarded(self.old)
        self.arm = {
            "namespace": "test",
            "name": "wake",
            "seconds": 10,
            "spec": {"minMember": 1},
        }
        self.delay.arm(self.arm)
        self.addCleanup(self.delay.release)

    def test_holds_changed_policy_but_forwards_pods_until_release(self):
        done = threading.Event()

        def forward():
            self.delay.delay(
                {"type": "MODIFIED", "object": self.new}, "/podgroups?watch=true"
            )
            self.delay.forwarded(self.new)
            done.set()

        thread = threading.Thread(target=forward)
        thread.start()
        try:
            with self.delay.condition:
                self.assertTrue(
                    self.delay.condition.wait_for(
                        lambda: self.delay.held, timeout=TEST_WAIT_SECONDS
                    )
                )
            pod = {
                "kind": "Pod",
                "metadata": {"name": "worker", "namespace": "test", "uid": "new"},
                "spec": {},
            }
            self.delay.delay(pod, "/pods?watch=true")
            self.delay.forwarded(pod)
            state = self.delay.status()
            self.assertFalse(done.is_set())
            self.assertEqual(state["lastGroup"]["spec"]["minMember"], 1)
            self.assertEqual(state["pods"]["worker"]["uid"], "new")
            self.delay.release()
            self.assertTrue(done.wait(TEST_WAIT_SECONDS))
            self.assertEqual(self.delay.status()["lastGroup"]["spec"]["minMember"], 5)
        finally:
            self.delay.release()
            thread.join(timeout=TEST_WAIT_SECONDS)
        self.assertFalse(thread.is_alive())

    def test_expiry_marks_injection_invalid_including_list_response(self):
        self.delay.deadline = time.monotonic() - 1
        item = {key: value for key, value in self.new.items() if key != "kind"}
        self.delay.delay({"kind": "PodGroupList", "items": [item]}, "/podgroups")
        self.assertTrue(self.delay.status()["expired"])
        self.assertFalse(self.delay.status()["active"])
        self.assertEqual(len(self.delay.held), 1)

    def test_other_group_and_same_policy_are_not_held(self):
        other = group(5, "3")
        other["metadata"]["name"] = "other"
        self.delay.delay(other, "/podgroups")
        self.delay.delay(self.old, "/podgroups")
        self.assertEqual(self.delay.status()["held"], [])

    def test_arm_rejects_unobserved_policy(self):
        self.delay.release()
        self.arm["spec"] = {"minMember": 5}
        with self.assertRaisesRegex(ValueError, "not been forwarded"):
            self.delay.arm(self.arm)

    def test_status_is_an_independent_snapshot(self):
        state = self.delay.status()
        state["lastGroup"]["spec"]["minMember"] = 99
        self.assertEqual(self.delay.status()["lastGroup"]["spec"]["minMember"], 1)


def group(minimum, version):
    return {
        "kind": "PodGroup",
        "metadata": {
            "name": "wake",
            "namespace": "test",
            "uid": "group",
            "resourceVersion": version,
        },
        "spec": {"minMember": minimum},
    }


if __name__ == "__main__":
    unittest.main()
