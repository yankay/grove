#!/usr/bin/env python3
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

"""Validate KAI PodGroup recovery and subgroup admission without installing Grove."""

from volcano_only import (
    MEMBER_LABEL,
    PODGROUP,
    VolcanoLab,
    arguments,
    assert_pending,
)

GROUP_ANNOTATION = "pod-group-name"
SUBGROUP_LABEL = "kai.scheduler/subgroup-name"
QUEUE_LABEL = "kai.scheduler/queue"
SKIP_ANNOTATION = "kai.scheduler/skip-podgrouper"


def policy(members, queue, sizes=None):
    sizes = sizes or {}
    return {
        "minMember": sum(sizes.get(member, 1) for member in members),
        "queue": queue,
        "subGroups": [
            {"name": member, "minMember": sizes.get(member, 1)} for member in members
        ],
        # Grove owns termination; waking members must not evict a surviving router.
        "stalenessGracePeriod": "-1s",
    }


class KaiLab(VolcanoLab):
    """Use the same lifecycle scenarios and negative oracle for both backends."""

    api_version = "scheduling.run.ai/v2alpha2"
    policy_field = "subGroups"
    queue_resource = "queues.scheduling.run.ai"
    extra_snapshot_resources = ("bindrequests.scheduling.run.ai",)

    def expected_policy(self, members=None):
        return policy(
            self.members if members is None else members, self.args.queue, self.sizes
        )

    def make_pod(self, name, member=None, *, cpu="1", control=False):
        pod = super().make_pod(name, member, cpu=cpu, control=control)
        pod["metadata"]["annotations"] = {}
        if member:
            pod["metadata"]["annotations"] = {
                GROUP_ANNOTATION: PODGROUP,
                SKIP_ANNOTATION: "true",
            }
            pod["metadata"]["labels"].update(
                {SUBGROUP_LABEL: member, QUEUE_LABEL: self.args.queue}
            )
        return pod

    def canary_pod(self):
        pod = self.make_pod("fault-canary", cpu="25m", control=True)
        pod["spec"]["schedulerName"] = "kai-scheduler"
        pod["metadata"]["annotations"] = {
            GROUP_ANNOTATION: "fault-canary",
            SKIP_ANNOTATION: "true",
        }
        pod["metadata"]["labels"] = {QUEUE_LABEL: self.args.queue}
        return pod

    def assert_pending(self, pods, names):
        complete = assert_pending(pods, names, "kai-scheduler", GROUP_ANNOTATION)
        for pod in pods:
            metadata = pod["metadata"]
            labels = metadata.get("labels", {})
            if not labels.get(MEMBER_LABEL) or labels.get(SUBGROUP_LABEL) != labels.get(
                MEMBER_LABEL
            ):
                raise AssertionError("Pod has an incorrect KAI subgroup label")
            if labels.get(QUEUE_LABEL) != self.args.queue:
                raise AssertionError("Pod has an incorrect KAI queue label")
            if metadata.get("annotations", {}).get(SKIP_ANNOTATION) != "true":
                raise AssertionError("Pod must opt out of the KAI podgrouper")
        return complete


def main():
    args = arguments(__doc__, "kai-scheduler", "test")
    raise SystemExit(KaiLab(args).run())


if __name__ == "__main__":
    main()
