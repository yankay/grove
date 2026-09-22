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

"""Independently audit binding order and immutable identities from saved evidence."""

import argparse
import json
from pathlib import Path

GROUP_LABEL = "grove.io/podcliquescalinggroup"
INDEX_LABEL = "grove.io/podcliquescalinggroup-replica-index"


def audit(directory):
    result = json.loads((directory / "result.json").read_text())
    summary = {
        "case": directory.name,
        "outcome": result["outcome"],
        "started": result["started"],
        "finished": result["finished"],
        "error": result.get("error"),
        "completedCycles": len(result["cycles"]),
        "windows": [
            {"name": c["name"], **c["detail"]}
            for c in result["checks"]
            if c["name"].endswith("blocked")
        ],
    }
    workload = json.loads((directory / "workload.json").read_text())
    summary["pcsSkipPodgrouper"] = (
        workload["metadata"].get("annotations", {}).get("kai.scheduler/skip-podgrouper")
    )
    if result["outcome"] != "PASS":
        return summary
    current = {}
    extra_bindings = []
    last_rv = 0
    for line in (directory / "pod-watch.jsonl").read_text().splitlines():
        event = json.loads(line)
        pod = event.get("object", {})
        metadata = pod.get("metadata", {})
        if pod.get("kind") != "Pod":
            continue
        labels = metadata.get("labels", {})
        uid = metadata["uid"]
        previous = current.get(uid, {})
        rv = int(metadata["resourceVersion"])
        if event["type"] == "MODIFIED" and rv < last_rv:
            raise AssertionError(f"{directory.name}: non-monotonic watch")
        last_rv = max(rv, last_rv)
        if event["type"] == "DELETED":
            current.pop(uid, None)
            continue
        current[uid] = pod
        if (
            labels.get(GROUP_LABEL)
            and labels.get(INDEX_LABEL) == "2"
            and pod["spec"].get("nodeName")
            and not previous.get("spec", {}).get("nodeName")
        ):
            quorum = [
                p
                for p in current.values()
                if p["metadata"].get("labels", {}).get(GROUP_LABEL)
                == labels[GROUP_LABEL]
                and p["metadata"].get("labels", {}).get(INDEX_LABEL) in ("0", "1")
                and not p["metadata"].get("deletionTimestamp")
                and p["spec"].get("nodeName")
            ]
            if len(quorum) != 4:
                raise AssertionError(
                    f"{directory.name}: extra {metadata['name']} bound "
                    f"with only {len(quorum)} live minimum Pods"
                )
            extra_bindings.append(
                {
                    "time": event["time"],
                    "pod": metadata["name"],
                    "uid": uid,
                    "liveMinimumCount": len(quorum),
                }
            )
    expected = 2 * (1 + len(result["cycles"]))
    if len(extra_bindings) != expected:
        raise AssertionError(
            f"{directory.name}: expected {expected} extra bindings, got {len(extra_bindings)}"
        )
    for cycle in result["cycles"]:
        if set(cycle["oldWorkerUIDs"]) & set(cycle["newWorkerUIDs"]):
            raise AssertionError(f"{directory.name}: worker UID survived idle")
        if len(cycle["newWorkerUIDs"]) != 6:
            raise AssertionError(f"{directory.name}: missing worker UID")
        idle = json.loads(
            (directory / f"cycle-{cycle['number']}-idle" / "pcsg.json").read_text()
        )["items"]
        if len(idle) != 1:
            raise AssertionError(f"{directory.name}: expected one idle PCSG")
        pcsg = idle[0]
        condition = next(
            (
                c
                for c in pcsg["status"]["conditions"]
                if c["type"] == "MinAvailableBreached"
            ),
            {},
        )
        if (
            pcsg["spec"]["replicas"] != 0
            or pcsg["spec"]["minAvailable"] != 2
            or condition.get("status") != "False"
            or condition.get("reason") != "Idle"
            or condition.get("observedGeneration") != pcsg["metadata"]["generation"]
            or any(
                pcsg["status"].get(field, 0) != 0
                for field in (
                    "replicas",
                    "availableReplicas",
                    "scheduledReplicas",
                    "updatedReplicas",
                )
            )
        ):
            raise AssertionError(f"{directory.name}: invalid idle status contract")
    if any(w["bound"] != 0 or w["seconds"] < 45 for w in summary["windows"]):
        raise AssertionError(f"{directory.name}: insufficient negative observation")
    summary["bindingOrderAudit"] = "PASS"
    summary["idleStatusAudit"] = "PASS"
    summary["extraBindings"] = extra_bindings
    summary["nativeUIDs"] = [
        {
            "cycle": cycle["number"],
            "before": cycle["oldNativeUIDs"],
            "after": cycle["wakeNativeUIDs"],
        }
        for cycle in result["cycles"]
    ]
    summary["nativeRecreation"] = [
        c for c in result["checks"] if c["name"].endswith("forced-native-recreation")
    ]
    return summary


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("directories", nargs="+", type=Path)
    parser.add_argument("--output", required=True, type=Path)
    args = parser.parse_args()
    cases = [audit(path) for path in args.directories]
    summary = {
        "cases": cases,
        "passedCases": sum(c["outcome"] == "PASS" for c in cases),
        "failedCases": sum(c["outcome"] == "FAIL" for c in cases),
        "completedCycles": sum(c["completedCycles"] for c in cases),
        "negativeWindows": sum(len(c["windows"]) for c in cases),
        "negativeSeconds": sum(w["seconds"] for c in cases for w in c["windows"]),
        "negativeSamples": sum(w["samples"] for c in cases for w in c["windows"]),
    }
    args.output.write_text(json.dumps(summary, indent=2) + "\n")
    print(json.dumps({k: v for k, v in summary.items() if k != "cases"}, indent=2))


if __name__ == "__main__":
    main()
