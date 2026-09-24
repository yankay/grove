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

"""Exercise PCSG sleep/wake against real schedulers on an isolated cluster."""

import argparse
import json
import subprocess
import threading
import time
from datetime import datetime, timezone
from pathlib import Path

POLL_SECONDS = 1
WAIT_SECONDS = 180
REQUEST_SECONDS = 30
GANG_GATE = "grove.io/podgang-pending-creation"
GROUP_LABEL = "grove.io/podcliquescalinggroup"
INDEX_LABEL = "grove.io/podcliquescalinggroup-replica-index"


def now():
    return datetime.now(timezone.utc).isoformat()


def ready(pod):
    return not pod["metadata"].get("deletionTimestamp") and any(
        c["type"] == "Ready" and c["status"] == "True"
        for c in pod.get("status", {}).get("conditions", [])
    )


class Lab:
    def __init__(self, args):
        self.args = args
        self.output = Path(args.output).resolve()
        self.output.mkdir(parents=True, exist_ok=False)
        self.namespace = args.namespace
        self.native = (
            "podgroups.scheduling.run.ai"
            if args.scheduler == "kai-scheduler"
            else "podgroups.scheduling.volcano.sh"
        )
        self.base = [
            "kubectl",
            "--kubeconfig",
            str(Path(args.kubeconfig).resolve()),
            f"--request-timeout={REQUEST_SECONDS}s",
        ]
        self.events = []
        self.watch = None
        self.watch_thread = None
        self.created_namespace = False
        self.result = {
            "started": now(),
            "configuration": vars(args),
            "checks": [],
            "cycles": [],
            "outcome": "RUNNING",
        }

    def command(self, *args, data=None):
        result = subprocess.run(
            self.base + list(args),
            input=json.dumps(data) if data else None,
            text=True,
            capture_output=True,
            timeout=REQUEST_SECONDS + 10,
            check=False,
        )
        with (self.output / "commands.jsonl").open("a") as log:
            log.write(
                json.dumps(
                    {
                        "time": now(),
                        "argv": list(args),
                        "returncode": result.returncode,
                        "stderr": result.stderr,
                        "stdout": result.stdout,
                    }
                )
                + "\n"
            )
        if result.returncode:
            raise RuntimeError(f"kubectl {args}: {result.stderr.strip()}")
        return result.stdout

    def get(self, resource, name=None):
        args = ["get", resource]
        if name:
            args.append(name)
        args += ["-n", self.namespace, "-o", "json"]
        return json.loads(self.command(*args))

    def apply(self, obj):
        self.command("apply", "-f", "-", data=obj)

    def pods(self):
        return [
            p
            for p in self.get("pods")["items"]
            if p["metadata"].get("labels", {}).get("app.kubernetes.io/part-of")
            == "wake"
        ]

    def check(self, name, detail):
        self.result["checks"].append({"time": now(), "name": name, "detail": detail})
        print(f"{now()} PASS {name}: {detail}", flush=True)

    def wait(self, name, predicate, timeout=None):
        if timeout is None:
            timeout = self.args.wait_timeout
        deadline = time.monotonic() + timeout
        last = None
        while time.monotonic() < deadline:
            last = predicate()
            if last:
                return last
            threading.Event().wait(POLL_SECONDS)
        raise AssertionError(f"Timed out after {timeout}s: {name}; last={last!r}")

    def snapshot(self, name):
        directory = self.output / name
        directory.mkdir(exist_ok=True)
        for resource in (
            "pods",
            "pcs",
            "pcsg",
            "pclq",
            "pgm",
            "podgang",
            self.native,
            "events",
        ):
            try:
                value = self.get(resource)
                (directory / f"{resource}.json").write_text(
                    json.dumps(value, indent=2) + "\n"
                )
            except Exception as error:
                (directory / f"{resource}.error").write_text(str(error) + "\n")

    def start_watch(self):
        self.watch = subprocess.Popen(
            self.base[:3]
            + [
                "get",
                "pods",
                "-n",
                self.namespace,
                "--watch",
                "--output-watch-events",
                "-o",
                "json",
            ],
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
        )

        def collect():
            decoder = json.JSONDecoder()
            buffer = ""
            with (self.output / "pod-watch.jsonl").open("w") as file:
                for line in self.watch.stdout:
                    buffer += line
                    while buffer.strip():
                        buffer = buffer.lstrip()
                        try:
                            value, offset = decoder.raw_decode(buffer)
                        except json.JSONDecodeError:
                            break
                        buffer = buffer[offset:]
                        event = {"time": now(), "monotonic": time.monotonic(), **value}
                        self.events.append(event)
                        file.write(json.dumps(event) + "\n")
                        file.flush()

        self.watch_thread = threading.Thread(target=collect, daemon=True)
        self.watch_thread.start()

    def stop_watch(self):
        if self.watch:
            self.watch.terminate()
            self.watch.wait(timeout=10)
            self.watch_thread.join(timeout=10)
            (self.output / "pod-watch.stderr").write_text(self.watch.stderr.read())

    def pod_spec(self, cpu="1", control=False):
        spec = {
            "schedulerName": self.args.scheduler,
            "terminationGracePeriodSeconds": 1,
            "nodeSelector": {
                "validation.grove.io/role": "control" if control else "workload"
            },
            "containers": [
                {
                    "name": "main",
                    "image": self.args.image,
                    "imagePullPolicy": "IfNotPresent",
                    "command": ["sh", "-c", "exec sleep 86400"],
                    "resources": {"requests": {"cpu": cpu, "memory": "16Mi"}},
                    "readinessProbe": {
                        "exec": {"command": ["sh", "-c", "exit 0"]},
                        "periodSeconds": 1,
                    },
                }
            ],
        }
        if control:
            spec["tolerations"] = [
                {
                    "key": "node-role.kubernetes.io/control-plane",
                    "operator": "Exists",
                    "effect": "NoSchedule",
                }
            ]
        return spec

    def workload(self):
        cliques = [
            {
                "name": role,
                "labels": {"kai.scheduler/queue": "test"},
                "spec": {
                    "roleName": role,
                    "replicas": 1,
                    "minAvailable": 1,
                    "podSpec": self.pod_spec(),
                },
            }
            for role in ("leader", "worker")
        ]
        if self.args.survivor:
            cliques.append(
                {
                    "name": "router",
                    "labels": {"kai.scheduler/queue": "test"},
                    "spec": {
                        "roleName": "router",
                        "replicas": 1,
                        "minAvailable": 1,
                        "podSpec": self.pod_spec("25m", control=True),
                    },
                }
            )
        workload = {
            "apiVersion": "grove.io/v1alpha1",
            "kind": "PodCliqueSet",
            "metadata": {
                "name": "wake",
                "namespace": self.namespace,
                "labels": {"kai.scheduler/queue": "test"},
                "annotations": {"kai.scheduler/skip-podgrouper": "true"},
            },
            "spec": {
                "replicas": 1,
                "template": {
                    "cliques": cliques,
                    "podCliqueScalingGroups": [
                        {
                            "name": "workers",
                            "replicas": 3,
                            "minAvailable": 2,
                            "cliqueNames": ["leader", "worker"],
                        }
                    ],
                },
            },
        }
        if self.args.omit_pcs_skip_podgrouper:
            workload["metadata"].pop("annotations")
        return workload

    def auxiliary_pod(self, name, cpu):
        spec = self.pod_spec(cpu)
        spec["schedulerName"] = "default-scheduler"
        return {
            "apiVersion": "v1",
            "kind": "Pod",
            "metadata": {"name": name, "namespace": self.namespace},
            "spec": spec,
        }

    def prove_remaining_capacity(self):
        name = "remaining-capacity-probe"
        self.apply(self.auxiliary_pod(name, "1"))
        self.wait(
            "one CPU remains schedulable after persistent capacity probes",
            lambda: ready(self.get("pod", name)),
        )
        self.command(
            "delete",
            "pod",
            name,
            "-n",
            self.namespace,
            "--wait=true",
            "--timeout=30s",
        )
        self.check(
            "remaining-capacity",
            "A temporary 1-CPU Pod became Ready while persistent blockers remained",
        )

    def scale(self, replicas):
        self.command(
            "scale",
            "pcsg/wake-0-workers",
            "-n",
            self.namespace,
            f"--replicas={replicas}",
        )

    def all_ready(self):
        items = self.pods()
        return (
            items
            if len(items) == 6 + int(self.args.survivor) and all(map(ready, items))
            else None
        )

    def verify_native(self):
        gangs = {g["metadata"]["name"]: g for g in self.get("podgang")["items"]}
        natives = {g["metadata"]["name"]: g for g in self.get(self.native)["items"]}
        if set(gangs) != set(natives):
            return False
        for name, gang in gangs.items():
            expected = {g["name"]: g["minReplicas"] for g in gang["spec"]["podgroups"]}
            native = natives[name]
            if native["spec"]["minMember"] != sum(expected.values()):
                return False
            if not any(
                o["uid"] == gang["metadata"]["uid"]
                for o in native["metadata"].get("ownerReferences", [])
            ):
                return False
            if self.args.scheduler == "kai-scheduler":
                actual = {
                    g["name"]: g.get("minMember")
                    for g in native["spec"].get("subGroups", [])
                }
            else:
                actual = {
                    g["name"]: g.get("subGroupSize")
                    for g in native["spec"].get("subGroupPolicy", [])
                }
            if actual != expected:
                return False
        annotation = (
            "pod-group-name"
            if self.args.scheduler == "kai-scheduler"
            else "scheduling.k8s.io/group-name"
        )
        for pod in self.pods():
            if pod["spec"]["schedulerName"] != self.args.scheduler:
                raise AssertionError("Workload is using the wrong scheduler")
            gang_name = pod["metadata"]["labels"].get("grove.io/podgang")
            if (
                not gang_name
                or pod["metadata"].get("annotations", {}).get(annotation) != gang_name
            ):
                return False
        return True

    def verify_survivors(self, survivors):
        current = {p["metadata"]["uid"]: p for p in self.pods()}
        for uid, node in survivors.items():
            if (
                uid not in current
                or not ready(current[uid])
                or current[uid]["spec"].get("nodeName") != node
            ):
                raise AssertionError(f"Surviving router disrupted: {uid}")

    def idle(self):
        items = self.pods()
        if len(items) != int(self.args.survivor) or not all(map(ready, items)):
            return False
        if any(
            GROUP_LABEL in c["metadata"].get("labels", {})
            for c in self.get("pclq")["items"]
        ):
            return False
        pcsg = self.get("pcsg", "wake-0-workers")
        if pcsg["spec"]["replicas"] != 0 or pcsg["spec"]["minAvailable"] != 2:
            return False
        for gang in self.get("podgang")["items"]:
            if any("-workers-" in g["name"] for g in gang["spec"]["podgroups"]):
                return False
        if len(self.get(self.native)["items"]) != int(self.args.survivor):
            return False
        return True

    def blocked(self, survivors):
        self.verify_survivors(survivors)
        items = [
            p for p in self.pods() if p["metadata"].get("labels", {}).get(GROUP_LABEL)
        ]
        if len(items) != 6:
            return False
        minimum, extra = [], []
        for pod in items:
            if pod["spec"].get("nodeName"):
                raise AssertionError(
                    f"Partial gang bound with insufficient CPU: {pod['metadata']['name']}"
                )
            index = int(pod["metadata"]["labels"][INDEX_LABEL])
            gates = {g["name"] for g in pod["spec"].get("schedulingGates", [])}
            if index < 2:
                minimum.append(pod)
                if gates:
                    return False
            else:
                extra.append(pod)
                if GANG_GATE not in gates:
                    raise AssertionError(
                        f"Scale-out escaped the current wake quorum: {pod['metadata']['name']}"
                    )
        return len(minimum) == 4 and len(extra) == 2 and self.verify_native()

    def verify_wake_membership(self):
        maps = self.get("pgm")["items"]
        if len(maps) != 1:
            return False
        entries = maps[0]["spec"]["entries"]
        anchors = [e for e in entries if e["role"] == "Anchor"]
        tails = [e for e in entries if e["role"] == "ScaleOut"]
        return (
            len(anchors) == 1
            and len(tails) == 1
            and anchors[0].get("pcsgReplicaIndices", {}).get("workers") == [0, 1]
            and tails[0].get("pcsgReplicaIndices", {}).get("workers") == [2]
        )

    def observe(self, name, survivors):
        start = time.monotonic()
        samples = 0
        while time.monotonic() - start < self.args.observe:
            if self.watch.poll() is not None:
                raise AssertionError(
                    "Pod watch ended during the negative scheduling window"
                )
            if not self.blocked(survivors):
                raise AssertionError(
                    f"Blocked wake lost its valid handoff state during {name}"
                )
            for event in self.events:
                pod = event.get("object", {})
                labels = pod.get("metadata", {}).get("labels", {})
                uid = pod.get("metadata", {}).get("uid")
                if event["monotonic"] >= start and uid in survivors:
                    if (
                        event.get("type") == "DELETED"
                        or not ready(pod)
                        or pod.get("spec", {}).get("nodeName") != survivors[uid]
                    ):
                        raise AssertionError(
                            f"Pod watch observed a disrupted survivor: {uid}"
                        )
                if (
                    event["monotonic"] >= start
                    and labels.get(GROUP_LABEL)
                    and pod.get("spec", {}).get("nodeName")
                ):
                    raise AssertionError(
                        f"Pod watch observed a forbidden binding: {pod['metadata']['name']}"
                    )
            samples += 1
            threading.Event().wait(POLL_SECONDS)
        self.check(
            name, {"seconds": time.monotonic() - start, "samples": samples, "bound": 0}
        )

    def cycle(self, number):
        self.wait("all initial pods Ready", self.all_ready)
        initial = self.pods()
        old_pod_uids = {
            p["metadata"]["uid"]
            for p in initial
            if p["metadata"].get("labels", {}).get(GROUP_LABEL)
        }
        survivors = {
            p["metadata"]["uid"]: p["spec"]["nodeName"]
            for p in initial
            if not p["metadata"].get("labels", {}).get(GROUP_LABEL)
        }
        old_native = {
            p["metadata"]["name"]: p["metadata"]["uid"]
            for p in self.get(self.native)["items"]
        }
        self.snapshot(f"cycle-{number}-initial")
        self.scale(0)
        self.wait(
            "idle membership and native PodGroup removal",
            self.idle,
            self.args.idle_timeout,
        )
        self.verify_survivors(survivors)
        self.check(
            f"cycle-{number}-idle",
            {"replicas": 0, "minAvailable": 2, "survivors": survivors},
        )
        self.snapshot(f"cycle-{number}-idle")

        blocker = self.auxiliary_pod("capacity-blocker", "3")
        self.apply(blocker)
        self.wait(
            "capacity blocker Ready", lambda: ready(self.get("pod", "capacity-blocker"))
        )
        self.apply(self.auxiliary_pod("partial-capacity-probe", "1"))
        self.wait(
            "one CPU pod can still run",
            lambda: ready(self.get("pod", "partial-capacity-probe")),
        )
        self.prove_remaining_capacity()
        self.check(
            f"cycle-{number}-partial-capacity",
            "A persistent 1-CPU probe is Ready alongside the 3-CPU blocker",
        )
        self.scale(3)
        self.wait(
            "live minimum exposed to scheduler; extras still gated",
            lambda: self.blocked(survivors),
        )
        self.wait(
            "minimum replicas in anchor; extra replica in ScaleOut",
            self.verify_wake_membership,
        )
        self.snapshot(f"cycle-{number}-blocked")
        self.observe(f"cycle-{number}-gang-blocked", survivors)

        if number == 1 and self.args.disrupt:
            self.command(
                "rollout", "restart", "deployment/grove-operator", "-n", "grove-system"
            )
            self.command(
                "rollout",
                "status",
                "deployment/grove-operator",
                "-n",
                "grove-system",
                "--timeout=120s",
            )
            self.wait(
                "blocked wake after operator restart", lambda: self.blocked(survivors)
            )
            self.observe(f"cycle-{number}-restart-blocked", survivors)
            natives = self.get(self.native)["items"]
            anchor = next(p for p in natives if p["spec"]["minMember"] >= 4)
            uid = anchor["metadata"]["uid"]
            name = anchor["metadata"]["name"]
            self.command(
                "delete", self.native, name, "-n", self.namespace, "--wait=false"
            )
            self.wait(
                "native anchor PodGroup reconstructed with new UID",
                lambda: any(
                    p["metadata"]["name"] == name and p["metadata"]["uid"] != uid
                    for p in self.get(self.native)["items"]
                ),
            )
            self.wait(
                "reconstructed policy reaches scheduler",
                lambda: self.blocked(survivors),
            )
            self.observe(f"cycle-{number}-recreated-gang-blocked", survivors)
            self.check(
                f"cycle-{number}-forced-native-recreation",
                {"name": name, "oldUID": uid},
            )

        new_native = {
            p["metadata"]["name"]: p["metadata"]["uid"]
            for p in self.get(self.native)["items"]
        }
        if not self.args.survivor and any(
            uid in old_native.values() for uid in new_native.values()
        ):
            raise AssertionError("All-idle wake retained an old native PodGroup UID")
        self.command(
            "delete",
            "pod",
            "partial-capacity-probe",
            "capacity-blocker",
            "-n",
            self.namespace,
            "--wait=true",
            "--timeout=30s",
        )
        self.wait(
            "all six workers and optional router Ready after capacity release",
            self.all_ready,
        )
        self.verify_survivors(survivors)
        final = self.pods()
        if old_pod_uids.intersection(p["metadata"]["uid"] for p in final):
            raise AssertionError("Worker Pod UID survived an idle/wake cycle")
        self.wait("native policies match live PodGangs", self.verify_native)
        self.snapshot(f"cycle-{number}-ready")
        self.check(
            f"cycle-{number}-recovery", {"workerReady": 6, "survivors": len(survivors)}
        )
        self.result["cycles"].append(
            {
                "number": number,
                "oldNativeUIDs": old_native,
                "wakeNativeUIDs": new_native,
                "oldWorkerUIDs": sorted(old_pod_uids),
                "newWorkerUIDs": sorted(
                    p["metadata"]["uid"]
                    for p in final
                    if p["metadata"].get("labels", {}).get(GROUP_LABEL)
                ),
            }
        )

    def run(self):
        try:
            self.command("create", "namespace", self.namespace)
            self.created_namespace = True
            self.start_watch()
            obj = self.workload()
            (self.output / "workload.json").write_text(json.dumps(obj, indent=2) + "\n")
            self.apply(obj)
            self.wait("initial workload Ready", self.all_ready)
            self.wait("initial native policies", self.verify_native)
            self.check("initial-ready", {"pods": 6 + int(self.args.survivor)})
            for number in range(1, self.args.cycles + 1):
                self.cycle(number)
            self.result["outcome"] = "PASS"
        except (Exception, KeyboardInterrupt) as error:
            self.result["outcome"] = "FAIL"
            self.result["error"] = str(error)
            print(f"{now()} FAIL {error}", flush=True)
        finally:
            self.snapshot("final")
            self.stop_watch()
            self.result["finished"] = now()
            (self.output / "result.json").write_text(
                json.dumps(self.result, indent=2) + "\n"
            )
            if self.args.cleanup and self.created_namespace:
                self.command("delete", "namespace", self.namespace, "--wait=false")
        return 0 if self.result["outcome"] == "PASS" else 1


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--kubeconfig", required=True)
    parser.add_argument(
        "--scheduler", required=True, choices=["kai-scheduler", "volcano"]
    )
    parser.add_argument("--namespace", required=True)
    parser.add_argument("--output", required=True)
    parser.add_argument("--image", required=True)
    parser.add_argument("--cycles", type=int, default=2)
    parser.add_argument("--observe", type=int, default=45)
    parser.add_argument("--idle-timeout", type=int, default=WAIT_SECONDS)
    parser.add_argument("--wait-timeout", type=int, default=WAIT_SECONDS)
    parser.add_argument("--survivor", action="store_true")
    parser.add_argument("--disrupt", action="store_true")
    parser.add_argument("--cleanup", action="store_true")
    parser.add_argument("--omit-pcs-skip-podgrouper", action="store_true")
    args = parser.parse_args()
    if min(args.cycles, args.observe, args.idle_timeout, args.wait_timeout) < 1:
        parser.error("cycles and timeouts must be positive")
    raise SystemExit(Lab(args).run())


if __name__ == "__main__":
    main()
