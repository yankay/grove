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

"""Validate Volcano PodGroup recreation and mutation without installing Grove."""

import argparse
import json
import threading
import time
from urllib.request import ProxyHandler, Request, build_opener

from run import Lab, now, ready

MEMBER_LABEL = "validation.grove.io/member"
WORKERS = ("leader-0", "worker-0", "leader-1", "worker-1")
PODGROUP = "wake"
GROUP_ANNOTATION = "scheduling.k8s.io/group-name"
POLL_SECONDS = 1
WAIT_SECONDS = 180
MULTI_WORKERS = ("leader-a", "leader-b", "leader-c", "worker-a", "worker-b")


def policy(members, queue, sizes=None):
    sizes = sizes or {}
    return {
        "minMember": sum(sizes.get(member, 1) for member in members),
        "queue": queue,
        "subGroupPolicy": [
            {
                "name": member,
                "labelSelector": {"matchLabels": {MEMBER_LABEL: member}},
                "matchLabelKeys": [MEMBER_LABEL],
                "subGroupSize": sizes.get(member, 1),
                "minSubGroups": 1,
            }
            for member in members
        ],
    }


def assert_pending(pods, names, scheduler="volcano", annotation=GROUP_ANNOTATION):
    if {p["metadata"]["name"] for p in pods} != set(names):
        return False
    for pod in pods:
        name = pod["metadata"]["name"]
        if pod["spec"].get("nodeName"):
            raise AssertionError(f"Forbidden partial gang binding: {name}")
        if pod["metadata"].get("deletionTimestamp"):
            raise AssertionError(f"Pending member is terminating: {name}")
        if pod["spec"].get("schedulingGates"):
            raise AssertionError(
                f"Scheduling-gated Pod is not scheduler evidence: {name}"
            )
        if pod["spec"].get("schedulerName") != scheduler:
            raise AssertionError(f"Wrong scheduler: {name}")
        if pod["metadata"].get("annotations", {}).get(annotation) != PODGROUP:
            raise AssertionError(f"Wrong PodGroup membership: {name}")
    return True


def assert_router_history(events, survivor):
    observed_ready = False
    for event in events:
        pod = event.get("object", {})
        if pod.get("metadata", {}).get("name") != "router":
            continue
        actual = (pod["metadata"].get("uid"), pod.get("spec", {}).get("nodeName"))
        healthy = actual == survivor and ready(pod) and event["type"] != "DELETED"
        if observed_ready and not healthy:
            raise AssertionError("Router continuity failed in full Pod-watch replay")
        observed_ready = observed_ready or healthy
    if not observed_ready:
        raise AssertionError("Pod watch never observed the router Ready")


class VolcanoLab(Lab):
    """Reuse only the existing kubectl, wait, watch, and evidence helpers."""

    api_version = "scheduling.volcano.sh/v1beta1"
    policy_field = "subGroupPolicy"
    queue_resource = "queues.scheduling.volcano.sh"
    extra_snapshot_resources = ()

    def __init__(self, args):
        super().__init__(args)
        self.members = list(WORKERS)
        self.worker_names = WORKERS
        self.sizes = {}
        if args.scenario == "multi-subgroup":
            self.members = ["leader", "worker"]
            self.worker_names = MULTI_WORKERS
            self.sizes = {"worker": 2}
        if args.mode == "inplace":
            self.members.insert(0, "router")
        self.survivor = None
        self.group_uid = None
        self.waking_uids = {}

    def expected_policy(self, members=None):
        return policy(
            self.members if members is None else members, self.args.queue, self.sizes
        )

    def snapshot(self, name):
        directory = self.output / name
        directory.mkdir(exist_ok=True)
        for resource in ("pods", self.native, "events", *self.extra_snapshot_resources):
            try:
                value = self.get(resource)
                (directory / f"{resource}.json").write_text(
                    json.dumps(value, indent=2) + "\n"
                )
            except Exception as error:
                (directory / f"{resource}.error").write_text(str(error) + "\n")

    def write(self, action, value):
        with (self.output / "inputs.jsonl").open("a") as stream:
            stream.write(
                json.dumps({"time": now(), "action": action, "body": value}) + "\n"
            )
        if action == "create":
            self.command("create", "-f", "-", data=value)
        else:
            self.command(
                "patch",
                self.native,
                PODGROUP,
                "-n",
                self.namespace,
                "--type=merge",
                "-p",
                json.dumps(value),
            )

    def set_policy(self, members, *, create=False):
        spec = self.expected_policy(members)
        if create:
            self.write(
                "create",
                {
                    "apiVersion": self.api_version,
                    "kind": "PodGroup",
                    "metadata": {"name": PODGROUP, "namespace": self.namespace},
                    "spec": spec,
                },
            )
        else:
            current = self.get(self.native, PODGROUP)
            self.write(
                "patch",
                {
                    "metadata": {
                        "resourceVersion": current["metadata"]["resourceVersion"]
                    },
                    "spec": spec,
                },
            )
        current = self.get(self.native, PODGROUP)
        for field, value in spec.items():
            if current["spec"].get(field) != value:
                raise AssertionError(
                    f"PodGroup did not retain {field}: {current['spec']}"
                )
        return current["metadata"]["uid"]

    def make_pod(self, name, member=None, *, cpu="1", control=False):
        spec = self.pod_spec(cpu=cpu, control=control)
        if member is None:
            spec["schedulerName"] = "default-scheduler"
        return {
            "apiVersion": "v1",
            "kind": "Pod",
            "metadata": {
                "name": name,
                "namespace": self.namespace,
                "labels": {MEMBER_LABEL: member} if member else {},
                "annotations": {
                    GROUP_ANNOTATION: PODGROUP,
                    "scheduling.volcano.sh/group-name": PODGROUP,
                }
                if member
                else {},
            },
            "spec": spec,
        }

    def assert_pending(self, pods, names):
        return assert_pending(pods, names)

    def canary_pod(self):
        pod = self.make_pod("fault-canary", cpu="25m", control=True)
        pod["spec"]["schedulerName"] = "volcano"
        pod["metadata"]["annotations"] = {GROUP_ANNOTATION: "fault-canary"}
        return pod

    def create_workers(self, *, missing_subgroup=False, blocked_worker=False):
        names = list(self.worker_names)
        if missing_subgroup:
            names[-1] = "decoy"
        for name in names:
            member = "leader-0" if name == "decoy" else name
            if self.args.scenario == "multi-subgroup":
                member = name.split("-")[0]
            pod = self.make_pod(name, member)
            if blocked_worker and name == "worker-b":
                pod["spec"]["nodeSelector"]["validation.grove.io/absent"] = (
                    self.namespace
                )
            self.write("create", pod)
        return names

    def workers(self):
        return [
            p
            for p in self.get("pods")["items"]
            if p["metadata"].get("labels", {}).get(MEMBER_LABEL)
            and p["metadata"]["name"] != "router"
        ]

    def verify_router(self):
        if self.survivor:
            pod = self.get("pod", "router")
            actual = (pod["metadata"]["uid"], pod["spec"].get("nodeName"))
            if actual != self.survivor or not ready(pod):
                raise AssertionError(
                    "Running router was replaced, moved, or became unready"
                )

    def all_ready(self):
        self.verify_router()
        pods = self.workers()
        return {p["metadata"]["name"] for p in pods} == set(self.worker_names) and all(
            ready(p) and p["spec"].get("nodeName") for p in pods
        )

    def delete_pods(self, names):
        self.command(
            "delete",
            "pods",
            *names,
            "-n",
            self.namespace,
            "--wait=false",
        )
        self.wait(
            "Pods deleted",
            lambda: (
                not any(
                    p["metadata"]["name"] in names for p in self.get("pods")["items"]
                )
            ),
        )

    def idle(self, phase):
        previous = {p["metadata"]["name"]: p["metadata"]["uid"] for p in self.workers()}
        if self.args.mode == "inplace":
            if self.set_policy(["router"]) != self.group_uid:
                raise AssertionError("In-place shrink replaced the PodGroup")
        self.delete_pods(list(self.worker_names))
        if self.args.mode == "recreate":
            self.command(
                "delete", self.native, PODGROUP, "-n", self.namespace, "--wait=false"
            )
            self.wait("PodGroup deleted", lambda: not self.get(self.native)["items"])
        self.verify_router()
        self.snapshot(f"{phase}-idle")
        return previous

    def wake(self, previous, *, missing_subgroup=False, blocked_worker=False):
        uid = self.set_policy(self.members, create=self.args.mode == "recreate")
        if (uid == self.group_uid) != (self.args.mode == "inplace"):
            raise AssertionError("Wrong PodGroup identity across sleep/wake")
        self.check("podgroup-identity", {"old": self.group_uid, "new": uid})
        self.group_uid = uid
        start = time.monotonic()
        names = self.create_workers(
            missing_subgroup=missing_subgroup, blocked_worker=blocked_worker
        )
        self.wait(
            "All waking members exist",
            lambda: len(self.workers()) == len(self.worker_names),
        )
        workers = self.workers()
        for pod in workers:
            if previous.get(pod["metadata"]["name"]) == pod["metadata"]["uid"]:
                raise AssertionError("Worker was not recreated")
        self.waking_uids = {
            pod["metadata"]["name"]: pod["metadata"]["uid"] for pod in workers
        }
        return names, start

    def check_watch(self, start, names):
        if self.watch.poll() is not None or not self.watch_thread.is_alive():
            raise AssertionError("Pod watch ended; negative evidence is incomplete")
        for event in self.events:
            if event.get("type") == "ERROR":
                raise AssertionError(f"Pod watch error: {event}")
            if event["monotonic"] < start:
                continue
            pod = event.get("object", {})
            name = pod.get("metadata", {}).get("name")
            if name in names and pod["metadata"].get("uid") == self.waking_uids.get(
                name
            ):
                if event["type"] == "DELETED" or pod.get("spec", {}).get("nodeName"):
                    raise AssertionError(
                        f"Pod watch caught a forbidden binding or deletion: {name}"
                    )
            if name == "router" and self.survivor:
                actual = (pod["metadata"]["uid"], pod.get("spec", {}).get("nodeName"))
                if (
                    event["type"] == "DELETED"
                    or actual != self.survivor
                    or not ready(pod)
                ):
                    raise AssertionError("Pod watch caught a disrupted router")

    def observe(self, phase, names, start, extra_check=None):
        self.snapshot(f"{phase}-blocked")
        begin = time.monotonic()
        samples = 0
        while time.monotonic() - begin < self.args.observe:
            workers = self.workers()
            if not self.assert_pending(workers, names):
                raise AssertionError("Missing waking members during observation")
            if {
                pod["metadata"]["name"]: pod["metadata"]["uid"] for pod in workers
            } != self.waking_uids:
                raise AssertionError("Waking members were replaced during observation")
            self.verify_router()
            current = self.get(self.native, PODGROUP)
            if current["metadata"]["uid"] != self.group_uid:
                raise AssertionError("PodGroup was replaced during observation")
            expected = self.expected_policy()
            if any(current["spec"].get(k) != v for k, v in expected.items()):
                raise AssertionError("Native gang policy changed unexpectedly")
            self.check_watch(start, names)
            if extra_check:
                extra_check()
            samples += 1
            threading.Event().wait(POLL_SECONDS)
        self.check_watch(start, names)
        self.check(
            phase, {"seconds": time.monotonic() - begin, "samples": samples, "bound": 0}
        )

    def capacity_cycle(self):
        previous = self.idle("capacity")
        for name, cpu in (("capacity-blocker", "3"), ("partial-capacity-probe", "1")):
            self.write("create", self.make_pod(name, cpu=cpu))
            self.wait(f"{name} Ready", lambda name=name: ready(self.get("pod", name)))
        self.prove_remaining_capacity()
        names, start = self.wake(previous, missing_subgroup=False)
        self.observe("insufficient-capacity", names, start)
        self.delete_pods(["capacity-blocker", "partial-capacity-probe"])
        self.wait("Recovered workers Ready", self.all_ready)
        self.snapshot("capacity-recovered")
        self.check("capacity-recovered", {"readyWorkers": 4})

    def subgroup_cycle(self):
        previous = self.idle("subgroup")
        self.write("create", self.make_pod("full-capacity-probe", cpu="4"))
        self.wait(
            "Four CPUs schedulable",
            lambda: ready(self.get("pod", "full-capacity-probe")),
        )
        self.delete_pods(["full-capacity-probe"])
        names, start = self.wake(previous, missing_subgroup=True)
        self.observe("missing-subgroup", names, start)
        self.delete_pods(["decoy"])
        self.write("create", self.make_pod("worker-1", "worker-1"))
        self.wait("Correct subgroup membership recovers", self.all_ready)
        self.snapshot("subgroup-recovered")
        self.check("subgroup-recovered", {"readyWorkers": 4})

    def multi_subgroup_cycle(self):
        previous = self.idle("multi-subgroup")
        self.write("create", self.make_pod("full-capacity-probe", cpu="5"))
        self.wait(
            "Five CPUs schedulable",
            lambda: ready(self.get("pod", "full-capacity-probe")),
        )
        self.delete_pods(["full-capacity-probe"])
        nodes = json.loads(self.command("get", "nodes", "-o", "json"))["items"]
        if any(
            node["metadata"].get("labels", {}).get("validation.grove.io/absent")
            == self.namespace
            for node in nodes
        ):
            raise AssertionError("Blocked worker selector unexpectedly matches a node")
        names, start = self.wake(previous, blocked_worker=True)
        self.observe("incomplete-worker-subgroup", names, start)
        old_uid = self.get("pod", "worker-b")["metadata"]["uid"]
        self.delete_pods(["worker-b"])
        self.write("create", self.make_pod("worker-b", "worker"))
        self.wait("Complete two-Pod worker subgroup recovers", self.all_ready)
        if self.get("pod", "worker-b")["metadata"]["uid"] == old_uid:
            raise AssertionError("Blocked worker was not replaced")
        self.snapshot("multi-subgroup-recovered")
        self.check("multi-subgroup-recovered", {"readyWorkers": 5})

    def fault(self, action="status", body=None):
        request = Request(
            self.args.fault_proxy.rstrip("/") + "/__fault/" + action,
            data=json.dumps(body or {}).encode() if action != "status" else None,
            headers={"Content-Type": "application/json"},
        )
        with build_opener(ProxyHandler({})).open(request, timeout=10) as response:
            value = json.load(response)
        with (self.output / "fault-control.jsonl").open("a") as stream:
            stream.write(json.dumps({"time": now(), "action": action, **value}) + "\n")
        return value

    def assert_fault_active(self):
        state = self.fault()
        if not state["active"] or state["expired"] or not state["held"]:
            raise AssertionError("PodGroup delay was not continuously active")
        if state.get("errors"):
            raise AssertionError("Proxy transport errors invalidate the delay test")
        if state["lastGroup"]["spec"] != state["frozenSpec"]:
            raise AssertionError("New policy leaked through the delay proxy")
        if any(
            state["pods"].get(name, {}).get("uid") != uid
            for name, uid in self.waking_uids.items()
        ):
            raise AssertionError("Proxy has not forwarded every waking Pod")
        return state

    def stale_policy_cycle(self):
        previous = self.idle("stale-policy")
        idle_spec = self.get(self.native, PODGROUP)["spec"]
        self.wait(
            "Proxy forwarded the idle policy",
            lambda: (
                self.fault()["groups"]
                .get(f"{self.namespace}/{PODGROUP}", {})
                .get("spec")
                == idle_spec
            ),
        )
        # Prime a separate gang before freezing the target's PodGroup watch.
        self.write(
            "create",
            {
                "apiVersion": self.api_version,
                "kind": "PodGroup",
                "metadata": {"name": "fault-canary", "namespace": self.namespace},
                "spec": {"minMember": 1, "queue": self.args.queue},
            },
        )
        self.wait(
            "Proxy forwarded the canary PodGroup",
            lambda: f"{self.namespace}/fault-canary" in self.fault()["groups"],
        )
        for name, cpu in (("capacity-blocker", "3"), ("partial-capacity-probe", "1")):
            self.write("create", self.make_pod(name, cpu=cpu))
            self.wait(f"{name} Ready", lambda name=name: ready(self.get("pod", name)))
        self.prove_remaining_capacity()
        self.fault(
            "arm",
            {
                "namespace": self.namespace,
                "name": PODGROUP,
                "seconds": self.args.observe + 90,
                "spec": idle_spec,
            },
        )
        violation = None
        try:
            names, start = self.wake(previous)
            self.wait(
                "Proxy is holding a new PodGroup policy and forwarding all Pods",
                lambda: self.fault_ready(),
            )
            state = self.assert_fault_active()
            self.check(
                "policy-delay-established",
                {
                    "lastForwardedMinMember": state["lastGroup"]["spec"]["minMember"],
                    "apiMinMember": self.get(self.native, PODGROUP)["spec"][
                        "minMember"
                    ],
                    "forwardedPodUIDs": self.waking_uids,
                    "heldResourceVersions": [
                        event["resourceVersion"] for event in state["held"]
                    ],
                },
            )
            self.write("create", self.canary_pod())
            self.wait(
                "Scheduler remains live while the policy is held",
                lambda: ready(self.get("pod", "fault-canary")),
            )
            self.check("scheduler-live-during-delay", {"canaryReady": True})
            self.observe(
                "stale-policy", names, start, extra_check=self.assert_fault_active
            )
        except AssertionError as error:
            violation = error
            self.snapshot("stale-policy-violation")
            self.result["stalePolicyViolation"] = str(error)
        finally:
            self.fault()
            self.fault("release")
        self.wait(
            "Proxy forwarded the restored policy",
            lambda: (
                self.fault()["groups"]
                .get(f"{self.namespace}/{PODGROUP}", {})
                .get("spec")
                == self.get(self.native, PODGROUP)["spec"]
            ),
        )
        self.delete_pods(["capacity-blocker", "partial-capacity-probe"])
        self.wait("Workers recover after delay and capacity release", self.all_ready)
        self.snapshot("stale-policy-recovered")
        self.check("stale-policy-recovered", {"readyWorkers": 4})
        if violation:
            raise violation

    def fault_ready(self):
        state = self.fault()
        return (
            state["active"]
            and not state["expired"]
            and state["held"]
            and all(
                state["pods"].get(name, {}).get("uid") == uid
                for name, uid in self.waking_uids.items()
            )
        )

    def preflight(self):
        crd = json.loads(self.command("get", "crd", self.native, "-o", "json"))
        supported = any(
            version["name"] == self.api_version.rsplit("/", 1)[1]
            and version.get("served")
            and self.policy_field
            in version.get("schema", {})
            .get("openAPIV3Schema", {})
            .get("properties", {})
            .get("spec", {})
            .get("properties", {})
            for version in crd["spec"]["versions"]
        )
        if not supported:
            raise AssertionError(f"Scheduler CRD must expose spec.{self.policy_field}")
        for role in ("control", "workload"):
            nodes = json.loads(
                self.command(
                    "get",
                    "nodes",
                    "-l",
                    f"validation.grove.io/role={role}",
                    "-o",
                    "json",
                )
            )["items"]
            if len(nodes) != 1:
                raise AssertionError(f"Expected exactly one {role} node")
            if role == "workload" and nodes[0]["status"]["allocatable"]["cpu"] not in (
                "7",
                "7000m",
            ):
                raise AssertionError(
                    "Workload node must have exactly 7 allocatable CPUs"
                )
        for filename, command in {
            "version": ("version", "-o", "json"),
            "nodes": ("get", "nodes", "-o", "json"),
            "deployments": ("get", "deployments", "-A", "-o", "json"),
            "crds": ("get", "crds", "-o", "json"),
            "queue": (
                "get",
                self.queue_resource,
                self.args.queue,
                "-o",
                "json",
            ),
        }.items():
            (self.output / f"{filename}.json").write_text(self.command(*command))

    def run(self):
        try:
            self.preflight()
            self.command("create", "namespace", self.namespace)
            self.created_namespace = True
            self.start_watch()
            self.group_uid = self.set_policy(self.members, create=True)
            if self.args.mode == "inplace":
                self.write(
                    "create", self.make_pod("router", "router", cpu="25m", control=True)
                )
            self.create_workers()
            self.wait("Initial gang Ready", self.all_ready)
            if self.args.mode == "inplace":
                self.wait("Router Ready", lambda: ready(self.get("pod", "router")))
                router = self.get("pod", "router")
                self.survivor = (router["metadata"]["uid"], router["spec"]["nodeName"])
            self.snapshot("initial")
            if self.args.scenario == "multi-subgroup":
                self.multi_subgroup_cycle()
            elif self.args.scenario == "stale-policy":
                self.stale_policy_cycle()
            else:
                self.capacity_cycle()
                self.subgroup_cycle()
            self.result["outcome"] = "PASS"
        except (Exception, KeyboardInterrupt) as error:
            self.result.update(outcome="FAIL", error=str(error))
            print(f"{now()} FAIL {error}", flush=True)
        finally:
            if self.created_namespace:
                self.snapshot("final")
            try:
                self.stop_watch()
                if self.survivor:
                    assert_router_history(self.events, self.survivor)
                    self.check(
                        "router-continuity",
                        {
                            "uid": self.survivor[0],
                            "node": self.survivor[1],
                        },
                    )
            except Exception as error:
                self.result.update(outcome="FAIL", watchError=str(error))
            if self.args.cleanup and self.created_namespace:
                try:
                    self.command("delete", "namespace", self.namespace, "--wait=false")
                except Exception as error:
                    self.result.update(outcome="FAIL", cleanupError=str(error))
            self.result["finished"] = now()
            (self.output / "result.json").write_text(
                json.dumps(self.result, indent=2) + "\n"
            )
        print(f"{self.result['outcome']}: {self.output / 'result.json'}", flush=True)
        return 0 if self.result["outcome"] == "PASS" else 1


def arguments(description=__doc__, scheduler="volcano", queue="default"):
    parser = argparse.ArgumentParser(description=description)
    parser.add_argument("--kubeconfig", required=True)
    parser.add_argument("--namespace", required=True)
    parser.add_argument("--output", required=True)
    parser.add_argument("--image", required=True)
    parser.add_argument("--mode", required=True, choices=("recreate", "inplace"))
    parser.add_argument(
        "--scenario",
        choices=("singleton", "multi-subgroup", "stale-policy"),
        default="singleton",
    )
    parser.add_argument("--fault-proxy")
    parser.add_argument("--queue", default=queue)
    parser.add_argument("--observe", type=int, default=45)
    parser.add_argument("--wait-timeout", type=int, default=WAIT_SECONDS)
    parser.add_argument("--cleanup", action="store_true")
    args = parser.parse_args()
    if min(args.observe, args.wait_timeout) < 1:
        parser.error("Timeouts must be positive")
    if args.scenario == "stale-policy" and (
        args.mode != "inplace" or not args.fault_proxy
    ):
        parser.error("stale-policy requires --mode inplace and --fault-proxy")
    args.scheduler = scheduler
    return args


def main():
    args = arguments()
    raise SystemExit(VolcanoLab(args).run())


if __name__ == "__main__":
    main()
