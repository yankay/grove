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

"""Disposable-cluster API proxy delaying one PodGroup, never its Pod events."""

import argparse
import copy
import http.client
import json
import os
import ssl
import subprocess
import threading
import time
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from urllib.parse import parse_qs, urlsplit

PORT = 18080
MAX_HOLD_SECONDS = 240
UPSTREAM_TIMEOUT = 360
SERVICE_ACCOUNT = Path("/var/run/secrets/kubernetes.io/serviceaccount")
HOP_HEADERS = {
    "connection",
    "keep-alive",
    "proxy-authenticate",
    "proxy-authorization",
    "te",
    "trailer",
    "transfer-encoding",
    "upgrade",
    "content-length",
}


def objects(value):
    if value.get("kind", "").endswith("List"):
        kind = value["kind"].removesuffix("List")
        return [{"kind": kind, **item} for item in value.get("items", [])]
    return [value.get("object", value)]


class Delay:
    def __init__(self):
        self.condition = threading.Condition()
        self.groups = {}
        self.target = None
        self.frozen_spec = None
        self.active = False
        self.expired = False
        self.deadline = 0
        self.held = []
        self.pods = {}
        self.bindings = []
        self.errors = []

    def status(self):
        with self.condition:
            return copy.deepcopy(
                {
                    "active": self.active,
                    "expired": self.expired,
                    "target": self.target,
                    "frozenSpec": self.frozen_spec,
                    "lastGroup": self.groups.get(self.target),
                    "groups": self.groups,
                    "held": self.held,
                    "pods": self.pods,
                    "bindings": self.bindings,
                    "errors": self.errors,
                }
            )

    def arm(self, body):
        target = f"{body['namespace']}/{body['name']}"
        seconds = body["seconds"]
        with self.condition:
            if self.active or not 0 < seconds <= MAX_HOLD_SECONDS:
                raise ValueError("Already armed or invalid hold duration")
            if self.groups.get(target, {}).get("spec") != body["spec"]:
                raise ValueError("Idle policy has not been forwarded")
            if body["spec"]["minMember"] != 1:
                raise ValueError("Expected the router-only policy")
            self.target, self.frozen_spec = target, copy.deepcopy(body["spec"])
            self.deadline = time.monotonic() + seconds
            self.active, self.expired = True, False
            self.held, self.pods, self.bindings, self.errors = [], {}, [], []
        return self.status()

    def release(self):
        with self.condition:
            self.active = False
            self.condition.notify_all()
        return self.status()

    def delay(self, value, path):
        for obj in objects(value):
            meta = obj.get("metadata", {})
            key = f"{meta.get('namespace')}/{meta.get('name')}"
            if obj.get("kind") != "PodGroup":
                continue
            with self.condition:
                if (
                    self.active
                    and key == self.target
                    and obj["spec"] != self.frozen_spec
                ):
                    self.held.append(
                        {
                            "path": path,
                            "resourceVersion": meta["resourceVersion"],
                            "spec": copy.deepcopy(obj["spec"]),
                            "time": time.time(),
                        }
                    )
                    self.condition.notify_all()
                    while self.active:
                        left = self.deadline - time.monotonic()
                        if left <= 0:
                            self.expired = True
                            self.active = False
                            self.condition.notify_all()
                            break
                        self.condition.wait(timeout=left)

    def forwarded(self, value):
        with self.condition:
            for obj in objects(value):
                meta = obj.get("metadata", {})
                key = f"{meta.get('namespace')}/{meta.get('name')}"
                if obj.get("kind") == "PodGroup":
                    self.groups[key] = {
                        "uid": meta.get("uid"),
                        "resourceVersion": meta.get("resourceVersion"),
                        "spec": copy.deepcopy(obj.get("spec")),
                        "time": time.time(),
                    }
                if (
                    obj.get("kind") == "Pod"
                    and self.target
                    and key.rsplit("/", 1)[0] == self.target.rsplit("/", 1)[0]
                ):
                    self.pods[meta["name"]] = {
                        "uid": meta["uid"],
                        "nodeName": obj.get("spec", {}).get("nodeName"),
                        "event": value.get("type"),
                        "time": time.time(),
                    }


class Handler(BaseHTTPRequestHandler):
    # HTTP/1.0 close-delimited responses preserve streaming without buffering.
    protocol_version = "HTTP/1.0"

    def log_message(self, *_args):
        pass

    def reply(self, code, value):
        body = json.dumps(value).encode()
        self.send_response(code)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def handle_request(self):
        delay = self.server.delay
        length = int(self.headers.get("Content-Length", "0"))
        body = self.rfile.read(length) if length else None
        if self.path.startswith("/__fault/"):
            try:
                action = self.path.rsplit("/", 1)[1]
                if self.command == "GET" and action == "status":
                    result = delay.status()
                elif self.command == "POST" and action == "arm":
                    result = delay.arm(json.loads(body))
                elif self.command == "POST" and action == "release":
                    result = delay.release()
                else:
                    raise ValueError("Unknown control request")
                self.reply(200, result)
            except (ValueError, KeyError, TypeError) as error:
                self.reply(400, {"error": str(error)})
            return

        upstream = http.client.HTTPSConnection(
            os.environ["KUBERNETES_SERVICE_HOST"],
            int(os.environ.get("KUBERNETES_SERVICE_PORT_HTTPS", "443")),
            context=self.server.tls,
            timeout=UPSTREAM_TIMEOUT,
        )
        started = False
        try:
            headers = {
                key: value
                for key, value in self.headers.items()
                if key.lower()
                not in HOP_HEADERS
                | {"host", "authorization", "accept", "accept-encoding"}
            }
            headers.update(
                {
                    "Authorization": "Bearer "
                    + (SERVICE_ACCOUNT / "token").read_text().strip(),
                    "Accept": "application/json",
                    "Accept-Encoding": "identity",
                }
            )
            if self.path.endswith("/binding") and delay.target:
                with delay.condition:
                    delay.bindings.append(
                        {"path": self.path, "time": time.time(), "held": delay.active}
                    )
            upstream.request(self.command, self.path, body=body, headers=headers)
            response = upstream.getresponse()
            watch = parse_qs(urlsplit(self.path).query).get("watch", ["false"])[0] in (
                "true",
                "1",
            )
            relevant = "/podgroups" in self.path or "/pods" in self.path
            data = None
            if not watch:
                data = response.read()
                if relevant and 200 <= response.status < 300 and data:
                    delay.delay(json.loads(data), self.path)
            self.send_response(response.status)
            for key, value in response.getheaders():
                if key.lower() not in HOP_HEADERS:
                    self.send_header(key, value)
            self.send_header("Connection", "close")
            self.end_headers()
            started = True
            if watch:
                while line := response.readline():
                    value = json.loads(line) if relevant else None
                    if value is not None:
                        delay.delay(value, self.path)
                    self.wfile.write(line)
                    self.wfile.flush()
                    if value is not None:
                        delay.forwarded(value)
            else:
                self.wfile.write(data)
                self.wfile.flush()
                if relevant and 200 <= response.status < 300 and data:
                    delay.forwarded(json.loads(data))
        except (BrokenPipeError, ConnectionResetError):
            pass
        except Exception as error:
            with delay.condition:
                delay.errors.append(
                    {
                        "type": type(error).__name__,
                        "path": self.path,
                        "time": time.time(),
                    }
                )
            print(
                json.dumps({"error": type(error).__name__, "path": self.path}),
                flush=True,
            )
            if not started:
                self.reply(502, {"error": type(error).__name__})
        finally:
            upstream.close()

    do_GET = handle_request
    do_POST = handle_request
    do_PUT = handle_request
    do_PATCH = handle_request
    do_DELETE = handle_request


def route_scheduler(container, scheduler):
    if scheduler == "kai-scheduler":
        container["env"] = [
            item for item in container.get("env", []) if item["name"] != "KUBECONFIG"
        ] + [{"name": "KUBECONFIG", "value": "/fault/kubeconfig"}]
    else:
        container["args"] = [arg for arg in container["args"] if arg != "2>&1"] + [
            "--kubeconfig=/fault/kubeconfig"
        ]
    container.setdefault("volumeMounts", []).append(
        {"name": "fault-proxy", "mountPath": "/fault", "readOnly": True}
    )


def install(args):
    output = Path(args.output)
    output.mkdir(parents=True, exist_ok=False)
    base = [
        "kubectl",
        "--kubeconfig",
        str(Path(args.kubeconfig).resolve()),
        "--request-timeout=30s",
    ]

    def command(*words, value=None):
        completed = subprocess.run(
            base + list(words),
            input=json.dumps(value) if value else None,
            text=True,
            capture_output=True,
            timeout=180,
            check=True,
        )
        return completed.stdout

    kai = args.scheduler == "kai-scheduler"
    namespace = "kai-scheduler" if kai else "volcano-system"
    deployment_name = "kai-scheduler-default" if kai else "volcano-scheduler"
    configmap_name = "kai-fault-proxy" if kai else "volcano-fault-proxy"
    deployment = json.loads(
        command(
            "get",
            "deployment",
            deployment_name,
            "-n",
            namespace,
            "-o",
            "json",
        )
    )
    (output / "original-deployment.json").write_text(
        json.dumps(deployment, indent=2) + "\n"
    )
    spec = deployment["spec"]["template"]["spec"]
    if deployment["spec"].get("replicas", 1) != 1 or any(
        c["name"] == "fault-proxy" for c in spec["containers"]
    ):
        raise ValueError("Requires one scheduler replica without an existing proxy")
    if len(spec["containers"]) != 1:
        raise ValueError("Requires exactly one unmodified scheduler container")
    if kai:
        # Freeze only deployment reconciliation in this disposable test cluster.
        operator = command("get", "deploy/kai-operator", "-n", namespace, "-o", "json")
        (output / "original-operator.json").write_text(operator)
        command("scale", "deploy/kai-operator", "-n", namespace, "--replicas=0")
        command(
            "wait",
            "pods",
            "-n",
            namespace,
            "-l",
            "app=kai-operator",
            "--for=delete",
            "--timeout=90s",
        )
    config = {
        "apiVersion": "v1",
        "kind": "Config",
        "clusters": [
            {"name": "proxy", "cluster": {"server": f"http://127.0.0.1:{PORT}"}}
        ],
        "contexts": [
            {"name": "proxy", "context": {"cluster": "proxy", "user": "proxy"}}
        ],
        "current-context": "proxy",
        "users": [{"name": "proxy", "user": {}}],
    }
    command(
        "create",
        "-f",
        "-",
        value={
            "apiVersion": "v1",
            "kind": "ConfigMap",
            "metadata": {"name": configmap_name, "namespace": namespace},
            "data": {
                "proxy.py": Path(__file__).read_text(),
                "kubeconfig": json.dumps(config),
            },
        },
    )
    route_scheduler(spec["containers"][0], args.scheduler)
    spec["containers"].append(
        {
            "name": "fault-proxy",
            "image": args.image,
            "imagePullPolicy": "IfNotPresent",
            "command": ["python3", "-u", "/fault/proxy.py", "--serve"],
            "volumeMounts": [
                {"name": "fault-proxy", "mountPath": "/fault", "readOnly": True}
            ],
            "resources": {
                "requests": {"cpu": "25m", "memory": "64Mi"},
                "limits": {"memory": "256Mi"},
            },
            "securityContext": {
                "runAsUser": 1000,
                "runAsNonRoot": True,
                "allowPrivilegeEscalation": False,
            },
        }
    )
    spec.setdefault("volumes", []).append(
        {"name": "fault-proxy", "configMap": {"name": configmap_name}}
    )
    patch = {
        "metadata": {"resourceVersion": deployment["metadata"]["resourceVersion"]},
        "spec": {
            "strategy": {"type": "Recreate", "rollingUpdate": None},
            "template": {"spec": spec},
        },
    }
    (output / "deployment-patch.json").write_text(json.dumps(patch, indent=2) + "\n")
    command(
        "patch",
        "deployment",
        deployment_name,
        "-n",
        namespace,
        "--type=merge",
        "-p",
        json.dumps(patch),
    )
    print(
        command(
            "rollout",
            "status",
            f"deployment/{deployment_name}",
            "-n",
            namespace,
            "--timeout=150s",
        )
    )


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    mode = parser.add_mutually_exclusive_group(required=True)
    mode.add_argument("--serve", action="store_true")
    mode.add_argument("--install", action="store_true")
    parser.add_argument("--kubeconfig")
    parser.add_argument("--image")
    parser.add_argument("--output")
    parser.add_argument(
        "--scheduler", choices=("volcano", "kai-scheduler"), default="volcano"
    )
    args = parser.parse_args()
    if args.install:
        if not all((args.kubeconfig, args.image, args.output)):
            parser.error("--install requires --kubeconfig, --image and --output")
        install(args)
    else:
        server = ThreadingHTTPServer(("127.0.0.1", PORT), Handler)
        server.delay = Delay()
        server.tls = ssl.create_default_context(cafile=str(SERVICE_ACCOUNT / "ca.crt"))
        server.serve_forever()


if __name__ == "__main__":
    main()
