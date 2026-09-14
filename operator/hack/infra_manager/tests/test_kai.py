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

"""KAI installation settings required by Grove's gang lifecycle."""

from unittest.mock import Mock

from infra_manager import kai
from infra_manager.config import KaiConfig


def test_install_disables_scheduler_owned_stale_gang_eviction(monkeypatch):
    helm = Mock()
    monkeypatch.setattr(kai.sh, "helm", helm)

    kai.install_kai_scheduler(KaiConfig(version="v0.16.9"))

    assert helm.call_count == 2
    args = helm.call_args.args
    assert args[0] == "install"
    setting = args.index("scheduler.args.default-staleness-grace-period=-1s")
    assert args[setting - 1] == "--set-string"
    assert args[args.index("--version") + 1] == "v0.16.9"
