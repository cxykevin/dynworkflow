import json
import pathlib
import sys
import textwrap
import time
from pathlib import Path
from typing import Any, cast

from src.dynworkflow.agent import AgentClientInterface

import pytest

from src.dynworkflow.agent import (
    Agent,
    AgentClient,
    AgentStatusEnum,
    MultiAgent,
)


FAKE_AGENT = r'''
import json
import sys

for line in sys.stdin:
    message = json.loads(line)
    method = message.get("method")
    request_id = message.get("id")
    if method == "initialize":
        print(json.dumps({"jsonrpc":"2.0", "id":request_id, "result": {
            "protocolVersion": 2, "capabilities": {"session": {}},
            "info": {"name":"fake", "version":"1"}}}), flush=True)
    elif method == "initialized":
        pass
    elif method == "session/new":
        print(json.dumps({"jsonrpc":"2.0", "id":request_id, "result": {"sessionId":"s1"}}), flush=True)
    elif method == "session/prompt":
        sid = message["params"]["sessionId"]
        print(json.dumps({"jsonrpc":"2.0", "method":"session/update", "params": {
            "sessionId":sid, "update":{"sessionUpdate":"state_update", "state":"running"}}}), flush=True)
        print(json.dumps({"jsonrpc":"2.0", "method":"session/update", "params": {
            "sessionId":sid, "update":{"sessionUpdate":"agent_message", "messageId":"m1",
            "content":[{"type":"text", "text":"hello "}]}}}), flush=True)
        print(json.dumps({"jsonrpc":"2.0", "method":"session/update", "params": {
            "sessionId":sid, "update":{"sessionUpdate":"agent_message_chunk", "messageId":"m1",
            "content":{"type":"text", "text":"world"}}}}), flush=True)
        print(json.dumps({"jsonrpc":"2.0", "method":"session/update", "params": {
            "sessionId":sid, "update":{"sessionUpdate":"tool_call_update", "toolCallId":"t1",
            "title":"search", "status":"in_progress", "rawInput":{"q":"x"}}}}), flush=True)
        print(json.dumps({"jsonrpc":"2.0", "method":"session/update", "params": {
            "sessionId":sid, "update":{"sessionUpdate":"tool_call_update", "toolCallId":"t1",
            "status":"completed", "rawOutput":"ok"}}}), flush=True)
        print(json.dumps({"jsonrpc":"2.0", "method":"session/update", "params": {
            "sessionId":sid, "update":{"sessionUpdate":"state_update", "state":"idle",
            "stopReason":"end_turn"}}}), flush=True)
        print(json.dumps({"jsonrpc":"2.0", "id":request_id, "result": {}}), flush=True)
'''


@pytest.fixture  # type: ignore[reportUntypedFunctionDecorator]
def fake_agent(tmp_path: Path) -> Path:
    script = tmp_path / "fake_agent.py"
    script.write_text(textwrap.dedent(FAKE_AGENT), encoding="utf-8")
    return script


def test_acp_v2_lifecycle_status_and_tool(fake_agent: Path, tmp_path: Path) -> None:
    acp = AgentClient(f"{sys.executable} {fake_agent}", timeout=2)
    status: list[Any] = []
    result = Agent("do it", path=tmp_path).run(status, acp)

    assert result == "hello world"
    assert len(status) == 1
    assert status[0].status is AgentStatusEnum.SUCCESS
    assert status[0].tool_calls == [{
        "tool_id": "t1", "name": "search", "args": {"q": "x"},
        "output": "ok", "finished": True,
    }]


def test_client_advertises_dyn_capability_and_hidden_session(fake_agent: Path, tmp_path: Path) -> None:
    acp = AgentClient(f"{sys.executable} {fake_agent}", timeout=2)
    acp.run("inspect", cwd=tmp_path)


def test_agent_reuse_has_fresh_status(fake_agent: Path, tmp_path: Path) -> None:
    acp = AgentClient(f"{sys.executable} {fake_agent}", timeout=2)
    agent = Agent("repeat", path=tmp_path)
    statuses: list[Any] = []
    agent.run(statuses, acp)
    agent.run(statuses, acp)
    assert len(statuses) == 2
    assert statuses[0] is not statuses[1]
    assert all(item.status is AgentStatusEnum.SUCCESS for item in statuses)


def test_multi_agent_preserves_order_and_limits_workers(tmp_path: Path) -> None:
    class FakeACP:
        active: int = 0
        peak: int = 0

        def run(self, prompt: str, **kwargs: Any) -> str:
            type(self).active += 1
            type(self).peak = max(type(self).peak, type(self).active)
            time.sleep(0.03 if prompt == "slow" else 0.01)
            type(self).active -= 1
            return prompt

    client = FakeACP()
    statuses: list[Any] = []
    group = MultiAgent(Agent("slow"), Agent("fast"), Agent("third"),
                       concurrency=2, acp=cast(AgentClientInterface, client))
    assert group.run(statuses) == ["slow", "fast", "third"]
    assert client.peak == 2
    assert [item.status for item in statuses] == [AgentStatusEnum.SUCCESS] * 3


def test_structure_output(fake_agent: Path, tmp_path: Path) -> None:
    class StructuredACP:
        def run(self, prompt: str, **kwargs: Any) -> str:
            return json.dumps({"ok": True})

    status: list[Any] = []
    value = Agent("structured", structure_output=True).run(
        status, cast(AgentClientInterface, StructuredACP()))
    assert value == {"ok": True}


def test_relative_paths_resolve_to_absolute_at_construction(
        tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """构造（启动）时就把相对路径锚定为绝对路径，后续切换目录不受影响。"""
    (tmp_path / "docs").mkdir()
    monkeypatch.chdir(tmp_path)

    agent = Agent("p", path="docs")
    assert agent.path == str(tmp_path / "docs")

    group = MultiAgent(Agent("a"), Agent("b"), path="sub")
    assert all(item.path == str(tmp_path / "sub") for item in group.agents)

    # 运行期切换工作目录后，已解析的路径保持不变。
    monkeypatch.chdir(tmp_path.parent)
    assert agent.path == str(tmp_path / "docs")
    assert pathlib.Path(agent.path).is_absolute()


def test_agent_client_anchors_existing_file_and_keeps_commands(
        tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """连接描述文件锚定为绝对路径；命令字符串与 ws URL 保持原样。"""
    monkeypatch.chdir(tmp_path)
    conn_file = tmp_path / "conn.json"
    conn_file.write_text('{"command": "true", "args": []}', encoding="utf-8")

    client = AgentClient("conn.json")
    assert client.path == str(conn_file)

    command_client = AgentClient("some-agent --flag")
    assert command_client.path == "some-agent --flag"

    ws_client = AgentClient("ws://127.0.0.1:7433/acp?k=a")
    assert ws_client.path == "ws://127.0.0.1:7433/acp?k=a"
