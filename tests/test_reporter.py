# coding: utf-8
"""stdio JSONL 状态上报测试。"""
import io
import json
import sys
from typing import Any, cast

import pytest

from src.dynworkflow import Agent, Flow, MultiAgent, Result
from src.dynworkflow.agent import AgentClientInterface, AgentStatusSingleAgent
from src.dynworkflow.agent import _UpdateCollector  # noqa: PLC2701
from src.dynworkflow.flow import Graph
from src.dynworkflow.reporter import (
    BRACKETED_PASTE_DISABLE,
    BRACKETED_PASTE_ENABLE,
    StatusReporter,
)


def _all_events(raw: str) -> list[dict[str, Any]]:
    """_all_events() 解析（可能多块的）标记包裹 JSONL 输出为事件列表。"""
    assert (_ENABLE := "\x1b[?2004h") in raw
    events: list[dict[str, Any]] = []
    rest = raw[raw.index(_ENABLE):]
    while rest:
        assert rest.startswith(_ENABLE)
        end = rest.index("\x1b[?2004l", len(_ENABLE))
        for line in rest[len(_ENABLE):end].splitlines():
            value: Any = json.loads(line)  # 每行必须是合法 JSON
            assert isinstance(value, dict)
            events.append(value)
        rest = rest[end + len("\x1b[?2004l"):]
    return events


class FakeACP:
    """FakeACP 最小 ACP 客户端桩。"""

    def __init__(self, reply: str = "ok", fail: bool = False) -> None:
        self.reply = reply
        self.fail = fail

    def run(self, prompt: str, **kwargs: Any) -> str:
        if self.fail:
            raise RuntimeError("boom")
        return self.reply


def test_emit_wraps_jsonl_with_bracketed_paste_markers() -> None:
    buf = io.StringIO()
    reporter = StatusReporter(buf)
    reporter.emit([{"a": 1}, {"b": "中文"}])
    raw = buf.getvalue()
    assert raw.startswith(BRACKETED_PASTE_ENABLE)
    assert raw.endswith(BRACKETED_PASTE_DISABLE)
    assert _all_events(raw) == [{"a": 1}, {"b": "中文"}]


def test_disabled_reporter_writes_nothing() -> None:
    buf = io.StringIO()
    StatusReporter(buf, enabled=False).emit([{"a": 1}])
    assert buf.getvalue() == ""


def test_report_graph_serializes_sets_to_sorted_lists() -> None:
    buf = io.StringIO()
    reporter = StatusReporter(buf)
    graph = cast(Graph, {
        "nodes": {"check_step": {"name": "check_step"}},
        "edges": {"scan_docs": {"check_step", "scan_project"},
                  "check_step": set()},
        "start": {"scan_docs"},
    })
    reporter.report_graph("action", graph)
    (event,) = _all_events(buf.getvalue())
    assert event["type"] == "graph"
    assert event["workflow"] == "action"
    assert event["graph"]["edges"]["scan_docs"] == [
        "check_step", "scan_project"]
    assert event["graph"]["start"] == ["scan_docs"]
    assert isinstance(event["time"], str)


def test_flow_execute_reports_graph_then_node_and_agents(
        tmp_path: Any, monkeypatch: pytest.MonkeyPatch,
        capsys: pytest.CaptureFixture[str]) -> None:
    monkeypatch.chdir(tmp_path)  # 隔离默认缓存库
    flow = Flow("wf", agent=cast(AgentClientInterface, FakeACP()),
                cache=False, report=True)

    @flow.node("n")
    def n() -> Any:
        result = yield Agent("p")  # pyright: ignore[reportUnknownVariableType]
        return Result(cast(str, result))

    flow.execute(n())
    events = _all_events(capsys.readouterr().out)
    # graph → 节点 running → 广播源码 → 启动 agents 前 → agent 状态 → done
    assert [e["type"] for e in events] == [
        "graph", "node", "node_code", "agents_start",
        "agent", "agent", "agent", "node"]
    code_event = events[2]
    assert code_event["type"] == "node_code"
    assert code_event["nodeId"] == "n"
    assert 'yield Agent("p")' in cast(str, code_event["code"])
    assert events[0]["workflow"] == "wf"
    node_running, node_done = events[1], events[-1]
    assert (node_running["state"], node_running["cached"],
            node_running["args"]) == ("running", False, {})
    assert node_done["state"] == "done"
    start = events[3]
    assert (start["nodeId"], start["callIndex"],
            start["count"], start["prompts"]) == ("n", 1, 1, ["p"])
    states = [e["state"] for e in events if e["type"] == "agent"]
    assert states == ["waiting", "running", "success"]
    agent_event = events[4]
    assert agent_event["prompt"] == "p"
    assert agent_event["nodeId"] == "n"
    assert (agent_event["callIndex"], agent_event["agentIndex"],
            agent_event["agentCount"]) == (1, 1, 1)


def test_cached_node_reports_status_and_args(
        tmp_path: Any, monkeypatch: pytest.MonkeyPatch,
        capsys: pytest.CaptureFixture[str]) -> None:
    monkeypatch.chdir(tmp_path)

    def build_flow(acp: FakeACP) -> tuple[Flow, Any]:
        flow = Flow("wfc", agent=cast(AgentClientInterface, acp),
                    cache=True, report=True)

        @flow.node("n")
        def n(x: int) -> Any:
            # pyright: ignore[reportUnknownMemberType]
            result = yield Agent(f"p{x}")
            return Result(cast(str, result))

        return flow, n

    first_flow, first_node = build_flow(FakeACP(reply="v1"))
    first_flow.execute(first_node(x=1))
    capsys.readouterr()  # 清空首轮输出，只检查缓存命中轮

    second_flow, second_node = build_flow(FakeACP(reply="v2"))
    second_flow.execute(second_node(x=1))
    events = _all_events(capsys.readouterr().out)
    types = [e["type"] for e in events]
    # 缓存命中：不再启动任何 agent，仅推送节点状态、源码与参数
    assert "agents_start" not in types and "agent" not in types
    code_events = [e for e in events if e["type"] == "node_code"]
    assert [e["nodeId"] for e in code_events] == ["n"]
    assert 'yield Agent(f"p{x}")' in cast(str, code_events[0]["code"])
    node_events = [e for e in events if e["type"] == "node"]
    assert [(e["state"], e["cached"]) for e in node_events] == [
        ("running", True), ("done", True)]
    assert all(e["nodeId"] == "n" for e in node_events)
    assert node_events[0]["args"] == {"x": 1}
    # 结果确实来自缓存而非第二次真实执行
    assert second_flow._result["n"] == "v1"


def test_agents_start_and_indices_reported(
        tmp_path: Any, monkeypatch: pytest.MonkeyPatch,
        capsys: pytest.CaptureFixture[str]) -> None:
    monkeypatch.chdir(tmp_path)
    flow = Flow("wfi", agent=cast(AgentClientInterface, FakeACP()),
                cache=False, report=True)

    @flow.node("hub")
    def hub() -> Any:
        # pyright: ignore[reportUnknownVariableType]
        first = yield Agent("solo")
        group = yield MultiAgent(Agent("m1"), Agent("m2"), Agent("m3"))
        return Result([first, cast(list[Any], group)])

    flow.execute(hub())
    events = _all_events(capsys.readouterr().out)
    starts = [e for e in events if e["type"] == "agents_start"]
    assert [(e["nodeId"], e["callIndex"], e["count"]) for e in starts] == [
        ("hub", 1, 1), ("hub", 2, 3)]
    assert starts[0]["prompts"] == ["solo"]
    assert starts[1]["prompts"] == ["m1", "m2", "m3"]
    agent_events = [e for e in events if e["type"] == "agent"]
    solo = agent_events[0]
    assert solo["prompt"] == "solo"
    assert (solo["nodeId"], solo["callIndex"], solo["agentIndex"],
            solo["agentCount"]) == ("hub", 1, 1, 1)
    m2 = next(e for e in agent_events if e["prompt"] == "m2")
    assert (m2["nodeId"], m2["callIndex"], m2["agentIndex"],
            m2["agentCount"]) == ("hub", 2, 2, 3)


def _plain_stdout(raw: str) -> str:
    """_plain_stdout() 移除所有标记包裹的 JSONL 块，返回剩余原生输出。"""
    plain: list[str] = []
    rest = raw
    while rest:
        if rest.startswith("\x1b[?2004h"):
            end = rest.index("\x1b[?2004l", 8)
            rest = rest[end + len("\x1b[?2004l"):]
        else:
            stop = rest.find("\x1b[?2004h")
            stop = len(rest) if stop < 0 else stop
            plain.append(rest[:stop])
            rest = rest[stop:]
    return "".join(plain)


def test_node_print_is_redirected_to_node_log(
        tmp_path: Any, monkeypatch: pytest.MonkeyPatch,
        capsys: pytest.CaptureFixture[str]) -> None:
    monkeypatch.chdir(tmp_path)
    print("main-thread-line")  # 主线程 print 不受影响
    flow = Flow("wfl", agent=cast(AgentClientInterface, FakeACP()),
                cache=False, report=True)

    @flow.node("loud")
    def loud() -> Any:
        print("step", 1, sep="-")
        print("to stderr", file=sys.stderr)  # 重定向到其他流时保持原生行为
        result = yield Agent("p")  # pyright: ignore[reportUnknownVariableType]
        print("done:", result)
        return Result(cast(str, result))

    flow.execute(loud())
    captured = capsys.readouterr()
    logs = [(e["nodeId"], e["message"])
            for e in _all_events(captured.out) if e["type"] == "node_log"]
    assert logs == [("loud", "step-1"), ("loud", "done: ok")]
    # 节点线程的 print 不再进入普通 stdout；主线程与 stderr 输出保持原样
    plain = _plain_stdout(captured.out)
    assert "main-thread-line" in plain
    assert "step-1" not in plain and "done:" not in plain
    assert "to stderr" in captured.err


def test_node_print_sep_end_and_multiple_args(
        tmp_path: Any, monkeypatch: pytest.MonkeyPatch,
        capsys: pytest.CaptureFixture[str]) -> None:
    monkeypatch.chdir(tmp_path)
    flow = Flow("wfm", agent=cast(AgentClientInterface, FakeACP()),
                cache=False, report=True)

    @flow.node("p")
    def p() -> Any:
        print("a", "b", "c")             # 多参数默认空格连接
        print("x", "y", sep="-")         # 自定义 sep
        print("no nl", end="")           # end="" 与下一次输出拼接
        print("+tail")
        print("part1\npart2")            # 内嵌换行拆成两条事件
        print("tab", end="\t")
        print()                          # 仅补行尾：与上一段合成一行
        print()                          # 空行
        return None

    flow.execute(p())
    logs = [(e["nodeId"], e["message"])
            for e in _all_events(capsys.readouterr().out)
            if e["type"] == "node_log"]
    assert logs == [
        ("p", "a b c"),
        ("p", "x-y"),
        ("p", "no nl+tail"),
        ("p", "part1"),
        ("p", "part2"),
        ("p", "tab\t"),
        ("p", ""),
    ]


def test_node_print_flush_and_thread_end_residual(
        tmp_path: Any, monkeypatch: pytest.MonkeyPatch,
        capsys: pytest.CaptureFixture[str]) -> None:
    monkeypatch.chdir(tmp_path)
    flow = Flow("wff", agent=cast(AgentClientInterface, FakeACP()),
                cache=False, report=True)

    @flow.node("f")
    def f() -> Any:
        print("frag", end="", flush=True)   # flush=True：残段立即发出
        print("next")                        # 不与前面的残段拼接
        print("残留", end="")                # 线程结束时补发
        return None

    flow.execute(f())
    logs = [(e["nodeId"], e["message"])
            for e in _all_events(capsys.readouterr().out)
            if e["type"] == "node_log"]
    assert logs == [("f", "frag"), ("f", "next"), ("f", "残留")]


def test_flow_report_disabled_writes_nothing(
        tmp_path: Any, monkeypatch: pytest.MonkeyPatch,
        capsys: pytest.CaptureFixture[str]) -> None:
    monkeypatch.chdir(tmp_path)
    flow = Flow("wf", agent=cast(AgentClientInterface, FakeACP()),
                cache=False, report=False)

    @flow.node("n")
    def n() -> Any:
        result = yield Agent("p")  # pyright: ignore[reportUnknownVariableType]
        return Result(cast(str, result))

    flow.execute(n())
    assert flow._report is False
    assert capsys.readouterr().out == ""


def test_agent_failure_is_reported(
        tmp_path: Any, monkeypatch: pytest.MonkeyPatch,
        capsys: pytest.CaptureFixture[str]) -> None:
    monkeypatch.chdir(tmp_path)
    flow = Flow("wf", agent=cast(AgentClientInterface, FakeACP(fail=True)),
                cache=False, report=True)

    @flow.node("n")
    def n() -> Any:
        result = yield Agent("p")  # pyright: ignore[reportUnknownVariableType]
        return Result(cast(str, result))

    with pytest.raises(BaseExceptionGroup):  # noqa: F821 - Python 3.12 内置
        flow.execute(n())
    out = capsys.readouterr().out
    states = [e["state"] for e in _all_events(out) if e["type"] == "agent"]
    assert states == ["waiting", "running", "failure"]
    # 节点以 error 状态收尾
    node_states = [e["state"] for e in _all_events(out) if e["type"] == "node"]
    assert node_states == ["running", "error"]


def test_tool_call_updates_are_streamed_in_realtime() -> None:
    buf = io.StringIO()
    reporter = StatusReporter(buf)
    status = AgentStatusSingleAgent(prompt="p")
    reporter.bind(status)  # 立即产生 waiting 事件
    collector = _UpdateCollector(status)
    collector.handle({"params": {"update": {
        "sessionUpdate": "tool_call", "toolCallId": "t1",
        "title": "search", "rawInput": {"q": "x"}}}})
    collector.handle({"params": {"update": {
        "sessionUpdate": "tool_call_update", "toolCallId": "t1",
        "status": "completed", "rawOutput": "ok"}}})
    events = [e for e in _all_events(buf.getvalue()) if e["type"] == "agent"]
    assert len(events) == 3  # bind + 两次工具更新
    assert events[0]["state"] == "waiting"
    first_tool = events[1]["tools"][0]
    assert first_tool == {"tool_id": "t1", "name": "search",
                          "args": {"q": "x"}, "output": "", "finished": False}
    last_tool = events[-1]["tools"][0]
    assert last_tool["finished"] is True
    assert last_tool["output"] == "ok"


def test_multi_agent_reports_every_child(
        capsys: pytest.CaptureFixture[str]) -> None:
    group = MultiAgent(Agent("a"), Agent("b"),
                       acp=cast(AgentClientInterface, FakeACP()))
    statuses: list[Any] = []
    group.run(statuses, report=True)
    by_prompt: dict[str, list[str]] = {}
    for event in _all_events(capsys.readouterr().out):
        if event["type"] == "agent":
            by_prompt.setdefault(event["prompt"], []).append(event["state"])
    assert by_prompt == {"a": ["waiting", "running", "success"],
                         "b": ["waiting", "running", "success"]}


def test_env_var_enables_reporting(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("ALKAID0_WORKFLOW_REPORT", "1")
    env_flow = Flow("wf", cache=False)
    assert env_flow._report is True
    monkeypatch.delenv("ALKAID0_WORKFLOW_REPORT")
    assert Flow("wf", cache=False)._report is False
    assert Flow("wf", cache=False, report=True)._report is True
