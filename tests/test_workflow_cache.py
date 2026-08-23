"""工作流 SQLite 缓存测试。

覆盖：Agent 结果缓存、MultiAgent 逐 agent 缓存、
ALKAID0_WORKFLOW_SESSION_ID 会话作用域、节点缓存、
不可序列化参数跳过缓存、未结束会话（session id + ACP load 续跑）、
不支持 load 的客户端不写会话条目、损坏数据库降级。
"""

import sqlite3
from collections import Counter
from typing import Any

import pytest

from src.dynworkflow import Agent, Flow, MultiAgent, Result


class CountingACP:
    """记录每次真实调用的假 ACP 客户端。"""

    def __init__(self) -> None:
        self.calls: Counter[str] = Counter()

    def run(self, prompt: str, **kwargs: Any) -> str:
        self.calls[prompt] += 1
        return f"out:{prompt}"


class FlakyResumeACP:
    """首轮建立会话后失败；支持 ``run_resumed`` 按需续跑。"""

    def __init__(self) -> None:
        self.real_runs = 0
        self.resumed_ids: list[str] = []

    def run(self, prompt: str, *, model: str = "", cwd: Any = ".",
            status: Any = None) -> str:
        self.real_runs += 1
        if status is not None:
            status.session_id = "sess-123"
        raise RuntimeError("interrupted")

    def run_resumed(self, prompt: str, session_id: str, *, model: str = "",
                    cwd: Any = ".", status: Any = None) -> str:
        self.resumed_ids.append(session_id)
        return "continued"


class FlakyPlainACP:
    """失败但没有任何 load 能力的客户端。"""

    def __init__(self) -> None:
        self.real_runs = 0

    def run(self, prompt: str, **kwargs: Any) -> str:
        self.real_runs += 1
        raise RuntimeError("boom")


def _make_solo_flow(name: str, acp: Any) -> tuple[Flow, Any, object]:
    """构造单 Agent 节点工作流；marker 参数不可序列化以隔离节点级缓存。"""
    flow = Flow(name, agent=acp)
    token: object = object()

    @flow.node("solo")
    def solo(marker: object) -> Any:
        result = yield Agent("hello")
        return Result(result)

    return flow, solo, token


def _agent_rows(db: Any) -> list[tuple[Any, ...]]:
    with sqlite3.connect(str(db)) as conn:
        return list(conn.execute("SELECT kind, value FROM agent_cache"))


def test_agent_result_cached_across_executions(
        tmp_path: Any, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.chdir(tmp_path)
    acp = CountingACP()
    flow, solo, token = _make_solo_flow("wf-agent", acp)

    flow.execute(solo(marker=token))
    assert flow._result["solo"] == "out:hello"
    assert acp.calls["hello"] == 1

    # 第二次执行：Agent 缓存命中，不再真实调用
    flow.execute(solo(marker=token))
    assert acp.calls["hello"] == 1
    assert flow._result["solo"] == "out:hello"
    assert (tmp_path / ".alkaid0" / "workflow.db").is_file()
    assert _agent_rows(tmp_path / ".alkaid0" / "workflow.db") == [
        ("result", '"out:hello"')]


def test_multi_agent_cached_per_agent(
        tmp_path: Any, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.chdir(tmp_path)
    acp = CountingACP()
    flow = Flow("wf-multi", agent=acp)
    token: object = object()
    holder: dict[str, Any] = {}

    @flow.node("fan")
    def fan(marker: object) -> Any:
        results = yield holder["group"]
        return Result(results)

    holder["group"] = MultiAgent(
        Agent("a"), Agent("b"), concurrency=2, acp=acp)
    flow.execute(fan(marker=token))
    assert flow._result["fan"] == ["out:a", "out:b"]
    assert acp.calls == Counter({"a": 1, "b": 1})

    # 相同 prompts：两个 agent 全部命中
    flow.execute(fan(marker=token))
    assert acp.calls == Counter({"a": 1, "b": 1})

    # 仅替换其中一个 prompt：只补跑新增的 agent
    holder["group"] = MultiAgent(
        Agent("a"), Agent("c"), concurrency=2, acp=acp)
    flow.execute(fan(marker=token))
    assert acp.calls == Counter({"a": 1, "b": 1, "c": 1})
    assert flow._result["fan"] == ["out:a", "out:c"]


def test_env_session_scope_partitions_cache(
        tmp_path: Any, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.chdir(tmp_path)
    acp = CountingACP()
    flow, solo, token = _make_solo_flow("wf-scope", acp)

    monkeypatch.setenv("ALKAID0_WORKFLOW_SESSION_ID", "s1")
    flow.execute(solo(marker=token))
    assert acp.calls["hello"] == 1

    # 换作用域 → 缓存未命中，真实执行
    monkeypatch.setenv("ALKAID0_WORKFLOW_SESSION_ID", "s2")
    flow.execute(solo(marker=token))
    assert acp.calls["hello"] == 2

    # 回到 s1 → 命中旧作用域缓存
    monkeypatch.setenv("ALKAID0_WORKFLOW_SESSION_ID", "s1")
    flow.execute(solo(marker=token))
    assert acp.calls["hello"] == 2


def test_node_cached_by_serializable_args(
        tmp_path: Any, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.chdir(tmp_path)
    flow = Flow("wf-node")
    counter: Counter[int] = Counter()

    @flow.node("double")
    def double(x: int) -> Any:
        counter[x] += 1
        return Result(x * 2)

    flow.execute(double(x=3))
    assert flow._result["double"] == 6
    assert counter[3] == 1

    flow.execute(double(x=3))
    assert counter[3] == 1  # 同参命中节点缓存

    flow.execute(double(x=4))
    assert counter[4] == 1  # 异参未命中
    assert flow._result["double"] == 8


def test_unserializable_arg_disables_node_cache(
        tmp_path: Any, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.chdir(tmp_path)
    flow = Flow("wf-opaque")
    counter: Counter[str] = Counter()

    @flow.node("opaque")
    def opaque(x: object) -> Any:
        counter["run"] += 1
        return Result("ok")

    flow.execute(opaque(x=object()))
    flow.execute(opaque(x=object()))
    # 参数无法序列化 → 完全不缓存，每次都真实执行
    assert counter["run"] == 2


def test_unfinished_session_resumed_via_acp_load(
        tmp_path: Any, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.chdir(tmp_path)
    acp = FlakyResumeACP()
    flow, solo, token = _make_solo_flow("wf-resume", acp)

    # 首轮：运行失败，仅缓存 session id
    with pytest.raises(ExceptionGroup):
        flow.execute(solo(marker=token))
    assert acp.real_runs == 1
    db = tmp_path / ".alkaid0" / "workflow.db"
    assert _agent_rows(db) == [("session", '"sess-123"')]

    # 第二轮（新 Flow 实例，等价于重跑进程）：命中 session 条目，
    # 按需 ACP load 续跑成功
    flow2, solo2, token2 = _make_solo_flow("wf-resume", acp)
    flow2.execute(solo2(marker=token2))
    assert acp.real_runs == 1
    assert acp.resumed_ids == ["sess-123"]
    assert flow2._result["solo"] == "continued"
    # 条目升级为正常结果缓存
    assert _agent_rows(db) == [("result", '"continued"')]


def test_no_session_cache_without_load_support(
        tmp_path: Any, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.chdir(tmp_path)
    acp = FlakyPlainACP()
    flow, solo, token = _make_solo_flow("wf-noload", acp)

    with pytest.raises(ExceptionGroup):
        flow.execute(solo(marker=token))
    assert acp.real_runs == 1

    db = tmp_path / ".alkaid0" / "workflow.db"
    assert _agent_rows(db) == []  # 不支持 load → 不对未结束会话做缓存

    flow2, solo2, token2 = _make_solo_flow("wf-noload", acp)
    with pytest.raises(ExceptionGroup):
        flow2.execute(solo2(marker=token2))
    assert acp.real_runs == 2


def test_corrupt_db_degrades_gracefully(
        tmp_path: Any, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.chdir(tmp_path)
    cache_dir = tmp_path / ".alkaid0"
    cache_dir.mkdir()
    (cache_dir / "workflow.db").write_bytes(b"not a sqlite database")

    acp = CountingACP()
    flow, solo, token = _make_solo_flow("wf-corrupt", acp)
    # 缓存层故障不影响工作流执行
    flow.execute(solo(marker=token))
    flow.execute(solo(marker=token))
    assert acp.calls["hello"] == 2  # 无缓存可用 → 两次都真实执行
    assert flow._result["solo"] == "out:hello"


def test_execute_chain_nodes_cached(
        tmp_path: Any, monkeypatch: pytest.MonkeyPatch) -> None:
    """返回 Execute 链的节点（如 scan_docs/check_step）也应被缓存。"""
    monkeypatch.chdir(tmp_path)
    acp = CountingACP()
    calls: Counter[str] = Counter()

    def build() -> tuple[Flow, Any]:
        flow = Flow("wf-chain", agent=acp)

        @flow.node("head")
        def head() -> Any:
            calls["head"] += 1
            result = yield Agent("seed")
            return tail(x=result)  # Execute 链

        @flow.node("tail")
        def tail(x: str) -> Any:
            calls["tail"] += 1
            return Result(f"tail:{x}")

        return flow, head

    f1, head1 = build()
    f1.execute(head1())
    assert f1._result["tail"] == "tail:out:seed"
    assert calls == Counter({"head": 1, "tail": 1})

    # 第二次执行：head（Execute 描述符）与 tail（Result）双双命中
    f2, head2 = build()
    f2.execute(head2())
    assert calls == Counter({"head": 1, "tail": 1})
    assert f2._result["tail"] == "tail:out:seed"


def test_partial_args_accumulation_replayed(
        tmp_path: Any, monkeypatch: pytest.MonkeyPatch) -> None:
    """缓存重放保留跨节点的部分参数累积语义。"""
    monkeypatch.chdir(tmp_path)
    calls: Counter[str] = Counter()

    def build() -> tuple[Flow, Any, Any]:
        flow = Flow("wf-partial")

        @flow.node("left")
        def left() -> Any:
            calls["left"] += 1
            return joiner(b="2")  # 部分参数，等待 right 补齐

        @flow.node("right")
        def right() -> Any:
            calls["right"] += 1
            return joiner(a="1")

        @flow.node("joiner")
        def joiner(a: str, b: str) -> Any:
            calls["joiner"] += 1
            return Result(a + b)

        return flow, left, right

    f1, left1, right1 = build()
    f1.execute(left1(), right1())
    assert f1._result["joiner"] == "12"
    assert calls == Counter({"left": 1, "right": 1, "joiner": 1})

    f2, left2, right2 = build()
    f2.execute(left2(), right2())
    assert calls == Counter({"left": 1, "right": 1, "joiner": 1})
    assert f2._result["joiner"] == "12"


def test_unserializable_downstream_args_not_cached(
        tmp_path: Any, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.chdir(tmp_path)
    calls: Counter[str] = Counter()

    def build() -> tuple[Flow, Any]:
        flow = Flow("wf-bridge")

        @flow.node("bridge")
        def bridge() -> Any:
            calls["bridge"] += 1
            return sink(x=object())  # 下游参数不可序列化

        @flow.node("sink")
        def sink(x: object) -> Any:
            return Result("sunk")

        return flow, bridge

    f1, bridge1 = build()
    f1.execute(bridge1())
    assert f1._result["sink"] == "sunk"

    f2, bridge2 = build()
    f2.execute(bridge2())
    assert f2._result["sink"] == "sunk"
    # 返回的 Execute 参数无法序列化 → bridge 不缓存，每次真实执行
    assert calls["bridge"] == 2


def test_node_code_body_hash_invalidates_cache(
        tmp_path: Any, monkeypatch: pytest.MonkeyPatch) -> None:
    """同名同参节点，函数体变更后旧缓存必须失效。"""
    monkeypatch.chdir(tmp_path)
    calls: Counter[str] = Counter()

    def build(variant: str) -> tuple[Flow, Any]:
        flow = Flow("wf-code")

        if variant == "a":
            @flow.node("calc")
            def calc(x: int) -> Any:
                calls["calc"] += 1
                return Result(x + 1)
        else:
            # 同名同参，仅函数体不同
            @flow.node("calc")
            def calc(x: int) -> Any:
                calls["calc"] += 1
                return Result(x + 2)

        return flow, calc

    f1, c1 = build("a")
    f1.execute(c1(x=1))
    assert f1._result["calc"] == 2

    # 相同函数体：命中缓存
    f2, c2 = build("a")
    f2.execute(c2(x=1))
    assert calls["calc"] == 1
    assert f2._result["calc"] == 2

    # 函数体变化（代码体 hash 不同）：未命中，重新执行并得到新结果
    f3, c3 = build("b")
    f3.execute(c3(x=1))
    assert calls["calc"] == 2
    assert f3._result["calc"] == 3
