from typing import Any, cast

import pytest

from src.dynworkflow import Agent
from src.dynworkflow.agent import AgentStatusEnum
from src.dynworkflow.flow import Flow, Result


def test_flow_passes_configured_acp_to_yielded_agent(
        tmp_path: Any, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.chdir(tmp_path)  # 隔离默认缓存库 .alkaid0/workflow.db

    class FakeACP:
        def run(self, prompt: str, **kwargs: Any) -> str:
            return "from-acp"

    flow = Flow("flow", agent=FakeACP())

    @flow.node("agent")
    def node() -> Any:
        # pyright: ignore[reportUnknownVariableType]
        result = yield Agent("prompt")
        typed_result: str = cast(str, result)
        return Result(typed_result)

    flow.execute(node())
    assert flow._result["node"] == "from-acp"
    assert node.agents[0][0].status is AgentStatusEnum.SUCCESS


def test_node_failure_does_not_stop_sibling_and_is_raised_afterward(
        tmp_path: Any, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.chdir(tmp_path)
    flow = Flow("flow", agent=object(), cache=False)
    sibling_finished = False

    @flow.node("broken")
    def broken() -> None:
        raise RuntimeError("broken node")

    @flow.node("sibling")
    def sibling() -> None:
        nonlocal sibling_finished
        sibling_finished = True

    with pytest.raises(ExceptionGroup) as exc_info:
        flow.execute(broken(), sibling())

    assert sibling_finished
    assert broken.status.name == "ERROR"
    assert any("broken node" in str(error)
               for error in exc_info.value.exceptions)


def test_node_can_return_result_and_downstream_together(
        tmp_path: Any, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.chdir(tmp_path)
    flow = Flow("flow", agent=object(), cache=False)

    @flow.node("head")
    def head() -> Any:
        return Result("head-result"), tail(x="from-head")

    @flow.node("tail")
    def tail(x: str) -> Any:
        return Result(f"tail:{x}")

    flow.execute(head())
    assert flow._result["head"] == "head-result"
    assert flow._result["tail"] == "tail:from-head"
    assert head.status.name == "DONE"
    assert tail.status.name == "DONE"


def test_node_can_mix_result_and_downstream_in_list(
        tmp_path: Any, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.chdir(tmp_path)
    flow = Flow("flow", agent=object(), cache=False)
    order: list[str] = []

    @flow.node("head")
    def head() -> Any:
        return [first(), Result("head-result"), second()]

    @flow.node("first")
    def first() -> Any:
        order.append("first")
        return Result("first-result")

    @flow.node("second")
    def second() -> Any:
        order.append("second")
        return Result("second-result")

    flow.execute(head())
    assert flow._result["head"] == "head-result"
    assert flow._result["first"] == "first-result"
    assert flow._result["second"] == "second-result"
    assert set(order) == {"first", "second"}


def test_multiple_results_in_one_node_is_error(
        tmp_path: Any, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.chdir(tmp_path)
    flow = Flow("flow", agent=object(), cache=False)

    @flow.node("broken")
    def broken() -> Any:
        return [Result("a"), Result("b")]

    with pytest.raises(ExceptionGroup) as exc_info:
        flow.execute(broken())
    assert any("at most one Result" in str(error)
               for error in exc_info.value.exceptions)
    assert broken.status.name == "ERROR"
