# coding: utf-8
"""基于 stdio 输出的 JSONL 状态上报。

工作流运行期间把四类事件以 JSONL（每行一个 JSON 对象）写到 stdout：

- ``graph``：``Flow.execute`` 生成的流程图（nodes/edges/start）；
- ``node``：节点状态变更（running/done/error，含全部已填充参数，
  缓存命中同样推送并以 ``cached`` 标记）；
- ``agents_start``：节点即将启动 Agent/MultiAgent（含本次调用的
  序号、prompt 列表）；
- ``agent``：子 Agent 的实时状态（waiting/running/success/failure、
  工具调用列表、session id、所属节点与序号等），每次状态变化立即上报；
- ``node_code``：节点启动时广播其源码；
- ``node_log``：节点线程内 ``print`` 的输出转发（替换内置 print 实现，按 ``\n`` 成行缓冲，正确处理多参数、sep、end 与 flush）。

每次输出 JSONL 时，先写入 ``\\x1b[?2004h``、结尾写入 ``\\x1b[?2004l``，
把整块上报输出包裹为宿主可识别的区段。上报过程绝不抛出异常干扰工作流执行。
"""

from __future__ import annotations

import builtins
import json
import sys
import threading
from collections.abc import Iterable
from datetime import datetime, timezone
from typing import TYPE_CHECKING, Any, TextIO

if TYPE_CHECKING:
    from .agent import AgentStatusSingleAgent
    from .flow import Graph

BRACKETED_PASTE_ENABLE = "\x1b[?2004h"  # JSONL 输出块起始标记
BRACKETED_PASTE_DISABLE = "\x1b[?2004l"  # JSONL 输出块结束标记

type ReportEvent = dict[str, Any]  # 单条上报事件


def _utc_now() -> str:
    "_utc_now() 当前 UTC 时间的 ISO8601 字符串"
    return datetime.now(timezone.utc).isoformat()


class StatusReporter:
    """StatusReporter 把生成的图与 Agent 实时状态以 JSONL 上报到 stdout。"""

    # 类级写锁：不同实例（例如并发子 Agent 各自创建的实例）写出互不交错
    _write_lock = threading.Lock()

    def __init__(self, stream: TextIO | None = None, *,
                 enabled: bool = True) -> None:
        self.stream: TextIO = sys.stdout if stream is None else stream
        self.enabled = enabled

    def emit(self, events: Iterable[ReportEvent]) -> None:
        """emit() 把一批事件序列化为 JSONL，整体包裹起止标记后写出到 stdout。"""
        if not self.enabled:
            return
        payload = "".join(
            json.dumps(event, ensure_ascii=False, default=str) + "\n"
            for event in events)
        if not payload:
            return
        with StatusReporter._write_lock:
            try:
                self.stream.write(BRACKETED_PASTE_ENABLE)
                self.stream.write(payload)
                self.stream.write(BRACKETED_PASTE_DISABLE)
                self.stream.flush()
            except Exception:
                # 状态上报不得干扰工作流执行（如管道关闭时的 BrokenPipe）。
                pass

    def bind(self, status: AgentStatusSingleAgent) -> None:
        """bind() 订阅单个 Agent 状态对象：之后每次状态变化都会自动上报。"""
        status.on_update = self.report_agent
        self.report_agent(status)  # 先上报一次初始 waiting 状态

    def report_agent(self, status: AgentStatusSingleAgent) -> None:
        """report_agent() 上报一次子 Agent 的当前完整快照。"""
        self.emit([{
            "type": "agent",
            "time": _utc_now(),
            "prompt": status.prompt,
            "path": status.path,
            "state": status.status.value,
            "sessionId": status.session_id,
            "nodeId": status.node_id,
            "callIndex": status.call_index,
            "agentIndex": status.agent_index,
            "agentCount": status.agent_count,
            "attempt": status.attempt,
            "tools": [dict(tool) for tool in list(status.tool_calls)],
        }])

    def report_node(self, workflow_id: str, node_id: str, state: str,
                    args: dict[str, Any], *, cached: bool = False) -> None:
        """report_node() 上报节点状态变更（含全部已填充参数）。"""
        self.emit([{
            "type": "node",
            "time": _utc_now(),
            "workflow": workflow_id,
            "nodeId": node_id,
            "state": state,
            "cached": cached,
            "args": dict(args),
        }])

    def report_agents_start(self, workflow_id: str, node_id: str,
                            call_index: int, agents: Any) -> None:
        """report_agents_start() 在节点即将启动 Agent/MultiAgent 前上报。

        ``agents`` 为 Agent 或 MultiAgent（鸭子类型，取其 prompt(s)）。
        """
        prompts: list[str] = []
        single = getattr(agents, "prompt", None)
        if isinstance(single, str):
            prompts.append(single)
        group = getattr(agents, "agents", None)
        if isinstance(group, (list, tuple)):
            for item in group:
                value = getattr(item, "prompt", None)
                if isinstance(value, str):
                    prompts.append(value)
        self.emit([{
            "type": "agents_start",
            "time": _utc_now(),
            "workflow": workflow_id,
            "nodeId": node_id,
            "callIndex": call_index,
            "count": len(prompts),
            "prompts": prompts,
        }])

    def report_node_log(self, workflow_id: str, node_id: str,
                        message: str) -> None:
        """report_node_log() 把节点线程内 print 的输出转发为 node_log 事件。"""
        self.emit([{
            "type": "node_log",
            "time": _utc_now(),
            "workflow": workflow_id,
            "nodeId": node_id,
            "message": message,
        }])

    def report_node_code(self, workflow_id: str, node_id: str, name: str,
                         code: str | None) -> None:
        """report_node_code() 在节点启动时广播其源码（不可得时为 null）。"""
        self.emit([{
            "type": "node_code",
            "time": _utc_now(),
            "workflow": workflow_id,
            "nodeId": node_id,
            "name": name,
            "code": code,
        }])

    def report_graph(self, workflow_id: str, graph: Graph) -> None:
        """report_graph() 上报 Flow 生成的流程图（set 序列化为有序列表）。"""
        self.emit([{
            "type": "graph",
            "time": _utc_now(),
            "workflow": workflow_id,
            "graph": {
                "nodes": dict(graph["nodes"]),
                "edges": {node_id: sorted(targets)
                          for node_id, targets in graph["edges"].items()},
                "start": sorted(graph["start"]),
            },
        }])


def optional_reporter(enabled: bool) -> StatusReporter | None:
    """optional_reporter() 按布尔开关创建默认 stdout 上报器，关闭时返回 None。"""
    return StatusReporter() if enabled else None

# === 节点线程 print 钩子：节点内 print 转为 node_log，其余线程照常 ===
# 语义与真实 stdout 对齐：sep 连接多参数后追加 end；按 \n 成行推送，
# 未成行的片段在线程结束或 flush=True 时补发为一条事件。

_REAL_PRINT = print  # 替换前捕获的原始内置 print
_NODE_PRINT_CONTEXTS: dict[int, tuple[StatusReporter, str, str]] = {}
_NODE_PRINT_BUFFERS: dict[int, list[str]] = {}  # 每线程待续写的未成行片段
_NODE_PRINT_LOCK = threading.Lock()


def _node_print(*args: Any, sep: str = " ", end: str = "\n",
                file: Any = None, flush: bool = False) -> None:
    "_node_print() 内置 print 的替身：节点线程内转为 node_log 事件"
    tid = threading.get_ident()
    context = _NODE_PRINT_CONTEXTS.get(tid)
    if context is None or (file is not None and file is not sys.stdout):
        # 非节点线程 / 显式重定向到其他流时保持原生行为
        _REAL_PRINT(*args, sep=sep, end=end, file=file, flush=flush)
        return
    reporter, workflow_id, node_id = context
    # 与内置 print 一致：str() 逐个转换参数，用 sep 连接，末尾追加 end
    buffer = _NODE_PRINT_BUFFERS.setdefault(tid, [])
    buffer.append(sep.join(str(arg) for arg in args) + end)
    data = "".join(buffer)
    *lines, pending = data.split("\n")
    for line in lines:
        reporter.report_node_log(workflow_id, node_id, line)
    buffer.clear()
    if pending:
        buffer.append(pending)
    if flush and pending:
        # flush=True：尚未成行的残段立即作为一条事件发出
        reporter.report_node_log(workflow_id, node_id, pending)
        buffer.clear()


def _flush_node_print_buffer(reporter: StatusReporter, workflow_id: str,
                             node_id: str, fragments: list[str]) -> None:
    "_flush_node_print_buffer() 把未成行的残段补发为一条 node_log 事件"
    text = "".join(fragments)
    if text:
        reporter.report_node_log(workflow_id, node_id, text)


def bind_node_print(reporter: StatusReporter, workflow_id: str,
                    node_id: str) -> None:
    """bind_node_print() 注册当前线程：该线程内的 print 转为 node_log。

    进程内首个注册会整体替换 ``builtins.print``；未注册线程的 print
    原样透传，行为不变。
    """
    with _NODE_PRINT_LOCK:
        if not _NODE_PRINT_CONTEXTS:
            setattr(builtins, "print", _node_print)
        _NODE_PRINT_CONTEXTS[threading.get_ident()] = (
            reporter, workflow_id, node_id)


def unbind_node_print() -> None:
    """unbind_node_print() 解除当前线程注册；全部解除后还原内置 print。

    解除前把该线程尚未成行的输出残段补发为最后一条 node_log，
    保证 ``end=""`` 之类不完整输出不丢失。
    """
    tid = threading.get_ident()
    with _NODE_PRINT_LOCK:
        context = _NODE_PRINT_CONTEXTS.pop(tid, None)
        fragments = _NODE_PRINT_BUFFERS.pop(tid, [])
        if not _NODE_PRINT_CONTEXTS:
            setattr(builtins, "print", _REAL_PRINT)
    if context is not None:
        _flush_node_print_buffer(context[0], context[1], context[2],
                                 fragments)
