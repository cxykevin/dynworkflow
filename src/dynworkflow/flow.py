# coding: utf-8
from typing import Any, Generator, Callable, NoReturn, TypedDict, cast
import hashlib
import json
import multiprocessing
import inspect
import os
import sys
import queue
import threading
import time
import ast
import textwrap
from enum import Enum

from .agent import (Agent, AgentClientInterface, MultiAgent, AgentStatus,
                    find_agent_group, gen_conn_info_from_env,
                    raise_in_thread)
from .cache import CacheContext, WorkflowCache, default_db_path
from .reporter import bind_node_print, optional_reporter, unbind_node_print

type StructureOutput = (
    dict[str, StructureOutput] |
    list[StructureOutput] |
    str |
    int |
    float |
    bool |
    None
)  # StructureOutput 结构化输出类型


class GraphNode(TypedDict):
    "GraphNode 节点图对象"
    name: str  # 节点名称


class Graph(TypedDict):
    "Graph 节点图"
    nodes: dict[str, GraphNode]    # 节点字典
    edges: dict[str, set[str]]  # 边列表
    start: set[str]             # 起始节点


class Result:
    "Result 向 Main Agent 返回结果"

    def __init__(self, result: Any) -> None:
        self.result_val = result

    def __repr__(self) -> str:
        return f"Result({self.result_val})"

    def __str__(self) -> str:
        return str(self.result)

    def result(self) -> Any:
        "Result.result() 获取结果"
        return self.result_val


class _Empty:
    "Empty 空对象"


_empty = _Empty()  # _empty 空对象


class NodeStatus(Enum):
    "NodeStatus 节点状态"

    WAIT = "wait"        # 等待
    QUEUE = "queue"      # 队列中
    RUNNING = "running"  # 运行中
    DONE = "done"        # 完成
    ERROR = "error"      # 完成


def _describe_node_result(ret: 'NodeResult') -> "dict[str, Any] | None":
    '"_describe_node_result() 把节点返回值序列化为缓存描述符；不可缓存时返回 None'

    if ret is None:
        return {"kind": "none"}
    if isinstance(ret, Result):
        # value 含不可序列化对象时由 put_node 整体放弃写入
        return {"kind": "result", "value": ret.result()}
    items = list(ret) if isinstance(ret, (list, tuple)) else [ret]
    exec_items: list[dict[str, Any]] = []
    for item in items:
        exec_items.append(
            {"node_id": item._node._node_id, "args": dict(item._args)})
    return {"kind": "executes", "items": exec_items}


class Node():
    "Node 节点对象"

    def __init__(self, node_name: str, node_id: str, func: 'TaskGen') -> None:
        self._node_name = node_name  # 节点名称
        self._node_id = node_id      # 节点 ID
        self._func = func            # 节点生成器
        # === 代码体哈希（参与缓存联合键，函数体变更即失效） ===
        self._source: str | None = None  # 节点源码缓存（启动时广播）
        self._code_hash: str | None = self._compute_code_hash()
        # === 节点状态 ===
        self._thread: threading.Thread | None = None  # 节点线程
        self._value_lock: threading.Lock = threading.Lock()  # 节点值锁
        # === 节点参数缓存 ===
        self._func_cache: dict[str, Any] = {}                  # 函数调用参数缓存
        self._func_cache_var_keyword: dict[str,
                                           Any] | None = None  # 函数调用可变参数缓存
        self._func_cache_filled_num: int = 0                   # 参数已填充数量
        self._func_default_args: dict[str, Any] = {}           # 默认参数
        self._init_func_cache()  # 初始化缓存
        # === 节点结果 ===
        self.status: NodeStatus = NodeStatus.WAIT  # 节点状态
        self.agents: list[AgentStatus] = []  # 节点代理

    def _compute_code_hash(self) -> str | None:
        "Node._compute_code_hash() 计算函数源码体的 sha256；源码不可得返回 None"
        try:
            source = inspect.getsource(self._func)
        except (OSError, TypeError):
            self._source = None
            return None
        # dedent + strip 归一化缩进，避免同等代码因粘贴缩进不同而误判变化
        normalized = textwrap.dedent(source).strip()
        self._source = normalized
        return hashlib.sha256(normalized.encode("utf-8")).hexdigest()

    def _init_func_cache(self) -> None:
        "Node._init_func_cache() 初始化函数调用参数缓存"
        with self._value_lock:
            sig = inspect.signature(self._func)
            for name, param in sig.parameters.items():
                if param.kind == inspect.Parameter.POSITIONAL_ONLY or param.kind == inspect.Parameter.VAR_POSITIONAL:
                    # 不支持位置参数
                    raise ValueError(
                        "Cannot use positional only or var positional arguments in node args")
                if param.kind == inspect.Parameter.VAR_KEYWORD:
                    # 特殊处理可变关键字参数
                    self._func_cache_var_keyword = {}
                    continue
                # 填充缓存
                self._func_cache[name] = _empty
                if param.default is not inspect.Parameter.empty:
                    # 填充默认值
                    self._func_cache[name] = param.default
                    self._func_cache_filled_num += 1
                    self._func_default_args[name] = param.default

    def _fill_args(self, **args: Any) -> None:
        "Node.fill_args() 填充参数"

        with self._value_lock:
            for name, value in args.items():
                if name not in self._func_cache:
                    # 参数未定义
                    if self._func_cache_var_keyword is None:
                        # 不支持可变关键字参数
                        raise ValueError(f"Unknown argument: {name}")
                    self._func_cache_var_keyword[name] = value
                    continue
                if isinstance(self._func_cache[name], _Empty):
                    self._func_cache_filled_num += 1
                self._func_cache[name] = value

    def _check_all_filled(self) -> bool:
        "Node.check_all_filled() 检查所有参数是否已填充"
        with self._value_lock:
            return self._func_cache_filled_num == len(self._func_cache)

    def __call__(self, **args: Any) -> 'Execute':
        "Node() 覆盖原本调用，用于参数填充"
        return Execute(self,  args)

    def _report_status(self, flow: 'Flow', state: NodeStatus,
                       args: dict[str, Any], *, cached: bool = False) -> None:
        "_report_status() 推送一次节点状态事件（未启用上报时为空操作）"
        if flow._reporter is None:
            return
        flow._reporter.report_node(flow._workflow_id, self._node_id,
                                   state.value, args, cached=cached)

    def _run(self, flow: 'Flow') -> None:
        "Node.run() 执行节点，并把异常延迟到工作流结束统一抛出。"
        try:
            self._run_impl(flow)
        except (NodeTerminated, WorkflowAborted):
            # 控制命令导致的终止不是节点执行错误。
            self.status = NodeStatus.ERROR
        except Exception as e:
            with self._value_lock:
                args = self._func_cache | (
                    self._func_cache_var_keyword
                    if self._func_cache_var_keyword is not None else {}
                )
            e.add_note("In node: " + str(self._node_id))
            setattr(e, "node_id", self._node_id)
            self.status = NodeStatus.ERROR
            flow._result[self._node_id] = ""
            flow._error[self._node_id] = e
            # 节点线程内的异常立即上报，但不打断其它节点。
            self._report_status(flow, NodeStatus.ERROR, args)

    def _run_impl(self, flow: 'Flow') -> None:
        "Node._run_impl() 执行节点主体。"
        self.status = NodeStatus.RUNNING
        with self._value_lock:
            dic = self._func_cache | (
                # 合并参数，填充可变关键字参数（若有）
                self._func_cache_var_keyword if
                self._func_cache_var_keyword is not None else {}
            )
        # === 节点级缓存上下文（携带代码体哈希，函数体变更即失效） ===
        cache_ctx = CacheContext(
            cache=flow.cache, workflow=flow._workflow_id,
            node_id=self._node_id, session_scope=flow._session_scope,
            code_hash=self._code_hash)
        node_info = cache_ctx.node_key(dic)
        hit: dict[str, Any] | None = None
        if node_info is not None and flow.cache is not None:
            hit = flow.cache.get_node(node_info)
        # 推送：节点开始运行（含全部已填充参数与缓存命中标记）
        self._report_status(flow, NodeStatus.RUNNING, dic,
                            cached=hit is not None)
        # 推送：广播本节点的源码
        if flow._reporter is not None:
            flow._reporter.report_node_code(flow._workflow_id, self._node_id,
                                            self._node_name, self._source)
        if hit is not None:
            cached = hit
            # 缓存命中：跳过节点函数执行，按描述符重放结果/下游调度
            try:
                self._replay_cached(flow, cached)
            except Exception as e:
                e.add_note("In node: " + str(self._node_id))
                setattr(e, "node_id", self._node_id)
                self.status = NodeStatus.ERROR
                flow._result[self._node_id] = ""
                flow._error[self._node_id] = e
                # 推送：缓存命中节点重放失败
                self._report_status(flow, NodeStatus.ERROR, dic,
                                    cached=True)
                return
            self.status = NodeStatus.DONE
            # 推送：缓存命中的节点同样推送完成状态与参数
            self._report_status(flow, NodeStatus.DONE, dic, cached=True)
            return

        ret: NodeResult = None  # 返回值
        try:
            if inspect.isgeneratorfunction(self._func):
                it: TaskGenerator = self._func(**dic)  # 调用生成器
                result: Any | None = None  # Agent 返回
                call_number: int = 0  # 本节点内第几次 Agent/MultiAgent 调用
                while True:
                    try:  # 循环调用生成器
                        agents_call = it.send(result)
                    except StopIteration as e:
                        # 生成器结束
                        ret = e.value
                        break
                    # 执行子 Agent（透传缓存上下文与调用序号）
                    call_number += 1
                    call_info: AgentStatus = []
                    self.agents.append(call_info)  # 利用引用
                    # 推送：节点即将启动本组 agents
                    if flow._reporter is not None:
                        flow._reporter.report_agents_start(
                            flow._workflow_id, self._node_id, call_number,
                            agents_call)
                    result = agents_call.run(call_info, flow._agent,
                                             cache_ctx=cache_ctx,
                                             report=flow._report,
                                             node_id=self._node_id,
                                             call_index=call_number)
            else:
                val = self._func(**dic)  # 调用函数
                assert not isinstance(val, Generator)
                ret = val
        except NodeTerminated:
            self.status = NodeStatus.ERROR
            self._report_status(flow, NodeStatus.ERROR, dic)
            if flow._reporter is not None:
                flow._reporter.report_node(flow._workflow_id, self._node_id,
                                           "terminated", dic)
            return
        except WorkflowAborted:
            self.status = NodeStatus.ERROR
            self._report_status(flow, NodeStatus.ERROR, dic)
            return
        except Exception as e:
            e.add_note("In node: " + str(self._node_id))
            setattr(e, "node_id", self._node_id)
            self.status = NodeStatus.ERROR
            flow._result[self._node_id] = ""
            flow._error[self._node_id] = e
            # 推送：节点执行失败
            self._report_status(flow, NodeStatus.ERROR, dic)
            return

        self.status = NodeStatus.DONE
        # 推送：节点执行完成
        self._report_status(flow, NodeStatus.DONE, dic)

        # 写入节点缓存（描述符含不可序列化成分时由 put_node 自行忽略）
        if flow.cache is not None and node_info is not None:
            descriptor = _describe_node_result(ret)
            if descriptor is not None:
                flow.cache.put_node(node_info, descriptor)

        # 无返回值
        if ret is None:
            return
        # 返回值处理
        if isinstance(ret, Result):
            flow._result[self._node_id] = ret.result()
            return

        # 执行流程下一步
        if isinstance(ret, Execute):
            ret = [ret]
        for i in ret:
            i.run(flow)

    def _replay_cached(self, flow: 'Flow', cached: dict[str, Any]) -> None:
        "Node._replay_cached() 重放缓存的节点结果（终值或下游调度）"
        kind = cached.get("kind")
        if kind == "none":
            return
        if kind == "result":
            flow._result[self._node_id] = cached.get("value")
            return
        if kind == "executes":
            known = {node._node_id: node for node in flow._nodes}
            items = cached.get("items")
            if not isinstance(items, list):
                items = []
            items = cast(list[dict[str, Any]], items)
            for item in items:
                target_id = str(item.get("node_id"))
                target = known.get(target_id)
                if target is None:
                    raise ValueError(
                        f"Cached target node not found: {target_id}")
                # 重建调用点参数并走正常填充/调度路径，
                # 完整保留跨节点部分参数累积语义。
                args: Any = item.get("args")
                args_value: dict[str, Any] = dict(cast(dict[str, Any], args)) if isinstance(
                    args, dict) else {}
                Execute(target, args_value).run(flow)
            return
        raise ValueError(f"Invalid cached node result kind: {kind!r}")

    def _create_thread_task(self, flow: 'Flow') -> threading.Thread:
        "Node.thread_tast() 启动线程"
        if (self._thread is not None and self._thread.is_alive()):
            # 线程已启动
            raise RuntimeError("Node thread already started")

        def _target() -> None:
            if flow._reporter is not None:
                # 子线程内替换内置 print：节点日志以 node_log 事件推送
                bind_node_print(flow._reporter, flow._workflow_id,
                                self._node_id)
            try:
                self._run(flow)
            finally:
                unbind_node_print()

        self._thread = threading.Thread(target=_target, daemon=True)
        self._thread.start()
        _NODE_RUNTIMES[self._node_id] = (self, self._thread, flow)
        return self._thread

    def clean_cache(self, *val: str) -> None:
        "Node.clean_cache() 清理缓存"

        with self._value_lock:
            if (len(val) == 0):  # 清空所有缓存
                for i in self._func_cache.keys():
                    self._func_cache[i] = _empty
                self._func_cache_var_keyword = {}
                self._func_cache_filled_num = 0

                # 填充默认值
                for i in self._func_default_args.keys():
                    self._func_cache[i] = self._func_default_args[i]
                    self._func_cache_filled_num += 1
            else:
                for i in val:
                    if i in self._func_cache:
                        # 标准参数
                        if isinstance(self._func_cache[i], _Empty):
                            continue
                        # 参数已填充
                        self._func_cache[i] = _empty
                        self._func_cache_filled_num -= 1
                        if i in self._func_default_args:  # 填充默认值
                            self._func_cache[i] = self._func_default_args[i]
                            self._func_cache_filled_num += 1
                    elif self._func_cache_var_keyword is not None and i in self._func_cache_var_keyword:
                        # 可变关键字参数
                        del self._func_cache_var_keyword[i]
                    else:  # 参数未定义
                        raise ValueError(f"Unknown argument: {i}")


class Execute:
    "Execute 执行对象"

    def __init__(self, node: Node, args: dict[str, Any]) -> None:
        self._node = node
        self._args = args

    def run(self, flow: 'Flow') -> None:
        "Execute.run() 执行节点"

        self._node._fill_args(**self._args)
        if self._node._check_all_filled():
            # 所有参数已填充，执行节点
            flow._taskque.put(self._node)


type Agents = (
    MultiAgent |
    Agent
)  # Agents 子 Agent 对象
type NodeResult = (
    Execute |
    list[Execute] |
    tuple[Execute, ...] |
    Result |
    None
)  # Nodes 节点返回对象
type TaskGenerator = (Generator[
    Agents,
    StructureOutput | str,
    NodeResult
])  # TaskGenerator 任务生成器
type TaskGen = (Callable[
    ...,
    TaskGenerator
] | Callable[
    ...,
    NodeResult
])  # TaskGen 任务生成函数


class NodeTerminated(BaseException):
    """stdin 命令终止正在运行的节点。"""


class WorkflowAborted(BaseException):
    """stdin 命令终止整个工作流。"""


_NODE_RUNTIMES: dict[str, tuple[Node, threading.Thread, "Flow"]] = {}


class Flow:
    "Flow 工作流对象"

    def __init__(self, workflow_id: str, /, *, agent: AgentClientInterface | None = None,
                 cache: bool = True, report: bool | None = None) -> None:
        if agent is None:
            agent = gen_conn_info_from_env()
        self._workflow_id = workflow_id
        self._nodes: list[Node] = []
        self._taskque: queue.Queue[Node] = queue.Queue()
        self._result: dict[str, Any] = {}
        self._error: dict[str, Exception | None] = {}
        self._agent = agent
        self._freeze = False
        self._graph: Graph | None = None
        self._entries: list[Node] = []
        # === 工作流缓存（.alkaid0/workflow.db，cache=False 可关闭） ===
        self.cache: WorkflowCache | None = (
            WorkflowCache(default_db_path()) if cache else None)
        self._session_scope: str = ""  # ALKAID0_WORKFLOW_SESSION_ID 作用域
        # === stdio JSONL 状态上报（report=True 或 ALKAID0_WORKFLOW_REPORT=1 开启） ===
        if report is None:
            report = os.environ.get("ALKAID0_WORKFLOW_REPORT") == "1"
        self._report: bool = report
        self._reporter = optional_reporter(report)
        self._abort = False

    def node(self, node_name: str, /) -> Callable[[TaskGen], Node]:
        "@Flow.node() 新增节点"

        if self._freeze:
            raise RuntimeError("Flow is frozen")

        def wrapper(func: TaskGen) -> Node:
            "Flow.node().wrapper() 节点包装器"

            if self._freeze:
                raise RuntimeError("Flow is frozen")

            # 包装函数
            funcname: str = func.__name__
            node = Node(node_name=node_name, func=func, node_id=funcname)
            self._nodes.append(node)
            return node
        return wrapper

    def _loop(self) -> None:
        "Flow._loop() 循环执行任务队列"

        threads: list[threading.Thread] = []
        while True:
            try:
                node = self._taskque.get(False)  # 取队列
            except queue.Empty:  # 队列为空
                alive = any(t.is_alive() for t in threads)
                if not alive or self._abort:
                    break
                time.sleep(1)
                continue
            if not self._abort:
                threads.append(node._create_thread_task(self))

    def _generate_graph_edges(self) -> dict[str, set[str]]:
        "Node._generate_graph_edges() 生成流程图图边"

        edges: dict[str, set[str]] = {}  # 边集
        known_node_ids = {
            node._node_id for node in self._nodes}  # 已知节点 ID，用于查找
        for nodex in self._nodes:  # 遍历节点
            func = nodex._func
            try:  # 获取函数源码
                src = inspect.getsource(func)
            except OSError:
                raise ValueError("Function source not found")
            try:  # 解析源码
                tree = ast.parse(textwrap.dedent(src))
            except SyntaxError:
                raise ValueError("Function source syntax error")

            # 取函数体
            func_def = None
            for ast_node in ast.walk(tree):
                if isinstance(ast_node, ast.FunctionDef) and ast_node.name == nodex._func.__name__:
                    func_def = ast_node
                    break
            if func_def is None:
                raise ValueError("Function definition not found")

            # 查找函数调用
            referenced: set[str] = set()

            class CallCollector(ast.NodeVisitor):
                "Node._generate_graph_edges.CallCollector 调用收集器"

                def visit_Call(self, node: ast.Call) -> Any:
                    "Node._generate_graph_edges.CallCollector.visit_Call() 遍历调用者"

                    # 查找调用者
                    if isinstance(node.func, ast.Name):
                        if node.func.id in known_node_ids:
                            referenced.add(node.func.id)

                    # 递归遍历参数及内部表达式
                    self.generic_visit(node)

            # 启动收集
            CallCollector().visit(func_def)

            # 添加边
            edges[nodex._node_id] = referenced

        return edges

    def _generate_graph(self, tasks: tuple[Execute, ...]) -> Graph:
        "Node._generate_graph() 生成流程图"

        edges = self._generate_graph_edges()
        return {
            "nodes": {
                node._node_id: {  # 节点信息
                    "name": node._node_id,
                } for node in self._nodes
            },
            "edges": edges,
            "start": {
                i._node._node_id
                for i in tasks
                if i._node._check_all_filled()  # 检查所有参数是否填充，可以正常执行
            }
        }

    def _raise_exceptions(self) -> None:
        "Flow._raise_exceptions() 抛出异常"
        exceptions: list[Exception] = [
            i for i in self._error.values() if i is not None]
        if len(exceptions) > 0:
            raise ExceptionGroup(
                f"{len(exceptions)} node(s) run failed", exceptions)

    def execute(self, *tasks: Execute) -> None:
        "Flow.execute() 执行工作流"

        # 每次执行时读取会话作用域，作为缓存联合键的组成部分
        self._session_scope = os.environ.get("ALKAID0_WORKFLOW_SESSION_ID", "")
        self._freeze = True
        for i in tasks:  # 填充参数
            i.run(self)
        self._graph = self._generate_graph(tasks)
        if self._reporter is not None:
            # 上报本次生成的流程图（JSONL，包裹 bracketed paste 标记）
            self._reporter.report_graph(self._workflow_id, self._graph)
        self._loop()
        self._raise_exceptions()

    def run(self, *tasks: Execute) -> NoReturn:
        """Node.run() 在 fork 子进程执行工作流，主进程监听 stdin JSONL。"""
        sys.stdout.write("\x1edynworkflow\x1f\n")
        sys.stdout.flush()
        self._report = True
        if self._reporter is None:
            self._reporter = optional_reporter(True)
        if self._agent is None:
            raise ValueError("No availble agent found!")
        control: Any = multiprocessing.get_context("fork").Queue()
        process = multiprocessing.get_context("fork").Process(
            target=self._execute_child, args=(tasks, control))
        process.start()
        stdin_done = threading.Event()

        def read_stdin() -> None:
            try:
                for line in sys.stdin:
                    try:
                        command = json.loads(line)
                    except json.JSONDecodeError:
                        continue
                    if isinstance(command, dict):
                        command = cast(dict[str, str], command)
                        control.put(command)
                        if command.get("cmd") == "shutdown":
                            return
            finally:
                stdin_done.set()

        # Reading stdin can block forever, so keep it off the main thread. The
        # main thread can then observe a worker crash or normal exit and stop.
        threading.Thread(target=read_stdin, daemon=True).start()
        try:
            while process.is_alive() and not stdin_done.is_set():
                process.join(0.1)
        finally:
            if process.is_alive():
                control.put({"cmd": "shutdown"})
            process.join(10)
            if process.is_alive():
                process.terminate()
                process.join(2)
        # Preserve a non-zero exit status when the child failed unexpectedly.
        sys.exit(process.exitcode if process.exitcode is not None else 0)

    def _execute_child(self, tasks: tuple[Execute, ...], control: Any) -> None:
        def listen() -> None:
            while True:
                command = control.get()
                if isinstance(command, dict):
                    command = cast(dict[str, str], command)
                    self._handle_command(command)
                    if command.get("cmd") == "shutdown":
                        return
        threading.Thread(target=listen, daemon=True).start()
        self.execute(*tasks)

    def _handle_command(self, command: dict[str, Any]) -> None:
        cmd = command.get("cmd")
        if cmd == "shutdown":
            self._abort = True
            for node, thread, _ in list(_NODE_RUNTIMES.values()):
                if thread.is_alive() and thread.ident is not None:
                    raise_in_thread(thread.ident, WorkflowAborted)
            return
        if cmd == "agent":
            group = find_agent_group(str(command.get("nodeId", "")),
                                     int(command.get("callIndex", 0)))
            if group is None:
                return
            index = int(command.get("agentIndex", 0)) - 1
            if command.get("action") == "terminate":
                group.terminate(index)
            elif command.get("action") == "retry":
                group.retry(index)
            return
        if cmd == "node":
            runtime = _NODE_RUNTIMES.get(str(command.get("nodeId", "")))
            if runtime is None:
                return
            node, thread, flow = runtime
            if not thread.is_alive() or thread.ident is None:
                return
            raise_in_thread(thread.ident, NodeTerminated)
            if command.get("action") == "restart":
                thread.join(2)
                if not thread.is_alive():
                    node._create_thread_task(flow)
