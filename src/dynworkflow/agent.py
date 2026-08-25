# coding: utf-8
"""ACP v2 客户端与工作流 Agent 基础组件。"""

from __future__ import annotations

import json
import ctypes
import os
import pathlib
import queue
import shlex
import subprocess
import sys
import threading
from dataclasses import dataclass, field
from enum import Enum
from typing import Any, Callable, Literal, Protocol, TypedDict, cast

from websockets.sync.client import ClientConnection, connect

from .cache import CacheContext
from .reporter import optional_reporter
from .schemas import (
    InitializeParams,
    InitializeResult,
    JsonObject,
    JsonRpcMessage,
    JsonValue,
    PromptParams,
    SessionNewParams,
    SessionResult,
    SessionResumeParams,
    TextContentBlock,
)


class AgentStatusEnum(Enum):
    """子 Agent 的生命周期状态。"""

    WAITING = "waiting"
    RUNNING = "running"
    SUCCESS = "success"
    FAILURE = "failure"


class ToolCall(TypedDict, total=False):
    """ACP 会话上报的一次工具调用。"""

    name: str
    tool_id: str
    args: dict[str, Any]
    output: str
    finished: bool


@dataclass
class AgentStatusSingleAgent:
    """单次 Agent 调用的实时状态。"""

    prompt: str
    path: str = "."
    tool_calls: list[ToolCall] = field(default_factory=list[ToolCall])
    session_id: str = ""  # 本次调用使用的 ACP 会话 ID（未结束会话缓存依赖）
    node_id: str = ""  # 所属节点 ID（工作流调度时填充，独立调用为空）
    call_index: int = 0  # 节点内第几次 Agent/MultiAgent 调用（1 起，0 表示未知）
    agent_index: int = 1  # 本次调用内第几个 agent（1 起）
    agent_count: int = 1  # 本次调用的 agent 总数
    attempt: int = 1  # 当前尝试次数
    # 状态订阅回调：status 赋值或显式 notify() 时触发（stdio 状态上报使用）
    on_update: Callable[["AgentStatusSingleAgent"], None] | None = field(
        default=None, repr=False, compare=False)

    def __post_init__(self) -> None:
        self._status = AgentStatusEnum.WAITING

    @property
    def status(self) -> AgentStatusEnum:
        """status 生命周期状态；每次赋值自动触发一次 on_update 回调。"""
        return self._status

    @status.setter
    def status(self, value: AgentStatusEnum) -> None:
        self._status = value
        self.notify()

    def notify(self) -> None:
        """notify() 手动触发一次状态上报（嵌套字段如工具调用变化后调用）。"""
        callback = self.on_update
        if callback is not None:
            callback(self)

    def reset_for_retry(self) -> None:
        """reset_for_retry() 清理本次状态并开始下一次尝试。"""
        self.attempt += 1
        self.tool_calls.clear()
        self.session_id = ""
        self.status = AgentStatusEnum.WAITING


type AgentStatus = list[AgentStatusSingleAgent]


def _resolve_path(value: str | pathlib.Path) -> str:
    """把工作流中的相对路径基于当前工作目录解析为绝对路径。"""
    return str(pathlib.Path(value).expanduser().resolve())


class AgentClientInterface(Protocol):
    """Agent 与 Flow 接受的最小客户端契约。"""

    def run(self, prompt: str, *, model: str = "", cwd: str | pathlib.Path = ".",
            status: AgentStatusSingleAgent | None = None) -> Any:
        ...


class _ACPConnection:
    """单条 ACP v2 stdio 连接，使用换行分隔的 JSON-RPC。"""

    def __init__(self, command: list[str], cwd: pathlib.Path, timeout: float) -> None:
        self.command = command
        self.timeout = timeout
        self.process = subprocess.Popen(
            command,
            cwd=str(cwd),
            stdin=subprocess.PIPE,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            bufsize=0,
        )
        assert self.process.stdin is not None
        assert self.process.stdout is not None
        self._stdin = self.process.stdin
        self._stdout = self.process.stdout
        self._messages: queue.Queue[JsonRpcMessage | Exception] = queue.Queue()
        self._write_lock = threading.Lock()
        self._next_id = 1
        self._reader = threading.Thread(target=self._read_loop, daemon=True)
        self._reader.start()
        self._stderr_reader = threading.Thread(
            target=self._drain_stderr, daemon=True)
        self._stderr_reader.start()

    def _drain_stderr(self) -> None:
        if self.process.stderr is not None:
            while self.process.stderr.readline():
                pass

    def _read_loop(self) -> None:
        try:
            while True:
                line: bytes = self._stdout.readline()
                if not line:
                    break
                try:
                    value: object = json.loads(line.decode("utf-8"))
                except json.JSONDecodeError:
                    # ACP 诊断信息应输出到 stderr；容忍杂散行，
                    # 避免一条坏诊断导致 JSON 流失去同步。
                    continue
                if isinstance(value, list):
                    messages = cast(list[object], value)
                    for message in messages:
                        if isinstance(message, dict):
                            self._messages.put(cast(JsonRpcMessage, message))
                elif isinstance(value, dict):
                    self._messages.put(cast(JsonRpcMessage, value))
        except Exception as exc:
            self._messages.put(exc)
        finally:
            self._messages.put(EOFError("ACP connection closed"))

    def send(self, message: JsonRpcMessage) -> None:
        payload = (json.dumps(message, separators=(",", ":"),
                   ensure_ascii=False) + "\n").encode()
        with self._write_lock:
            if self.process.poll() is not None:
                raise RuntimeError("ACP process exited")
            self._stdin.write(payload)
            self._stdin.flush()

    def request(self, method: str, params: JsonObject | None = None,
                on_notification: Callable[[JsonRpcMessage], None] | None = None) -> JsonValue:
        request_id = self._next_id
        self._next_id += 1
        self.send({
            "jsonrpc": "2.0",
            "id": request_id,
            "method": method,
            "params": {} if params is None else params,
        })
        return self.wait_for_response(request_id, method, on_notification)

    def wait_for_response(self, request_id: int | str, method: str,
                          on_notification: Callable[[JsonRpcMessage], None] | None = None) -> JsonValue:
        while True:
            message = self._get_message(method)
            if "method" in message:
                self._handle_incoming_request(message)
                if on_notification is not None:
                    on_notification(message)
                continue
            if message.get("id") != request_id:
                continue
            if "error" in message:
                error = message["error"]
                detail = error.get("message", "ACP error")
                raise RuntimeError(f"ACP {method} failed: {detail}")
            return message.get("result")

    def wait_for_notification(self, on_notification: Callable[[JsonRpcMessage], None],
                              predicate: Callable[[JsonRpcMessage], bool],
                              method: str) -> JsonRpcMessage:
        while True:
            message = self._get_message(method)
            if "method" in message:
                self._handle_incoming_request(message)
                on_notification(message)
                if predicate(message):
                    return message

    def _get_message(self, operation: str) -> JsonRpcMessage:
        try:
            message = self._messages.get(timeout=self.timeout)
        except queue.Empty as exc:
            raise TimeoutError(f"ACP request timed out: {operation}") from exc
        if isinstance(message, Exception):
            raise RuntimeError(str(message)) from message
        return message

    def _handle_incoming_request(self, message: JsonRpcMessage) -> None:
        """回复不支持的 Agent→Client 请求，避免协议死锁。"""
        if "id" not in message:
            return
        self.send({
            "jsonrpc": "2.0",
            "id": message["id"],
            "error": {"code": -32601, "message": "Client method not supported"},
        })

    def close(self) -> None:
        if self.process.poll() is None:
            try:
                self._stdin.close()
            except OSError:
                pass
            try:
                self.process.wait(timeout=1)
            except subprocess.TimeoutExpired:
                self.process.terminate()
                try:
                    self.process.wait(timeout=1)
                except subprocess.TimeoutExpired:
                    self.process.kill()
                    self.process.wait()


class _ACPWebSocketConnection:
    """ACP v2 WebSocket 传输，每帧承载一条 JSON 消息。"""

    def __init__(self, url: str, timeout: float) -> None:
        self.timeout = timeout
        self._socket: ClientConnection = connect(url, open_timeout=timeout)
        self._write_lock = threading.Lock()
        self._next_id = 1

    def send(self, message: JsonRpcMessage) -> None:
        payload = json.dumps(message, separators=(
            ",", ":"), ensure_ascii=False)
        with self._write_lock:
            self._socket.send(payload)

    def request(self, method: str, params: JsonObject | None = None,
                on_notification: Callable[[JsonRpcMessage], None] | None = None) -> JsonValue:
        request_id = self._next_id
        self._next_id += 1
        self.send({"jsonrpc": "2.0", "id": request_id, "method": method,
                   "params": {} if params is None else params})
        return self.wait_for_response(request_id, method, on_notification)

    def _receive(self, operation: str) -> JsonRpcMessage:
        try:
            raw = self._socket.recv(timeout=self.timeout)
        except TimeoutError as exc:
            raise TimeoutError(f"ACP request timed out: {operation}") from exc
        if not isinstance(raw, str):
            raise RuntimeError("ACP WebSocket message must be text")
        value: object = json.loads(raw)
        if not isinstance(value, dict):
            raise RuntimeError("ACP WebSocket message must be a JSON object")
        return cast(JsonRpcMessage, value)

    def wait_for_response(self, request_id: int | str, method: str,
                          on_notification: Callable[[JsonRpcMessage], None] | None = None) -> JsonValue:
        while True:
            message = self._receive(method)
            if "method" in message:
                self._handle_incoming_request(message)
                if on_notification is not None:
                    on_notification(message)
                continue
            if message.get("id") != request_id:
                continue
            if "error" in message:
                raise RuntimeError(
                    f"ACP {method} failed: {message['error'].get('message', 'ACP error')}")
            return message.get("result")

    def wait_for_notification(self, on_notification: Callable[[JsonRpcMessage], None],
                              predicate: Callable[[JsonRpcMessage], bool],
                              method: str) -> JsonRpcMessage:
        while True:
            message = self._receive(method)
            if "method" in message:
                self._handle_incoming_request(message)
                on_notification(message)
                if predicate(message):
                    return message

    def _handle_incoming_request(self, message: JsonRpcMessage) -> None:
        if "id" in message:
            self.send({"jsonrpc": "2.0", "id": message["id"],
                       "error": {"code": -32601, "message": "Client method not supported"}})

    def close(self) -> None:
        self._socket.close()


class _UpdateCollector:
    """收集 v2 session/update 通知，并把工具状态同步到 AgentStatus。"""

    def __init__(self, status: AgentStatusSingleAgent | None) -> None:
        self.status = status
        self._messages: dict[str, list[JsonValue]] = {}
        self._message_order: list[str] = []
        self._anonymous: list[JsonValue] = []
        self.idle_update: JsonObject | None = None

    @staticmethod
    def _text(content: JsonValue) -> str:
        if isinstance(content, str):
            return content
        if isinstance(content, dict):
            text = content.get("text")
            return text if isinstance(text, str) else ""
        if isinstance(content, list):
            return "".join(_UpdateCollector._text(item) for item in content)
        return ""

    def _message_content(self, update: JsonObject) -> str:
        message_id = update.get("messageId")
        content = update.get("content")
        if not isinstance(message_id, str):
            return self._text(content)
        if message_id not in self._messages:
            self._messages[message_id] = []
            self._message_order.append(message_id)
        session_update = update.get("sessionUpdate")
        if isinstance(session_update, str) and session_update.endswith("_chunk"):
            if content is not None:
                self._messages[message_id].append(content)
        elif "content" in update:
            self._messages[message_id] = [] if content is None else list(
                content) if isinstance(content, list) else [content]
        return self._text(self._messages[message_id])

    def _tool(self, update: JsonObject) -> None:
        if self.status is None:
            return
        tool_id = update.get("toolCallId", update.get("tool_call_id"))
        if not isinstance(tool_id, str) or not tool_id:
            return
        tool: ToolCall | None = next(
            (item for item in self.status.tool_calls if item.get("tool_id") == tool_id), None)
        if tool is None:
            new_tool: ToolCall = {"tool_id": tool_id, "name": "",
                                  "args": {}, "output": "", "finished": False}
            self.status.tool_calls.append(new_tool)
            tool = new_tool
        if "title" in update:
            tool["name"] = str(update["title"])
        if "kind" in update and not tool.get("name"):
            tool["name"] = str(update["kind"])
        if "rawInput" in update:
            raw_input = update["rawInput"]
            tool["args"] = raw_input if isinstance(raw_input, dict) else {}
        if "rawOutput" in update:
            value = update["rawOutput"]
            tool["output"] = value if isinstance(
                value, str) else json.dumps(value, ensure_ascii=False)
        if "content" in update:
            content = update["content"]
            value = self._text(content)
            if value:
                tool["output"] = value
        tool["finished"] = update.get("status") in {
            "completed", "failed", "cancelled"}

    def handle(self, message: JsonRpcMessage) -> None:
        params = message.get("params")
        if not isinstance(params, dict):
            return
        update_value = params.get("update")
        if not isinstance(update_value, dict):
            return
        update: JsonObject = update_value
        kind = update.get("sessionUpdate")
        if kind in {"agent_message", "agent_message_chunk"}:
            # 仅返回助手消息；思考内容保留在内部。
            self._message_content(update)
        elif kind == "state_update" and update.get("state") == "idle":
            self.idle_update = update
            self._touch()
        elif kind in {"tool_call_update", "tool_call", "tool_call_content_chunk"}:
            self._tool(update)
            if kind == "tool_call_content_chunk" and self.status is not None:
                tool_id = update.get("toolCallId")
                tool = next((item for item in self.status.tool_calls if item.get(
                    "tool_id") == tool_id), None)
                if tool is not None:
                    content = update.get("content", "")
                    tool["output"] = tool.get(
                        "output", "") + self._text(content)
            self._touch()

    def _touch(self) -> None:
        """_touch() 工具调用/回合状态变化后触发实时上报。"""
        if self.status is not None:
            self.status.notify()

    def text(self) -> str:
        return "".join(self._text(self._messages[key]) for key in self._message_order) + self._text(self._anonymous)


class AgentClient:
    """AgentClient 无第三方重依赖的 ACP v2 客户端，支持 stdio 与 WebSocket 两种传输。

    ``path`` 可接受可执行命令、shell 风格命令字符串、含 ``command``/``args``
    的 JSON 描述文件，或 ``ws://``/``wss://`` URL。每次 ``run`` 都会新建连接、
    完成 initialize/initialized 握手，并通过 ``session/new`` 创建全新会话。
    v2 使用 ``session/resume`` 恢复会话；``session_load`` 作为兼容名称保留，
    供将来需要加载/恢复钩子的调用方使用。
    """

    protocol_version = 2

    def __init__(self, path: str | pathlib.Path, *, timeout: float = 300.0) -> None:
        self.timeout = timeout
        self._connection: _ACPConnection | _ACPWebSocketConnection | None = None
        self._connection_lock = threading.Lock()
        self.agent_info: dict[str, Any] = {}
        self.agent_capabilities: dict[str, Any] = {}
        raw_path = os.fspath(path)
        self._is_websocket = raw_path.startswith(("ws://", "wss://"))
        if self._is_websocket:
            # WebSocket URL 不是文件系统路径，保持原样。
            self.path: str | pathlib.Path = raw_path
        elif pathlib.Path(raw_path).is_file():
            # 指向真实文件的连接描述（JSON 描述文件 / 脚本）在构造时
            # 即锚定为绝对路径，避免运行期工作目录变化导致找不到。
            self.path = _resolve_path(raw_path)
        else:
            # 其余情况是命令字符串，不能按路径解析，保持原样。
            self.path = raw_path
        self._websocket_url = raw_path if self._is_websocket else ""

    def _command(self) -> list[str]:
        if self._is_websocket:
            raise RuntimeError("WebSocket ACP endpoint has no command")
        value = os.fspath(self.path)
        candidate = pathlib.Path(value)
        if candidate.is_file() and candidate.suffix.lower() == ".json":
            try:
                loaded: object = json.loads(
                    candidate.read_text(encoding="utf-8"))
            except (OSError, json.JSONDecodeError) as exc:
                raise ValueError(
                    f"Invalid ACP connection file: {value}") from exc
            if not isinstance(loaded, dict):
                raise ValueError(
                    "ACP connection file must contain a JSON object")
            info = cast(dict[str, object], loaded)
            command_value = info.get("command", info.get("executable"))
            args_value = info.get("args", [])
            if not isinstance(command_value, str) or not isinstance(args_value, list):
                raise ValueError(
                    "ACP connection file requires command and args")
            typed_args = cast(list[object], args_value)
            command_args: list[str] = [command_value]
            for arg in typed_args:
                if isinstance(arg, (str, int, float, bool)):
                    command_args.append(str(arg))
            return command_args
        command = shlex.split(value)
        if not command:
            raise ValueError("ACP command cannot be empty")
        if candidate.is_file() and candidate.suffix.lower() == ".py":
            return [sys.executable, *command]
        return command

    def _open(self, cwd: str | pathlib.Path = ".") -> _ACPConnection | _ACPWebSocketConnection:
        if self._is_websocket:
            return _ACPWebSocketConnection(self._websocket_url, self.timeout)
        workdir = pathlib.Path(cwd).expanduser().resolve()
        if not workdir.is_dir():
            raise FileNotFoundError(
                f"ACP working directory does not exist: {workdir}")
        return _ACPConnection(self._command(), workdir, self.timeout)

    def _initialize(self, connection: _ACPConnection | _ACPWebSocketConnection) -> InitializeResult:
        params: InitializeParams = {
            "protocolVersion": self.protocol_version,
            "capabilities": {"dyn.cxykevin.top": {}},
            "info": {"name": "dynworkflow", "title": "dynworkflow", "version": "0.1.0"},
        }
        result = connection.request("initialize", cast(JsonObject, params))
        if not isinstance(result, dict) or result.get("protocolVersion") != self.protocol_version:
            raise RuntimeError(
                "ACP agent did not negotiate protocol version 2")
        info = result.get("info")
        capabilities = result.get("capabilities")
        self.agent_info = info if isinstance(info, dict) else {}
        self.agent_capabilities = capabilities if isinstance(
            capabilities, dict) else {}
        connection.send(
            {"jsonrpc": "2.0", "method": "initialized", "params": {}})
        return cast(InitializeResult, result)

    def _session_params(self, cwd: str | pathlib.Path,
                        mcp_servers: list[JsonObject] | None = None) -> SessionNewParams:
        params: SessionNewParams = {
            "cwd": str(pathlib.Path(cwd).expanduser().resolve()),
            "mcpServers": [] if mcp_servers is None else mcp_servers,
        }
        # The extension field is ACP metadata at the session params root.
        cast(JsonObject, params)["dyn.cxykevin.top/hidden"] = True
        return params

    @staticmethod
    def _session_id(result: JsonValue) -> str:
        if not isinstance(result, dict):
            raise RuntimeError("ACP session response has no sessionId")
        value = result.get("sessionId")
        if not isinstance(value, str):
            raise RuntimeError("ACP session response has no sessionId")
        return value

    def initialize(self, *, cwd: str | pathlib.Path = ".") -> dict[str, Any]:
        """初始化一条持久连接，供显式会话操作使用。"""
        with self._connection_lock:
            if self._connection is None:
                connection = self._open(cwd)
                try:
                    self._initialize(connection)
                except Exception:
                    connection.close()
                    raise
                self._connection = connection
            return {"protocolVersion": self.protocol_version, "info": self.agent_info,
                    "capabilities": self.agent_capabilities}

    def _persistent_request(self, method: str, params: JsonObject,
                            cwd: str | pathlib.Path) -> JsonValue:
        with self._connection_lock:
            if self._connection is None:
                connection = self._open(cwd)
                try:
                    self._initialize(connection)
                except Exception:
                    connection.close()
                    raise
                self._connection = connection
            return self._connection.request(method, params)

    def session_new(self, *, cwd: str | pathlib.Path = ".", model: str = "",
                    mcp_servers: list[JsonObject] | None = None) -> SessionResult:
        params = self._session_params(cwd, mcp_servers)
        if model:
            params["model"] = model
        result = self._persistent_request(
            "session/new", cast(JsonObject, params), cwd)
        if not isinstance(result, dict):
            raise RuntimeError("ACP session/new returned an invalid response")
        return cast(SessionResult, result)

    new_session = session_new

    def session_resume(self, session_id: str, *, cwd: str | pathlib.Path = ".",
                       mcp_servers: list[JsonObject] | None = None,
                       replay_from: JsonObject | None = None) -> SessionResult:
        params: SessionResumeParams = {
            "cwd": str(pathlib.Path(cwd).expanduser().resolve()),
            "mcpServers": [] if mcp_servers is None else mcp_servers,
            "sessionId": session_id,
        }
        if replay_from is not None:
            params["replayFrom"] = replay_from
        result = self._persistent_request(
            "session/resume", cast(JsonObject, params), cwd)
        if not isinstance(result, dict):
            raise RuntimeError(
                "ACP session/resume returned an invalid response")
        return cast(SessionResult, result)

    def session_load(self, session_id: str, *, cwd: str | pathlib.Path = ".",
                     mcp_servers: list[JsonObject] | None = None) -> SessionResult:
        """预留的加载接口，映射到 ACP v2 的 session/resume。"""
        return self.session_resume(session_id, cwd=cwd, mcp_servers=mcp_servers,
                                   replay_from={"type": "start"})

    load_session = session_load

    def run(self, prompt: str, *, model: str = "", cwd: str | pathlib.Path = ".",
            status: AgentStatusSingleAgent | None = None) -> str:
        """在新建的 ACP v2 连接与会话中执行一次 prompt。"""
        connection = self._open(cwd)
        collector = _UpdateCollector(status)
        try:
            self._initialize(connection)
            params = self._session_params(cwd)
            if model:
                params["model"] = model
            session = connection.request(
                "session/new", cast(JsonObject, params))
            session_id = self._session_id(session)
            if status is not None:
                status.session_id = session_id
                status.notify()
            return self._collect_prompt(connection, session_id, prompt,
                                        collector)
        finally:
            connection.close()

    execute = run

    def run_resumed(self, prompt: str, session_id: str, *, model: str = "",
                    cwd: str | pathlib.Path = ".",
                    status: AgentStatusSingleAgent | None = None) -> str:
        """在按需加载（ACP v2 ``session/resume``）的既有会话中继续执行 prompt。

        供工作流缓存命中“未结束会话”条目时调用；加载或执行失败时抛出
        异常，由调用方决定回退策略。
        """
        connection = self._open(cwd)
        collector = _UpdateCollector(status)
        try:
            self._initialize(connection)
            resume_params: SessionResumeParams = {
                "cwd": str(pathlib.Path(cwd).expanduser().resolve()),
                "mcpServers": [],
                "sessionId": session_id,
                "replayFrom": {"type": "start"},
            }
            resumed = connection.request(
                "session/resume", cast(JsonObject, resume_params))
            active_session_id = session_id
            if isinstance(resumed, dict):
                resumed_id = resumed.get("sessionId")
                if isinstance(resumed_id, str) and resumed_id:
                    active_session_id = resumed_id
            if status is not None:
                status.session_id = active_session_id
                status.notify()
            return self._collect_prompt(connection, active_session_id, prompt,
                                        collector)
        finally:
            connection.close()

    def _collect_prompt(self,
                        connection: _ACPConnection | _ACPWebSocketConnection,
                        session_id: str, prompt: str,
                        collector: _UpdateCollector) -> str:
        """发送 session/prompt 并等待回合结束，返回助手回复文本。"""
        prompt_block: TextContentBlock = {"type": "text", "text": prompt}
        prompt_params: PromptParams = {
            "sessionId": session_id, "prompt": [prompt_block]}
        connection.request(
            "session/prompt", cast(JsonObject, prompt_params),
            cast(Callable[[JsonRpcMessage], None], collector.handle),
        )
        if collector.idle_update is None:
            def is_idle(message: JsonRpcMessage) -> bool:
                params = message.get("params", {})
                update_value = params.get("update")
                if not isinstance(update_value, dict):
                    return False
                update: JsonObject = update_value
                return update.get("sessionUpdate") == "state_update" and update.get("state") == "idle"
            connection.wait_for_notification(
                cast(Callable[[JsonRpcMessage], None], collector.handle),
                is_idle, "session/prompt")
        update = collector.idle_update or {}
        stop_reason = update.get("stopReason")
        if stop_reason in {"refusal", "cancelled"}:
            raise RuntimeError(f"ACP prompt stopped: {stop_reason}")
        return collector.text()

    def close(self) -> None:
        with self._connection_lock:
            if self._connection is not None:
                self._connection.close()
                self._connection = None


def _supports_session_load(client: object) -> bool:
    """_supports_session_load() 探测客户端是否支持按需加载（resume）会话。

    只有具备 ``run_resumed`` 的客户端才会参与“未结束会话”的缓存与恢复。
    """
    return callable(getattr(client, "run_resumed", None))


def _agent_lookup(cache_ctx: CacheContext | None, prompt: str,
                  path: str) -> tuple[str, str] | None:
    """_agent_lookup() 查询 Agent 缓存条目，返回 (kind, value) 或 None"""
    if cache_ctx is None or cache_ctx.cache is None:
        return None
    info = cache_ctx.agent_key(prompt, path)
    if info is None:
        return None
    return cache_ctx.cache.get_agent(info)


def _store_agent_result(cache_ctx: CacheContext | None, prompt: str,
                        path: str, result: Any) -> None:
    """_store_agent_result() 缓存 Agent 正常结束的运行结果"""
    if cache_ctx is None or cache_ctx.cache is None:
        return
    info = cache_ctx.agent_key(prompt, path)
    if info is not None:
        cache_ctx.cache.put_agent(info, "result", result)


def _store_agent_session(cache_ctx: CacheContext | None, prompt: str,
                         path: str, session_id: str) -> None:
    """_store_agent_session() 仅记录未结束会话的 session id"""
    if cache_ctx is None or cache_ctx.cache is None:
        return
    info = cache_ctx.agent_key(prompt, path)
    if info is not None:
        cache_ctx.cache.put_agent(info, "session", session_id)


class Agent:
    """通过 ACP v2 执行的子 Agent。

    ``path`` 在构造时会立即基于当前工作目录解析为绝对路径，
    之后即使进程切换工作目录也不受影响。
    """

    def __init__(self, prompt: str, /, *,
                 model: Literal["haiku", "sonnet", "opus", ""] | str = "",
                 path: str | pathlib.Path = ".", structure_output: bool = False,
                 acp: AgentClientInterface | None = None,
                 retry: int = 3) -> None:
        if retry < 0:
            raise ValueError("retry must be non-negative")
        self.prompt = prompt
        self.model = model
        self.path = _resolve_path(path)
        self.structure_output = structure_output
        self.acp = acp
        self.retry = retry
        self._call_info: AgentStatusSingleAgent | None = None

    def run_with_status(self, call_info: AgentStatus,
                        status: AgentStatusSingleAgent, acp: AgentClientInterface | None,
                        *,
                        cache_ctx: CacheContext | None = None,
                        report: bool = False) -> Any:
        reporter = optional_reporter(report)
        if reporter is not None:
            # 绑定后，任何状态赋值（含随后的 RUNNING）都会自动上报。
            reporter.bind(status)
        self._call_info = status
        status.status = AgentStatusEnum.RUNNING
        client = acp or self.acp
        if client is None:
            status.status = AgentStatusEnum.FAILURE
            raise RuntimeError("No ACP client configured for Agent")
        # === 缓存查询 ===
        entry = _agent_lookup(cache_ctx, self.prompt, self.path)
        if entry is not None:
            kind, value = entry
            if kind == "result":
                # 正常缓存命中：直接返回结果
                status.status = AgentStatusEnum.SUCCESS
                return value
            # kind == "session"：按需使用 ACP load 恢复会话后继续执行；
            # 加载失败则回退到全新会话真实执行。
            if _supports_session_load(client):
                try:
                    result = cast(Any, client).run_resumed(
                        self.prompt, value, model=self.model,
                        cwd=self.path, status=status)
                except Exception:
                    pass
                else:
                    result = self._apply_structure_output(result)
                    _store_agent_result(cache_ctx, self.prompt, self.path,
                                        result)
                    status.status = AgentStatusEnum.SUCCESS
                    return result
        try:
            result = client.run(self.prompt, model=self.model,
                                cwd=self.path, status=status)
        except Exception:
            # 未正常结束：仅记录 session id 供下次 ACP load 续跑；
            # 客户端不支持加载时不对未结束会话做任何缓存。
            if status.session_id and _supports_session_load(client):
                _store_agent_session(cache_ctx, self.prompt, self.path,
                                     status.session_id)
            status.status = AgentStatusEnum.FAILURE
            raise
        result = self._apply_structure_output(result)
        _store_agent_result(cache_ctx, self.prompt, self.path, result)
        status.status = AgentStatusEnum.SUCCESS
        return result

    def _apply_structure_output(self, result: Any) -> Any:
        """_apply_structure_output() structure_output 开启时尝试解析 JSON"""
        if self.structure_output and isinstance(result, str):
            try:
                return json.loads(result)
            except json.JSONDecodeError:
                return result
        return result

    def run(self, call_info: AgentStatus, acp: AgentClientInterface | None = None,
            *, cache_ctx: CacheContext | None = None,
            report: bool = False, node_id: str = "",
            call_index: int = 0) -> Any:
        """启动该 Agent，并在整个 ACP 会话期间持续更新其状态。

        ``node_id``/``call_index`` 标识所属节点与节点内第几次调用，
        会随状态上报一并推送。
        """
        status = AgentStatusSingleAgent(
            prompt=self.prompt, path=str(self.path), node_id=node_id,
            call_index=call_index, agent_index=1, agent_count=1)
        call_info.append(status)
        group = _AgentGroup([self], [status], acp or self.acp, cache_ctx,
                            report, 1, call_info, node_id, call_index)
        _register_agent_group(node_id, call_index, group)
        try:
            return group.wait_all()[0]
        finally:
            _unregister_agent_group(node_id, call_index)


class _AgentGroup:
    """一次 Agent/MultiAgent 调用的可控运行组。"""

    def __init__(self, agents: list[Agent] | tuple[Agent, ...],
                 statuses: list[AgentStatusSingleAgent],
                 client: AgentClientInterface | None, cache_ctx: CacheContext | None,
                 report: bool, concurrency: int, call_info: AgentStatus,
                 node_id: str, call_index: int) -> None:
        self.agents = list(agents)
        self.retry_limits = [agent.retry for agent in self.agents]
        self.statuses = statuses
        self.client = client
        self.cache_ctx = cache_ctx
        self.report = report
        self.call_info = call_info
        self.node_id = node_id
        self.call_index = call_index
        self.results: list[Any] = [None] * len(self.agents)
        self.errors: list[BaseException | None] = [None] * len(self.agents)
        self.threads: list[threading.Thread | None] = [None] * len(self.agents)
        self.tids: list[int | None] = [None] * len(self.agents)
        self.cancelled: list[bool] = [False] * len(self.agents)
        self.gate = threading.Semaphore(concurrency)

    @property
    def done(self) -> bool:
        return all(t is None or not t.is_alive() for t in self.threads)

    def _spawn(self, index: int) -> None:
        def worker() -> None:
            self.tids[index] = threading.get_ident()
            try:
                with self.gate:
                    result = self.agents[index].run_with_status(
                        self.call_info, self.statuses[index], self.client,
                        cache_ctx=self.cache_ctx, report=self.report)
                self.results[index] = result
            except BaseException as exc:
                if self.cancelled[index]:
                    self.statuses[index].status = AgentStatusEnum.FAILURE
                    self.results[index] = None
                else:
                    self.errors[index] = exc
        thread = threading.Thread(target=worker, daemon=True)
        self.threads[index] = thread
        thread.start()

    def start(self) -> None:
        for index in range(len(self.agents)):
            self._spawn(index)

    def wait_all(self) -> list[Any]:
        self.start()
        # retry() 可能在等待期间替换某个 worker；持续观察当前线程槽位。
        while True:
            for thread in list(self.threads):
                if thread is not None:
                    thread.join(0.1)
            if self.done:
                break
        for error in self.errors:
            if error is not None:
                raise error
        return list(self.results)

    def terminate(self, index: int) -> bool:
        if self.done or not 0 <= index < len(self.threads):
            return False
        thread = self.threads[index]
        tid = self.tids[index]
        if thread is None or not thread.is_alive() or tid is None:
            return False
        self.cancelled[index] = True
        return raise_in_thread(tid, AgentCancelled)

    def retry(self, index: int) -> bool:
        if self.done or not 0 <= index < len(self.threads):
            return False
        # retry 表示额外重试次数；attempt=1 是首次执行。
        if self.statuses[index].attempt > self.retry_limits[index]:
            return False
        thread = self.threads[index]
        tid = self.tids[index]
        if thread is not None and thread.is_alive():
            if tid is None:
                return False
            self.cancelled[index] = True
            if not raise_in_thread(tid, AgentCancelled):
                return False
            thread.join(5.0)
            if thread.is_alive():
                return False
        self.statuses[index].reset_for_retry()
        self.cancelled[index] = False
        self.errors[index] = None
        self.results[index] = None
        self._spawn(index)
        return True


_AGENT_GROUPS: dict[tuple[str, int], _AgentGroup] = {}
_AGENT_GROUPS_LOCK = threading.Lock()


def _register_agent_group(node_id: str, call_index: int, group: _AgentGroup) -> None:
    with _AGENT_GROUPS_LOCK:
        _AGENT_GROUPS[(node_id, call_index)] = group


def _unregister_agent_group(node_id: str, call_index: int) -> None:
    with _AGENT_GROUPS_LOCK:
        _AGENT_GROUPS.pop((node_id, call_index), None)


def find_agent_group(node_id: str, call_index: int) -> _AgentGroup | None:
    with _AGENT_GROUPS_LOCK:
        return _AGENT_GROUPS.get((node_id, call_index))


class AgentCancelled(BaseException):
    """Agent 被 stdin 控制命令终止。"""


def raise_in_thread(thread_id: int, exception: type[BaseException]) -> bool:
    """向指定 Python 线程注入异步异常。"""
    result = ctypes.pythonapi.PyThreadState_SetAsyncExc(
        ctypes.c_ulong(thread_id), ctypes.py_object(exception))
    return int(result) == 1


class MultiAgent:
    """并发运行多个子 Agent，遵循配置的并发数。"""

    def __init__(self, *agents: Agent, concurrency: int = 3,
                 model: Literal["haiku", "sonnet", "opus", ""] | str = "",
                 path: str | pathlib.Path = ".", structure_output: bool = False,
                 acp: AgentClientInterface | None = None) -> None:
        if concurrency < 1:
            raise ValueError("concurrency must be at least 1")
        self.agents = agents
        self.concurrency = concurrency
        self.acp = acp
        resolved_path = _resolve_path(path)
        for agent in self.agents:
            agent.model = model
            agent.path = resolved_path
            agent.structure_output = structure_output
            if acp is not None:
                agent.acp = acp

    def run(self, call_info: AgentStatus, acp: AgentClientInterface | None = None,
            *, cache_ctx: CacheContext | None = None, report: bool = False,
            node_id: str = "", call_index: int = 0) -> list[Any]:
        """并发运行所有子 Agent，并支持运行中终止或重试。"""
        client = acp or self.acp
        total = len(self.agents)
        statuses = [AgentStatusSingleAgent(
            prompt=agent.prompt, path=str(agent.path), node_id=node_id,
            call_index=call_index, agent_index=index + 1, agent_count=total)
            for index, agent in enumerate(self.agents)]
        call_info.extend(statuses)
        if not self.agents:
            return []
        group = _AgentGroup(self.agents, statuses, client, cache_ctx, report,
                            self.concurrency, call_info, node_id, call_index)
        _register_agent_group(node_id, call_index, group)
        try:
            return group.wait_all()
        finally:
            _unregister_agent_group(node_id, call_index)


def gen_conn_info_from_env() -> AgentClient | None:
    value = os.environ.get("ALKAID0_WORKFLOW_CONN_INFO")
    return AgentClient(path=value) if value else None
