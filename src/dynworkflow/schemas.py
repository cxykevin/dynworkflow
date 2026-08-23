"""ACP v2 客户端使用的类型化 JSON schema 定义。"""

from __future__ import annotations

from typing import Literal, NotRequired, TypeAlias, TypedDict


JsonPrimitive: TypeAlias = None | bool | int | float | str
JsonValue: TypeAlias = JsonPrimitive | list["JsonValue"] | dict[str, "JsonValue"]
JsonObject: TypeAlias = dict[str, JsonValue]


class JsonRpcError(TypedDict):
    code: int
    message: str
    data: NotRequired[JsonValue]


class JsonRpcMessage(TypedDict, total=False):
    jsonrpc: Literal["2.0"]
    id: int | str | None
    method: str
    params: JsonObject
    result: JsonValue
    error: JsonRpcError


class ClientInfo(TypedDict):
    name: str
    title: NotRequired[str]
    version: str


class InitializeParams(TypedDict):
    protocolVersion: int
    capabilities: JsonObject
    info: ClientInfo


class InitializeResult(TypedDict):
    protocolVersion: int
    capabilities: JsonObject
    info: JsonObject
    authMethods: NotRequired[list[JsonObject]]


class TextContentBlock(TypedDict):
    type: Literal["text"]
    text: str


class SessionNewParams(TypedDict):
    cwd: str
    mcpServers: list[JsonObject]
    model: NotRequired[str]


class SessionResumeParams(SessionNewParams):
    sessionId: str
    replayFrom: NotRequired[JsonObject]


class SessionResult(TypedDict):
    sessionId: str


class PromptParams(TypedDict):
    sessionId: str
    prompt: list[TextContentBlock]


class SessionUpdate(TypedDict, total=False):
    sessionUpdate: str
    messageId: str
    content: JsonValue
    state: str
    stopReason: str
    toolCallId: str
    title: str
    kind: str
    status: str
    rawInput: JsonValue
    rawOutput: JsonValue


class SessionUpdateParams(TypedDict):
    sessionId: str
    update: SessionUpdate
