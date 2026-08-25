"""ACP v2 WebSocket 传输测试。"""

import json
import threading
from typing import Any

import pytest

from src.dynworkflow.agent import AgentClient


class _FakeAcpWebSocketHandler:
    """最小化的 ACP v2 agent，每条 WebSocket 帧承载一条 JSON 消息。"""

    def __init__(self) -> None:
        self.received: list[dict[str, Any]] = []

    def __call__(self, connection: Any) -> None:
        for raw in connection:
            if isinstance(raw, bytes):
                raw = raw.decode("utf-8")
            message = json.loads(raw)
            self.received.append(message)
            method = message.get("method")
            request_id = message.get("id")
            reply: dict[str, Any] | None = None
            if method == "initialize":
                reply = {"jsonrpc": "2.0", "id": request_id, "result": {
                    "protocolVersion": 2,
                    "capabilities": {},
                    "info": {"name": "fake-ws", "version": "1"},
                }}
            elif method == "session/new":
                reply = {"jsonrpc": "2.0", "id": request_id,
                         "result": {"sessionId": "ws-session"}}
            elif method == "session/prompt":
                sid = message["params"]["sessionId"]
                updates = [
                    {"sessionUpdate": "agent_message", "messageId": "m1",
                     "content": [{"type": "text", "text": "ws hello"}]},
                    {"sessionUpdate": "state_update", "state": "idle",
                     "stopReason": "end_turn"},
                ]
                for update in updates:
                    connection.send(json.dumps({
                        "jsonrpc": "2.0", "method": "session/update",
                        "params": {"sessionId": sid, "update": update},
                    }))
                reply = {"jsonrpc": "2.0", "id": request_id, "result": {}}
            if reply is not None:
                connection.send(json.dumps(reply))


@pytest.fixture
def ws_server() -> Any:
    try:
        from websockets.sync.server import serve
    except ImportError as exc:  # pragma: no cover - 环境兜底
        pytest.skip(f"websockets unavailable: {exc}")

    handler = _FakeAcpWebSocketHandler()
    server = serve(handler, "127.0.0.1", 0)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    yield type("Server", (), {
        "server": server,
        "handler": handler,
        "port": server.socket.getsockname()[1],
    })()
    server.shutdown()


def test_websocket_acp_v2_lifecycle(ws_server: Any) -> None:
    acp = AgentClient(f"ws://127.0.0.1:{ws_server.port}/acp?k=a", timeout=5)
    status: list[Any] = []
    result = acp.run("hi", status=status[0] if status else None)
    assert result == "ws hello"
    methods = [message.get("method") for message in ws_server.handler.received]
    assert methods == ["initialize", "initialized", "session/new", "session/prompt"]
    initialize = ws_server.handler.received[0]
    assert initialize["params"]["protocolVersion"] == 2
    assert initialize["params"]["capabilities"] == {"dyn.cxykevin.top": {}}
    session_new = ws_server.handler.received[2]
    assert session_new["params"]["dyn.cxykevin.top/hidden"] is True
