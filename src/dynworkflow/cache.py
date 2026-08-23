# coding: utf-8
"""WorkflowCache 工作流 SQLite 持久缓存。

缓存内容分两类：
- ``agent_cache``：Agent 运行结果（MultiAgent 按单个 agent 分别缓存），
  以及未正常结束会话的 ACP sessionId（供下次按需 ACP load 续跑）；
- ``node_cache``：节点执行结果——终值（Result/None）或下游调度描述
  （Execute 链序列化为 [目标节点 id + 调用时参数]，命中时重建重放）。

联合键统一为 [会话作用域 + 工作流名 + 节点 ID + 载荷 hash]；节点载荷
包含函数源码体 hash（代码变更即失效）与全部有效参数。
任一成分无法 JSON 序列化时该条目不读也不写。
所有 sqlite 故障都降级为“无缓存”，绝不影响工作流运行。
"""

from __future__ import annotations

import hashlib
import json
import os
import pathlib
import sqlite3
import sys
import threading
import time
from dataclasses import dataclass
from typing import Any

DEFAULT_CACHE_DIRNAME = ".alkaid0"
DEFAULT_DB_FILENAME = "workflow.db"

_SCHEMA = """
CREATE TABLE IF NOT EXISTS agent_cache (
    key TEXT PRIMARY KEY,
    kind TEXT NOT NULL CHECK(kind IN ('result', 'session')),
    value TEXT NOT NULL,
    session_scope TEXT NOT NULL DEFAULT '',
    workflow TEXT NOT NULL,
    node_id TEXT NOT NULL,
    payload_hash TEXT NOT NULL,
    updated_at REAL NOT NULL
);
CREATE TABLE IF NOT EXISTS node_cache (
    key TEXT PRIMARY KEY,
    value TEXT NOT NULL,
    session_scope TEXT NOT NULL DEFAULT '',
    workflow TEXT NOT NULL,
    node_id TEXT NOT NULL,
    payload_hash TEXT NOT NULL,
    updated_at REAL NOT NULL
);
"""


def default_db_path() -> pathlib.Path:
    """default_db_path() 当前工作目录下的默认缓存库路径 .alkaid0/workflow.db"""
    return pathlib.Path.cwd() / DEFAULT_CACHE_DIRNAME / DEFAULT_DB_FILENAME


def canonical_json(value: Any) -> str:
    """canonical_json() 确定性 JSON 序列化，作为哈希与存储的规范形式"""
    return json.dumps(value, sort_keys=True, ensure_ascii=False,
                      separators=(",", ":"))


def try_hash(value: Any) -> str | None:
    """try_hash() 值可 JSON 序列化时返回 sha256，否则返回 None（不可序列化）"""
    try:
        payload = canonical_json(value)
    except (TypeError, ValueError, OverflowError):
        return None
    return hashlib.sha256(payload.encode("utf-8")).hexdigest()


def _try_serialize(value: Any) -> str | None:
    """_try_serialize() 可序列化则返回规范 JSON 文本，否则 None"""
    try:
        return canonical_json(value)
    except (TypeError, ValueError, OverflowError):
        return None


def union_key(session_scope: str, workflow: str, node_id: str,
              payload_hash: str) -> str:
    """union_key() [会话作用域+工作流名+节点ID+载荷hash] 的联合键"""
    joined = "\x1f".join((session_scope, workflow, node_id, payload_hash))
    return hashlib.sha256(joined.encode("utf-8")).hexdigest()


@dataclass(frozen=True)
class CacheKey:
    """CacheKey 联合键及其组成成分（成分写入观测列，便于排查）"""

    key: str
    session_scope: str
    workflow: str
    node_id: str
    payload_hash: str


@dataclass(frozen=True)
class CacheContext:
    """CacheContext 单次节点执行对应的缓存上下文

    ``code_hash`` 为节点函数源码体的 sha256：函数体变更即产生新键，
    旧缓存自动失效。为 None（源码不可得）时不做节点缓存。
    """

    cache: WorkflowCache | None
    workflow: str
    node_id: str
    session_scope: str
    code_hash: str | None = None

    def agent_key(self, prompt: str, cwd: str) -> CacheKey | None:
        """agent_key() Agent 运行的联合键（prompt+workdir 拼合 hash）"""
        return self._make_key(try_hash({"prompt": prompt, "cwd": cwd}))

    def node_key(self, args: dict[str, Any]) -> CacheKey | None:
        """node_key() 节点的联合键（代码体 hash + 函数所有参数拼合 hash）

        参数或代码体哈希不可用时返回 None（不缓存）。
        """
        if self.code_hash is None:
            return None
        return self._make_key(try_hash({"code": self.code_hash, "args": args}))

    def _make_key(self, payload_hash: str | None) -> CacheKey | None:
        if payload_hash is None or self.cache is None:
            return None
        return CacheKey(
            key=union_key(self.session_scope, self.workflow, self.node_id,
                          payload_hash),
            session_scope=self.session_scope,
            workflow=self.workflow,
            node_id=self.node_id,
            payload_hash=payload_hash,
        )


class WorkflowCache:
    """WorkflowCache 基于 SQLite 的容错缓存。

    任一操作失败都会把实例标记为 broken 并降级为“读未命中/写忽略”，
    保证缓存层故障不影响工作流本身。线程安全（内部互斥锁 + WAL）。
    """

    def __init__(self, db_path: str | os.PathLike[str]) -> None:
        self.db_path = pathlib.Path(db_path)
        self._lock = threading.Lock()
        self._broken = False
        self._conn: sqlite3.Connection | None = None
        try:
            self.db_path.parent.mkdir(parents=True, exist_ok=True)
            connection = sqlite3.connect(str(self.db_path),
                                         check_same_thread=False)
            connection.execute("PRAGMA journal_mode=WAL")
            connection.executescript(_SCHEMA)
            connection.commit()
            self._conn = connection
        except (OSError, sqlite3.Error) as exc:
            self._disable(exc)

    # === 内部工具 ===

    def _disable(self, exc: Exception) -> None:
        """_disable() 首次故障时提示一次并停用缓存"""
        already_broken = self._broken
        self._broken = True
        self._conn = None
        if not already_broken:
            print(f"[dynworkflow] workflow cache disabled: {exc}",
                  file=sys.stderr)

    def get_agent(self, info: CacheKey) -> tuple[str, str] | None:
        """get_agent() 查询 Agent 条目，返回 (kind, value)；未命中返回 None"""
        conn = self._conn
        if conn is None:
            return None
        try:
            with self._lock:
                row = conn.execute(
                    "SELECT kind, value FROM agent_cache WHERE key = ?",
                    (info.key,),
                ).fetchone()
        except sqlite3.Error as exc:
            self._disable(exc)
            return None
        if row is None:
            return None
        kind = str(row[0])
        if kind not in {"result", "session"}:
            return None
        try:
            parsed: Any = json.loads(str(row[1]))
        except json.JSONDecodeError:
            return None
        return kind, parsed

    def put_agent(self, info: CacheKey, kind: str, value: Any) -> bool:
        """put_agent() 写入 Agent 条目；value 不可序列化时忽略"""
        serialized = _try_serialize(value)
        if serialized is None or kind not in {"result", "session"}:
            return False
        return self._put("agent_cache", info, serialized, extra_kind=kind)

    def get_node(self, info: CacheKey) -> dict[str, Any] | None:
        """get_node() 查询节点条目。

        返回形如 ``{"kind": "none"}``、``{"kind": "result", "value": ...}``
        或 ``{"kind": "executes", "items": [{"node_id": ..., "args": ...}]}``；
        格式非法视为未命中。
        """
        conn = self._conn
        if conn is None:
            return None
        try:
            with self._lock:
                row = conn.execute(
                    "SELECT value FROM node_cache WHERE key = ?",
                    (info.key,),
                ).fetchone()
        except sqlite3.Error as exc:
            self._disable(exc)
            return None
        if row is None:
            return None
        try:
            parsed: Any = json.loads(str(row[0]))
        except json.JSONDecodeError:
            return None
        if not isinstance(parsed, dict):
            return None
        kind = parsed.get("kind")
        if kind not in {"none", "result", "executes"}:
            return None
        if kind == "executes":
            items = parsed.get("items")
            if not isinstance(items, list):
                return None
            for item in items:
                if (not isinstance(item, dict)
                        or not isinstance(item.get("node_id"), str)
                        or not isinstance(item.get("args"), dict)):
                    return None
        return parsed

    def put_node(self, info: CacheKey, descriptor: dict[str, Any]) -> bool:
        """put_node() 写入节点条目；描述符不可整体序列化时忽略"""
        payload = _try_serialize(descriptor)
        if payload is None:
            return False
        return self._put("node_cache", info, payload, extra_kind=None)

    def close(self) -> None:
        """close() 关闭底层连接（进程退出前非必须调用）"""
        conn = self._conn
        self._conn = None
        if conn is not None:
            try:
                with self._lock:
                    conn.close()
            except sqlite3.Error:
                pass

    def _put(self, table: str, info: CacheKey, serialized: str,
             *, extra_kind: str | None) -> bool:
        """_put() INSERT OR REPLACE 一条记录；kind 仅 agent_cache 使用"""
        conn = self._conn
        if conn is None:
            return False
        columns = ("key, value, session_scope, workflow, node_id,"
                   " payload_hash, updated_at")
        placeholders = "?, ?, ?, ?, ?, ?, ?"
        params: list[Any] = [
            info.key, serialized, info.session_scope, info.workflow,
            info.node_id, info.payload_hash, time.time(),
        ]
        if extra_kind is not None:
            columns += ", kind"
            placeholders += ", ?"
            params.append(extra_kind)
        sql = (f"INSERT OR REPLACE INTO {table} ({columns}) "
               f"VALUES ({placeholders})")
        try:
            with self._lock:
                conn.execute(sql, params)
                conn.commit()
        except sqlite3.Error as exc:
            self._disable(exc)
            return False
        return True
