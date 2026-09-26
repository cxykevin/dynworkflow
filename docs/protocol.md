# 协议说明

本文档说明 `Flow.run()` 暴露的面向机器的协议以及 JSONL 状态上报协议，供接入宿主进程或 UI 的开发者阅读。

## stdin 控制协议

`Flow.run()` 从 stdin 中逐行读取 JSON 对象。无效 JSON 行会被忽略。读取到的对象通过 multiprocessing 队列转发给工作流子进程。

### 关闭工作流

```json
{"cmd":"shutdown"}
```

工作流会被标记为中止，正在运行的节点线程会收到工作流中止信号。随后父进程等待子进程优雅退出；如果仍未退出，则强制终止子进程。

### 节点控制

```json
{"cmd":"node","nodeId":"scan_docs","action":"terminate"}
```

字段：

- `cmd`：必须为 `node`。
- `nodeId`：已注册的节点函数 ID。
- `action`：`terminate` 或 `restart`。

`terminate` 只中断指定节点。`restart` 会先中断指定节点，短暂等待线程结束，并在条件允许时重新启动该节点。

### Agent 控制

```json
{"cmd":"agent","nodeId":"scan_project","callIndex":1,"agentIndex":1,"action":"retry"}
```

字段：

- `cmd`：必须为 `agent`。
- `nodeId`：所属节点 ID。
- `callIndex`：节点内 Agent/MultiAgent 调用序号，从 1 开始。
- `agentIndex`：本次调用中的 Agent 序号，从 1 开始。
- `action`：`terminate` 或 `retry`。

## stdout 状态上报协议

启用上报后，每个事件都会作为一行 JSON 对象输出。输出使用 bracketed-paste 标记包裹：

- 开始标记：`\u001b[?2004h`
- 结束标记：`\u001b[?2004l`

每个事件都包含 UTC ISO-8601 格式的 `time` 字段。

### graph

工作流图生成后发送。

```json
{"type":"graph","time":"...","workflow":"flow-1","graph":{"nodes":{},"edges":{},"start":[]}}
```

`graph` 包含 `nodes`、`edges` 和 `start`。`nodes` 的键是节点 id（节点函数名），值里的 `name` 是 `@flow.node("...")` 的显示名。

### node

节点生命周期状态发生变化时发送。

```json
{"type":"node","time":"...","workflow":"flow-1","nodeId":"scan_docs","state":"running","cached":false,"args":{}}
```

`state` 通常为 `running`、`done` 或 `error`。`cached` 表示是否命中缓存，`args` 包含节点已填充的参数。

### node_result

节点函数返回 `Result(value)` 时发送（缓存命中重放同样发送），携带该节点的终值。

```json
{"type":"node_result","time":"...","workflow":"flow-1","nodeId":"scan_docs","result":"..."}
```

`result` 为 `Result` 携带的值；不可 JSON 序列化时序列化为字符串。返回 `None` 或只返回下游任务的节点不发送该事件。

### agents_start

节点即将启动 Agent 或 MultiAgent 调用时发送。

```json
{"type":"agents_start","time":"...","workflow":"flow-1","nodeId":"scan_docs","callIndex":1,"count":1,"prompts":["Summarize docs"]}
```

### agent

单个 Agent 状态变化时发送。

字段包括：

- `prompt`、`path`；
- `state`：`waiting`、`running`、`success` 或 `failure`；
- `sessionId`；
- `nodeId`、`callIndex`、`agentIndex`、`agentCount`；
- `attempt`；
- `tools`：工具调用快照。

### node_code

节点启动时发送。

```json
{"type":"node_code","time":"...","workflow":"flow-1","nodeId":"scan_docs","name":"Scan Docs","code":"..."}
```

如果无法通过源码检查获取节点函数源码，`code` 为 `null`。

### node_log

节点线程中调用 `print()` 时生成的按行事件。

```json
{"type":"node_log","time":"...","workflow":"flow-1","nodeId":"scan_docs","message":"Scan Docs Start"}
```

状态上报采用尽力而为策略。管道断开以及 Reporter 序列化或写入错误会被忽略，不会改变工作流执行结果。
