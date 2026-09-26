# dynworkflow

`dynworkflow` 是一个 Python 3.12 库，用于编排动态工作流，工作流节点可以调用一个或多个 ACP Agent。

## 安装

使用项目包管理器安装：

```bash
uv sync
```

运行时依赖为 `websockets`，开发依赖包括 `pytest` 和 `mypy`。

## 快速开始

```python
import dynworkflow as d

flow = d.Flow(
    "document-flow",
    agent=d.AgentClient("ws://127.0.0.1:7433/acp?k=a"),
)

@flow.node("summarize")
def summarize() -> d.TaskGenerator:
    result = yield d.Agent("Summarize the documents in docs/test", path="docs/test")
    return d.Result(result)

flow.execute(summarize())
```

使用 `@flow.node(name)` 声明节点。普通节点可以返回 `None`、`Result` 或一个或多个下游 `Execute` 任务。生成器节点可以 `yield Agent` 或 `MultiAgent`，接收 Agent 结果后返回节点结果。

## 公共 API

- `Flow`：创建并执行工作流。
- `Flow.node(name)`：注册节点函数。
- `Flow.execute(*tasks)`：在当前进程中执行任务，并在所有节点结束后抛出节点异常。
- `Flow.run(*tasks)`：在 fork 子进程中运行工作流，并监听 stdin 中的 JSONL 控制命令。
- `Agent`：描述一次 Agent 请求。
- `MultiAgent`：并发运行多个 Agent 请求。
- `AgentClient`：连接 ACP WebSocket 端点。
- `Result`：保存节点的最终结果。

## 执行语义

- 相互独立的节点会并发运行。
- 节点失败后会立即变为 `error` 状态，并在启用上报时立即发送状态。
- 失败节点不会停止兄弟节点。
- 所有节点异常会在最后统一以 `ExceptionGroup` 抛出。
- 节点和 Agent 的终止命令只影响指定目标；工作流关闭会终止整个工作流。

## 缓存与状态上报

缓存默认启用。可以使用 `Flow(..., cache=False)` 关闭缓存。缓存文件位于 `.alkaid0/workflow.db`；设置 `ALKAID0_WORKFLOW_SESSION_ID` 可以隔离不同会话。

可以通过 `Flow(..., report=True)` 或环境变量 `ALKAID0_WORKFLOW_REPORT=1` 开启 JSONL 状态上报。`Flow.run()` 会自动开启上报。事件包括 `graph`、`node`、`node_result`、`agent`、`agents_start`、`node_code` 和 `node_log`。

## 示例

参见 `example.py`，其中包含使用 `AgentClient`、`Agent`、`MultiAgent`、分支和回退参数的完整工作流示例。

## 测试

运行测试套件：

```bash
python -m pytest -q
```

运行静态编译检查：

```bash
python -m compileall -q src tests
```

## 文档索引

- `docs/SKILL.md`：仅供 AI Agent 读取的使用说明，保持英文。
- `docs/architecture.md`：面向人的实现与生命周期说明。
- `docs/protocol.md`：stdin 控制命令和 JSONL 上报事件说明。
- `docs/test/ai_must_read.md`：示例工作流读取的测试文档。
