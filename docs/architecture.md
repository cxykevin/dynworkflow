# 架构说明

## 运行时分层

`dynworkflow` 将工作流编排、Agent 执行、持久化和状态上报分开处理：

- `src/dynworkflow/flow.py`：定义 `Flow`、`Node` 和任务调度。
- `src/dynworkflow/agent.py`：管理 ACP 客户端、Agent 组、终止、重试和 Agent 状态。
- `src/dynworkflow/cache.py`：保存工作流和节点缓存记录。
- `src/dynworkflow/reporter.py`：发送 JSONL 事件并转发节点线程日志。

## 工作流生命周期

1. `Flow.node()` 注册 `Node`，并初始化函数参数缓存。
2. 调用节点函数时返回 `Execute` 对象，不会立即执行函数。
3. `Flow.execute()` 冻结流程图，填充初始任务参数，生成流程图元数据并启动任务循环。
4. 任务循环为每个已就绪节点启动 daemon 线程。
5. 节点完成后可以继续加入下游 `Execute` 任务。
6. 任务循环等待任务队列为空且所有节点线程结束。
7. 收集到的节点异常会统一组成 `ExceptionGroup` 抛出。

## 异常隔离

节点执行由 `Node._run()` 包装。普通异常会在节点线程内处理：

- 节点状态立即变为 `ERROR`；
- 异常保存到 `Flow._error`；
- 启用状态上报时立即发送 error 事件；
- 节点线程结束，不向调度器传播异常；
- 兄弟节点继续运行；
- 调度器清空任务后，`Flow._raise_exceptions()` 统一抛出已保存的异常。

终止异常属于独立的控制流信号。终止节点只影响被选中的节点；工作流关闭会将流程标记为中止，并中断正在运行的节点。

## 线程与进程模型

`Flow.execute()` 在当前进程中运行调度器，并为节点使用 daemon 线程。`Flow.run()` 在支持 fork 的平台为工作流启动 fork 子进程，父进程负责监听 stdin 控制命令和清理子进程；没有 fork 的平台（如 Windows）在当前进程内执行工作流，由 daemon 线程把 stdin 命令转发到控制队列。

stdin 读取在 daemon 线程中执行，因为遍历 stdin 可能永久阻塞。父进程独立监控子进程是否存活，并在清理时发送关闭命令或强制终止子进程。

## 缓存

节点缓存键包含工作流标识、节点标识、会话作用域、函数代码哈希和已填充参数。缓存的节点描述可以表示：

- 没有返回值；
- 一个终值 `Result`；
- 用于重放的下游 Execute 调用。

## 状态上报

`StatusReporter` 将事件序列化为 JSONL，并使用 bracketed-paste 标记包裹输出。上报失败会被吞掉，因此管道关闭或 stdout 不可用不会破坏工作流执行。

节点线程中的 `print()` 会临时转为 `node_log` 事件。类级写锁用于避免多个并发 Reporter 实例的输出互相交错。
