# DataAcquisitionManager CPU 占用分析

## 结论
高 CPU 主要由**主动忙等 + 1ms 高频发送 + 高优先级线程**叠加造成，核心热点并不在业务计算本身。

## 关键热点

1. `GetCollectVol()` 中有 `while (true)` 内层循环，当串口队列为空时直接 `continue`，没有任何 `Sleep/Delay`，会形成忙等（spin）并持续占用一个核心。
2. `ExecuteDischarge()` 使用 `ThreadPriority.Highest` + `timeBeginPeriod(1)` + 目标 1ms 发送节拍，并在临界区使用 `Thread.SpinWait(20)`，该组合会显著提升调度与 CPU 压力。
3. 高频日志：`SocketClient.taskt()` 每秒固定输出队列日志，`DataAcquisitionManager` 还有异步日志分发和文件刷盘线程，数据高峰时会增加额外 CPU 与 I/O 压力。
4. `ProcessDataFrameAsync()` 在每帧都做 JSON 序列化与（可选）加密批处理，数据量大时会放大 CPU 消耗，但通常属于次要热点。

## 建议

- 优先修复内层忙等：串口队列为空时增加 `await Task.Delay(1~5)` 或改用阻塞式消费。
- 将发送频率从 1ms 调整为设备可接受的更低频率（如 2~10ms），并避免最高线程优先级。
- 降低非关键日志频率（如队列长度日志改为 5~10s 一次，或仅在阈值变化时输出）。
- 对 `ProcessDataFrameAsync` 做采样 profiler 验证，确认是否需要减少序列化次数、复用对象。
