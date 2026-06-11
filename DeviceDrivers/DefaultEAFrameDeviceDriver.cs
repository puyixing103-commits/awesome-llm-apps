using System;
using System.Collections.Concurrent;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

using DataAcquisitionLibrary;
using DataAcquisitionLibrary.Utils;

using DC_0003.Core.Interfaces;
using DC_0003.Models;
using DC_0003.Services.Implements;
using DC_0003.Services.Implements.Processors;
using DC_0003.Services.Infrastructure;

using Newtonsoft.Json;

using SocketA0Demo;

namespace DC_0003.Services.DeviceDrivers
{
    /// <summary>
    /// 默认 EA 帧协议设备驱动 — 封装当前系统的 TCP Socket + 串口采集逻辑。
    /// 实现 IDeviceCollector 接口，替换设备时只需创建新的驱动类。
    ///
    /// 封装内容：
    ///   - TCP 连接 / 断开 / 自动重连（通过 SocketClient）
    ///   - EA 帧放电数据采集（ExecuteDischarge 逻辑）
    ///   - EA 帧波形数据采集（ExecuteWaveform 逻辑，通过 TcpCollectorClient）
    ///   - 串口电压采集（SerialPortDataReader）
    ///   - 帧解析和事件回调
    ///   - 可选：集成策略、处理器和上传器
    /// </summary>
    public class DefaultEAFrameDeviceDriver : IDeviceCollector
    {
        private readonly LogServices _logServices = LogServices.Instance;
        private readonly SessionState _state;

        // TCP 客户端（放电数据用）
        private SocketA0Demo.SocketClient _socketClient;

        // 波形数据采集客户端
        private DC_0003.Services.Implements.TcpCollectorClient _waveformClient;

        // 串口电压读取器
        private SerialPortDataReader _serialPortReader;

        // 策略和处理器（可选，注入后自动在帧回调中使用）
        private IVoltageStrategy _voltageStrategy;
        private IDischargeStrategy _dischargeStrategy;
        private IDataFrameProcessor _frameProcessor;
        private IJsonUploader _jsonUploader;

        // 批处理
        private readonly ConcurrentQueue<string> _batchQueue = new ConcurrentQueue<string>();
        private int _batchCount;
        private int _batchSize = 500;
        private readonly object _batchLock = new object();
        private string _deviceCode;
        private string _dataCollectDeviceNo;
        private string _macAddress;

        // 采集任务引用
        private Task _dischargeTask;
        private Task _waveformTask;
        private Task _serialTask;
        private Task _heartbeatTask;

        private string _host;
        private int _port;
        private string _dischargeCommandHex;
        private string _waveformCommandHex;
        private bool _disposed;

        // --- IDeviceCollector 实现 ---
        public string Name => "EA-Frame-Device-Driver";
        public bool IsConnected { get; private set; }
        public bool IsCollecting { get; private set; }

        public event EventHandler<byte[]> OnRawDataReceived;
        public event EventHandler<bool> OnConnectionStateChanged;
        public event EventHandler<Exception> OnError;

        public DefaultEAFrameDeviceDriver(SessionState state)
        {
            _state = state ?? throw new ArgumentNullException(nameof(state));
        }

        /// <summary>
        /// 注入策略、处理器和上传器（可选，注入后帧回调会自动处理数据）
        /// </summary>
        public void SetStrategies(
            IVoltageStrategy voltageStrategy,
            IDischargeStrategy dischargeStrategy,
            IDataFrameProcessor frameProcessor = null,
            IJsonUploader jsonUploader = null)
        {
            _voltageStrategy = voltageStrategy;
            _dischargeStrategy = dischargeStrategy;
            _frameProcessor = frameProcessor;
            _jsonUploader = jsonUploader;
        }

        /// <summary>设置设备标识信息（用于构建上传数据）</summary>
        public void SetDeviceInfo(string deviceCode, string dataCollectDeviceNo, string macAddress)
        {
            _deviceCode = deviceCode;
            _dataCollectDeviceNo = dataCollectDeviceNo;
            _macAddress = macAddress;
        }

        /// <summary>设置批处理大小</summary>
        public void SetBatchSize(int batchSize) => _batchSize = batchSize;

        /// <summary>
        /// 使用 CollectConfig 初始化设备参数（替代原来 Init 中的硬编码逻辑）
        /// </summary>
        public void Configure(CollectConfig config)
        {
            _host = config.Host;
            _port = config.Port;
            _dischargeCommandHex = "";  // 由外部设置
            _waveformCommandHex = "";
        }

        /// <summary>设置放电采集命令（十六进制字符串）</summary>
        public void SetDischargeCommand(string hexCommand) => _dischargeCommandHex = hexCommand;

        /// <summary>设置波形采集命令（十六进制字符串）</summary>
        public void SetWaveformCommand(string hexCommand) => _waveformCommandHex = hexCommand;

        /// <summary>初始化串口读取器</summary>
        public void SetSerialPortReader(SerialPortDataReader reader) => _serialPortReader = reader;

        // --- 连接 ---

        public async Task ConnectAsync(CancellationToken cancellationToken = default)
        {
            _logServices.Info($"正在连接设备 {_host}:{_port}...");

            _socketClient = new SocketA0Demo.SocketClient(_state.Cts);
            bool connected = await _socketClient.Start(_host, _port);

            if (connected)
            {
                IsConnected = true;
                _logServices.Info($"设备 {_host}:{_port} 连接成功");
                OnConnectionStateChanged?.Invoke(this, true);

                // 启动心跳
                _heartbeatTask = Task.Run(() => HeartbeatLoopAsync(_state.Cts.Token));
            }
            else
            {
                IsConnected = false;
                _logServices.Error($"设备 {_host}:{_port} 连接失败");
            }
        }

        public void Disconnect()
        {
            _logServices.Info("正在断开设备连接...");
            IsConnected = false;
            IsCollecting = false;

            try { _socketClient?.Stop(); } catch { }
            try { _waveformClient?.Disconnect(); } catch { }
            try { _waveformClient?.Dispose(); } catch { }

            OnConnectionStateChanged?.Invoke(this, false);
            _logServices.Info("设备连接已断开");
        }

        // --- 数据采集 ---

        public async Task StartCollectAsync(CollectConfig config, CancellationToken cancellationToken = default)
        {
            if (!IsConnected)
            {
                _logServices.Warning("设备未连接，无法启动采集");
                return;
            }

            IsCollecting = true;
            _logServices.Info("数据采集开始");

            // 初始化策略
            _voltageStrategy?.Initialize();
            _dischargeStrategy?.Initialize();

            // 挂载帧解析回调
            _socketClient.OnFrameParsed -= OnFrameParsed;
            _socketClient.OnFrameParsed += OnFrameParsed;

            // 启动串口电压采集
            if (_serialPortReader != null && config.EnableSerialVoltage)
            {
                _serialTask = Task.Run(() => SerialCollectLoopAsync(_state.Cts.Token));
            }

            // 启动放电数据采集（高优先级线程，1ms 精度）
            _dischargeTask = Task.Run(() => DischargeCollectLoopAsync(config, _state.Cts.Token));

            // 启动波形数据采集
            _waveformTask = Task.Run(() => WaveformCollectLoopAsync(config, _state.Cts.Token));
        }

        public async Task StopCollectAsync()
        {
            IsCollecting = false;
            _logServices.Info("正在停止数据采集...");

            try { _state.Cts?.Cancel(); } catch { }

            // 等待任务结束
            if (_dischargeTask != null) await Task.WhenAny(_dischargeTask, Task.Delay(3000));
            if (_waveformTask != null) await Task.WhenAny(_waveformTask, Task.Delay(3000));

            _logServices.Info("数据采集已停止");
        }

        public async Task SendHeartbeatAsync(CancellationToken cancellationToken = default)
        {
            if (!IsConnected || _socketClient == null) return;

            try
            {
                // 通过 SocketClient 发送空数据触发心跳（实际心跳在各采集循环中独立维护）
                await Task.CompletedTask;
            }
            catch (Exception ex)
            {
                _logServices.Warning($"心跳发送异常: {ex.Message}");
            }
        }

        // --- 采集循环 ---

        /// <summary>放电数据采集循环（高精度 1ms）</summary>
        private async Task DischargeCollectLoopAsync(CollectConfig config, CancellationToken token)
        {
            await Task.Delay(2000, token); // 等待连接稳定

            if (string.IsNullOrEmpty(_dischargeCommandHex))
            {
                _logServices.Warning("放电采集命令未配置");
                return;
            }

            byte[] sendData = StringUtil.HexStringToByteArray(_dischargeCommandHex);

            uint timeBeginPeriod = 1;
            WinmmHelper.TimeBeginPeriod(timeBeginPeriod);

            try
            {
                long freq = System.Diagnostics.Stopwatch.Frequency;
                long ticksPerMs = freq / 1000;
                long nextTick = System.Diagnostics.Stopwatch.GetTimestamp() + ticksPerMs;

                while (!token.IsCancellationRequested && IsConnected)
                {
                    try
                    {
                        WaitUntilStopwatchTicks(nextTick, ticksPerMs, token);

                        if (!token.IsCancellationRequested && IsConnected)
                        {
                            await _socketClient.SendAsync(sendData);
                        }

                        nextTick += ticksPerMs;
                        long now = System.Diagnostics.Stopwatch.GetTimestamp();
                        if (now > nextTick + ticksPerMs + 2)
                            nextTick = now + ticksPerMs;
                    }
                    catch (OperationCanceledException) { break; }
                    catch (Exception ex)
                    {
                        _logServices.Error($"放电采集异常: {ex.Message}\n{ex}");
                        OnError?.Invoke(this, ex);
                    }
                }
            }
            finally
            {
                WinmmHelper.TimeEndPeriod(timeBeginPeriod);
            }
        }

        /// <summary>波形数据采集循环</summary>
        private async Task WaveformCollectLoopAsync(CollectConfig config, CancellationToken token)
        {
            await Task.Delay(2500, token);

            if (string.IsNullOrEmpty(_waveformCommandHex))
            {
                _logServices.Warning("波形采集命令未配置");
                return;
            }

            byte[] sendData = StringUtil.HexStringToByteArray(_waveformCommandHex);

            _waveformClient = new DC_0003.Services.Implements.TcpCollectorClient(_host, _port);
            _waveformClient.OnLog += (msg) => _logServices.Debug(msg);

            try
            {
                while (!token.IsCancellationRequested && IsConnected)
                {
                    try
                    {
                        await _waveformClient.SendRequestAsync(sendData);
                        await Task.Delay(config.WaveformIntervalMs, token);
                    }
                    catch (OperationCanceledException) { break; }
                    catch (Exception ex)
                    {
                        _logServices.Error($"波形采集异常: {ex.Message}\n{ex}");
                    }
                }
            }
            finally
            {
                try { _waveformClient?.Disconnect(); } catch { }
            }
        }

        /// <summary>串口电压采集循环</summary>
        private async Task SerialCollectLoopAsync(CancellationToken token)
        {
            if (_serialPortReader == null) return;

            while (!token.IsCancellationRequested)
            {
                try
                {
                    await _serialPortReader.ReadAsStringAsync("");
                    await Task.Delay(500, token);
                }
                catch (OperationCanceledException) { break; }
                catch (Exception ex)
                {
                    _logServices.Error($"串口采集异常: {ex.Message}\n{ex}");
                }
            }
        }

        /// <summary>心跳保活循环</summary>
        private async Task HeartbeatLoopAsync(CancellationToken token)
        {
            while (!token.IsCancellationRequested && IsConnected)
            {
                try
                {
                    // 心跳通过 SocketClient 发送实际数据帧时隐式维持
                    await Task.Delay(30000, token);
                    _logServices.Debug("心跳信号");
                }
                catch (OperationCanceledException) { break; }
            }
        }

        // --- 帧回调（桥接 SocketClient → 事件） ---

        private async Task OnFrameParsed(DC_0003.Models.DataFrame frame)
        {
            if (frame?.Payload != null)
            {
                OnRawDataReceived?.Invoke(this, frame.Payload);

                // 如果注入了处理器和上传器，自动处理并上传数据
                if (_frameProcessor != null && _jsonUploader != null)
                {
                    try
                    {
                        double voltage = _voltageStrategy?.GetCurrentValue() ?? 0;
                        double discharge = _dischargeStrategy?.GetCurrentValue() ?? 0;

                        var processed = _frameProcessor.ProcessFrame(frame.Payload, voltage, discharge);
                        if (processed != null)
                        {
                            // 包装为上传格式
                            string jsonItem = JsonConvert.SerializeObject(processed);
                            EnqueueForBatchUpload(jsonItem);
                        }
                    }
                    catch (Exception ex)
                    {
                        _logServices.Error($"帧数据处理失败: {ex.Message}\n{ex}");
                    }
                }
            }
            await Task.CompletedTask;
        }

        /// <summary>
        /// 将数据项加入批处理队列，达到 batchSize 时自动上传
        /// </summary>
        private void EnqueueForBatchUpload(string jsonItem)
        {
            lock (_batchLock)
            {
                _batchQueue.Enqueue(jsonItem);
                _batchCount++;

                if (_batchCount >= _batchSize)
                {
                    FlushBatch();
                }
            }
        }

        /// <summary>刷新批处理队列并上传</summary>
        private void FlushBatch()
        {
            var items = new System.Collections.Generic.List<string>();
            lock (_batchLock)
            {
                while (_batchQueue.TryDequeue(out string item))
                {
                    items.Add($"{{\"plain\":{item}}}");
                }
                _batchCount = 0;
            }

            if (items.Count == 0) return;

            long timestamp = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds();
            string itemsArray = $"[{string.Join(",", items)}]";
            string requestJson = $"{{\"timestamp\":{timestamp},\"deviceMac\":\"{_macAddress}\",\"deviceCode\":\"{_deviceCode}\",\"dataCollectDeviceNO\":\"{_dataCollectDeviceNo}\",\"items\":{itemsArray}}}";

            _logServices.Debug($"批量数据就绪: {items.Count} 条，准备上传");

            // 使用Task.Run避免在lock内执行异步操作，异常可被观察
            _ = Task.Run(async () =>
            {
                try
                {
                    bool success = await _jsonUploader.UploadAsync(requestJson);
                    if (!success)
                    {
                        _logServices.Warning("批量数据上传失败");
                    }
                }
                catch (Exception ex)
                {
                    _logServices.Error($"批量数据上传异常: {ex.Message}");
                }
            });
        }

        // --- 高精度等待 ---

        private static void WaitUntilStopwatchTicks(long targetTicks, long ticksPerMs, CancellationToken token)
        {
            while (!token.IsCancellationRequested)
            {
                long now = System.Diagnostics.Stopwatch.GetTimestamp();
                if (now >= targetTicks) return;

                long remain = targetTicks - now;
                if (remain > ticksPerMs)
                    Thread.Sleep(1);
                else if (remain > ticksPerMs / 2)
                    Thread.Sleep(0);
                else
                    Thread.SpinWait(8);
            }
        }

        // --- 释放 ---

        public void Dispose()
        {
            if (_disposed) return;
            _disposed = true;

            Disconnect();
            try { _serialPortReader?.Dispose(); } catch { }
            GC.SuppressFinalize(this);
        }
    }

    /// <summary>Winmm 辅助类（时间精度控制）</summary>
    internal static class WinmmHelper
    {
        [System.Runtime.InteropServices.DllImport("winmm.dll")]
        private static extern uint timeBeginPeriod(uint uPeriod);

        [System.Runtime.InteropServices.DllImport("winmm.dll")]
        private static extern uint timeEndPeriod(uint uPeriod);

        public static void TimeBeginPeriod(uint period) => timeBeginPeriod(period);
        public static void TimeEndPeriod(uint period) => timeEndPeriod(period);
    }
}
