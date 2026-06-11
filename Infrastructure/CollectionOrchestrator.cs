using System;
using System.Threading.Tasks;

using DataAcquisitionLibrary;

using DC_0003.Core.Interfaces;
using DC_0003.Models;
using DC_0003.Services.Implements.Processors;

namespace DC_0003.Services.Infrastructure
{
    /// <summary>
    /// 采集编排器 — 替代原始 DataAcquisitionManager 上帝类（~250行 vs ~1500行）。
    /// 只负责编排调度，具体设备逻辑在 IDeviceCollector 中，数据处理在 IDataPipeline 中。
    ///
    /// 职责：
    ///   1. 接收配置 → 初始化设备驱动 + 管道
    ///   2. 启停采集 → 委托给 IDeviceCollector
    ///   3. 组件生命周期管理
    ///   4. 对外的统一入口
    /// </summary>
    public class CollectionOrchestrator
    {
        private readonly IDeviceCollector _deviceCollector;
        private readonly IConfigProvider _configProvider;
        private readonly LogServices _logServices = LogServices.Instance;
        private readonly IDataPipeline _dataPipeline;
        private readonly SessionState _state;

        // 可选策略（注入后由设备驱动使用）
        private readonly IVoltageStrategy _voltageStrategy;
        private readonly IDischargeStrategy _dischargeStrategy;
        private readonly IDataFrameProcessor _frameProcessor;
        private readonly IJsonUploader _jsonUploader;

        // 外部组件
        private TcpConfigReceiver _tcpConfigReceiver;
        private PortChecker _portChecker;
        private JavaServiceManager _javaServiceManager;

        // 事件（供 UI 绑定）
        public event Action<string, bool> OnLogOutput;
        public event Action<string> OnStateChanged;

        /// <summary>当前是否正在采集</summary>
        public bool IsCollecting => _state.IsDataCollecting;

        /// <summary>设备是否已连接</summary>
        public bool IsConnected => _deviceCollector?.IsConnected ?? false;

        /// <summary>当前配置</summary>
        public AppConfig CurrentConfig { get; private set; }

        public CollectionOrchestrator(
            IDeviceCollector deviceCollector,
            IConfigProvider configProvider,
            IDataPipeline dataPipeline,
            SessionState state,
            IVoltageStrategy voltageStrategy = null,
            IDischargeStrategy dischargeStrategy = null,
            IDataFrameProcessor frameProcessor = null,
            IJsonUploader jsonUploader = null)
        {
            _deviceCollector = deviceCollector ?? throw new ArgumentNullException(nameof(deviceCollector));
            _configProvider = configProvider ?? throw new ArgumentNullException(nameof(configProvider));
            _dataPipeline = dataPipeline ?? throw new ArgumentNullException(nameof(dataPipeline));
            _state = state ?? throw new ArgumentNullException(nameof(state));

            _voltageStrategy = voltageStrategy;
            _dischargeStrategy = dischargeStrategy;
            _frameProcessor = frameProcessor;
            _jsonUploader = jsonUploader;

            // 管道批量就绪 → 数据上传（这里暂用日志记录）
            if (_dataPipeline is DataUploadPipeline pipeline)
            {
                pipeline.OnBatchReady += async (json) =>
                {
                    _logServices.Debug($"批量数据就绪: {json}");
                    await Task.CompletedTask;
                };
            }
        }

        /// <summary>
        /// 启动组件基础设施：
        ///   - TCP 配置接收器（接收配置 JSON 和启停指令）
        ///   - 端口检查器（检查 8080 是否可达）
        ///   - Java 服务管理
        ///   - 日志后台任务
        /// </summary>
        public async Task BootstrapAsync()
        {
            OutputLog("正在启动基础设施组件...");

            // TCP 配置接收器（7000 端口）
            _tcpConfigReceiver = new TcpConfigReceiver(7000, "数采配置文件.json", () => _state.IsDataCollecting, OnConfigReceived);
            _tcpConfigReceiver.OnLog += (msg) => OutputLog(msg, false);
            _tcpConfigReceiver.StopCommade += () => HandleStopCommand(null);
            _tcpConfigReceiver.StartCommade += HandleStartCommand;
            await _tcpConfigReceiver.StartListenAsync();

            // 端口检查器
            _portChecker = new PortChecker(8080);
            _portChecker.OnLog += (msg) => OutputLog(msg, false);
            _portChecker.RestartJavaServiceAsync += RestartJavaServiceAsync;
            _portChecker.Start();

            // Java 服务
            _javaServiceManager = new JavaServiceManager();
            _javaServiceManager.OnOutputLog += (msg) => OutputLog(msg, false);

            OutputLog("基础设施组件启动完成");
        }

        /// <summary>停止所有组件</summary>
        public async Task ShutdownAsync()
        {
            OutputLog("正在关闭所有组件...");

            await _deviceCollector.StopCollectAsync();
            _deviceCollector.Disconnect();

            try { _portChecker?.Stop(); } catch { }
            try { _tcpConfigReceiver?.Stop(); } catch { }
            try { (_javaServiceManager as IDisposable)?.Dispose(); } catch { }
            _state.Shutdown();

            OutputLog("所有组件已关闭");
        }

        // --- 配置加载 ---

        private void OnConfigReceived(string jsonConfig)
        {
            try
            {
                if (_configProvider is ConfigManager cm)
                    cm.LoadFromJson(jsonConfig);

                CurrentConfig = (_configProvider as ConfigManager)?.AppConfig;
                _state.IsConfigLoaded = true;
                OutputLog("配置已接收并加载");

                // 根据配置初始化设备驱动
                InitializeDeviceFromConfig();
            }
            catch (Exception ex)
            {
                _logServices.Error($"配置加载失败: {ex.Message}\n{ex}");
                OutputLog($"配置加载失败: {ex.Message}", true);
            }
        }

        private void InitializeDeviceFromConfig()
        {
            if (CurrentConfig == null) return;

            var config = new CollectConfig
            {
                Host = CurrentConfig.IP?.value ?? "127.0.0.1",
                Port = int.TryParse(CurrentConfig.PORT?.value, out int port) ? port : 8100,
                DischargeIntervalMs = int.TryParse(CurrentConfig.INTERVAL?.value, out int interval) ? interval : 1,
                WaveformIntervalMs = 1000,
                SerialPortName = CurrentConfig.SerialPort?.value ?? "COM1",
                SerialBaudRate = int.TryParse(CurrentConfig.BaundRate?.value, out int baudRate) ? baudRate : 9600,
                EnableSerialVoltage = !string.IsNullOrEmpty(CurrentConfig.SerialPort?.value),
                ReconnectIntervalMs = 3000,
            };

            if (_deviceCollector is DeviceDrivers.DefaultEAFrameDeviceDriver driver)
            {
                driver.Configure(config);

                // 设置放电和波形命令
                var iniManager = (_configProvider as ConfigManager);
                string dischargeCmd = CurrentConfig.TcpCommand?.value
                    ?? iniManager?.GetIniValue("NEWDATACOLLECTION", "COMMAND");
                string waveformCmd = CurrentConfig.TcpWaveCommand?.value
                    ?? iniManager?.GetIniValue("NEWDATACOLLECTION", "WAVEFORMCOMMAND");

                driver.SetDischargeCommand(dischargeCmd);
                driver.SetWaveformCommand(waveformCmd);

                // 设置串口读取器
                if (config.EnableSerialVoltage)
                {
                    var serialReader = new SerialPortDataReader(
                        config.SerialPortName, 2, CurrentConfig.Command?.value, 500,
                        config.SerialBaudRate, System.IO.Ports.Parity.None,
                        int.TryParse(CurrentConfig.DataBits?.value, out int dataBits) ? dataBits : 8,
                        Enum.TryParse<System.IO.Ports.StopBits>(CurrentConfig.StopBits?.value ?? "One", out System.IO.Ports.StopBits sb) ? sb : System.IO.Ports.StopBits.One,
                        0, 48, _state.Cts);
                    driver.SetSerialPortReader(serialReader);
                }
            }

            OutputLog($"设备驱动已配置: {config.Host}:{config.Port}");
        }

        // --- 启停命令 ---

        private async void HandleStartCommand(string json)
        {
            if (_state.IsDataCollecting)
            {
                OutputLog("数据采集已在运行中", true);
                _logServices.Warning("收到开始采集指令，但数据采集已在运行中");
                return;
            }
            if (!_state.IsConfigLoaded || CurrentConfig == null)
            {
                OutputLog("请先通过 TCP 发送配置文件", true);
                _logServices.Warning("收到开始采集指令，但配置未加载");
                return;
            }

            _state.IsDataCollecting = true;
            OutputLog("正在启动数据采集...", true);

            try
            {
                _state.ResetCancellationToken();
                InitializeDeviceFromConfig();

                await _deviceCollector.ConnectAsync(_state.Cts.Token);

                if (_deviceCollector.IsConnected)
                {
                    var config = BuildCollectConfig();
                    await _deviceCollector.StartCollectAsync(config, _state.Cts.Token);
                    OutputLog("数据采集已启动", true);
                    OnStateChanged?.Invoke("collecting");
                }
                else
                {
                    _state.IsDataCollecting = false;
                    OutputLog("设备连接失败，采集未启动", true);
                    OnStateChanged?.Invoke("error");
                }
            }
            catch (Exception ex)
            {
                _state.IsDataCollecting = false;
                _logServices.Error($"启动采集失败: {ex.Message}\n{ex}");
                OutputLog($"启动采集失败: {ex.Message}", true);
                OnStateChanged?.Invoke("error");
            }
        }

        private async void HandleStopCommand(string json)
        {
            if (!_state.IsDataCollecting)
            {
                OutputLog("数据采集未运行", true);
                return;
            }

            await _deviceCollector.StopCollectAsync();
            _state.IsDataCollecting = false;
            OutputLog("数据采集已停止", true);
            OnStateChanged?.Invoke("idle");
        }

        public async Task StartCollectionAsync()
        {
            HandleStartCommand(null);
            await Task.CompletedTask;
        }

        public async Task StopCollectionAsync()
        {
            HandleStopCommand(null);
            await Task.CompletedTask;
        }

        /// <summary>
        /// 从 JSON 字符串加载配置并启动采集（用于直接加载文件场景）。
        /// 等效于原 DataAcquisitionManager.Init() + HandleStartCommand()
        /// </summary>
        public async Task LoadConfigAndStartAsync(string jsonConfig)
        {
            OnConfigReceived(jsonConfig);
            await StartCollectionAsync();
        }

        // --- Java 服务 ---

        private async Task RestartJavaServiceAsync()
        {
            var iniManager = (_configProvider as ConfigManager);
            var javaJar = iniManager?.GetIniValue("NEWDATACOLLECTION", "JarPath");
            var javaHome = @"C:\Program Files\Common Files\Oracle\Java\javapath";

            _javaServiceManager.OnOutputLog -= OnJavaLog;
            await Task.Delay(100);
            _javaServiceManager.OnOutputLog += OnJavaLog;

            await _javaServiceManager.RestartJavaServiceAsync(javaHome, javaJar);
        }

        private void OnJavaLog(string msg) => OutputLog(msg, false);

        // --- 日志输出 ---

        private void OutputLog(string message, bool isFlag = false)
        {
            _logServices.Info(message);
            OnLogOutput?.Invoke(message, isFlag);
        }

        // --- 辅助 ---

        private CollectConfig BuildCollectConfig()
        {
            return new CollectConfig
            {
                Host = CurrentConfig?.IP?.value ?? "127.0.0.1",
                Port = int.TryParse(CurrentConfig?.PORT?.value, out int p) ? p : 8100,
                DischargeIntervalMs = int.TryParse(CurrentConfig?.INTERVAL?.value, out int interval) ? interval : 1,
                WaveformIntervalMs = 1000,
                SerialPortName = CurrentConfig?.SerialPort?.value ?? "COM1",
                SerialBaudRate = int.TryParse(CurrentConfig?.BaundRate?.value, out int br) ? br : 9600,
                EnableSerialVoltage = !string.IsNullOrEmpty(CurrentConfig?.SerialPort?.value),
                ReconnectIntervalMs = 3000,
                HeartbeatIntervalSec = 30,
            };
        }
    }
}
