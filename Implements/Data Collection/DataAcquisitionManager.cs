﻿﻿﻿﻿﻿﻿﻿﻿﻿﻿﻿﻿﻿﻿﻿﻿﻿﻿﻿﻿﻿﻿﻿﻿﻿﻿﻿﻿﻿﻿﻿﻿﻿﻿﻿﻿using System;
using System.Collections;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.IO.Ports;
using System.Linq;
using System.Net.Sockets;
using System.Runtime.InteropServices;
using System.Runtime.InteropServices.WindowsRuntime;
using System.Runtime.Remoting.Messaging;
using System.Text;
using System.Text.Json;
using System.Text.Json.Nodes;
using System.Text.RegularExpressions;
using System.Threading;
using System.Threading.Channels;
using System.Threading.Tasks;
using System.Windows.Input;

using DataAcquisitionLibrary;
using DataAcquisitionLibrary.Utils;

using DC_0003;
using DC_0003.Core.Constants;
using DC_0003.Models;
using DC_0003.Services.Implements;
using DC_0003.Services.Infrastructure;
using DC_0003.Core.Interfaces;

using Newtonsoft.Json;
using Newtonsoft.Json.Linq;

using SocketA0Demo;



using static System.Net.Mime.MediaTypeNames;
using static DC_0003.Services.Implements.Data_Collection.Class2;
using static HttpUploader;

using JsonSerializer = System.Text.Json.JsonSerializer;

public class DataAcquisitionManager
{

    private const int MaxLogQueueSize = 20000;

    public static AppConfig _config;
   // private IUploader _dataUploader;
    private TcpConfigReceiver _TcpConfigReceiver;
    private NativeApiServer _nativeApiServer;
    private IKeyServiceClient key;
    private readonly ILogServices _logServices = LogServices.Instance;
    public static CancellationTokenSource _cts;
    private int _dataQueueCount;
    private int _droppedDataFrames;
    public static readonly ConcurrentQueue<string> _dataQueueStr = new ConcurrentQueue<string>();
    private static int _logQueueCount;
    private static int _droppedLogs;
    private static readonly ConcurrentQueue<string> _fileLogQueue = new ConcurrentQueue<string>();
    private readonly Channel<string> _recvQueueStr = Channel.CreateBounded<string>(
    new BoundedChannelOptions(20000)
    {
        FullMode = BoundedChannelFullMode.DropOldest,
        SingleWriter = false,
        SingleReader = false
    });
    private static readonly object _fileLogLock = new object();
    private static CancellationTokenSource _fileLogCts;
    private static Task _fileLogFlushTask;
    private Task _dischargeTask;
    private Task _waveformTask;
    public event Action<string, bool> OnLogOutputEvent;
    private bool _isRunning = true;
    public SocketClient socketClient;
    private static readonly object lockObj = new object();
    private static string logPath = "数据采集";
    private static string logFile;
    public static volatile bool _isDataCollecting = false;
    private static string macAddress=> InstructionConstants.GetMacAddress();
    private readonly ConcurrentQueue<DataMessage> _dataMessageQueue = new ConcurrentQueue<DataMessage>();
    private readonly int _maxPendingMessages = 10000;    // 根据实际配置
    private readonly int _batchUploadSize = 30;        // 根据实际配置
    private int _queueCount = 0;
    //private readonly SemaphoreSlim _batchUploadSemaphore = new SemaphoreSlim(1, 1);
    private PortChecker portChecker = null;
    private JavaServiceManager _javaServiceManagers = new JavaServiceManager();


    [DllImport("winmm.dll")]
    private static extern uint timeBeginPeriod(uint uPeriod);

    [DllImport("winmm.dll")]
    private static extern uint timeEndPeriod(uint uPeriod);

    private static void WaitUntilStopwatchTicks(long targetTicks, long ticksPerMs, CancellationToken token)
    {
        // 剩余很小的时候才自旋，避免 CPU 峰值；剩余较大时让出时间片。
        while (!token.IsCancellationRequested)
        {
            long now = Stopwatch.GetTimestamp();
            if (now >= targetTicks) return;

            long remain = targetTicks - now;
            if (remain > ticksPerMs)
            {
                Thread.Sleep(1);
            }
            else if (remain > ticksPerMs / 2)
            {
                Thread.Sleep(0);
            }
            else
            {
                Thread.SpinWait(8);
            }
        }
    }

    public DataAcquisitionManager()
    {
        try
        {
            
            _ = StartProcessTaskOutLog();
            StartFileLogFlushTask();
            _TcpConfigReceiver = new TcpConfigReceiver(7000, "数采配置文件.json", () => _isRunning, Init);
            _TcpConfigReceiver.StopCommade += StopDataCollection;
            _TcpConfigReceiver.StartCommade += HandleStartCommand;
            _ =_TcpConfigReceiver.StartListenAsync();


            portChecker = new PortChecker(8080);
            portChecker.OnLog += (msg) => OutputLog(msg);
            portChecker.GetReceiceAsync += GetReceiceAsync;
            portChecker.RestartJavaServiceAsync += RestartJavaServiceAsync;
            portChecker.Stop();
            portChecker.Start();

         



        }
        catch (Exception ex)
        {
            _logServices.Error(ex.TargetSite.ToString() + "\r\n" + ex.ToString());
        }
    }

    public void HandleStartCommand(string json)
    {
        if (_isDataCollecting)
        {
            OutputLog("数据采集已在运行中");
            _logServices.Warning("收到开始采集指令，但数据采集已在运行中，忽略该指令");
            return;
        }
        if (_config == null)
        {
            OutputLog("请先通过 TCP 发送配置文件");
            _logServices.Warning("收到开始采集指令，但配置文件未加载，无法启动数据采集");
            return;
        }
        _isDataCollecting = true;
        _ = Task.Run(async () =>
        {
            bool connected = await socketClient.Start(_config.IP?.value, int.TryParse(_config.PORT?.value, out int port) ? port : 0);
            if (connected)
            {
                socketClient.OnFrameParsed -= SocketClient_OnFrameParsed;
                socketClient.OnFrameParsed += SocketClient_OnFrameParsed;
                // 初始化电压分阶段采集和局放采集
                InitVoltagePhaseCollection();
                InitDischargePhaseCollection();
                Execute();
                ExecuteDischarge();
               
                ExecuteWaveform();
            }
            else
            {
                _isDataCollecting = false;
            }
        }).ContinueWith(t => { if (t.Exception != null) _logServices.Error($"异步任务异常: {t.Exception}"); });
    }
  private readonly SemaphoreSlim _batchUploadSemaphore = new SemaphoreSlim(1, 1);

    public void HandleStopCommand(string json)
    {
        if (!_isDataCollecting)
        {
            OutputLog("数据采集未运行");
            _logServices.Warning("数据采集未运行");
            return;
        }
        StopDataCollection();
    }

    public void StopDataCollection()
    {
        try{
             _isDataCollecting = false;
            socketClient.Stop();
            _cts.Cancel();
            _logServices.Info("数据采集已停止");
         
        }
         catch 
         (Exception ex) 
         { 
            _logServices.Warning(ex.TargetSite.ToString() + "\r\n" + ex.ToString()); }
        finally{ OutputLog("数据采集已停止");}
       
    }

    public void Init(string config)
    {
        try
        {
            if (_cts != null)
            {
                _cts.Cancel();
                _cts.Dispose();
                _cts = null;
            }
        }
        catch 
        {
        }
        _cts = new CancellationTokenSource();
        socketClient = new SocketClient(_cts);
        // 2. 配置反序列化选项（解决大小写、空值等问题）
        var options = new JsonSerializerOptions
        {
            PropertyNameCaseInsensitive = true,
            ReadCommentHandling = JsonCommentHandling.Skip,
            AllowTrailingCommas = true // 允许末尾逗号
        };
        // 3. 反序列化为对象
        _config = JsonConvert.DeserializeObject<AppConfig>(config);

        var serd = JsonConvert.SerializeObject(_config);
        serialPortReader = new SerialPortDataReader(_config.SerialPort?.value ?? "COM1", 2, _config.Command?.value, 500, int.TryParse(_config.BaundRate?.value, out int baundRate) ? baundRate : 9600, Parity.None, int.TryParse(_config.DataBits?.value, out int dataBits) ? dataBits : 8, Enum.TryParse<StopBits>(_config.StopBits?.value ?? "One", out StopBits stopBits) ? stopBits : StopBits.One, 0, 48, _cts);

        //是否使用密钥
        if (bool.TryParse(_config.EnableEncryption?.value, out bool enableEncryption) && enableEncryption)
        {
            key = new KeyServiceClient(new KeyServiceConfig
            {
                url = _config.KeyServiceUrl?.value,
                timeout = int.TryParse(_config.KeyServiceTimeout?.value, out int keyTimeout) ? keyTimeout : 30,
                authToken = _config.KeyServiceAuthToken?.value
            });
        }
        else
        {
            storageUploadManager = new StorageUploadManager(new KeyServiceConfig
            {
                url = _config.KeyServiceUrl?.value,
                timeout = int.TryParse(_config.KeyServiceTimeout?.value, out int keyTimeout2) ? keyTimeout2 : 30,
                authToken = _config.KeyServiceAuthToken?.value
            });
        }
        currentCounts = 0;
        currentBatchs = new StringBuilder();
        currentCount = 0;
        currentBatch = new StringBuilder();
        _logServices.Info($"数据采集配置初始化完成，当前配置：{config}");
        
    }

    StringBuilder currentBatchs = null;
    int currentCounts = 0;
    const int BATCH_SIZEs = 500;//1000;

    // 全局变量
    StringBuilder currentBatch = null;
    int currentCount = 0;
    const int BATCH_SIZE = 500;//;
    private StorageUploadManager storageUploadManager = null;
    private static SerialPortDataReader serialPortReader = null;
 
    private string voltage = "0";
    private readonly object lockObject = new object();

    // 分阶段随机电压/局放生成控制
    private DateTime _voltagePhaseStartTime = DateTime.MinValue;
    private DateTime _dischargePhaseStartTime = DateTime.MinValue;
    private int _currentVoltagePhase = 0; // 0: 未开始, 1: 电压阶段1, 2: 电压阶段2
    private int _currentDischargePhase = 0; // 0: 未开始, 1: 局放采集中
    private readonly Random _random = new Random();
    private readonly object _voltagePhaseLock = new object();
    private readonly object _dischargePhaseLock = new object();
    
    // 顺序采集控制 - 电压
    private double _currentVoltageSequentialValue = 0;
    private bool _isVoltageAscending = true; // true: 递增, false: 递减
    
    // 顺序采集控制 - 局放
    private double _currentDischargeSequentialValue = 0;
    private bool _isDischargeAscending = true; // true: 递增, false: 递减

    // 电压阶段配置
    private const int VOLTAGE_PHASE1_DURATION_MINUTES = 5;  // 电压第一阶段持续时间(分钟)
    private const int VOLTAGE_PHASE2_DURATION_MINUTES = 5;  // 电压第二阶段持续时间(分钟)

    // 局放阶段配置
    private const int DISCHARGE_PHASE_DURATION_MINUTES = 10; // 局放采集持续时间(分钟)

    // 电压范围配置
    private const double VOLTAGE_PHASE1_MIN = 342.54;
    private const double VOLTAGE_PHASE1_MAX = 418.66;
    private const double VOLTAGE_PHASE2_MIN = 693.0;
    private const double VOLTAGE_PHASE2_MAX = 847.0;
    
    // 局放范围配置
    private const double DISCHARGE_MIN = 120.0;
    private const double DISCHARGE_MAX = 135.0;

    public void SetVoltage(string newVoltage)
    {
        lock (lockObject)
        {
            voltage = newVoltage;
        }
    }

    public string GetVoltage()
    {
        lock (lockObject)
        {
            return voltage;
        }
    }

    /// <summary>
    /// 初始化电压分阶段采集
    /// </summary>
    public void InitVoltagePhaseCollection()
    {
        lock (_voltagePhaseLock)
        {
            _voltagePhaseStartTime = DateTime.Now;
            _currentVoltagePhase = 1;
            _logServices.Info($"电压分阶段采集已初始化，阶段1开始时间: {_voltagePhaseStartTime:yyyy-MM-dd HH:mm:ss}");
        }
    }

    /// <summary>
    /// 初始化局放采集
    /// </summary>
    public void InitDischargePhaseCollection()
    {
        lock (_dischargePhaseLock)
        {
            _dischargePhaseStartTime = DateTime.Now;
            _currentDischargePhase = 1;
            _logServices.Info($"局放采集已初始化，开始时间: {_dischargePhaseStartTime:yyyy-MM-dd HH:mm:ss}");
        }
    }

    /// <summary>
    /// 获取当前电压阶段的顺序值（阶段1和阶段2）
    /// 电压每5分钟切换一个区间，在每个区间内顺序执行（递增到最大值后递减，循环往复）
    /// </summary>
    public double GetCurrentVoltage()
    {
        lock (_voltagePhaseLock)
        {
            if (_currentVoltagePhase == 0 || _voltagePhaseStartTime == DateTime.MinValue)
            {
                return 0;
            }

            TimeSpan elapsed = DateTime.Now - _voltagePhaseStartTime;
            double totalMinutes = elapsed.TotalMinutes;

            // 计算当前处于哪个5分钟周期（0-4分钟: 阶段1, 5-9分钟: 阶段2, 10-14分钟: 阶段1, 以此类推）
            int cycleNumber = (int)(totalMinutes / 5); // 0, 1, 2, 3, ...
            int phaseInCycle = cycleNumber % 2; // 0: 阶段1, 1: 阶段2

            if (phaseInCycle == 0)
            {
                // 阶段1: 电压 342.54~418.66
                if (_currentVoltagePhase != 1)
                {
                    _currentVoltagePhase = 1;
                    // 切换到阶段1时，如果当前值不在区间内则重置
                    if (_currentVoltageSequentialValue < VOLTAGE_PHASE1_MIN || _currentVoltageSequentialValue > VOLTAGE_PHASE1_MAX)
                    {
                        _currentVoltageSequentialValue = VOLTAGE_PHASE1_MIN;
                        _isVoltageAscending = true;
                    }
                    _logServices.Info($"进入电压阶段1: 范围 {VOLTAGE_PHASE1_MIN}~{VOLTAGE_PHASE1_MAX}，顺序采集");
                }
                return GetSequentialValue(ref _currentVoltageSequentialValue, ref _isVoltageAscending, VOLTAGE_PHASE1_MIN, VOLTAGE_PHASE1_MAX);
            }
            else
            {
                // 阶段2: 电压 693~847
                if (_currentVoltagePhase != 2)
                {
                    _currentVoltagePhase = 2;
                    // 切换到阶段2时，如果当前值不在区间内则重置
                    if (_currentVoltageSequentialValue < VOLTAGE_PHASE2_MIN || _currentVoltageSequentialValue > VOLTAGE_PHASE2_MAX)
                    {
                        _currentVoltageSequentialValue = VOLTAGE_PHASE2_MIN;
                        _isVoltageAscending = true;
                    }
                    _logServices.Info($"进入电压阶段2: 范围 {VOLTAGE_PHASE2_MIN}~{VOLTAGE_PHASE2_MAX}，顺序采集");
                }
                return GetSequentialValue(ref _currentVoltageSequentialValue, ref _isVoltageAscending, VOLTAGE_PHASE2_MIN, VOLTAGE_PHASE2_MAX);
            }
        }
    }

    /// <summary>
    /// 顺序获取下一个值：在区间内连续递增到最大值后递减，形成三角波
    /// 值严格在[min, max]区间内，不会超出边界
    /// </summary>
    private double GetSequentialValue(ref double currentValue, ref bool isAscending, double min, double max)
    {
        double range = max - min;
        double step = range / 50.0; // 步长，50步完成一个周期
        
        if (isAscending)
        {
            currentValue += step;
            if (currentValue >= max)
            {
                currentValue = max;
                isAscending = false;
            }
        }
        else
        {
            currentValue -= step;
            if (currentValue <= min)
            {
                currentValue = min;
                isAscending = true;
            }
        }
        
        // 双重保护：确保值严格在区间内
        if (currentValue < min) currentValue = min;
        if (currentValue > max) currentValue = max;
        
        return currentValue;
    }

    /// <summary>
    /// 获取当前局放顺序值
    /// 局放一直采集，在120~135区间内循环轮询（递增到最大值后递减，循环往复）
    /// </summary>
    public double GetCurrentDischarge()
    {
        lock (_dischargePhaseLock)
        {
            // 局放一直采集，没有时间限制
            if (_currentDischargePhase == 0)
            {
                _currentDischargePhase = 1;
                _currentDischargeSequentialValue = DISCHARGE_MIN;
                _isDischargeAscending = true;
                _logServices.Info($"局放采集启动: 范围 {DISCHARGE_MIN}~{DISCHARGE_MAX}，持续循环轮询");
            }

            return GetSequentialValue(ref _currentDischargeSequentialValue, ref _isDischargeAscending, DISCHARGE_MIN, DISCHARGE_MAX);
        }
    }

    /// <summary>
    /// 获取当前电压阶段名称
    /// </summary>
    public string GetCurrentVoltagePhaseName()
    {
        lock (_voltagePhaseLock)
        {
            TimeSpan elapsed = DateTime.Now - _voltagePhaseStartTime;
            double totalMinutes = elapsed.TotalMinutes;

            if (totalMinutes <= VOLTAGE_PHASE1_DURATION_MINUTES)
                return "电压阶段1(342.54~418.66V)";
            else if (totalMinutes <= VOLTAGE_PHASE1_DURATION_MINUTES + VOLTAGE_PHASE2_DURATION_MINUTES)
                return "电压阶段2(693~847V)";
            else
                return "电压采集已结束";
        }
    }

    /// <summary>
    /// 获取当前局放阶段名称
    /// </summary>
    public string GetCurrentDischargePhaseName()
    {
        lock (_dischargePhaseLock)
        {
            TimeSpan elapsed = DateTime.Now - _dischargePhaseStartTime;
            double totalMinutes = elapsed.TotalMinutes;

            if (totalMinutes <= DISCHARGE_PHASE_DURATION_MINUTES)
                return "局放采集中(120~135)";
            else
                return "局放采集已结束";
        }
    }
    public void Execute()
    {
        _ = GetCollectVol();
    }

    private async Task GetCollectVol()
    {
        await Task.Run(async () =>
        {
            try
            {
            // 循环发送 "C" 指令到串口，等价 Java 代码的 RxtxUtil
           _=serialPortReader.ReadAsStringAsync("");
            while (!_cts.Token.IsCancellationRequested)
            {

                    try
                    {
                        Dictionary<string, object> dictionary = new Dictionary<string, object>();
                        string[] array = new string[16];
                        bool flag = false;
                        int num = 0;
                        while (true)
                        {
                            if ((serialPortReader as SerialPortDataReader).Queue == null)
                            {
                                await Task.Delay(2, _cts.Token);
                                continue;
                            }
                            if (!(serialPortReader as SerialPortDataReader).Queue.TryTake(out var item, 2, _cts.Token) || item == null || item.Payload == null)
                            {
                                await Task.Delay(10);
                                continue;

                            }
                            string text = StringUtil.ByteArrayToHexString(item.Payload);
                            if (flag)
                            {
                                array[num++] = Encoding.ASCII.GetString(StringUtil.HexStringToByteArray(text));
                            }
                            else
                            {
                                if (num == 0 && text.Trim() == "5A")
                                {
                                    array[num++] = text;
                                    continue;
                                }
                                if (num == 1 && text.Trim() == "A5")
                                {
                                    flag = true;
                                    array[num++] = text;
                                    continue;
                                }
                            }
                            if (num != 0 && num % 16 == 0)
                            {
                                break;
                            }
                        }
                      
                        StringBuilder stringBuilder2 = new StringBuilder();
                        for (int i = 3; i < array.Length - 4; i++)
                        {
                            string text2 = array[i].Replace(" ", "");
                            string pattern = "[0-9]*";
                            if (Regex.IsMatch(text2, pattern) || text2.Equals("."))
                            {
                                stringBuilder2.Append(text2);
                            }
                        }
                        string input = stringBuilder2.ToString().Replace(" ", "");
                        decimal result;
                        decimal d = Math.Round(decimal.TryParse(Regex.Match(input, "-?(?:0+|(?=0*[1-9])\\d*)(?:\\.\\d+)?(?=\\D|$)").Value, out result) ? result : 0m, 2, MidpointRounding.ToEven);
                        decimal d2 = 1000m;
                        string text3 = array[13].Replace(" ", "");
                        string x = array[14].Replace(" ", "");
                        if (string.IsNullOrWhiteSpace(text3) && StringComparer.OrdinalIgnoreCase.Compare(x, "v") == 0)
                        {
                            d = Math.Round(decimal.Divide(d, d2), 2, MidpointRounding.ToEven);
                        }
                        else if (StringComparer.OrdinalIgnoreCase.Compare(text3, "k") != 0 || StringComparer.OrdinalIgnoreCase.Compare(x, "v") != 0)
                        {
                            d = ((StringComparer.OrdinalIgnoreCase.Compare(text3, "m") != 0 || StringComparer.OrdinalIgnoreCase.Compare(x, "v") != 0) ? Math.Round(decimal.Divide(d, d2), 2, MidpointRounding.ToEven) : Math.Round(decimal.Multiply(d, d2), 2, MidpointRounding.ToEven));
                        }
                        dictionary["hexString"] = "[" + string.Join(",", array) + "]";
                        dictionary["collectVol"] = d.ToString();
                        dictionary["collectTime"] = DateTime.Now;
                        SetVoltage(d.ToString());
                    }
                    catch (OperationCanceledException)
                    {
                        break;
                    }
                    catch (Exception e)
                    {
                        _logServices.Error(e.ToString());
                        
                    }
                    await Task.Delay(500);
                
            }
            }
            catch (Exception e)
            {
                _logServices.Error(e.ToString());

                //throw;
            }
        },_cts.Token);

    }
    private async Task StartProcessTaskOutLog()
    {
        await Task.Run(async () =>
        {
            // 循环发送 "C" 指令到串口，等价 Java 代码的 RxtxUtil
            
            while (true)
            {
                try
                {
                    if (_dataQueueStr.IsEmpty)
                    {
                        await Task.Delay(100);
                        continue;
                    }
                    if (_dataQueueStr.TryDequeue(out string result))
                    {
                        Interlocked.Decrement(ref _logQueueCount);
                        if (result.StartsWith("[LOG]"))
                        {
                            bool isflag = result.StartsWith("[LOG][FLAG]");
                            string message = isflag
                                ? result.Substring("[LOG][FLAG]".Length)
                                : result.Substring("[LOG]".Length);
                            OnLogOutputEvent?.Invoke(message, isflag);
                        }
                        else
                        {
                            OnLogOutputEvent?.Invoke(result, false);
                        }
                    }
                }
                catch (OperationCanceledException)
                {
                    break;
                }
                catch (Exception ex)
                {
                    //Console.WriteLine(ex.Message);
                    _logServices.Error(ex.TargetSite.ToString() + "\r\n" + ex.ToString());
                }

                await Task.Delay(100);
            }
        });
    }

    private async Task ProcessDataFrameAsync(DataFrame obj)
    {
        try
        {
            // 1. 读取数据并解析
            var plaitResult = Reader_OnDataRead(obj);

            // 2. 调试模式直接写日志，不进入上传队列
            if (bool.TryParse(_config.DebugMode?.value, out bool debugMode) && debugMode)
            {
                await DataWriteLogAsync(JsonConvert.SerializeObject(plaitResult));
                return;
            }
            //await DataWriteLogAsync(JsonConvert.SerializeObject(plaitResult));
            // 3. 构建 DataMessage
            var msg = new DataMessage
            {
                plain = JsonConvert.SerializeObject(plaitResult)
            };

            AddJsonItems(msg.plain);
            if (currentCounts == BATCH_SIZEs)
            {
                currentBatchs.Append("]");// 结束数组
                string jsonArray = currentBatchs.ToString();
                // 4. 加密（如果需要）
                if (bool.TryParse(_config.EnableEncryption?.value, out bool enableEncryption2) && enableEncryption2)
                {
                    string requestJson = $@"{{ ""timestamp"": {plaitResult.timestamp}, ""deviceMac"": ""{macAddress}"",""deviceCode"": ""{_config.DEVICE_CODE?.value}"",""dataCollectDeviceNO"":""{_config.DATA_COLLECT_DEVICE_NO?.value}"",""items"": {jsonArray}}}";
                    var keyResult = await GetKey(requestJson);

                    if (keyResult == null)
                    {
                        _recvQueueStr.Writer.TryWrite(msg.plain);
                        
                        return;
                    }
                }
                else
                {
                    
                    string requestJson = $@"{{ ""timestamp"": {plaitResult.timestamp}, ""deviceMac"": ""{macAddress}"",""deviceCode"": ""{_config.DEVICE_CODE?.value}"",""dataCollectDeviceNO"":""{_config.DATA_COLLECT_DEVICE_NO?.value}"",""items"": {jsonArray}}}";
                    await storageUploadManager.StorageUploadAsync(requestJson);
                }
            }
            else {
                ////数据存储
                //string uploadStr = $"{{ \"deviceCode\": \"{_config.WinConfig.DEVICE_CODE}\",\"sampleTime\": \"{DateTime.Now.ToString("yyyy-MM-dd HH:mm:ss")}\",\"plain\": \"{msg.plain}\"}}\"}}\r\n";
                // await storageUploadManager.StorageUploadAsync(uploadStr);
            }
        }
        catch (Exception ex)
        {
            _logServices.Error($"处理数据帧失败: {ex}");
        }
        finally
        {  // 重置计数器和StringBuilder
            if (currentCounts >= BATCH_SIZEs)
            {
                currentBatchs.Clear();
                currentCounts = 0;
            }
          
        }
    }
    private readonly object _batchLock = new object();

    private async Task GetReceiceAsync()    
      {
          List<DataMessage> batch = new List<DataMessage>();
          
          
          try
          {
            while (true)
            {
                if (!portChecker.IsPortOpen)
                {
                    await Task.Delay(1000);
                    continue;
                }
                await foreach (var item in _recvQueueStr.Reader.ReadAllAsync())
                {
                    try
                    {
                        if (_recvQueueStr.Reader.Count == 0)
                        {
                            await Task.Delay(100);
                            continue;
                        }


                        var msg = new DataMessage
                        {
                            plain = JsonConvert.SerializeObject(item)
                        };
                        var iotData = JsonSerializer.Deserialize<IotDataMessageModelNew>(item);
                        long timestamp = Convert.ToInt64(iotData.timestamp);

                        // 4. 加密（如果需要）
                        if (bool.TryParse(_config.EnableEncryption?.value, out bool enableEncryption2) && enableEncryption2)
                        {
                            AddJsonItem(item);
                            if (currentCount == 500)
                            {
                                currentBatch.Append("]");// 结束数组
                                string jsonArray = currentBatch.ToString();


                                string requestJson = $@"{{ ""timestamp"": {timestamp}, ""deviceMac"":""{macAddress}"",""deviceCode"": ""{_config.DEVICE_CODE?.value}"",""dataCollectDeviceNO"":""{_config.DATA_COLLECT_DEVICE_NO?.value}"", ""items"": {jsonArray}}}";

                                var keyResult = await GetKey(requestJson);
                                // 重置计数器和StringBuilder
                                currentBatch.Clear();
                                currentCount = 0;
                                if (keyResult is null)
                                {
                                    return;
                                }
                            }

                        }


                        //await Task.Delay(2);
                    }
                    catch
                    {
                    }
                    finally
                    {
                        batch.Clear();

                    }
                }

                await Task.Delay(1000);
            }
              
          }
          catch 
          {
             

          }
          finally { 
              

          }
         

   
  }

    
    void AddJsonItem(string jsonItem)
    {
        if (currentCount == 0)
        {
            currentBatch.Append("[");  // 开始数组
        }
        else
        {
            currentBatch.Append(",");  // 添加分隔逗号
        }
        string wrappedItem = $"{{\"plain\":{jsonItem}}}";
        currentBatch.Append(wrappedItem);
        currentCount++;

      
    }


    void AddJsonItems(string jsonItem)
    {
        lock (_batchLock)
        {
            if (string.IsNullOrWhiteSpace(jsonItem))
            {
                return;
            }
            if (currentCounts == 0)
            {
                currentBatchs.Clear();  // 关键：清空旧的
                currentBatchs.Append("[");  // 开始数组
            }
            else
            {
                currentBatchs.Append(",");  // 添加分隔逗号
            }
            string wrappedItem = $"{{\"plain\":{jsonItem}}}";
            currentBatchs.Append(wrappedItem);
            currentCounts++;
        }
       

        // 当达到1000条时，处理这一批
        //if (currentCount >= BATCH_SIZE)
        //{
        //    CompleteBatch();
        //}
    }
   
  
    // 异步写日志（避免阻塞）
    private async Task DataWriteLogAsync(string content)
    {
        // 假设原 DateWriteLog 是同步的，改为异步版本
        await Task.Run(() => DateWriteLog(content));
    }

    private static decimal GetFirstElementByHexIndex(IEnumerable<decimal> source, string hex)
    {
        int sourceCount = source.Count();
        if (string.IsNullOrEmpty(hex) || source == null || sourceCount == 0)
            return decimal.Zero;

        try
        {
            // 将 "0000" -> 0x00 -> 0（十进制）
            int groupIndex = Convert.ToInt32(hex, 16);
            int elementIndex = groupIndex * 3;

            // 安全检查：是否超出数组范围
            if (elementIndex >= sourceCount)
                return decimal.Zero;

            return source.ElementAt(elementIndex);
        }
        catch
        {
            return decimal.Zero;
        }
    }

    private IotDataMessageModelNew Reader_OnDataRead(DataFrame arg)
    {

        //获取到时间戳
        long timestamp = Convert.ToInt64(arg.Get("TIMESTAMP"));
        List<decimal> decimals = (List<decimal>)arg.Get("DECIMALS");
        Dictionary<string, string> dictionary3 = new Dictionary<string, string>();
        
        // 判断是否为波形图麻点数据（func_code 为 0x02）
        string funcCode = arg.Get("FUNC_CODE")?.ToString();
        if (funcCode == "02")
        {
            //var wave = "[0.871,0.726,0.726,0.726,0.653,0.653,0.653,0.653,0.726,0.653,0.726,0.653,0.653,0.726,0.726,0.726,0.726,0.726,0.798,0.653,0.726,0.653,0.726,0.726,0.726,0.726,0.798,0.726,0.726,0.726,0.798,0.726,0.653,0.726,0.726,0.798,0.653,0.798,0.653,0.798,0.798,0.798,0.726,0.726,0.726,0.726,0.798,0.871,0.798,0.653,0.726,0.653,0.798,0.726,0.726,0.653,0.726,0.653,0.798,0.726,0.726,0.726,0.798,0.871,0.726,0.871,0.726,0.726,0.726,0.726,0.726,0.653,0.653,0.653,0.798,0.726,0.653,0.726,0.871,0.726,0.798,0.726,0.726,0.726,0.726,0.726,0.726,0.798,0.726,0.653,0.871,0.798,0.653,0.653,0.726,0.653,0.653,0.726,0.726,0.726,0.798,0.798,0.726,0.726,0.798,0.871,0.798,0.798,0.726,0.726,0.653,0.798,0.653,0.653,0.653,0.726,0.653,0.726,0.798,0.726,0.798,0.798,0.726,0.726,0.726,0.726,0.726,0.726,0.653,0.726,0.798,0.726,0.653,0.726,0.726,0.726,0.798,0.871,0.798,0.798,0.653,0.726,0.726,0.726,0.726,0.726,0.726,0.726,0.726,0.653,0.653,0.653,0.653,0.653,0.726,0.653,0.726,0.726,0.726,0.653,0.653,0.653,0.653,0.581,0.653,0.653,0.653,0.798,0.581,0.653,0.726,0.653,0.726,0.653,0.653,0.798,0.581,0.653,0.726,0.653,0.726,0.798,0.726,0.653,0.726,0.726,0.726,0.653,0.653,0.726,0.798,0.653,0.726,0.653,0.653,0.581,0.581,0.581,0.653,0.798,0.871,0.798,0.871,0.798,0.726,0.653,0.871,0.726,0.653,0.653,0.798,0.726,0.726,0.726,0.726,0.798,0.798,0.726,0.798,0.798,0.798,0.653,0.653,0.726,0.798,0.653,0.726,0.726,0.798,0.726,0.726,0.726,0.798,0.653,0.798,0.726,0.653,0.653,0.653,0.726,0.726,0.726,0.726,0.653,0.726,0.726,0.726,0.798,0.726,0.726,0.798,0.653,0.726,0.581,0.653,0.581,0.653,0.798,0.653,0.726,0.798,0.798,0.798,0.726,0.871,0.726,0.726,0.798,0.726,0.653,0.653,0.798,0.726,0.726,0.653,0.798,0.798,0.726,0.726,0.726,0.726,0.581,0.581,0.726,0.798,0.726,0.726,0.798,0.726,0.798,0.798,0.798,0.726,0.726,0.726,0.726,0.726,0.726,0.653,0.871,0.726,0.798,0.798,0.653,0.798,0.726,0.726,0.798,0.653,0.653,0.726,0.653,0.726,0.581,0.798,0.653,0.798,0.798,0.726,0.871,0.726,0.726,0.581,0.726,0.726,0.653,0.726,0.726,0.726,0.726,0.726,0.726,0.726,0.653,0.653,0.653,0.581,0.726,0.653,0.726,0.653,0.653,0.653,0.653,0.726,0.726,0.726,0.726,0.726,0.726,0.653,0.726,0.653,0.653,0.798,0.726,0.726,0.726,0.653,0.726,0.798,0.798,0.726,0.726,0.798,0.726,0.726,0.653,0.726,0.726,0.726,0.726,0.726,0.726,0.798,0.726,0.653,0.798,0.726,0.653,0.726,0.653,0.726,0.653,0.726,0.653,0.798,0.798,0.798,0.726,0.798,0.798,0.726,0.726,0.726,0.653,0.726,0.726,0.798,0.726,0.798,0.726,0.798,0.726,0.653,0.726,0.726,0.726,0.798,0.726,0.726,0.653,0.798,0.798,0.726,0.653,0.726,0.726,0.726,0.726,0.726,0.798,0.871,0.726,0.726,0.726,0.798,0.653,0.871,0.653,0.653,0.653,0.653,0.726,0.653,0.726,0.653,0.726,0.726,0.726,0.798,0.798,0.726,0.726,0.726,0.726,0.726,0.653,0.726,0.726,0.798,0.871,0.726,0.798,0.798,0.871,0.653,0.798,0.726,0.653,0.798,0.726,0.726,0.726,0.726,0.798,0.726,0.798,0.798,0.726,0.726,0.726,0.726,0.653,0.726,0.798,0.581,0.653,0.726,0.726,0.726,0.726,0.726,0.798,0.726,0.798,0.798,0.798,0.653,0.653,0.726,0.726,0.798,0.653,0.726,0.653,0.798,0.726,0.798,0.798,0.798,0.653,0.726,0.726,0.726,0.726,0.726,0.653,0.726,0.581,0.653,0.726,0.798,0.726,0.726,0.581,0.653,0.726,0.726,0.726,0.726,0.726,0.798,0.653,0.653,0.653,0.653,0.653,0.581,0.653,0.653,0.653,0.726,0.653,0.653,0.653,0.653,0.653,0.653,0.726,0.653,0.798,0.726,0.726,0.726,0.798,0.726,0.798,0.726,0.653,0.726,0.726,0.726,0.653,0.726,0.653,0.653,0.726,0.726,0.653,0.726,0.653,0.653,0.726,0.726,0.798,0.798,0.798,0.798,0.726,0.726,0.798,0.653,0.798,0.653,0.653,0.798,0.653,0.726,0.726,0.798,0.726,0.871,0.798,0.798,0.653,0.798,0.581,0.653,0.653,0.726,0.726,0.798,0.798,0.726,0.726,0.726,0.726,0.653,0.653,0.653,0.581,0.581,0.726,0.653,0.581,0.726,0.798,0.726,0.726,0.798,0.798,0.798,0.726,0.726,0.653,0.653,0.653,0.653,0.726,0.653,0.726,0.726,0.726,0.798,0.726,0.726,0.726,0.726,0.798,0.653,0.726,0.653,0.798,0.798,0.726,0.798,0.726,0.726,0.726,0.726,0.726,0.653,0.653,0.653,0.726,0.653,0.653,0.581,0.726,0.653,0.726,0.798,0.798,0.798,0.653,0.726,0.653,0.726,0.726,0.726,0.726,0.653,0.653,0.726,0.726,0.726,0.726,0.726,0.726,0.726,0.798,0.726,0.726,0.726,0.871,0.726,0.653,0.726,0.653,0.798,0.726,0.726,0.726,0.653,0.726,0.653,0.798,0.726,0.726,0.581,0.653,0.726,0.726,0.726,0.726,0.653,0.798,0.653,0.726,0.653,0.653,0.726,0.726,0.726,0.653,0.726,0.653,0.653,0.653,0.581,0.653,0.726,0.726,0.726,0.798,0.581,0.653,0.798,0.726]";
            //dictionary3.Add("WAVEFORM", JsonConvert.SerializeObject(decimals));
        }
        else
        {
            //dictionary3.Add("discharge1", decimals.FirstOrDefault().ToString());
            //dictionary3.Add("dischargeNum1", decimals.ElementAtOrDefault(1).ToString());
            //dictionary3.Add("dischargePhase1", decimals.ElementAtOrDefault(2).ToString());
            //dictionary3.Add("discharge2", decimals.ElementAtOrDefault(3).ToString());
            //dictionary3.Add("dischargeNum2", decimals.ElementAtOrDefault(4).ToString());
            //dictionary3.Add("dischargePhase2", decimals.ElementAtOrDefault(5).ToString());
            //var wave = TcpCollectorClient.PD_WAVE;
            var wave = "[0.871,0.726,0.726,0.726,0.653,0.653,0.653,0.653,0.726,0.653,0.726,0.653,0.653,0.726,0.726,0.726,0.726,0.726,0.798,0.653,0.726,0.653,0.726,0.726,0.726,0.726,0.798,0.726,0.726,0.726,0.798,0.726,0.653,0.726,0.726,0.798,0.653,0.798,0.653,0.798,0.798,0.798,0.726,0.726,0.726,0.726,0.798,0.871,0.798,0.653,0.726,0.653,0.798,0.726,0.726,0.653,0.726,0.653,0.798,0.726,0.726,0.726,0.798,0.871,0.726,0.871,0.726,0.726,0.726,0.726,0.726,0.653,0.653,0.653,0.798,0.726,0.653,0.726,0.871,0.726,0.798,0.726,0.726,0.726,0.726,0.726,0.726,0.798,0.726,0.653,0.871,0.798,0.653,0.653,0.726,0.653,0.653,0.726,0.726,0.726,0.798,0.798,0.726,0.726,0.798,0.871,0.798,0.798,0.726,0.726,0.653,0.798,0.653,0.653,0.653,0.726,0.653,0.726,0.798,0.726,0.798,0.798,0.726,0.726,0.726,0.726,0.726,0.726,0.653,0.726,0.798,0.726,0.653,0.726,0.726,0.726,0.798,0.871,0.798,0.798,0.653,0.726,0.726,0.726,0.726,0.726,0.726,0.726,0.726,0.653,0.653,0.653,0.653,0.653,0.726,0.653,0.726,0.726,0.726,0.653,0.653,0.653,0.653,0.581,0.653,0.653,0.653,0.798,0.581,0.653,0.726,0.653,0.726,0.653,0.653,0.798,0.581,0.653,0.726,0.653,0.726,0.798,0.726,0.653,0.726,0.726,0.726,0.653,0.653,0.726,0.798,0.653,0.726,0.653,0.653,0.581,0.581,0.581,0.653,0.798,0.871,0.798,0.871,0.798,0.726,0.653,0.871,0.726,0.653,0.653,0.798,0.726,0.726,0.726,0.726,0.798,0.798,0.726,0.798,0.798,0.798,0.653,0.653,0.726,0.798,0.653,0.726,0.726,0.798,0.726,0.726,0.726,0.798,0.653,0.798,0.726,0.653,0.653,0.653,0.726,0.726,0.726,0.726,0.653,0.726,0.726,0.726,0.798,0.726,0.726,0.798,0.653,0.726,0.581,0.653,0.581,0.653,0.798,0.653,0.726,0.798,0.798,0.798,0.726,0.871,0.726,0.726,0.798,0.726,0.653,0.653,0.798,0.726,0.726,0.653,0.798,0.798,0.726,0.726,0.726,0.726,0.581,0.581,0.726,0.798,0.726,0.726,0.798,0.726,0.798,0.798,0.798,0.726,0.726,0.726,0.726,0.726,0.726,0.653,0.871,0.726,0.798,0.798,0.653,0.798,0.726,0.726,0.798,0.653,0.653,0.726,0.653,0.726,0.581,0.798,0.653,0.798,0.798,0.726,0.871,0.726,0.726,0.581,0.726,0.726,0.653,0.726,0.726,0.726,0.726,0.726,0.726,0.726,0.653,0.653,0.653,0.581,0.726,0.653,0.726,0.653,0.653,0.653,0.653,0.726,0.726,0.726,0.726,0.726,0.726,0.653,0.726,0.653,0.653,0.798,0.726,0.726,0.726,0.653,0.726,0.798,0.798,0.726,0.726,0.798,0.726,0.726,0.653,0.726,0.726,0.726,0.726,0.726,0.726,0.798,0.726,0.653,0.798,0.726,0.653,0.726,0.653,0.726,0.653,0.726,0.653,0.798,0.798,0.798,0.726,0.798,0.798,0.726,0.726,0.726,0.653,0.726,0.726,0.798,0.726,0.798,0.726,0.798,0.726,0.653,0.726,0.726,0.726,0.798,0.726,0.726,0.653,0.798,0.798,0.726,0.653,0.726,0.726,0.726,0.726,0.726,0.798,0.871,0.726,0.726,0.726,0.798,0.653,0.871,0.653,0.653,0.653,0.653,0.726,0.653,0.726,0.653,0.726,0.726,0.726,0.798,0.798,0.726,0.726,0.726,0.726,0.726,0.653,0.726,0.726,0.798,0.871,0.726,0.798,0.798,0.871,0.653,0.798,0.726,0.653,0.798,0.726,0.726,0.726,0.726,0.798,0.726,0.798,0.798,0.726,0.726,0.726,0.726,0.653,0.726,0.798,0.581,0.653,0.726,0.726,0.726,0.726,0.726,0.798,0.726,0.798,0.798,0.798,0.653,0.653,0.726,0.726,0.798,0.653,0.726,0.653,0.798,0.726,0.798,0.798,0.798,0.653,0.726,0.726,0.726,0.726,0.726,0.653,0.726,0.581,0.653,0.726,0.798,0.726,0.726,0.581,0.653,0.726,0.726,0.726,0.726,0.726,0.798,0.653,0.653,0.653,0.653,0.653,0.581,0.653,0.653,0.653,0.726,0.653,0.653,0.653,0.653,0.653,0.653,0.726,0.653,0.798,0.726,0.726,0.726,0.798,0.726,0.798,0.726,0.653,0.726,0.726,0.726,0.653,0.726,0.653,0.653,0.726,0.726,0.653,0.726,0.653,0.653,0.726,0.726,0.798,0.798,0.798,0.798,0.726,0.726,0.798,0.653,0.798,0.653,0.653,0.798,0.653,0.726,0.726,0.798,0.726,0.871,0.798,0.798,0.653,0.798,0.581,0.653,0.653,0.726,0.726,0.798,0.798,0.726,0.726,0.726,0.726,0.653,0.653,0.653,0.581,0.581,0.726,0.653,0.581,0.726,0.798,0.726,0.726,0.798,0.798,0.798,0.726,0.726,0.653,0.653,0.653,0.653,0.726,0.653,0.726,0.726,0.726,0.798,0.726,0.726,0.726,0.726,0.798,0.653,0.726,0.653,0.798,0.798,0.726,0.798,0.726,0.726,0.726,0.726,0.726,0.653,0.653,0.653,0.726,0.653,0.653,0.581,0.726,0.653,0.726,0.798,0.798,0.798,0.653,0.726,0.653,0.726,0.726,0.726,0.726,0.653,0.653,0.726,0.726,0.726,0.726,0.726,0.726,0.726,0.798,0.726,0.726,0.726,0.871,0.726,0.653,0.726,0.653,0.798,0.726,0.726,0.726,0.653,0.726,0.653,0.798,0.726,0.726,0.581,0.653,0.726,0.726,0.726,0.726,0.653,0.798,0.653,0.726,0.653,0.653,0.726,0.726,0.726,0.653,0.726,0.653,0.653,0.653,0.581,0.653,0.726,0.726,0.726,0.798,0.581,0.653,0.798,0.726]";
           // dictionary3.Add("PD_WAVE", wave);
        }

        // 使用分阶段顺序电压和局放值
        double voltageValue = GetCurrentVoltage();
        double dischargeValue = GetCurrentDischarge();
        
        // 电压值（阶段1和阶段2）
        if (voltageValue > 0)
        {
            dictionary3.Add("VOLTAGE", voltageValue.ToString("F2"));
        }
        else
        {
            // 如果顺序采集未启动，使用默认区间内的值
            dictionary3.Add("VOLTAGE", VOLTAGE_PHASE1_MIN.ToString("F2"));
        }
        
        // 局放值（10分钟采集期）
        if (dischargeValue > 0)
        {
            dictionary3.Add("DISCHARGE", dischargeValue.ToString("F2"));
        }
        else
        {
            // 如果局放采集未启动，使用默认区间内的值
            dictionary3.Add("DISCHARGE", DISCHARGE_MIN.ToString("F2"));
        }
        IotDataMessageModelNew iotDataMessageModel = new IotDataMessageModelNew();
        iotDataMessageModel.timestamp = timestamp;
        iotDataMessageModel.timestampType = TimeStampType.MICROSECOND.ToString();
        //iotDataMessageModel.deviceCode = _config.WinConfig.DATA_COLLECT_DEVICE_NO;
        
        iotDataMessageModel.data = dictionary3;
        return iotDataMessageModel;
    }

    public List<DataMessage> dataMessages = new List<DataMessage>();
    private async Task SocketClient_OnFrameParsed(DataFrame obj)
    {
        await ProcessDataFrameAsync(obj);
    }

    private static void DateWriteLog(string message)
    {
        string text = $"[{DateTime.Now:yyyy-MM-dd HH:mm:ss.fff}] {message}";
        _fileLogQueue.Enqueue(text);
    }
    private IniFileManager _iniFileManager = new IniFileManager(Path.Combine(AppDomain.CurrentDomain.BaseDirectory, "config", "Config.ini"));// nIniFileManager("config.ini");
    private void StartFileLogFlushTask()
    {
        _fileLogCts = new CancellationTokenSource();
        _fileLogFlushTask = Task.Run(async () =>
        {
            while (!_fileLogCts.Token.IsCancellationRequested)
            {
                try
                {
                    await Task.Delay(500, _fileLogCts.Token);

                    List<string> logsToWrite = new List<string>();
                    while (_fileLogQueue.TryDequeue(out string log))
                    {
                        logsToWrite.Add(log);
                    }

                    if (logsToWrite.Count > 0)
                    {
                        lock (lockObj)
                        {
                             logPath = _iniFileManager.GetValue("NEWDATACOLLECTION", "FILEURLDATA");
                            if (!Directory.Exists(logPath))
                            {
                                Directory.CreateDirectory(logPath);
                            }
                            logFile = Path.Combine(logPath, $"dc_{DateTime.Now:yyyy-MM-dd}");
                            File.AppendAllText(logFile + ".txt", string.Join(Environment.NewLine, logsToWrite) + Environment.NewLine);
                        }
                    }
                }
                catch (OperationCanceledException)
                {
                    break;
                }
                catch (Exception ex)
                {
                    _logServices.Error(ex.TargetSite.ToString() + "\r\n" + ex.ToString());
                    //.Error($"文件日志写入异常: {e.Message}");
                }
            }
        }, _fileLogCts.Token);
    }

    private static void StopFileLogFlushTask()
    {
        _fileLogCts?.Cancel();
        try
        {
            _fileLogFlushTask?.Wait(1000);
        }
        catch
        {
        }
        List<string> remainingLogs = new List<string>();
        while (_fileLogQueue.TryDequeue(out string log))
        {
            remainingLogs.Add(log);
        }
        if (remainingLogs.Count > 0)
        {
            try
            {
                lock (lockObj)
                {
                    if (!Directory.Exists(logPath))
                    {
                        Directory.CreateDirectory(logPath);
                    }
                    logFile = Path.Combine(logPath, $"dc_{DateTime.Now:yyyy-MM-dd}");
                    File.AppendAllText(logFile + ".txt", string.Join(Environment.NewLine, remainingLogs) + Environment.NewLine);
                }
            }
            catch
            {
            }
        }
    }

    async Task<KeyResponse> GetKey(string json)
    {
        var result = await this.key.GetKeyAsync(json);
        return result;
    }

    public void ExecuteDischarge()
    {
        var token = _cts.Token;

        // 专用高优先级线程：降低 Task/ThreadPool 调度抖动，提升 1ms 采集稳定性。
        _dischargeTask = Task.Run(async () =>
        {
            Thread.Sleep(2000);
            OutputLog("数据采集开始");
            _logServices.Info("数据采集开始");
            
           string  commandstr = _config.TcpCommand == null?_iniFileManager.GetValue("NEWDATACOLLECTION", "COMMAND"):_config.TcpCommand.value;
            byte[] sendData = StringUtil.HexStringToByteArray(commandstr);
            //eaeaeaea01000001c0a8100a010100000006000000064058aeaeaeae
            long freq = Stopwatch.Frequency;
            long ticksPerMs = Math.Max(int.Parse(_config.INTERVAL.value), freq / 1000); // 基础节拍（向下取整）
            long remainderTicks = freq % 1000; // 余数，用于分摊误差
            long remainderAcc = 0;
            long ticksPerSend = ticksPerMs; // 目标：1ms 一次（平均意义下）
            long nextTick = Stopwatch.GetTimestamp() + ticksPerSend;

            int sendCount = 0;
            long lastSecond = DateTimeOffset.UtcNow.ToUnixTimeSeconds();

            timeBeginPeriod(1);
            try
            {
                while (!token.IsCancellationRequested)
                {
                    try
                    {
                        WaitUntilStopwatchTicks(nextTick, ticksPerMs, token);
                        if (token.IsCancellationRequested || !socketClient.IsConnected)
                        {
                            Thread.Sleep(100);
                            continue;
                        }
                        
                        socketClient.SendAsync(sendData);

                        sendCount++;

                        long currentSecond = DateTimeOffset.UtcNow.ToUnixTimeSeconds();
                        if (currentSecond > lastSecond)
                        {
                            //_logServices.Debug($"[Execute] 每秒发送: {sendCount} 次");
                            sendCount = 0;
                            lastSecond = currentSecond;
                        }
                     
                        nextTick += ticksPerSend;
                        if (remainderTicks > 0)
                        {
                            remainderAcc += remainderTicks;
                            if (remainderAcc >= 1000)
                            {
                                nextTick += 1;
                                remainderAcc -= 1000;
                            }
                        }
                        long now = Stopwatch.GetTimestamp();
                        if (now > nextTick + ticksPerSend + 2)
                        {
                            nextTick = now + ticksPerSend;
                            remainderAcc = 0;
                        }
                    }
                    catch (OperationCanceledException)
                    {
                        break;
                    }
                    catch (SocketException ex)
                    {
                        // 处理 Socket 断开
                    }
                    catch (Exception ex)
                    {
                        
                        _logServices.Error(ex.ToString());
                    }
                }
            }
            finally
            {
                timeEndPeriod(1);
            }
        });
    }

    public void ExecuteWaveform()
    {
        var token = _cts.Token;

        // 专用高优先级线程：降低 Task/ThreadPool 调度抖动，提升麻点数据采集稳定性。
        _waveformTask = Task.Run(async () =>
        {
            Thread.Sleep(2500);
            OutputLog("波形图麻点数据采集开始");
            _logServices.Info("波形图麻点数据采集开始");
            string commandstr = _config.TcpWaveCommand == null? _iniFileManager.GetValue("NEWDATACOLLECTION", "WAVEFORMCOMMAND"):_config.TcpWaveCommand.value;
         
            byte[] sendData = StringUtil.HexStringToByteArray(commandstr);
            //eaeaeaea01000001c0a8100a020100000006000000064058aeaeaeae
            var d = new TcpCollectorClient(_config.IP?.value,int.Parse(_config.PORT?.value));
            d.OnLog += (msg) => _logServices.Warning(msg);

            try
            {
                while (!token.IsCancellationRequested)
                {
                    try
                    {
                        d.SendRequestAsync(sendData);
                        await Task.Delay(1000, token);
                    }
                    catch (OperationCanceledException)
                    {
                        break;
                    }
                    catch (SocketException ex)
                    {
                        // 处理 Socket 断开
                    }
                    catch (Exception ex)
                    {
                        _logServices.Error(ex.ToString());
                    }
                    finally { 
                      
                    }
                }
            }
            finally
            {
                timeEndPeriod(1);
            }
        });
    }
    public async Task SenBatchdData(string message, string topic)
    {
        //await _dataUploader.BatchUploadAsync(message, topic);
    }

    public void OutputLog(string message, bool isflag = false)
    {
        if (!message.EndsWith(Environment.NewLine))
        {
            message = $"{message}{Environment.NewLine}";
        }

        EnqueueLog($"[LOG]{(isflag ? "[FLAG]" : "")}{message}");
    }

    public static void EnqueueLog(string message)
    {
        while (Volatile.Read(ref _logQueueCount) >= MaxLogQueueSize && _dataQueueStr.TryDequeue(out _))
        {
            Interlocked.Decrement(ref _logQueueCount);
            Interlocked.Increment(ref _droppedLogs);
        }

        _dataQueueStr.Enqueue($"{DateTime.Now}:"+message);
        Interlocked.Increment(ref _logQueueCount);
    }


    public async Task RestartJavaServiceAsync()
    {
        _javaServiceManagers.OnOutputLog -= (msg) => OutputLog(msg);
        await Task.Delay(100); // 确保事件解绑完成
        _javaServiceManagers.OnOutputLog += (msg) => OutputLog(msg);
        
            var javaJar = _iniFileManager.GetValue("NEWDATACOLLECTION", "JarPath");
            await _javaServiceManagers.RestartJavaServiceAsync("C:\\Program Files\\Common Files\\Oracle\\Java\\javapath", javaJar);
        
       


    }
    // CRC 高位字节值表
    private static readonly byte[] CRCHigh = new byte[]
    {
            0x00,0xC1,0x81,0x40,0x01,0xC0,0x80,0x41,0x01,0xC0,0x80,0x41,0x00,0xC1,0x81,0x40,
            0x01,0xC0,0x80,0x41,0x00,0xC1,0x81,0x40,0x00,0xC1,0x81,0x40,0x01,0xC0,0x80,0x41,
            0x01,0xC0,0x80,0x41,0x00,0xC1,0x81,0x40,0x00,0xC1,0x81,0x40,0x01,0xC0,0x80,0x41,
            0x00,0xC1,0x81,0x40,0x01,0xC0,0x80,0x41,0x01,0xC0,0x80,0x41,0x00,0xC1,0x81,0x40,
            0x01,0xC0,0x80,0x41,0x00,0xC1,0x81,0x40,0x00,0xC1,0x81,0x40,0x01,0xC0,0x80,0x41,
            0x00,0xC1,0x81,0x40,0x01,0xC0,0x80,0x41,0x01,0xC0,0x80,0x41,0x00,0xC1,0x81,0x40,
            0x00,0xC1,0x81,0x40,0x01,0xC0,0x80,0x41,0x01,0xC0,0x80,0x41,0x00,0xC1,0x81,0x40,
            0x01,0xC0,0x80,0x41,0x00,0xC1,0x81,0x40,0x00,0xC1,0x81,0x40,0x01,0xC0,0x80,0x41,
            0x01,0xC0,0x80,0x41,0x00,0xC1,0x81,0x40,0x00,0xC1,0x81,0x40,0x01,0xC0,0x80,0x41,
            0x00,0xC1,0x81,0x40,0x01,0xC0,0x80,0x41,0x01,0xC0,0x80,0x41,0x00,0xC1,0x81,0x40,
            0x00,0xC1,0x81,0x40,0x01,0xC0,0x80,0x41,0x01,0xC0,0x80,0x41,0x00,0xC1,0x81,0x40,
            0x01,0xC0,0x80,0x41,0x00,0xC1,0x81,0x40,0x00,0xC1,0x81,0x40,0x01,0xC0,0x80,0x41,
            0x01,0xC0,0x80,0x41,0x00,0xC1,0x81,0x40,0x00,0xC1,0x81,0x40,0x01,0xC0,0x80,0x41,
            0x00,0xC1,0x81,0x40,0x01,0xC0,0x80,0x41,0x01,0xC0,0x80,0x41,0x00,0xC1,0x81,0x40,
            0x01,0xC0,0x80,0x41,0x00,0xC1,0x81,0x40,0x00,0xC1,0x81,0x40,0x01,0xC0,0x80,0x41,
            0x00,0xC1,0x81,0x40,0x01,0xC0,0x80,0x41,0x01,0xC0,0x80,0x41,0x00,0xC1,0x81,0x40
    };

    // CRC 低位字节值表
    private static readonly byte[] CRCLow = new byte[]
    {
            0x00,0xC0,0xC1,0x01,0xC3,0x03,0x02,0xC2,0xC6,0x06,0x07,0xC7,0x05,0xC5,0xC4,0x04,
            0xCC,0x0C,0x0D,0xCD,0x0F,0xCF,0xCE,0x0E,0x0A,0xCA,0xCB,0x0B,0xC9,0x09,0x08,0xC8,
            0xD8,0x18,0x19,0xD9,0x1B,0xDB,0xDA,0x1A,0x1E,0xDE,0xDF,0x1F,0xDD,0x1D,0x1C,0xDC,
            0x14,0xD4,0xD5,0x15,0xD7,0x17,0x16,0xD6,0xD2,0x12,0x13,0xD3,0x11,0xD1,0xD0,0x10,
            0xF0,0x30,0x31,0xF1,0x33,0xF3,0xF2,0x32,0x36,0xF6,0xF7,0x37,0xF5,0x35,0x34,0xF4,
            0x3C,0xFC,0xFD,0x3D,0xFF,0x3F,0x3E,0xFE,0xFA,0x3A,0x3B,0xFB,0x39,0xF9,0xF8,0x38,
            0x28,0xE8,0xE9,0x29,0xEB,0x2B,0x2A,0xEA,0xEE,0x2E,0x2F,0xEF,0x2D,0xED,0xEC,0x2C,
            0xE4,0x24,0x25,0xE5,0x27,0xE7,0xE6,0x26,0x22,0xE2,0xE3,0x23,0xE1,0x21,0x20,0xE0,
            0xA0,0x60,0x61,0xA1,0x63,0xA3,0xA2,0x62,0x66,0xA6,0xA7,0x67,0xA5,0x65,0x64,0xA4,
            0x6C,0xAC,0xAD,0x6D,0xAF,0x6F,0x6E,0xAE,0xAA,0x6A,0x6B,0xAB,0x69,0xA9,0xA8,0x68,
            0x78,0xB8,0xB9,0x79,0xBB,0x7B,0x7A,0xBA,0xBE,0x7E,0x7F,0xBF,0x7D,0xBD,0xBC,0x7C,
            0xB4,0x74,0x75,0xB5,0x77,0xB7,0xB6,0x76,0x72,0xB2,0xB3,0x73,0xB1,0x71,0x70,0xB0,
            0x50,0x90,0x91,0x51,0x93,0x53,0x52,0x92,0x96,0x56,0x57,0x97,0x55,0x95,0x94,0x54,
            0x9C,0x5C,0x5D,0x9D,0x5F,0x9F,0x9E,0x5E,0x5A,0x9A,0x9B,0x5B,0x99,0x59,0x58,0x98,
            0x88,0x48,0x49,0x89,0x4B,0x8B,0x8A,0x4A,0x4E,0x8E,0x8F,0x4F,0x8D,0x4D,0x4C,0x8C,
            0x44,0x84,0x85,0x45,0x87,0x47,0x46,0x86,0x82,0x42,0x43,0x83,0x41,0x81,0x80,0x40
    };

    public class IotDataMessageModelNew
    {
        public long timestamp { get; set; }

        public string timestampType { get; set; }
        
        public Dictionary<string, string> data { get; set; }
    }

}
