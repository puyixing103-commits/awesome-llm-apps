﻿using System;
using System.Collections;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.IO.Ports;
using System.Linq;
using System.Runtime.InteropServices;
using System.Runtime.InteropServices.WindowsRuntime;
using System.Runtime.Remoting.Messaging;
using System.Text;
using System.Text.Json;
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

using Newtonsoft.Json;

using SocketA0Demo;

using static System.Net.Mime.MediaTypeNames;
using static DC_0003.Services.Implements.Data_Collection.Class2;
using static HttpUploader;

using JsonSerializer = System.Text.Json.JsonSerializer;

public class DataAcquisitionManager
{

    private const int MaxLogQueueSize = 20000;

    public static AppConfig _config;
    private IUploader _dataUploader;
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
    public event Action<string, bool> OnLogOutputEvent;
    private bool _isRunning = true;
    public SocketClient socketClient;
    private static readonly object lockObj = new object();
    private static string logPath = "数据采集";
    private static string logFile;
    public static bool _isDataCollecting = false;
    private static string macAddress=> InstructionConstants.GetMacAddress();
    private readonly ConcurrentQueue<DataMessage> _dataMessageQueue = new ConcurrentQueue<DataMessage>();
    private readonly int _maxPendingMessages = 10000;    // 根据实际配置
    private readonly int _batchUploadSize = 30;        // 根据实际配置
    private int _queueCount = 0;
    //private readonly SemaphoreSlim _batchUploadSemaphore = new SemaphoreSlim(1, 1);
    private PortChecker portChecker = null;
    private JavaServiceManager _javaServiceManagers = new JavaServiceManager();
    private HttpHeartbeatSender _heartbeatSender;

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
            if (_cts != null)
            {
                _cts.Cancel();
                _cts.Dispose();
                _cts = null;
            }
            _cts = new CancellationTokenSource();
            socketClient = new SocketClient(_cts);
            _ = StartProcessTaskOutLog();
            StartFileLogFlushTask();
            _TcpConfigReceiver = new TcpConfigReceiver(7000, "数采配置文件.json", Init);
             _=_TcpConfigReceiver.StartListenAsync();

            _nativeApiServer = new NativeApiServer(5002);
            _nativeApiServer.OnStartCommand += HandleStartCommand;
            _nativeApiServer.OnStopCommand += HandleStopCommand;
            _nativeApiServer.OnLogOutput += (msg) => OutputLog(msg);
            _nativeApiServer.Start();
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
            bool connected = await socketClient.Start(_config.TCP_IP.IP, _config.TCP_IP.PORT);
            if (connected)
            {
                socketClient.OnFrameParsed -= SocketClient_OnFrameParsed;
                socketClient.OnFrameParsed += SocketClient_OnFrameParsed;
               Execute();
                ExecuteDischarge();
            }
            else
            {
                _isDataCollecting = false;
            }
        });
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
            //socketClient.OnFrameParsed -= SocketClient_OnFrameParsed;
            socketClient.Stop();
        }
         catch 
         (Exception ex) 
         { 
            _logServices.Warning(ex.TargetSite.ToString() + "\r\n" + ex.ToString()); }
        finally{ OutputLog("数据采集已停止");}
       
    }

    public void Init(string config)
    {
        // 2. 配置反序列化选项（解决大小写、空值等问题）
        var options = new JsonSerializerOptions
        {
            PropertyNameCaseInsensitive = true,
            ReadCommentHandling = JsonCommentHandling.Skip,
            AllowTrailingCommas = true // 允许末尾逗号
        };
        // 3. 反序列化为对象
        _config = JsonSerializer.Deserialize<AppConfig>(config, options);
        serialPortReader = new SerialPortDataReader(_config.COM.SerialPort ?? "COM1", 2, _config.COM.Command, 500, _config.COM.BaundRate, Parity.None, _config.COM.DataBits, Enum.TryParse<StopBits>(_config.COM.StopBits ?? "One", out StopBits stopBits) ? stopBits : StopBits.One, 0, 48, _cts);
        var mqtt = _config.UpMQTT;
        // 根据配置创建数据上传器
        if (_config.Common.HeartbeatProtocol == "HTTP")
            _dataUploader = new HttpUploader(_config.UpHTTP);
        else if (_config.Common.HeartbeatProtocol == "MQTT")
        {
            var mqttUploader = new MqttUploader(new CustomMQTTConfigModel()
            {
                clientId = mqtt.ClientId,
                mqttIp = mqtt.mqttIP,
                mqttPort = mqtt.mqttPort,
                passWord = mqtt.Password,
                userName = mqtt.UserName,
                CommandTopic = mqtt.CommandTopic,
                CommandResponseTopic = mqtt.CommandDataTopic,
                HeartbeatEnable=_config.Common.HeartbeatEnable
            }, _config.WinConfig, _config.HBMqtt);
            mqttUploader.OnStartCommand += HandleStartCommand;
            mqttUploader.OnStopCommand += HandleStopCommand;
            mqttUploader.OnLogOutput += (msg) => OutputLog(msg);
            _dataUploader = mqttUploader;
        }
        else
            throw new NotSupportedException($"未支持的上传协议: {_config.Common.HeartbeatProtocol}");

        //// 创建心跳发送器
        if (_config.Common.HeartbeatProtocol == "HTTP" && _config.Common.HeartbeatEnable)
        {
            _heartbeatSender = new HttpHeartbeatSender(_config.HeartbeatHTTP);
        }
        else if (_config.Common.HeartbeatProtocol == "MQTT") { }
        //else
            //throw new NotSupportedException($"未支持的心跳协议: {_config.Common.HeartbeatProtocol}");

        portChecker = new PortChecker(8080);
        portChecker.OnLog += (msg) => OutputLog(msg);
        portChecker.GetReceiceAsync += GetReceiceAsync;
        portChecker.RestartJavaServiceAsync+= RestartJavaServiceAsync;
        portChecker.Stop();
        portChecker.Start();

        //是否使用密钥
        if (_config.Common.EnableEncryption)
        {
            key = new KeyServiceClient(_config.KeyService);
        }

         

        _logServices.Info($"数据采集配置初始化完成，当前配置：{config}");
        

    }
  private static SerialPortDataReader serialPortReader = null;
 
private string voltage = "0";
    private readonly object lockObject = new object();

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
                        _logServices.Debug("开始计算局放");
                        _logServices.Info("开始计算局放");
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
            
            while (!_cts.Token.IsCancellationRequested)
            {
                try
                {
                    if (_dataQueueStr.IsEmpty)
                    {
                        await Task.Delay(100, _cts.Token);
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

                await Task.Delay(100, _cts.Token);
            }
        }, _cts.Token);
    }

    private async Task ProcessDataFrameAsync(DataFrame obj)
    {
        try
        {
            // 1. 读取数据并解析
            var plaitResult = Reader_OnDataRead(obj);

            // 2. 调试模式直接写日志，不进入上传队列
            if (_config.Common.DebugMode)
            {
                await DataWriteLogAsync(JsonConvert.SerializeObject(plaitResult));
                return;
            }

            // 3. 构建 DataMessage
            var msg = new DataMessage
            {
                plain = JsonConvert.SerializeObject(plaitResult)
            };

            // 4. 加密（如果需要）
            if (_config.Common.EnableEncryption)
            {
                AddJsonItems(msg.plain);
                if (currentCounts == BATCH_SIZEs)
                {
                    currentBatchs.Append("]");// 结束数组
                    string jsonArray = currentBatchs.ToString();
                    string requestJson = $@"{{ ""timestamp"": {plaitResult.timestamp}, ""deviceMac"": ""{macAddress}"",""deviceCode"": ""{_config.WinConfig.DATA_COLLECT_DEVICE_NO}"",""items"": {jsonArray}}}";
                    var keyResult = await GetKey(requestJson);
                 
                    if (keyResult == null)
                    {
                        _recvQueueStr.Writer.TryWrite(msg.plain);

                        return;
                    }
                    //msg.key = keyResult.key;
                    //msg.cipher = keyResult.cipher;
                    //msg.time_range = keyResult.time_range.ToArray();
                    //msg.isencrypted = true;
                    //msg.sign = keyResult.sign;
                    //msg.time_Id = keyResult.time_Id;
                    //msg.ivBase64 = keyResult.ivBase64;
                    // 6. 尝试批量上传（非阻塞，可能会在后台执行）
                    var result = new 
                    {
                        deviceMac = macAddress,
                        deviceCode= _config.WinConfig.DATA_COLLECT_DEVICE_NO,
                        dateCollectDeivceNO = _config.WinConfig.DEVICE_CODE,
                        items = keyResult.items.ToList()

                    };
                    await SenBatchdData(JsonConvert.SerializeObject(result), _config.UpMQTT.IOTDataTopic);
                }
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
              await foreach (var item in _recvQueueStr.Reader.ReadAllAsync(_cts.Token))
              {
                  if (!portChecker.IsPortOpen)
                  {
                      await Task.Delay(1000);
                      continue;
                  }

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
                    if (_config.Common.EnableEncryption)
                      {
                        AddJsonItem(item);
                        if (currentCount ==1000)
                        {
                            currentBatch.Append("]");// 结束数组
                            string jsonArray = currentBatch.ToString();
                         

                            string requestJson = $@"{{ ""timestamp"": {timestamp}, ""deviceMac"":""{macAddress}"", ""items"": {jsonArray}}}";
                            var keyResult = await GetKey(requestJson);
                            // 重置计数器和StringBuilder
                            currentBatch.Clear();
                            currentCount = 0;
                            if (keyResult is null)
                            {
                                return;
                            }

                            var result = new
                            {
                                deviceMac=macAddress,
                                deviceCode = _config.WinConfig.DATA_COLLECT_DEVICE_NO,
                                dateCollectDeivceNO = _config.WinConfig.DEVICE_CODE,
                                items = keyResult.items.ToList()

                            };
                             
                            await SenBatchdData(JsonConvert.SerializeObject(result), _config.UpMQTT.CommandDataTopic);
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
          }
          catch 
          {
             

          }
          finally { 
              

          }
         

   
  }


    StringBuilder currentBatchs = new StringBuilder();
    int currentCounts = 0;
    const int BATCH_SIZEs = 1000;

    // 全局变量
    StringBuilder currentBatch = new StringBuilder();
    int currentCount = 0;
    const int BATCH_SIZE = 1000;
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
    // 将消息加入队列，丢弃最旧的消息以控制内存
    private void EnqueueMessage(DataMessage msg)
    {
        // 如果队列已满，先丢弃一条最旧的消息
        if (_queueCount >= _maxPendingMessages)
        {
            _dataMessageQueue.TryDequeue(out _);
            Interlocked.Decrement(ref _queueCount);
        }

        _dataMessageQueue.Enqueue(msg);
        Interlocked.Increment(ref _queueCount);
    }

    // 尝试批量取出并上传
    private async Task TryBatchUploadAsync()
    {
        //await _batchUploadSemaphore.WaitAsync();
        try
        {
            // 如果队列长度不足批量大小，跳过
            if (_queueCount < _batchUploadSize)
                return;

            var batch = new List<DataMessage>(_batchUploadSize);
            for (int i = 0; i < _batchUploadSize; i++)
            {
                if (_dataMessageQueue.TryDequeue(out var msg))
                {
                    batch.Add(msg);
                    Interlocked.Decrement(ref _queueCount);
                }
                else
                    break;
            }

            if (batch.Count > 0)
            {
                var jsonBatch = JsonConvert.SerializeObject(batch);
                _= SenBatchdData(jsonBatch, _config.UpMQTT.IOTDataTopic);
            }
        }
        finally
        {
            //_batchUploadSemaphore.Release();
        }
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
        dictionary3.Add("DISCHARGE",  JsonConvert.SerializeObject(decimals));
        //SetVoltage
        //Math.Round(GetFirstElementByHexIndex(decimals, _config.Common.CHANNEL), 2, MidpointRounding.ToEven).ToString()
        dictionary3.Add("VOLTAGE", GetVoltage());
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
                             logPath = _iniFileManager.GetValue("NEWDATACOLLECTION", "FILEURL");
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
        var thread = new Thread(() =>
        {
            Thread.Sleep(2000);
            OutputLog("数据采集开始");
            _logServices.Info("数据采集开始");
           string  commandstr = _iniFileManager.GetValue("NEWDATACOLLECTION", "COMMAND");
            byte[] sendData = StringUtil.HexStringToByteArray(commandstr);
            //eaeaeaea01000001c0a8100a010100000006000000064058aeaeaeae
            long freq = Stopwatch.Frequency;
            long ticksPerMs = Math.Max(1, freq / 1000); // 基础节拍（向下取整）
            long remainderTicks = freq % 1000; // 余数，用于分摊误差
            long remainderAcc = 0;
            long ticksPerSend = ticksPerMs; // 目标：1ms 一次（平均意义下）
            long nextTick = Stopwatch.GetTimestamp() + ticksPerSend;

            int sendCount = 0;
            long lastSecond = DateTimeOffset.UtcNow.ToUnixTimeSeconds();

            timeBeginPeriod(1);
            try
            {
                while (!token.IsCancellationRequested && socketClient.IsConnected)
                {
                    try
                    {
                        WaitUntilStopwatchTicks(nextTick, ticksPerMs, token);
                        if (token.IsCancellationRequested || !socketClient.IsConnected) break;

                        
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

        thread.IsBackground = true;
        thread.Priority = ThreadPriority.Highest;
        thread.Start();
    }
    public async Task SenBatchdData(string message, string topic)
    {
        await _dataUploader.BatchUploadAsync(message, topic);
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


    public void Stop()
    {
        _isDataCollecting = false;
        StopFileLogFlushTask();
        _cts.Cancel();
        _heartbeatSender = null;
        _dataUploader = null;
        socketClient.Stop();
        _nativeApiServer?.Stop();
        _logServices.Info("数据采集已停止");
        OutputLog("数据采集结束");
    }

    public async Task RestartJavaServiceAsync()
    {
        _javaServiceManagers.OnOutputLog -= (msg) => OutputLog(msg);
        await Task.Delay(100); // 确保事件解绑完成
        _javaServiceManagers.OnOutputLog += (msg) => OutputLog(msg);
        await _javaServiceManagers.RestartJavaServiceAsync(_config.JavaConfig.JavaPath, _config.JavaConfig.JarPath);


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
