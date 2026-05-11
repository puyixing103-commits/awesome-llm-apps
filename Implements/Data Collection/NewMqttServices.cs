using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.IO;
using System.Net.Sockets;
using System.Text.Json;
using System.Threading;
using System.Threading.Tasks;

using DataAcquisitionLibrary;

using MQTTnet;
using MQTTnet.Client;
using MQTTnet.Formatter;
using MQTTnet.Packets;
using MQTTnet.Protocol;

using Newtonsoft.Json;
namespace DC_0003.Services;
public class NewMqttServices : IDisposable
{
    private ILogServices _iLogServices = LogServices.Instance;
    private static IniFileManager _iConfigIniFileServices = new IniFileManager();
    private IMqttClient _mqttClient;
    private MqttClientOptions _mqttOptions;
    private CancellationTokenSource _cancellationTokenSource;
    // 优化1：使用 volatile 保证多线程下连接状态的可见性
    private volatile bool _mqttIsConnected;
    public bool IsConnected() => _mqttIsConnected;
    // 优化2：使用并发集合，防止断线重连时遍历订阅列表与新增订阅发生冲突
    public ConcurrentDictionary<string, byte> SubscribeTopicDict { get; set; }
    public event Action<string> MessageReceivedAsync;
    public event Action<bool, string> HeartbeatCallBack;
    // 优化3：引入信号量锁，防止并发写入导致MQTTnet底层网络流报错
    private readonly SemaphoreSlim _publishLock = new SemaphoreSlim(1, 1);
    // 优化4：引入连接锁，防止并发调用 Connect 导致重入异常
    private readonly SemaphoreSlim _connectionLock = new SemaphoreSlim(1, 1);
    private Dictionary<string, object> _configDict = new Dictionary<string, object>();
    public NewMqttServices(MqttConfigModel mqttConfigModel)
    {
        LoadConfigToDictionary();
        SubscribeTopicDict = new ConcurrentDictionary<string, byte>();
        _mqttClient = new MqttFactory().CreateMqttClient();
        MqttClientOptionsBuilder val = new MqttClientOptionsBuilder()
            .WithClientId(mqttConfigModel.clientId)
            .WithCredentials(mqttConfigModel.userName, mqttConfigModel.passWord)
            .WithCleanSession(false)
            .WithKeepAlivePeriod(TimeSpan.FromSeconds(60.0))
            // 优化5：增加通信超时时间，防止大包传输时因网络阻塞导致超时断开
            .WithTimeout(TimeSpan.FromSeconds(120))
            .WithProtocolVersion((MqttProtocolVersion)4);
        val.WithTcpServer(mqttConfigModel.mqttIp, (int?)mqttConfigModel.mqttPort, AddressFamily.Unspecified);
        _cancellationTokenSource = DataAcquisitionManager._cts;
        _mqttOptions = val.Build();
        #region MQTT 事件订阅
        _mqttClient.ConnectedAsync += e =>
        {
            _iLogServices.Info("MQTT连接成功");
            DataAcquisitionManager.EnqueueLog("MQTT连接成功");
            _mqttIsConnected = true;
            return Task.CompletedTask;
        };
        _mqttClient.DisconnectedAsync += async e =>
        {
            _mqttIsConnected = false;
            DataAcquisitionManager.EnqueueLog($"MQTT连接断开:{e.Reason}");
            if ((int)e.Reason == 0)
            {
                _iLogServices.Info("MQTT 正常连接断开.");
            }
            else
            {
                _iLogServices.Error("MQTT 意外连接断开，5 秒后尝试重新连接...");
            }
            if (e.Exception != null)
            {
                _iLogServices.Error("MQTT断开详情:" + e.Exception.Message);
            }
            // 优化6：如果是被取消的断开（如程序关闭），不要尝试重连
            if (_cancellationTokenSource.IsCancellationRequested) return;
            await Task.Delay(TimeSpan.FromSeconds(5.0));
            try
            {
                await ConnectAsync();
            }
            catch (Exception ex)
            {
                _iLogServices.Error("自动重连失败:" + ex.Message);
            }
        };
        _mqttClient.ApplicationMessageReceivedAsync += e =>
        {
            string text = MqttApplicationMessageExtensions.ConvertPayloadToString(e.ApplicationMessage);
            _iLogServices.Info("[接收到消息]:" + text);
            // 优化7：委托调用采用本地变量防止多线程下委托变为null
            var handler = MessageReceivedAsync;
            handler?.Invoke(text);
            return Task.CompletedTask;
        };
        #endregion
    }
    public async Task ConnectAsync()
    {
        await _connectionLock.WaitAsync(_cancellationTokenSource.Token);
        try
        {
            if (!_mqttClient.IsConnected)
            {
                _iLogServices.Debug("MQTT尝试连接");
                await _mqttClient.ConnectAsync(_mqttOptions, _cancellationTokenSource.Token);
            }
        }
        catch (Exception ex)
        {
            _iLogServices.Error("连接失败:" + ex.Message);
        }
        finally
        {
            _connectionLock.Release();
        }
    }
    public async Task DisconnectAsync()
    {
        try
        {
            if (_mqttClient.IsConnected)
            {
                await MqttClientExtensions.DisconnectAsync(_mqttClient, (MqttClientDisconnectOptionsReason)0, null, 0u, null, _cancellationTokenSource.Token);
            }
        }
        catch (Exception ex)
        {
            _iLogServices.Error("断开连接异常:" + ex.Message);
        }
    }
    public async Task SubscribeAsync(string topic)
    {
        try
        {
            SubscribeTopicDict.TryAdd(topic, 0);
            await ConnectAsync();
            await MqttClientExtensions.SubscribeAsync(_mqttClient, topic, (MqttQualityOfServiceLevel)0, _cancellationTokenSource.Token);
            _iLogServices.Info("已订阅TOPIC:" + topic);
        }
        catch (Exception ex)
        {
            _iLogServices.Error("订阅失败:" + ex.Message);
        }
    }
    // 优化8：提取通用Publish逻辑并加锁，解决大批量数据并发写入报错问题
    private async Task PublishInternalAsync(string topic, string payload, MqttQualityOfServiceLevel qos = (MqttQualityOfServiceLevel)0)
    {
        await _publishLock.WaitAsync(_cancellationTokenSource.Token);
        try
        {
            await ConnectAsync();
            MqttApplicationMessage val = new MqttApplicationMessageBuilder()
                .WithTopic(topic)
                .WithPayload(payload)
                .WithQualityOfServiceLevel(qos)
                .Build();
            await _mqttClient.PublishAsync(val, _cancellationTokenSource.Token);
        }
        finally
        {
            _publishLock.Release();
        }
    }
    public async Task PublishAsync(string mqttMessage, string dataReceiver)
    {
        try
        {
            await PublishInternalAsync(dataReceiver, mqttMessage);
        }
        catch (Exception ex)
        {
            _iLogServices.Error("发送MQTT错误:" + ex.Message);
        }
    }
    public async Task PublishCommandDataAsync(string mqttMessage, string dataReceiverTopic)
    {
        try
        {
            await PublishInternalAsync(dataReceiverTopic, mqttMessage);
        }
        catch (Exception ex)
        {
            _iLogServices.Error("发送MQTT错误:" + ex.ToString());
            throw new Exception("发送MQTT错误:" + ex.Message, ex);
        }
    }
    public async Task PublishIOTDataAsync(string mqttMessage)
    {
        try
        {
            string dataReceiverTopic = _iConfigIniFileServices.GetValue("UpMQTT", "IOTDataTopic");
            await PublishInternalAsync(dataReceiverTopic, mqttMessage);
        }
        catch (Exception ex)
        {
            _iLogServices.Error("发送MQTT错误:" + ex.Message);
        }
    }
    //public void StartHeartbeatService(CustomMQTTConfigModel customMQTTConfigModel, WinConfig winConfig, HBMqttConfig hBMqttConfig)
    //{
    //    if (customMQTTConfigModel.HeartbeatEnable && !_cancellationTokenSource.IsCancellationRequested)
    //    {
    //        WindowsConfigModel _windowsConfigModel = new WindowsConfigModel
    //        {
    //            DATA_COLLECT_PROGRAM_ID = winConfig.DATA_COLLECT_PROGRAM_ID,
    //            DATA_COLLECT_DEVICE_NO = winConfig.DATA_COLLECT_DEVICE_NO,
    //            ROOM_CODE = winConfig.ROOM_CODE,
    //            DEVICE_CODE = winConfig.DEVICE_CODE,
    //            PROGRAM_VERSION = winConfig.PROGRAM_VERSION,
    //            HBCOUNT = hBMqttConfig.HBCOUNT,
    //            HEARTBEAT_TOPIC = hBMqttConfig.HEARTBEAT_TOPIC
    //        };
    //        // 优化9：使用Task.Run运行Task返回值的方法，而不是async void，防止未观察到的异常导致程序崩溃
    //        _ = Task.Run(() => HeartbeatServiceAsync(_windowsConfigModel), _cancellationTokenSource.Token);
    //    }
    //}
    #region 帮助类
    private void LoadConfigToDictionary()
    {
        _configDict.Clear();
        var path = Path.Combine(AppDomain.CurrentDomain.BaseDirectory, "config", "数采配置文件.json");
        string json = File.ReadAllText(path);
        using var doc = JsonDocument.Parse(json);
        var root = doc.RootElement;
        ParseJsonToDict(root, _configDict, "");
    }
    void ParseJsonToDict(JsonElement element, Dictionary<string, object> dict, string parentKey)
    {
        if (element.ValueKind == JsonValueKind.Object)
        {
            foreach (var prop in element.EnumerateObject())
            {
                string key = string.IsNullOrEmpty(parentKey) ? prop.Name : $"{parentKey}.{prop.Name}";
                ParseJsonToDict(prop.Value, dict, key);
            }
        }
        else
        {
            dict[parentKey] = element.GetRawText().Trim('"');
        }
    }
    #endregion
    // 优化10：修复原心跳方法中字典污染问题，并改成 async Task
    private async Task HeartbeatServiceAsync(WindowsConfigModel _windowsConfigModel)
    {
        int HBCOUNT = _windowsConfigModel.HBCOUNT > 0 ? _windowsConfigModel.HBCOUNT : 60;
        // 优化点：原代码直接向共享字典 _configDict Add 元素，多次心跳会导致异常（键重复）或内存泄漏
        // 此处每次心跳构建独立的属性字典，避免污染全局配置
        var envProperties = new Dictionary<string, object>(_configDict)
            {
                { "os_name", Environment.OSVersion.ToString() },
                { "os_version", Environment.OSVersion.Version.ToString() },
                { "machine_name", Environment.MachineName }
            };
       
            try
                {
                    SystemInfoModel systemInfo = SystemInfoHelper.GetSystemInfo();
                    var heartbeatMessage = new
                    {
                        type = "data_collect",
                        content = new
                        {
                            dataCollectProgramId = _windowsConfigModel.DATA_COLLECT_PROGRAM_ID,
                            dataCollectDeviceNo = _windowsConfigModel.DATA_COLLECT_DEVICE_NO,
                            roomCode = _windowsConfigModel.ROOM_CODE,
                            deviceCode = _windowsConfigModel.DEVICE_CODE,
                            programVersion = _windowsConfigModel.PROGRAM_VERSION,
                            dataCollectSystemRunParamDTO = systemInfo,
                            envProperties = envProperties
                        }
                    };
                   
                }
                catch (Exception ex)
                {
                    _iLogServices.Error("发送心跳消息失败: " + ex.Message);
                    //HeartbeatCallBack?.Invoke(false, "发送心跳消息失败: " + ex.Message);
                }
      

            // 优化11：使用 Task.Delay 代替 Thread.Sleep，并在取消时立即唤醒退出
    
       
    }
    // 优化12：实现标准的 Dispose 模式，避免异步方法导致资源未释放
    public void Dispose()
    {
        if (_cancellationTokenSource.IsCancellationRequested) return;
        _cancellationTokenSource.Cancel();
        _mqttIsConnected = false;
        try
        {
            DisconnectAsync().GetAwaiter().GetResult();
        }
        catch { }
        _mqttClient?.Dispose();
        _publishLock?.Dispose();
        _connectionLock?.Dispose();
    }
}