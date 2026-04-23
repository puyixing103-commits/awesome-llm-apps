#region 程序集 DataAcquisitionLibrary, Version=1.0.0.0, Culture=neutral, PublicKeyToken=null
// D:\workspace\tuoli\CSharp\References\DataAcquisitionLibrary\Libs\Release\DataAcquisitionLibrary.dll
// Decompiled with ICSharpCode.Decompiler 8.2.0.7535
#endregion

using System;
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

using static System.Net.Mime.MediaTypeNames;

namespace DC_0003.Services;

public class NewMqttServices
{
    private ILogServices _iLogServices = LogServices.Instance;

    private static IniFileManager _iConfigIniFileServices = new IniFileManager();

    private IMqttClient mqttClient;

    private MqttClientOptions mqttOptions;

    private CancellationTokenSource _cancellationTokenSource;

    private bool HeartbeatServiceIsStart;

    private bool MqttIsConnected;

    public IList<string> SubscribeTopicList { get; set; }

    public event Action<string> MessageReceivedAsync;

    public event Action<bool, string> HeartbeatCallBack;

    public NewMqttServices(MqttConfigModel mqttConfigModel)
    {
        LoadConfigToDictionary();
        //IL_001d: Unknown result type (might be due to invalid IL or missing references)
        //IL_002c: Unknown result type (might be due to invalid IL or missing references)
        SubscribeTopicList = new List<string>();
        mqttClient = new MqttFactory().CreateMqttClient();
        MqttClientOptionsBuilder val = new MqttClientOptionsBuilder().WithClientId(mqttConfigModel.clientId).WithCredentials(mqttConfigModel.userName, mqttConfigModel.passWord).WithCleanSession(false)
            .WithKeepAlivePeriod(TimeSpan.FromSeconds(60.0))
            .WithProtocolVersion((MqttProtocolVersion)4);
        val.WithTcpServer(mqttConfigModel.mqttIp, (int?)mqttConfigModel.mqttPort, AddressFamily.Unspecified);
        // 使用全局的取消令牌
        _cancellationTokenSource = DataAcquisitionManager._cts;
        mqttOptions = val.Build();
        mqttClient.ConnectedAsync += async delegate
        {
            Console.WriteLine("MQTT连接成功");
            DataAcquisitionManager.EnqueueLog("MQTT连接成功");
            _iLogServices.Info("MQTT连接成功");
            MqttIsConnected = true;
        };
        mqttClient.DisconnectedAsync += async delegate (MqttClientDisconnectedEventArgs e)
        {
            DataAcquisitionManager.EnqueueLog($"MQTT正常连接断开:{e.Reason}");
            MqttIsConnected = false;
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

            await Task.Delay(TimeSpan.FromSeconds(5.0));
            await mqttClient.ConnectAsync(mqttOptions, default(CancellationToken));
            if (SubscribeTopicList != null && SubscribeTopicList.Count > 0)
            {
                for (int i = 0; i < SubscribeTopicList.Count; i++)
                {
                    await MqttClientExtensions.SubscribeAsync(mqttClient, SubscribeTopicList[i], (MqttQualityOfServiceLevel)0, default(CancellationToken));
                }
            }
        };
        mqttClient.ApplicationMessageReceivedAsync += async delegate (MqttApplicationMessageReceivedEventArgs e)
        {
            string text = MqttApplicationMessageExtensions.ConvertPayloadToString(e.ApplicationMessage);
            _iLogServices.Info("[接收到消息]:" + text);
            this.MessageReceivedAsync?.Invoke(text);
            await Task.CompletedTask;
        };
    }

    public async Task Connect()
    {
        try
        {
            if (!MqttIsConnected)
            {
                _iLogServices.Debug("MQTT尝试连接");
                await mqttClient.ConnectAsync(mqttOptions, default(CancellationToken));
            }
        }
        catch (Exception ex)
        {
            _iLogServices.Error("连接失败:" + ex.Message);
        }
    }

    public async Task DisconnectAsync()
    {
        if (IsConnected())
        {
            await MqttClientExtensions.DisconnectAsync(mqttClient, (MqttClientDisconnectOptionsReason)0, (string)null, 0u, (List<MqttUserProperty>)null, default(CancellationToken));
        }

        _cancellationTokenSource.Cancel();
        ((IDisposable)mqttClient).Dispose();
    }

    public async Task SubscribeAsync(string topic)
    {
        _ = 1;
        try
        {
            if (!SubscribeTopicIsExist(topic))
            {
                SubscribeTopicList.Add(topic);
            }

            await Connect();
            await MqttClientExtensions.SubscribeAsync(mqttClient, topic, (MqttQualityOfServiceLevel)0, default(CancellationToken));
            _iLogServices.Info("已订阅TOPIC:" + topic);
        }
        catch (Exception ex)
        {
            _iLogServices.Error("订阅失败:" + ex.Message);
        }
    }

    private async Task Mqtt_client_received(MqttApplicationMessageReceivedEventArgs arg)
    {
        await Task.Run(delegate
        {
            try
            {
                string text = MqttApplicationMessageExtensions.ConvertPayloadToString(arg.ApplicationMessage);
                _iLogServices.Info("[接收到消息]:" + text);
                if (this.MessageReceivedAsync != null)
                {
                    this.MessageReceivedAsync(text);
                }
            }
            catch (Exception ex)
            {
                _iLogServices.Error("接收数据异常:" + ex.Message);
            }
        });
    }

    public async Task PublishAsync(string mqttMessage, string dataReceiver)
    {
        _ = 1;
        try
        {
            await Connect();
            MqttApplicationMessage val = new MqttApplicationMessageBuilder().WithTopic(dataReceiver).WithPayload(mqttMessage).WithQualityOfServiceLevel((MqttQualityOfServiceLevel)0)
                .Build();
            await mqttClient.PublishAsync(val, default(CancellationToken));
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
            await PublishAsync(mqttMessage, dataReceiverTopic);
           // _iLogServices.Warning("[发送MQTT消息]TOPIC:" + dataReceiverTopic + "消息内容:" + mqttMessage);
        }
        catch (Exception ex)
        {
            _iLogServices.Error("发送MQTT错误:" + ex.ToString());
            throw new Exception("发送MQTT错误:" + ex.Message);
        }
    }

    public async Task PublishIOTDataAsync(string mqttMessage)
    {
        try
        {
            string dataReceiverTopic = _iConfigIniFileServices.GetValue("UpMQTT", "IOTDataTopic");
            await PublishAsync(mqttMessage, dataReceiverTopic);
            //_iLogServices.Info("[发送MQTT消息]TOPIC:" + dataReceiverTopic + "消息内容:" + mqttMessage);
        }
        catch (Exception ex)
        {
            _iLogServices.Error("发送MQTT错误:" + ex.Message);
        }
    }

    private bool SubscribeTopicIsExist(string topic)
    {
        if (SubscribeTopicList == null)
        {
            return false;
        }

        for (int i = 0; i < SubscribeTopicList.Count; i++)
        {
            if (SubscribeTopicList[i] == topic)
            {
                return true;
            }
        }

        return false;
    }

    public void StartHeartbeatService(CustomMQTTConfigModel customMQTTConfigModel,WinConfig winConfig, HBMqttConfig hBMqttConfig)
    {
        if (!HeartbeatServiceIsStart)
        {
            HeartbeatServiceIsStart = true;
            WindowsConfigModel _windowsConfigModel = new WindowsConfigModel();
            _windowsConfigModel.DATA_COLLECT_PROGRAM_ID = winConfig.DATA_COLLECT_PROGRAM_ID;
            _windowsConfigModel.DATA_COLLECT_DEVICE_NO = winConfig.DATA_COLLECT_DEVICE_NO;
            _windowsConfigModel.ROOM_CODE = winConfig.ROOM_CODE;
            _windowsConfigModel.DEVICE_CODE = winConfig.DEVICE_CODE;
            _windowsConfigModel.PROGRAM_VERSION = winConfig.PROGRAM_VERSION;
            _windowsConfigModel.HBCOUNT = hBMqttConfig.HBCOUNT;
            _windowsConfigModel.HEARTBEAT_TOPIC = hBMqttConfig.HEARTBEAT_TOPIC;

            if (customMQTTConfigModel.HeartbeatEnable)
            {
                Task.Run(delegate
                {
                    HeartbeatService(_windowsConfigModel);
                });
            }
           
        }
    }


    #region 帮助类
    // 存放所有子项的总字典
    Dictionary<string, object> _configDict = new Dictionary<string, object>();

    private void LoadConfigToDictionary()
    {
            _configDict.Clear();
            // 1. 找到配置文件路径（WinForm 根目录）
            
            var path = Path.Combine(AppDomain.CurrentDomain.BaseDirectory, "config", "数采配置文件.json");
           string json = File.ReadAllText(path);

            // 2. 解析 JSON
            using var doc = JsonDocument.Parse(json);
            var root = doc.RootElement;

            // 3. 递归把所有嵌套子项 放入字典
            ParseJsonToDict(root, _configDict, "");
       
    }

    // 递归解析 JSON（自动处理嵌套 { }）
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
    private async void HeartbeatService(WindowsConfigModel _windowsConfigModel)
    {
        int HBCOUNT = ((_windowsConfigModel.HBCOUNT > 0) ? _windowsConfigModel.HBCOUNT : 60);
        
        Dictionary<string, object> ConfigDiv = _configDict;// DataHelper.GetAllIniKeyValueDic(strPath);
        ConfigDiv.Add("os_name", Environment.OSVersion.ToString());
        ConfigDiv.Add("os_version", Environment.OSVersion.Version.ToString());
        ConfigDiv.Add("machine_name", Environment.MachineName);
        while (!_cancellationTokenSource.IsCancellationRequested)
        {
            if (MqttIsConnected)
            {
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
                            envProperties = ConfigDiv
                        }
                    };
                    MqttApplicationMessage val = new MqttApplicationMessageBuilder().WithTopic(_windowsConfigModel.HEARTBEAT_TOPIC).WithPayload(JsonConvert.SerializeObject(heartbeatMessage)).WithQualityOfServiceLevel((MqttQualityOfServiceLevel)1)
                        .Build();
                    await mqttClient.PublishAsync(val, default(CancellationToken));
                    //_iLogServices.Info("发送心跳消息: " + JsonConvert.SerializeObject(heartbeatMessage));
                    if (this.HeartbeatCallBack != null)
                    {
                        this.HeartbeatCallBack(arg1: true, JsonConvert.SerializeObject(heartbeatMessage));
                    }
                }
                catch (Exception ex)
                {
                    _iLogServices.Error("发送心跳消息失败: " + ex.Message);
                    if (this.HeartbeatCallBack != null)
                    {
                        this.HeartbeatCallBack(arg1: false, "发送心跳消息失败: " + ex.Message);
                    }
                }
            }
            else
            {
                _iLogServices.Error("发送心跳消息失败:MTQQ服务未连接");
                if (this.HeartbeatCallBack != null)
                {
                    this.HeartbeatCallBack(arg1: false, "发送心跳消息失败:MTQQ服务未连接");
                }
            }

            await Task.Delay(HBCOUNT * 1000);
        }
    }

    public bool IsConnected()
    {
        return MqttIsConnected;
    }

   

    public async void Dispose()
    {
        MqttIsConnected = false;
        await DisconnectAsync();
        ((IDisposable)mqttClient).Dispose();
    }
}
