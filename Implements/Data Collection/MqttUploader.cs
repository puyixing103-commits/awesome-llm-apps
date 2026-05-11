using System;
using System.Reflection;
using System.Runtime.InteropServices;
using System.Runtime.Remoting.Messaging;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

using DataAcquisitionLibrary;

using DC_0003;
using DC_0003.Core.Interfaces;
using DC_0003.Models;
using DC_0003.Services;
using DC_0003.Services.Implements.Sockets;

using Newtonsoft.Json;

using static DC_0003.Services.Implements.Data_Collection.Class2;

using static HttpUploader;

 
//public class MqttUploader: IUploader
//{
//    private readonly string _topic;

//    private static NewMqttServices _mqttServices;

//    private HBMqttConfig _hBMqtt;

//    private WinConfig winConfig;

//    private CustomMQTTConfigModel _mqttConfig;

//    public event Action<string> OnStartCommand;
//    public event Action<string> OnStopCommand;
//    public event Action<string> OnLogOutput;

//    private readonly ILogServices logServices = LogServices.Instance;

//    public MqttUploader(CustomMQTTConfigModel mqttConfig, WinConfig _winConfig, HBMqttConfig hBMqttConfig)
//    {
//        winConfig = _winConfig;
//        _mqttConfig = mqttConfig;
//        _hBMqtt = hBMqttConfig;
//        _=Init();
//    }
//    public async Task Init()
//    {
//        if (_mqttServices != null)
//        {
//            _mqttServices.Dispose();
//            _mqttServices = null;
//        }
//        if (_mqttServices == null)
//        {
//            _mqttServices = new NewMqttServices(_mqttConfig);
//            await _mqttServices.ConnectAsync()
//                .ContinueWith(_ => {
//                    if (_mqttConfig.HeartbeatEnable)
//                    {
//                        _mqttServices.StartHeartbeatService(_mqttConfig, winConfig, _hBMqtt);
//                    }
                   
//                })
//                .ContinueWith(_ =>
//                {
//                       _mqttServices.SubscribeAsync(_mqttConfig.CommandTopic).Wait();
                    
//                })
//                .ContinueWith(_ => OutputLog($"MQTT服务初始化完成"));
//            _mqttServices.HeartbeatCallBack += (b, s) => OutputLog($"心跳检测{(b ? "成功" : "失败")}：{s}{Environment.NewLine}");
//            _mqttServices.MessageReceivedAsync += MqttServices_MessageReceivedAsync;
//        }
//    }

//    public void OutputLog(string message)
//    {
//        if (!message.EndsWith(Environment.NewLine))
//        {
//            message = $"{message}{Environment.NewLine}";
//        }
//      //  OnLogOutputEvent?.Invoke(message);
//    }

//    private void MqttServices_MessageReceivedAsync(string mqttMessageStr)
//    {
       
//            MQTTMessage mqttMessage = JsonConvert.DeserializeObject<MQTTMessage>(mqttMessageStr);
//            if (mqttMessage == null) return;
//            logServices.Info($"MQTT 接收到消息: {mqttMessageStr}");
//            switch (mqttMessage.commandCode)
//            {
//                case "SEND_ENTITY_IDS":
//                case "COMMAND_START":
//                    OnLogOutput?.Invoke($"MQTT 接收到开始采集指令:{mqttMessageStr}");
//                    OnStartCommand?.Invoke(mqttMessageStr);
//                    break;

//                case "SEND_STOP_ENTITY_IDS":
//                case "COMMAND_COMPLETE":
//                    OnLogOutput?.Invoke($"MQTT 接收到停止采集指令:{mqttMessageStr}");
//                    OnStopCommand?.Invoke(mqttMessageStr);
//                    break;

//                default:
//                    OnLogOutput?.Invoke($"MQTT 接收到未知指令:{mqttMessage.commandCode}");
//                    break;
//            }
      
//    }

//    public async Task publish(string topic, string payload)
//    {
//         await _mqttServices.PublishAsync(topic, payload);
//    }


//    public async Task<bool> UploadAsync(DataMessage message,string topic)
//    {
//        try
//        {
//            var json = JsonConvert.SerializeObject(message);
//            await _mqttServices.PublishCommandDataAsync(json, topic);
//            return true;
//        }
//        catch
//        {
//            return false;
//        }
//    }


//    public void Dispose()
//    {
//        _mqttServices?.Dispose();
//    }

//    public async Task<bool> BatchUploadAsync(string message, string topic)
//    {
//        try
//        {
//            await _mqttServices.PublishCommandDataAsync(message, topic);
//            return true;
//        }
//        catch
//        {
//            return false;
//        }
//    }
//}