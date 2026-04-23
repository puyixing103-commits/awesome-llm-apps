using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

using DataAcquisitionLibrary;
using DataAcquisitionLibrary.Utils;

using DC_0003;
using DC_0003.Core.Interfaces;
using DC_0003.Models;

using Newtonsoft.Json;

namespace DC_0003.Services.Implements.Processors
{
    public class TCPDataProcessor : IDataProcessor, INotifyLogOutput
    {
        private readonly IDataReader _reader;
        private readonly IDataSender _sender;
        private IMqttServices _mqttServices;
        private readonly CancellationTokenSource _cancellationToken;
        private Instruction _baseInstruction;
        private MqttConfigModel _mqttConfig;
        private ILogServices _logger;
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

        public event Action<string> OnLogOutputEvent;

        public TCPDataProcessor(IDataReader reader,IDataSender sender,MqttConfigModel mqttConfig,CancellationTokenSource cancellationToken)
        {
            _logger = LogServices.Instance;
            _reader = reader;
            _sender = sender;
            _cancellationToken = cancellationToken;
            _mqttConfig = mqttConfig;
            
            _reader.OnDataRead += Reader_OnDataRead;
            if(reader is INotifyLogOutput readerLog)
            {
                readerLog.OnLogOutputEvent += OutputLog;
            }
            if(sender is INotifyLogOutput senderLog)
            {
                senderLog.OnLogOutputEvent += OutputLog;
            }
        }

        private void MqttServices_MessageReceivedAsync(string mqttMessageStr)
        {
            MQTTMessage mqttMessage = JsonConvert.DeserializeObject<MQTTMessage>(mqttMessageStr);
            if(_baseInstruction  == null)
            {
                _baseInstruction = new Instruction();
            }
            _baseInstruction.Set("MQTTMessage", mqttMessage);
            switch (mqttMessage.commandCode)
            {
                case "COMMAND_START":
                case "SEND_ENTITY_IDS":
                    // 执行采集任务
                    _logger.Info($"接收到开始采集指令");
                    OutputLog($"接收到开始采集指令");
                    Execute(_baseInstruction);
                    break;
                case "COMMAND_COMPLETE":
                case "SEND_STOP_ENTITY_IDS":
                    // 停止采集任务
                    _logger.Info($"接收到结束采集指令");
                    OutputLog($"接收到结束采集指令");
                    _cancellationToken.Cancel();
                    break;
            }
        }

        private void Reader_OnDataRead(IDataReader sender, DataFrame arg)
        {
            string payload = StringUtil.ByteArrayToHexString(arg.Payload).Replace(" ","");
            int headLen = Convert.ToInt32(arg.Get("HEAD_LEN"));
            int tailLen = Convert.ToInt32(arg.Get("TAIL_LEN"));
            string data = payload.Substring(headLen, payload.Length - headLen - tailLen);
            int funcCode = Convert.ToInt32(arg.Get("FUNC_CODE"));
            List<decimal> decimals = StringUtil.HexToFloatDecimals(data);
            OutputLog($"接收到数据:{JsonConvert.SerializeObject(decimals)}");
            MQTTMessage originMessage = _baseInstruction.Get("MQTTMessage") as MQTTMessage;
            string commandSerial = StringUtil.GenerateUniqueRandomString();
            object mqttData = null;
            long timestamp = CommonTimeStamp.GetUnixTimeStampSeconds(DateTime.Now);
            switch (funcCode)
            {
                case 0x01:
                    // 码点数据
                    mqttData = new
                    {
                        commandSerial,
                        timestamp,
                        commandMessage = originMessage,
                        code = "0000",
                        msg = "success",
                        data = new
                        {
                            discharge1 = decimals.FirstOrDefault(),
                            dischargeNum1 = decimals.ElementAtOrDefault(1),
                            dischargePhase1 = decimals.ElementAtOrDefault(2),
                            discharge2 = decimals.ElementAtOrDefault(3),
                            dischargeNum2 = decimals.ElementAtOrDefault(4),
                            dischargePhase2 = decimals.ElementAtOrDefault(5),
                        }
                    };
                    break;
                case 0x02:
                    // 结果数据
                    mqttData = new
                    {
                        commandSerial,
                        timestamp,
                        commandMessage=originMessage,
                        code="0000",
                        msg="success",
                        data = new
                        {
                            PD_Wave = decimals
                        }
                    };
                    break;
                case 0x18:
                default:
                    mqttData = new
                    {
                        commandSerial,
                        timestamp,
                        commandMessage = originMessage,
                        code = "0001",
                        msg = "暂不支持该指令"
                    };
                    break;
            }
            string response = JsonConvert.SerializeObject(mqttData, Formatting.Indented);
            if(_mqttConfig is CustomMQTTConfigModel customModel)
            {
                _mqttServices.PublishAsync(response, customModel.CommandResponseTopic);
            }
        }

        public void Execute(Instruction instruction)
        {
            Task.Run(async () =>
            {
                while (!_cancellationToken.IsCancellationRequested)
                {
                    string deviceIp = instruction.Get("DEVICE_IP")?.ToString()?.Replace(".", " "); // 设备 IP
                    string typeCode = instruction.Get("TYPE_CODE")?.ToString(); // 类型码
                    string funcCode = instruction.Get("FUNC_CODE")?.ToString(); // 功能码
                    string dataLen = instruction.Get("DATA_LEN")?.ToString(); // 数据长度
                    string channel = instruction.Get("CHANNEL")?.ToString(); // 通道号
                    string dataNum = instruction.Get("DATA_NUM")?.ToString(); //寄存器数量 / 波形数据数量
                    string payload = $"{deviceIp} {typeCode} {funcCode} {dataLen} {channel} {dataNum}".Replace(" ", "");
                    string crcCode = CRCUtil.GetCRC3(CRCHigh, CRCLow, StringUtil.HexStringToByteArray(payload)).ToString();
                    string sendMessage = $"EAEAEAEA1001{payload}{crcCode}AEAEAEAE"; // 发送的消息
                    await _sender.SendAsync(instruction.Source, StringUtil.HexStringToByteArray(sendMessage));
                    await _reader.ReadAsStringAsync(instruction.Source);
                    await Task.Delay(TimeSpan.FromMilliseconds(int.TryParse(instruction.Get("INTERVAL").ToString(), out int interval) ? interval : 1));
                }
            }, _cancellationToken.Token);
        }

        public void OutputLog(string message)
        {
            if (!message.EndsWith(Environment.NewLine))
            {
                message = $"{message}{Environment.NewLine}";



            }
            OnLogOutputEvent?.Invoke(message);
        }

        public void SetBaseInstruction(Instruction instruction)=>_baseInstruction = instruction;

        public void Dispose()
        {
            _cancellationToken.Cancel();
            _mqttServices.DisconnectAsync().Wait();
            _mqttServices = null;
        }

        public void Init(Instruction instruction)
        {
            if(_mqttServices != null)
            {
                _mqttServices = new MqttServices(_mqttConfig);
                _mqttServices.Connect()
                    .ContinueWith(_ => _mqttServices.StartHeartbeatService())
                    .ContinueWith(_ =>
                    {
                        if (_mqttConfig is CustomMQTTConfigModel customModel)
                        {
                            _mqttServices.SubscribeAsync(customModel.CommandTopic);
                        }
                    });
                _mqttServices.HeartbeatCallBack += (b, s) => OutputLog($"心跳检测{(b ? "成功" : "失败")}：{s}{Environment.NewLine}");
                _mqttServices.MessageReceivedAsync += MqttServices_MessageReceivedAsync;
            }
        }
    }
}
