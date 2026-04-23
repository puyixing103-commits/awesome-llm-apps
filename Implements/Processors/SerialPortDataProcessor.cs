using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Reflection;
using System.Text;
using System.Text.RegularExpressions;
using System.Threading;
using System.Threading.Tasks;

using DataAcquisitionLibrary;
using DataAcquisitionLibrary.Utils;

using DC_0003.Core.Constants;
using DC_0003.Core.Interfaces;
using DC_0003.Models;
using DC_0003.Services.Implements.Sockets;

using Newtonsoft.Json;

namespace DC_0003.Services.Implements.Processors
{
    public class SerialPortDataProcessor : IDataProcessor, IDisposable, INotifyLogOutput
    {
        private IDataReader _reader;
        private CustomMQTTConfigModel _mqttConfig;
        private CancellationTokenSource _cancellationTokenSource;
        private readonly CancellationTokenSource _parentCancellationToken;
        private IMqttServices _mqttServices;
        private Instruction _baseInstruction;
        private ILogServices _logServices;
        private ISocketDataReader _socketDataReader;
        // CRC 高位字节值表
        private static readonly byte[] CRCHigh = new byte[]
        {
            0x00, 0xC1, 0x81, 0x40, 0x01, 0xC0, 0x80, 0x41, 0x01, 0xC0,
    0x80, 0x41, 0x00, 0xC1, 0x81, 0x40, 0x01, 0xC0, 0x80, 0x41,
    0x00, 0xC1, 0x81, 0x40, 0x00, 0xC1, 0x81, 0x40, 0x01, 0xC0,
    0x80, 0x41, 0x01, 0xC0, 0x80, 0x41, 0x00, 0xC1, 0x81, 0x40,
    0x00, 0xC1, 0x81, 0x40, 0x01, 0xC0, 0x80, 0x41, 0x00, 0xC1,
    0x81, 0x40, 0x01, 0xC0, 0x80, 0x41, 0x01, 0xC0, 0x80, 0x41,
    0x00, 0xC1, 0x81, 0x40, 0x01, 0xC0, 0x80, 0x41, 0x00, 0xC1,
    0x81, 0x40, 0x00, 0xC1, 0x81, 0x40, 0x01, 0xC0, 0x80, 0x41,
    0x00, 0xC1, 0x81, 0x40, 0x01, 0xC0, 0x80, 0x41, 0x01, 0xC0,
    0x80, 0x41, 0x00, 0xC1, 0x81, 0x40, 0x00, 0xC1, 0x81, 0x40,
    0x01, 0xC0, 0x80, 0x41, 0x01, 0xC0, 0x80, 0x41, 0x00, 0xC1,
    0x81, 0x40, 0x01, 0xC0, 0x80, 0x41, 0x00, 0xC1, 0x81, 0x40,
    0x00, 0xC1, 0x81, 0x40, 0x01, 0xC0, 0x80, 0x41, 0x01, 0xC0,
    0x80, 0x41, 0x00, 0xC1, 0x81, 0x40, 0x00, 0xC1, 0x81, 0x40,
    0x01, 0xC0, 0x80, 0x41, 0x00, 0xC1, 0x81, 0x40, 0x01, 0xC0,
    0x80, 0x41, 0x01, 0xC0, 0x80, 0x41, 0x00, 0xC1, 0x81, 0x40,
    0x00, 0xC1, 0x81, 0x40, 0x01, 0xC0, 0x80, 0x41, 0x01, 0xC0,
    0x80, 0x41, 0x00, 0xC1, 0x81, 0x40, 0x01, 0xC0, 0x80, 0x41,
    0x00, 0xC1, 0x81, 0x40, 0x00, 0xC1, 0x81, 0x40, 0x01, 0xC0,
    0x80, 0x41, 0x00, 0xC1, 0x81, 0x40, 0x01, 0xC0, 0x80, 0x41,
    0x01, 0xC0, 0x80, 0x41, 0x00, 0xC1, 0x81, 0x40, 0x01, 0xC0,
    0x80, 0x41, 0x00, 0xC1, 0x81, 0x40, 0x00, 0xC1, 0x81, 0x40,
    0x01, 0xC0, 0x80, 0x41, 0x01, 0xC0, 0x80, 0x41, 0x00, 0xC1,
    0x81, 0x40, 0x00, 0xC1, 0x81, 0x40, 0x01, 0xC0, 0x80, 0x41,
    0x00, 0xC1, 0x81, 0x40, 0x01, 0xC0, 0x80, 0x41, 0x01, 0xC0,
    0x80, 0x41, 0x00, 0xC1, 0x81, 0x40
        };

        // CRC 低位字节值表
        private static readonly byte[] CRCLow = new byte[]
        {
            0x00, 0xC0, 0xC1, 0x01, 0xC3, 0x03, 0x02, 0xC2, 0xC6, 0x06,
        0x07, 0xC7, 0x05, 0xC5, 0xC4, 0x04, 0xCC, 0x0C, 0x0D, 0xCD,
        0x0F, 0xCF, 0xCE, 0x0E, 0x0A, 0xCA, 0xCB, 0x0B, 0xC9, 0x09,
        0x08, 0xC8, 0xD8, 0x18, 0x19, 0xD9, 0x1B, 0xDB, 0xDA, 0x1A,
        0x1E, 0xDE, 0xDF, 0x1F, 0xDD, 0x1D, 0x1C, 0xDC, 0x14, 0xD4,
        0xD5, 0x15, 0xD7, 0x17, 0x16, 0xD6, 0xD2, 0x12, 0x13, 0xD3,
        0x11, 0xD1, 0xD0, 0x10, 0xF0, 0x30, 0x31, 0xF1, 0x33, 0xF3,
        0xF2, 0x32, 0x36, 0xF6, 0xF7, 0x37, 0xF5, 0x35, 0x34, 0xF4,
        0x3C, 0xFC, 0xFD, 0x3D, 0xFF, 0x3F, 0x3E, 0xFE, 0xFA, 0x3A,
        0x3B, 0xFB, 0x39, 0xF9, 0xF8, 0x38, 0x28, 0xE8, 0xE9, 0x29,
        0xEB, 0x2B, 0x2A, 0xEA, 0xEE, 0x2E, 0x2F, 0xEF, 0x2D, 0xED,
        0xEC, 0x2C, 0xE4, 0x24, 0x25, 0xE5, 0x27, 0xE7, 0xE6, 0x26,
        0x22, 0xE2, 0xE3, 0x23, 0xE1, 0x21, 0x20, 0xE0, 0xA0, 0x60,
        0x61, 0xA1, 0x63, 0xA3, 0xA2, 0x62, 0x66, 0xA6, 0xA7, 0x67,
        0xA5, 0x65, 0x64, 0xA4, 0x6C, 0xAC, 0xAD, 0x6D, 0xAF, 0x6F,
        0x6E, 0xAE, 0xAA, 0x6A, 0x6B, 0xAB, 0x69, 0xA9, 0xA8, 0x68,
        0x78, 0xB8, 0xB9, 0x79, 0xBB, 0x7B, 0x7A, 0xBA, 0xBE, 0x7E,
        0x7F, 0xBF, 0x7D, 0xBD, 0xBC, 0x7C, 0xB4, 0x74, 0x75, 0xB5,
        0x77, 0xB7, 0xB6, 0x76, 0x72, 0xB2, 0xB3, 0x73, 0xB1, 0x71,
        0x70, 0xB0, 0x50, 0x90, 0x91, 0x51, 0x93, 0x53, 0x52, 0x92,
        0x96, 0x56, 0x57, 0x97, 0x55, 0x95, 0x94, 0x54, 0x9C, 0x5C,
        0x5D, 0x9D, 0x5F, 0x9F, 0x9E, 0x5E, 0x5A, 0x9A, 0x9B, 0x5B,
        0x99, 0x59, 0x58, 0x98, 0x88, 0x48, 0x49, 0x89, 0x4B, 0x8B,
        0x8A, 0x4A, 0x4E, 0x8E, 0x8F, 0x4F, 0x8D, 0x4D, 0x4C, 0x8C,
        0x44, 0x84, 0x85, 0x45, 0x87, 0x47, 0x46, 0x86, 0x82, 0x42,
        0x43, 0x83, 0x41, 0x81, 0x80, 0x40
        };

        public static bool IsStartFlag { get; set; }

        public event Action<string> OnLogOutputEvent;

        public SerialPortDataProcessor(IDataReader reader, CustomMQTTConfigModel mqttConfig, CancellationTokenSource cancellationTokenSource)
        {
            _logServices = LogServices.Instance;
            _reader = reader;
            _mqttConfig = mqttConfig;
            _parentCancellationToken = cancellationTokenSource;
            _cancellationTokenSource = CancellationTokenSource.CreateLinkedTokenSource(_parentCancellationToken.Token);
        }

        private void Reader_OnDataRead(IDataReader reader, DataFrame frame)
        {
            string asciiString = Encoding.ASCII.GetString(frame.Payload);
            string[] dataParts = asciiString.Split(',');
            object mqttData = null;
            string commandSerial = StringUtil.GenerateUniqueRandomString();
            long timestamp = CommonTimeStamp.GetUnixTimeStampSeconds(DateTime.Now);
            MQTTMessage originMessage = _baseInstruction.Get("MQTTMessage") as MQTTMessage;
            if (dataParts.Length > 5)
            {
                string uStatus = dataParts.FirstOrDefault();
                string eStatus = dataParts.ElementAtOrDefault(1);
                string percentage = dataParts.ElementAtOrDefault(2);
                string value1 = dataParts.ElementAtOrDefault(3);
                string value2 = dataParts.ElementAtOrDefault(4).TrimEnd().Replace(Environment.NewLine,"").Replace('\r','\0');
                mqttData = new
                {
                    commandSerial,
                    timestamp,
                    commandMessage=originMessage,
                    code = "0000",
                    msg = "success",
                    data = new
                    {
                        U=uStatus,
                        E=eStatus,
                        percentage,
                        value1,
                        value2
                    }
                };
            }
            else
            {
                mqttData = new { };
            }
            string response = JsonConvert.SerializeObject(mqttData, Formatting.Indented);
            if (_mqttConfig is CustomMQTTConfigModel customModel)
            {
                _mqttServices.PublishAsync(response, customModel.CommandResponseTopic);
            }
        }

        private void MqttServices_MessageReceivedAsync(string mqttMessageStr)
        {
            MQTTMessage mqttMessage = JsonConvert.DeserializeObject<MQTTMessage>(mqttMessageStr);
            if (_baseInstruction == null)
            {
                _baseInstruction = new Instruction();
            }
            _baseInstruction.Set("MQTTMessage", mqttMessage);
            switch (mqttMessage.commandCode)
            {
                case "SEND_ENTITY_IDS":
                    // 执行采集任务
                    _logServices.Info($"接收到开始采集指令:{mqttMessageStr}");
                    IsStartFlag = true;
                    OutputLog($"接收到开始采集指令:{mqttMessageStr}");
                    _cancellationTokenSource = CancellationTokenSource.CreateLinkedTokenSource(_parentCancellationToken.Token);
                    _reader.Set("_cancellationTokenSource", CancellationTokenSource.CreateLinkedTokenSource(_cancellationTokenSource.Token), BindingFlags.Instance | BindingFlags.NonPublic);
                    Execute(_baseInstruction);
                    break;
                case "COMMAND_START":
                    // 执行采集任务
                    _logServices.Info($"接收到开始采集指令:{mqttMessageStr}");
                    IsStartFlag = true;
                    OutputLog($"接收到开始采集指令:{mqttMessageStr}");
                    _cancellationTokenSource = CancellationTokenSource.CreateLinkedTokenSource(_parentCancellationToken.Token);
                    _reader.Set("_cancellationTokenSource", CancellationTokenSource.CreateLinkedTokenSource(_cancellationTokenSource.Token), BindingFlags.Instance | BindingFlags.NonPublic);
                    Execute(_baseInstruction);
                    break;
                case "SEND_STOP_ENTITY_IDS":
                    // 停止采集任务
                    IsStartFlag = false;
                    _logServices.Info($"接收到结束采集指令:{mqttMessageStr}");
                    OutputLog($"接收到结束采集指令:{mqttMessageStr}");
                    _cancellationTokenSource.Cancel();
                    break;
                case "COMMAND_COMPLETE":
                    // 停止采集任务
                    IsStartFlag = false;
                    _logServices.Info($"接收到结束采集指令:{mqttMessageStr}");
                    OutputLog($"接收到结束采集指令:{mqttMessageStr}");
                    _cancellationTokenSource.Cancel();
                    break;
            }
        }

        public void Dispose()
        {
            SendCollectData(); // 关闭时手动调用一次，等价 Java 代码的 stop_ex
            _cancellationTokenSource.Cancel();
            if(_reader is IDisposable disposable)
            {
                disposable.Dispose();
            }
            _mqttServices.DisconnectAsync().Wait();
            _mqttServices = null;
        }

        public void Execute(Instruction instruction)
        {
            // 循环发送 "C" 指令到串口，等价 Java 代码的 RxtxUtil
            _reader.ReadAsStringAsync("");

            // 开启任务循环发送收集到的数据，等价 Java 代码的 SendData
            Task.Run(async () =>
            {
                while (!_cancellationTokenSource.IsCancellationRequested)
                {
                    try
                    {
                        SendCollectData();
                    }
                    catch (Exception ex)
                    {
                        _logServices.Error($"发送采集数据发送错误:{ex.WrapExceptionInfo()}");
                        OutputLog($"发送采集数据发送错误:{ex.WrapExceptionInfo()}");
                    }

                    await Task.Delay(1);
                }
            });
        }

        public void SetBaseInstruction(Instruction instruction)=>_baseInstruction = instruction;

        public void OutputLog(string message)
        {
            if (!message.EndsWith(Environment.NewLine))
            {
                message = $"{message}{Environment.NewLine}";
            }
            OnLogOutputEvent?.Invoke(message);
        }

        private void SendCollectData()
        {
            Dictionary<string, object> collectVol = GetCollectVol();
            _logServices.Info("开始采集电压");
            byte[] partialDischarge = GetPartialDischarge("01", _baseInstruction.Get("CHANNEL")?.ToString() ?? "0000", "0006");
            Dictionary<string, object> dictionary = ProcessPartialDischarge(partialDischarge);
            _logServices.Info("开始采集波形图");
            byte[] partialDischarge2 = GetPartialDischarge("02", _baseInstruction.Get("CHANNEL")?.ToString() ?? "0000", "02D0");
            Dictionary<string, object> dictionary2 = ProcessPartialDischarge(partialDischarge2);
            List<decimal> list = new List<decimal>();
            if (dictionary.TryGetValue("DECIMALS", out var value) && value is List<decimal>)
            {
                list = value as List<decimal>;
            }
            List<decimal> list2 = new List<decimal>();
            if (dictionary2.TryGetValue("DECIMALS", out var value2) && value2 is List<decimal>)
            {
                list2 = value2 as List<decimal>;
            }
            DateTime dt = DateTime.Now;
            if (collectVol.TryGetValue("collectTime", out var value3) && value3 is DateTime)
            {
                dt = (DateTime)value3;
            }
            long timeStampMilliseconds = CommonTimeStamp.GetTimeStampMilliseconds(dt);
            string commandSerial = StringUtil.GenerateUniqueRandomString();
            MQTTMessage commandMessage = _baseInstruction.Get("MQTTMessage") as MQTTMessage;
            Dictionary<string, string> dictionary3 = new Dictionary<string, string>();
            if (collectVol.TryGetValue("collectVol", out var value4) && !string.IsNullOrWhiteSpace(value4?.ToString()))
            {
                dictionary3["VOLTAGE"] = value4.ToString();
            }
            if (list.Count > 0)
            {
                dictionary3["DISCHARGE"] = Math.Round(GetFirstElementByHexIndex(list, _baseInstruction.Get("CHANNEL")?.ToString() ?? "0000"), 2, MidpointRounding.ToEven).ToString();
            }
            if (list2 != null && list2.Count != 0)
            {
                dictionary3["PD_WAVE"] = JsonConvert.SerializeObject(list2);
            }
            _ = new
            {
                commandSerial = commandSerial,
                timestamp = timeStampMilliseconds,
                commandMessage = commandMessage,
                code = "000000",
                msg = "success",
                data = dictionary3
            };
            IotDataMessageModel iotDataMessageModel = new IotDataMessageModel();
            iotDataMessageModel.timestamp = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds();// CommonTimeStamp.GetTimeStampMicrosecond(DateTime.Now);
            iotDataMessageModel.timestampType = TimeStampType.MICROSECOND.ToString();
            iotDataMessageModel.deviceCode = _baseInstruction.Get("DEVICE_CODE").ToString();
            iotDataMessageModel.tags = null;
            iotDataMessageModel.data = dictionary3;
            string text = JsonConvert.SerializeObject(iotDataMessageModel, Formatting.Indented);
            _mqttServices.PublishIOTDataAsync(text);
            _logServices.Debug("发送数据:" + Environment.NewLine + text);
            Dictionary<string, string> dictionary4 = new Dictionary<string, string>(dictionary3);
            if (dictionary4.ContainsKey("PD_WAVE"))
            {
                dictionary4["PD_WAVE"] = "[" + string.Join(",", list2.Take(2)) + " ... " + string.Join(",", list2.Skip(list2.Count - 2).Take(2)) + "]";
            }
            iotDataMessageModel.data = dictionary4;
            text = JsonConvert.SerializeObject(iotDataMessageModel, Formatting.Indented);
            OutputLog("发送数据:" + Environment.NewLine + text);
        }


        private byte[] GetPartialDischarges(string funcCode, string channel, string dataNum)
        {
            string deviceIp = StringUtil.IntToHex(_baseInstruction.Get("DEVICE_IP")?.ToString()?.Split('.').Select(p => int.TryParse(p, out int val) ? val : 0).ToList()); // 设备 IP
            string typeCode = _baseInstruction.Get("TYPE_CODE")?.ToString(); // 类型码
            string dataLen = _baseInstruction.Get("DATA_LEN")?.ToString(); // 数据长度
            //string dataNum =  _baseInstruction.Get("DATA_NUM")?.ToString(); //寄存器数量 / 波形数据数量
            string payload = $"{deviceIp} {typeCode} {funcCode} {dataLen} {channel} {dataNum}".Replace(" ", "");
            _logServices.Debug($"计算{payload}的CRC16");
            string crcCode = CRCUtil.ComputeChecksumHex(CRCHigh, CRCLow, StringUtil.HexStringToByteArray(payload)).ToString();
            _logServices.Debug($"计算{payload}的CRC16结果:{crcCode}");
            string sendMessage = $"eaeaeaea01000001c0a8100a010200000006000102d08596aeaeaeae";//$"EAEAEAEA01000001{payload}{crcCode}AEAEAEAE"; // 发送的消息
            string destination = _baseInstruction.Source;
            _logServices.Info(sendMessage);
            var parts = destination.Split(InstructionConstants.INSTRUCTION_SPLIT_CHARS, 2);
            if (parts.Length != 2)
            {
                _logServices.Warning($"[Socket] 指令集格式错误，当前指令：[{destination}]，应为 \"ip:port\" 或 \"ip:port\"");
                OutputLog($"[Socket] 指令集格式错误，当前指令：[{destination}]，应为 \"ip:port\" 或 \"ip:port\"");
                throw new ArgumentException($"指令集格式错误，当前指令：[{destination}]，应为 \"ip:port\" 或 \"ip:port\"");
            }
            string ip = parts.ElementAtOrDefault(0);
            if (!int.TryParse(parts.ElementAtOrDefault(1), out int port) || (port <= 0 || port > 65535))
            {
                _logServices.Warning($"[Socket] 配置的端口号[{parts.ElementAtOrDefault(1)}]无效");
                throw new ArgumentException($"配置的端口号[{parts.ElementAtOrDefault(1)}]无效");
            }
            return _socketDataReader.SendAndReadImmediately(StringUtil.HexStringToByteArray(sendMessage), ip, port);
        }

        private Dictionary<string, object> GetCollectVol()
        {
            Dictionary<string, object> dictionary = new Dictionary<string, object>();
            string[] array = new string[16];
            bool flag = false;
            int num = 0;
            Stopwatch stopwatch = Stopwatch.StartNew();
            StringBuilder stringBuilder = new StringBuilder(16);
            while (true)
            {
                if ((_reader as SerialPortDataReader).Queue == null || !(_reader as SerialPortDataReader).Queue.TryTake(out var item) || item == null || item.Payload == null)
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
            WriteToFile(dictionary);
            return dictionary;
        }

        private void WriteToFile(Dictionary<string, object> result)
        {
            if(result == null || result.Count <= 0)
            {
                return;
            }
            try
            {
                MQTTMessage message = _baseInstruction.Get("MQTTMessage") as MQTTMessage;
                string fileName = $"source_{DateTime.Today:yyyy_MM_dd}.txt";
                StringBuilder sb = new StringBuilder();
                sb.Append(result["collectTime"])
                    .Append(" ")
                    .Append(result["collectVol"]?.ToString()??"0".PadRight(9, ' '))
                    .Append(result["collectVol"]?.ToString()??"0".PadRight(9, ' '))
                    .Append(result["hexString"])
                    .AppendLine();
                File.AppendAllText(fileName, sb.ToString(), Encoding.UTF8);
            }catch(Exception e)
            {
                _logServices.Error($"输出数据到文件发生错误:{e.WrapExceptionInfo()}");
            }
        }

        public void Init(Instruction instruction)
        {
            if (_mqttServices == null)
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
                    })
                    .ContinueWith(_ => OutputLog($"MQTT服务初始化完成"));
                _mqttServices.HeartbeatCallBack += (b, s) => OutputLog($"心跳检测{(b ? "成功" : "失败")}：{s}{Environment.NewLine}");
                _mqttServices.MessageReceivedAsync += MqttServices_MessageReceivedAsync;
            }
            if (_reader is INotifyLogOutput readerLog)
            {
                readerLog.OnLogOutputEvent += OutputLog;
            }
            if(_socketDataReader != null)
            {
                _socketDataReader.Dispose();
            }
            _socketDataReader = new TCPDataReader();
            if(_socketDataReader is INotifyLogOutput socketLogger)
            {
                socketLogger.OnLogOutputEvent += OutputLog;
            }
            _socketDataReader.Invoke("InitServerSocket",BindingFlags.Instance | BindingFlags.NonPublic);
        }

        public static decimal GetFirstElementByHexIndex(IEnumerable<decimal> source, string hex)
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

        private Dictionary<string, object> ProcessPartialDischarge(byte[] baodingDevicePayload)
        {
            Dictionary<string, object> dictionary = new Dictionary<string, object>();
            _logServices.Info($"处理网口数据:{baodingDevicePayload.Length}");
            string text = StringUtil.ByteArrayToHexString(baodingDevicePayload);
            _logServices.Info("处理网口数据，明文报文:" + text);
            foreach (string item in (from p in text.Split(new string[1] { "AE AE AE AE" }, StringSplitOptions.None)
                                     where p.Trim().StartsWith("EA EA EA EA")
                                     select (p + "AE AE AE AE").Replace(" ", "")).ToList())
            {
                _logServices.Info("解析单条网口数据:" + item);
                dictionary["VERSION"] = item.Substring(8, 8);
                dictionary["DEVICE_IP"] = item.Substring(16, 8);
                dictionary["TYPE_CODE"] = item.Substring(24, 2);
                dictionary["FUNC_CODE"] = item.Substring(26, 2);
                dictionary["DATA_LEN"] = item.Substring(28, 8);
                dictionary["CHANNEL"] = item.Substring(36, 4);
                dictionary["DATA_NUM"] = item.Substring(40, 4);
                dictionary["HEAD_NUM"] = dictionary.Values.Sum((object p) => (p?.ToString()?.Length).GetValueOrDefault()) + 8;
                dictionary["CRC"] = item.Substring(item.Length - 12, 4);
                dictionary["TAIL_LEN"] = (dictionary["CRC"]?.ToString()?.Length).GetValueOrDefault() + 8;
                dictionary["REAL_DATA_LEN"] = Convert.ToInt32(dictionary["HEAD_NUM"]) + Convert.ToInt32(dictionary["TAIL_LEN"]);
                string text2 = item.Substring(Convert.ToInt32(dictionary["HEAD_NUM"]), item.Length - Convert.ToInt32(dictionary["HEAD_NUM"]) - Convert.ToInt32(dictionary["TAIL_LEN"]));
                _logServices.Info("解析到网口数据:" + text2);
                List<decimal> value = StringUtil.HexToLongToDecimals(text2);
                _logServices.Info("解析出详细数据:" + JsonConvert.SerializeObject(value));
                dictionary["DECIMALS"] = value;
            }
            dictionary["PAYLOAD"] = text;
            return dictionary;
        }

        private byte[] GetPartialDischarge(string funcCode, string channel, string dataNum)
        {
            int result2;
            string text = StringUtil.IntToHex((from p in _baseInstruction.Get("DEVICE_IP")?.ToString()?.Split('.')
                                               select int.TryParse(p, out result2) ? result2 : 0).ToList());
            string text2 = _baseInstruction.Get("TYPE_CODE")?.ToString();
            string text3 = _baseInstruction.Get("DATA_LEN")?.ToString();
            string text4 = (text + " " + text2 + " " + funcCode + " " + text3 + " " + channel + " " + dataNum).Replace(" ", "");
            _logServices.Debug("计算" + text4 + "的CRC16");
            string text5 = CRCUtil.ComputeChecksumHex(CRCHigh, CRCLow, StringUtil.HexStringToByteArray(text4)).ToString();
            _logServices.Debug("计算" + text4 + "的CRC16结果:" + text5);
            string hexString = "EAEAEAEA01000001" + text4 + text5 + "AEAEAEAE";
            _logServices.Debug($"发送采集命令:{hexString}");
            string source = _baseInstruction.Source;
            string[] array = source.Split(InstructionConstants.INSTRUCTION_SPLIT_CHARS, 2);
            if (array.Length != 2)
            {
                _logServices.Warning("[Socket] 指令集格式错误，当前指令：[" + source + "]，应为 \"ip:port\" 或 \"ip:port\"");
                OutputLog("[Socket] 指令集格式错误，当前指令：[" + source + "]，应为 \"ip:port\" 或 \"ip:port\"");
                throw new ArgumentException("指令集格式错误，当前指令：[" + source + "]，应为 \"ip:port\" 或 \"ip:port\"");
            }
            string ip = array.ElementAtOrDefault(0);
            if (!int.TryParse(array.ElementAtOrDefault(1), out var result) || result <= 0 || result > 65535)
            {
                _logServices.Warning("[Socket] 配置的端口号[" + array.ElementAtOrDefault(1) + "]无效");
                throw new ArgumentException("配置的端口号[" + array.ElementAtOrDefault(1) + "]无效");
            }
            return _socketDataReader.SendAndReadImmediately(StringUtil.HexStringToByteArray(hexString), ip, result);
        }
    }
}
