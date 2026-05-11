using System;
using System.Buffers;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Net;
using System.Net.Sockets;
using System.Runtime.InteropServices.ComTypes;
using System.Text;
using System.Text.Json;
using System.Threading;
using System.Threading.Tasks;

using ApiModels;

using DataAcquisitionLibrary;
using DataAcquisitionLibrary.Utils;

using DC_0003.Models;
using DC_0003.Services.Implements.Data_Collection.Dto;

using Newtonsoft.Json;

namespace DC_0003.Services.Implements
{
    /// <summary>
    /// TCP数据采集客户端 - 处理2908字节二进制报文
    /// 报文格式: EA EA EA EA [头部16字节] [数据点数量2字节] [浮点数据2880字节] [CRC2字节] AE AE AE AE
    /// </summary>
    public class TcpCollectorClient : IDisposable
    {
        private Socket _socket;
        private readonly string _host;
        private readonly int _port;
        private CancellationTokenSource _cts;
        private Task _receiveTask;
        private Task _heartbeatTask;
        private readonly List<byte> _frameBuffer = new List<byte>(4096);
        private readonly object _bufferLock = new object();

        // 统计
        private long _totalFrames = 0;
        private long _totalBytes = 0;
        private long _errorCount = 0;

        public bool IsConnected => _socket?.Connected ?? false;
        //public event Action<FrameData> OnFrameReceived;
        public event Action<string> OnLog;
        public event Action OnDisconnected;

        public TcpCollectorClient(string host, int port)
        {
            _host = host;
            _port = port;
            _=ConnectAsync();
        }

        /// <summary>
        /// 建立连接
        /// </summary>
        public async Task<bool> ConnectAsync()
        {
            try
            {
                DisposeSocket();
                _cts = new CancellationTokenSource();

                _socket = new Socket(AddressFamily.InterNetwork, SocketType.Stream, ProtocolType.Tcp);
                _socket.NoDelay = true;
                _socket.ReceiveBufferSize = 64 * 1024;
                _socket.SendBufferSize = 64 * 1024;

                // 设置TCP保活
                _socket.SetSocketOption(SocketOptionLevel.Socket, SocketOptionName.KeepAlive, true);

                await _socket.ConnectAsync(new IPEndPoint(IPAddress.Parse(_host), _port));

                Log($"✅ 连接成功: {_host}:{_port}");

                // 启动接收循环
                _receiveTask = Task.Run(() => ReceiveLoopAsync(_cts.Token));

                // 启动心跳
                _heartbeatTask = Task.Run(() => HeartbeatLoopAsync(_cts.Token));

                return true;
            }
            catch (Exception ex)
            {
                Log($"❌ 连接失败: {ex.Message}");
                _errorCount++;
                return false;
            }
        }

        /// <summary>
        /// 发送请求帧（触发服务端返回2908字节数据）
        /// </summary>
        public async Task<bool> SendRequestAsync(byte[] bytes)
        {
            if (!IsConnected) return false;

            try
            {
                // 构造请求帧 - 根据协议定义
                

                await _socket.SendAsync(new ArraySegment<byte>(bytes), SocketFlags.None);
                //Log($"📤 发送请求帧: {bytes.Length} 字节");
                return true;
            }
            catch (Exception ex)
            {
                Log($"❌ 发送失败: {ex.Message}");
                return false;
            }
        }

        /// <summary>
        /// 接收循环 - 核心：处理粘包/拆包，提取完整帧
        /// </summary>
        private async Task ReceiveLoopAsync(CancellationToken ct)
        {
            byte[] buffer = ArrayPool<byte>.Shared.Rent(8192);  // 8KB缓冲区

            try
            {
                while (!ct.IsCancellationRequested)
                {
                    try
                    {
                        if (!IsConnected)
                        {
                            await Task.Delay(100, ct);
                            continue;
                        }
                        // 接收数据
                        int len = await _socket.ReceiveAsync(
                            new ArraySegment<byte>(buffer),
                            SocketFlags.None
                        );

                        if (len == 0)
                        {
                            Log("⚠️ 对端关闭连接");
                            OnDisconnected?.Invoke();
                            await ReconnectAsync();
                            continue;
                        }

                        _totalBytes += len;

                        // 拼接到帧缓冲区
                        lock (_bufferLock)
                        {
                            for (int i = 0; i < len; i++)
                                _frameBuffer.Add(buffer[i]);
                        }

                        // 解析完整帧
                        ParseFrames();
                    }
                    catch (SocketException ex) when (ex.SocketErrorCode == SocketError.ConnectionReset)
                    {
                        Log($"⚠️ 连接被重置: {ex.Message}");
                        OnDisconnected?.Invoke();
                        await ReconnectAsync();
                    }
                    catch (SocketException ex)
                    {
                        Log($"❌ Socket异常: {ex.SocketErrorCode} - {ex.Message}");
                        _errorCount++;
                        OnDisconnected?.Invoke();
                        await Task.Delay(1000, ct);
                    }
                    catch (OperationCanceledException)
                    {
                        break;
                    }
                    catch (Exception ex)
                    {
                        Log($"❌ 接收异常: {ex}");
                        _errorCount++;
                        await Task.Delay(1000, ct);
                    }
                }
            }
            finally
            {
                ArrayPool<byte>.Shared.Return(buffer);
            }
        }

        /// <summary>
        /// 解析帧缓冲区，提取完整帧
        /// </summary>
        private void ParseFrames()
        {
            lock (_bufferLock)
            {
                while (true)
                {
                    // 至少要有帧头+帧尾 = 8字节
                    if (_frameBuffer.Count < 8) break;

                    // 找帧头 EA EA EA EA
                    int headIdx = FindPattern(_frameBuffer, new byte[] { 0xEA, 0xEA, 0xEA, 0xEA });

                    if (headIdx == -1)
                    {
                        // 没有帧头，保留最后3字节防止截断，其余丢弃
                        int keep = Math.Min(3, _frameBuffer.Count);
                        _frameBuffer.RemoveRange(0, _frameBuffer.Count - keep);
                        break;
                    }

                    // 丢弃帧头前的垃圾数据
                    if (headIdx > 0)
                        _frameBuffer.RemoveRange(0, headIdx);

                    // 找帧尾 AE AE AE AE（从帧头后第4字节开始）
                    int tailIdx = FindPattern(_frameBuffer, 4, new byte[] { 0xAE, 0xAE, 0xAE, 0xAE });

                    if (tailIdx == -1)
                    {
                        // 帧还没收完，等下次
                        break;
                    }

                    // 提取完整帧
                    int frameLen = tailIdx + 4;
                    byte[] frame = new byte[frameLen];
                    _frameBuffer.CopyTo(0, frame, 0, frameLen);
                    _frameBuffer.RemoveRange(0, frameLen);

                    // 处理帧
                    ProcessFrame(frame);
                }
            }
        }


        public static string PD_WAVE = "";
        /// <summary>
        /// 处理完整帧 - 解析为结构化数据
        /// </summary>
        private void ProcessFrame(byte[] frame)
        {
           var d = ProcessPartialDischarge(frame);
         //  var dd = (d["DECIMALS"].ToString());
            PD_WAVE = JsonConvert.SerializeObject(d["DECIMALS"]);
            //try
            //{
            //    if (frame.Length < 32)
            //    {
            //        Log($"⚠️ 帧太短: {frame.Length} 字节，丢弃");
            //        return;
            //    }

            //    // 解析头部
            //    var header = new FrameHeader
            //    {
            //        SyncHeader = new byte[] { frame[0], frame[1], frame[2], frame[3] },
            //        Version = (ushort)((frame[4] << 8) | frame[5]),
            //        DeviceId = (ushort)((frame[6] << 8) | frame[7]),
            //        IpAddress = $"{frame[8]}.{frame[9]}.{frame[10]}.{frame[11]}",
            //        Port = (ushort)((frame[12] << 8) | frame[13]),
            //        DataLength = (ushort)((frame[14] << 8) | frame[15]),
            //        Reserved = (ushort)((frame[16] << 8) | frame[17]),
            //        FunctionCode = (ushort)((frame[18] << 8) | frame[19]),
            //        DataCount = (ushort)((frame[20] << 8) | frame[21])
            //    };

            //    // 解析浮点数据（大端序 IEEE 754）
            //    var floatValues = new List<float>(header.DataCount);
            //    for (int i = 0; i < header.DataCount; i++)
            //    {
            //        int offset = 22 + i * 4;
            //        if (offset + 4 > frame.Length - 6) break;  // 留出CRC和帧尾

            //        // 大端序转float
            //        byte[] floatBytes = new byte[4];
            //        Array.Copy(frame, offset, floatBytes, 0, 4);
            //        // 如果是小端系统，需要反转
            //        if (BitConverter.IsLittleEndian)
            //            Array.Reverse(floatBytes);

            //        float val = BitConverter.ToSingle(floatBytes, 0);
            //        floatValues.Add(val);
            //    }

            //    // CRC校验（示例，实际需要实现CRC算法）
            //    ushort crc = (ushort)((frame[frame.Length - 6] << 8) | frame[frame.Length - 5]);

            //    var frameData = new FrameData
            //    {
            //        Header = header,
            //        FloatValues = floatValues,
            //        Crc = crc,
            //        RawFrame = frame,
            //        ReceiveTime = DateTime.UtcNow,
            //        TotalBytes = frame.Length
            //    };

            //    _totalFrames++;

            //    // 触发事件
            //    OnFrameReceived?.Invoke(frameData);

            //    Log($"✅ 解析帧 #{_totalFrames}: 设备{header.DeviceId:X4}, IP:{header.IpAddress}, " +
            //        $"数据点:{header.DataCount}, 浮点范围:[{floatValues.Min():F3}, {floatValues.Max():F3}], " +
            //        $"帧大小:{frame.Length} 字节");
            //}
            //catch (Exception ex)
            //{
            //    Log($"❌ 帧解析失败: {ex.Message}");
            //    _errorCount++;
            //}
        }


        private Dictionary<string, object> ProcessPartialDischarge(byte[] baodingDevicePayload)
        {
            Dictionary<string, object> dictionary = new Dictionary<string, object>();
           // _logServices.Info($"处理网口数据:{baodingDevicePayload.Length}");
            string text = StringUtil.ByteArrayToHexString(baodingDevicePayload);
           // _logServices.Info("处理网口数据，明文报文:" + text);
            foreach (string item in (from p in text.Split(new string[1] { "AE AE AE AE" }, StringSplitOptions.None)
                                     where p.Trim().StartsWith("EA EA EA EA")
                                     select (p + "AE AE AE AE").Replace(" ", "")).ToList())
            {
               // _logServices.Info("解析单条网口数据:" + item);
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
               // _logServices.Info("解析到网口数据:" + text2);
                List<decimal> value = StringUtil.HexToLongToDecimals(text2);
               // _logServices.Info("解析出详细数据:" + JsonConvert.SerializeObject(value));
                dictionary["DECIMALS"] = value;
            }
            dictionary["PAYLOAD"] = text;
            return dictionary;
        }

        /// <summary>
        /// 心跳循环 - 保持连接
        /// </summary>
        private async Task HeartbeatLoopAsync(CancellationToken ct)
        {
            var heartbeatFrame = new byte[]
            {
                0xEA, 0xEA, 0xEA, 0xEA,
                0x01, 0x00,
                0x00, 0x01,
                0xC0, 0xA8, 0x10, 0x0C,
                0x02, 0x02,
                0x00, 0x00,
                0x0B, 0x46,
                0x00, 0x02,  // 功能码: 心跳
                0x00, 0x00,
                0xCE, 0x63,
                0xAE, 0xAE, 0xAE, 0xAE
            };

            while (!ct.IsCancellationRequested)
            {
                try
                {
                    if (IsConnected)
                    {
                        await _socket.SendAsync(
                            new ArraySegment<byte>(heartbeatFrame),
                            SocketFlags.None
                        );
                        // Log("💓 心跳发送");
                    }
                }
                catch (Exception ex)
                {
                    Log($"❌ 心跳失败: {ex.Message}");
                }

                await Task.Delay(TimeSpan.FromSeconds(30), ct);
            }
        }

        /// <summary>
        /// 重连
        /// </summary>
        private async Task ReconnectAsync()
        {
            Log("🔄 准备重连...");
            DisposeSocket();
            await Task.Delay(3000);
            await ConnectAsync();
        }

        /// <summary>
        /// 断开连接
        /// </summary>
        public void Disconnect()
        {
            _cts?.Cancel();
            DisposeSocket();
            Log("👋 已断开");
        }

        private void DisposeSocket()
        {
            try
            {
                _socket?.Shutdown(SocketShutdown.Both);
            }
            catch { }

            try
            {
                _socket?.Close();
            }
            catch { }

            try
            {
                _socket?.Dispose();
            }
            catch { }

            _socket = null;
        }

        private void Log(string msg)
        {
            string log = $"[{DateTime.Now:yyyy-MM-dd HH:mm:ss.fff}] {msg}";
            Console.WriteLine(log);
            OnLog?.Invoke(log);
        }

        public void Dispose()
        {
            Disconnect();
            _cts?.Dispose();
        }

        // 辅助方法：在列表中查找字节模式
        private int FindPattern(List<byte> data, byte[] pattern)
        {
            return FindPattern(data, 0, pattern);
        }

        private int FindPattern(List<byte> data, int startIndex, byte[] pattern)
        {
            for (int i = startIndex; i <= data.Count - pattern.Length; i++)
            {
                bool match = true;
                for (int j = 0; j < pattern.Length; j++)
                {
                    if (data[i + j] != pattern[j])
                    {
                        match = false;
                        break;
                    }
                }
                if (match) return i;
            }
            return -1;
        }
    }

    /// <summary>
    /// 帧头部结构
    /// </summary>
    public class FrameHeader
    {
        public byte[] SyncHeader { get; set; }
        public ushort Version { get; set; }
        public ushort DeviceId { get; set; }
        public string IpAddress { get; set; }
        public ushort Port { get; set; }
        public ushort DataLength { get; set; }
        public ushort Reserved { get; set; }
        public ushort FunctionCode { get; set; }
        public ushort DataCount { get; set; }
    }

    /// <summary>
    /// 完整帧数据
    /// </summary>
    public class FrameData
    {
        public FrameHeader Header { get; set; }
        public List<float> FloatValues { get; set; }
        public ushort Crc { get; set; }
        public byte[] RawFrame { get; set; }
        public DateTime ReceiveTime { get; set; }
        public int TotalBytes { get; set; }

        public override string ToString()
        {
            var sb = new StringBuilder();
            sb.AppendLine($"=== 帧数据 ===");
            sb.AppendLine($"设备ID: {Header.DeviceId:X4}");
            sb.AppendLine($"IP地址: {Header.IpAddress}:{Header.Port}");
            sb.AppendLine($"功能码: {Header.FunctionCode:X4}");
            sb.AppendLine($"数据点: {Header.DataCount}");
            sb.AppendLine($"浮点值: {FloatValues.Count} 个");
            sb.AppendLine($"  最小值: {FloatValues.Min():F6}");
            sb.AppendLine($"  最大值: {FloatValues.Max():F6}");
            sb.AppendLine($"  平均值: {FloatValues.Average():F6}");
            sb.AppendLine($"CRC: {Crc:X4}");
            sb.AppendLine($"总字节: {TotalBytes}");
            sb.AppendLine($"接收时间: {ReceiveTime:yyyy-MM-dd HH:mm:ss.fff}");
            return sb.ToString();
        }
    }
}
