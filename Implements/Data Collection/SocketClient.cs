using System;
using System.Buffers;
using System.Collections.Generic;
using System.Linq;
using System.Net.Sockets;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

using DataAcquisitionLibrary;
using DataAcquisitionLibrary.Utils;

using DC_0003.Models;

namespace SocketA0Demo
{
    public readonly struct ReceivedData
    {
        public byte[] Data { get; }
        public int Length { get; }
        public long ReceiveTimestamp { get; }

        public ReceivedData(byte[] data, int length, long receiveTimestamp)
        {
            Data = data;
            Length = length;
            ReceiveTimestamp = receiveTimestamp;
        }
    }

    public sealed class SynchronousSocketRequestClient
    {
        private readonly FrameParser _parser = new();
        private readonly int _receiveTimeoutMs;

        public SynchronousSocketRequestClient(int receiveTimeoutMs = 5000)
        {
            _receiveTimeoutMs = receiveTimeoutMs;
        }

        public List<ReceivedData> SendAndReceive(Socket socket, byte[] request, object syncRoot)
        {
            if (socket == null) throw new ArgumentNullException(nameof(socket));
            if (request == null || request.Length == 0) return new List<ReceivedData>();

            lock (syncRoot)
            {
                socket.Send(request);
                return ReceiveFrames(socket);
            }
        }

        private List<ReceivedData> ReceiveFrames(Socket socket)
        {
            byte[] buffer = ArrayPool<byte>.Shared.Rent(4096);
            try
            {
                while (true)
                {
                    int len = socket.Receive(buffer, 0, buffer.Length, SocketFlags.None);
                    if (len == 0)
                    {
                        throw new SocketException((int)SocketError.ConnectionReset);
                    }

                    long receiveTimestamp = CreateTimestamp();
                    var frames = _parser.Parse(buffer, len);
                    if (frames.Count == 0)
                    {
                        continue;
                    }

                    List<ReceivedData> results = new List<ReceivedData>(frames.Count);
                    foreach (var frame in frames)
                    {
                        results.Add(new ReceivedData(frame, frame.Length, receiveTimestamp));
                    }

                    return results;
                }
            }
            catch (SocketException ex) when (ex.SocketErrorCode == SocketError.TimedOut)
            {
                throw new TimeoutException($"同步请求等待响应超时，超时时间 {_receiveTimeoutMs}ms", ex);
            }
            finally
            {
                ArrayPool<byte>.Shared.Return(buffer);
            }
        }

        private static long CreateTimestamp()
        {
            var dateNow = DateTime.UtcNow;
            return (new DateTimeOffset(dateNow).ToUnixTimeMilliseconds() * 1000) + (dateNow.Ticks % 10000) / 10;
        }
    }

    public class SocketClient
    {
        private const int DefaultReceiveTimeoutMs = 5000;
        private Socket _socket;

        public bool IsConnected { get; private set; } = false;

        private string _ip;
        private int _port;
        private Task _reconnectTask;
        private readonly object _reconnectLock = new object();
        private bool _isUserStopped = false;
        private CancellationTokenSource _cts;
        private readonly SynchronousSocketRequestClient _requestClient = new(DefaultReceiveTimeoutMs);

        public event Func<DataFrame, Task> OnFrameParsed;

        /// <summary>日志输出事件（替代对 DataAcquisitionManager 的静态依赖）</summary>
        public event Action<string> OnLog;

        private readonly ILogServices _logServices = LogServices.Instance;
        private readonly object _sendLock = new object();


        public SocketClient(CancellationTokenSource cts)
        {
            _cts = cts;
        }

        private void Log(string message) => OnLog?.Invoke(message);

        public async Task<bool> Start(string ip, int port)
        {
            try
            {
                _ip = ip;
                _port = port;
                _isUserStopped = false;
                _socket = CreateSocket();
                await _socket.ConnectAsync(ip, port);
                IsConnected = true;
                Log($"数据源设备:{ip}:{port}连接成功");

                return true;
            }
            catch (Exception exp)
            {
                IsConnected = false;
                Log($"数据源设备:{ip}:{port}失败,Error:{exp.Message}");
                return false;
            }

        }

        private static Socket CreateSocket()
        {
            Socket socket = new Socket(AddressFamily.InterNetwork, SocketType.Stream, ProtocolType.Tcp);
            socket.NoDelay = true;
            socket.ReceiveTimeout = DefaultReceiveTimeoutMs;
            socket.SendTimeout = DefaultReceiveTimeoutMs;
            return socket;
        }

        private async Task HandleReceivedDataAsync(ReceivedData receivedData)
        {
            try
            {
                long processTimestamp = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds();
                long delay = processTimestamp - receivedData.ReceiveTimestamp;

                DataFrame df = new DataFrame
                {
                    Payload = receivedData.Data,
                    DataLength = receivedData.Length
                };
                df.Add("RECEIVE_TIMESTAMP", receivedData.ReceiveTimestamp);
                df.Add("PROCESS_TIMESTAMP", processTimestamp);
                df.Add("QUEUE_DELAY_MS", delay);
                df.Add("TIMESTAMP", receivedData.ReceiveTimestamp);
                ParseFrame(receivedData.Data, df);
                await InvokeCallbackAsync(df);
            }
            catch (Exception ex)
            {
                _logServices.Error(ex.ToString());
            }
        }


        async Task InvokeCallbackAsync(DataFrame frame)
        {
            var handler = OnFrameParsed;
            if (handler == null)
            {
                return;
            }

            await handler(frame);
        }

        public DataFrame ParseFrame(byte[] frame, DataFrame existingFrame = null)
        {
            DataFrame df = existingFrame ?? new DataFrame();

            string text = StringUtil.ByteArrayToHexString(frame);

            Dictionary<string, object> dictionary = new Dictionary<string, object>();

            foreach (string item in (from p in text.Split(new string[1] { "AE AE AE AE" }, StringSplitOptions.None)
                                     where p.Trim().StartsWith("EA EA EA EA")
                                     select (p + "AE AE AE AE").Replace(" ", "")).ToList())
            {
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

                List<decimal> value = StringUtil.HexToLongToDecimals(text2);

                dictionary["DECIMALS"] = value;
            }

            df.Add("DECIMALS", dictionary["DECIMALS"]);

            return df;
        }


        public Task SendAsync(byte[] data)
        {
            if (!IsConnected)
            {
                throw new InvalidOperationException("Socket 未连接");
            }
            if (data == null || data.Length == 0)
            {
                return Task.CompletedTask;
            }
            try
            {
                List<ReceivedData> responses = _requestClient.SendAndReceive(_socket, data, _sendLock);
                foreach (var response in responses)
                {
                    // 使用Task.Run避免同步等待异步回调导致的死锁
                    Task.Run(() => HandleReceivedDataAsync(response)).Wait();
                }
            }
            catch (SocketException ex) when (
                ex.SocketErrorCode == SocketError.ConnectionReset ||
                ex.SocketErrorCode == SocketError.ConnectionAborted ||
                ex.SocketErrorCode == SocketError.Shutdown)
            {
                IsConnected = false;
                Log($"数据源设备连接断开: {ex.SocketErrorCode}");
                TryReconnect();
                throw;
            }
            catch (ObjectDisposedException)
            {
                IsConnected = false;
                TryReconnect();
                throw;
            }

            return Task.CompletedTask;
        }


        public void Stop()
        {
            _isUserStopped = true;
            IsConnected = false;
            _socket?.Close();
        }

        private void TryReconnect()
        {
            lock (_reconnectLock)
            {
                if (_isUserStopped || _cts.Token.IsCancellationRequested) return;
                if (_reconnectTask != null && !_reconnectTask.IsCompleted) return;

                _reconnectTask = Task.Run(async () =>
                {
                    int delayMs = 3000;
                    int maxDelayMs = 30000;
                    while (!_cts.Token.IsCancellationRequested && !_isUserStopped && !IsConnected)
                    {
                        try
                        {
                            Log($"数据源设备 {_ip}:{_port} 正在重连...");
                            _socket?.Close();
                            _socket?.Dispose();
                            _socket = CreateSocket();
                            await _socket.ConnectAsync(_ip, _port);
                            IsConnected = true;
                            Log($"数据源设备:{_ip}:{_port}重连成功");
                            return;
                        }
                        catch (Exception ex)
                        {
                            Log($"数据源设备:{_ip}:{_port}重连失败,Error:{ex.Message}");
                        }

                        try
                        {
                            await Task.Delay(delayMs, _cts.Token);
                        }
                        catch (OperationCanceledException)
                        {
                            break;
                        }

                        delayMs = Math.Min(delayMs * 2, maxDelayMs); // 指数退避，最大30秒
                    }
                });
            }
        }

    }



    public class FrameParser
    {
        private byte[] _buffer = new byte[8192];
        private int _bufferLength = 0;
        private readonly object _bufferLock = new object();

        private static readonly byte[] HEADER = { 0xEA, 0xEA, 0xEA, 0xEA };
        private static readonly byte[] TAIL = { 0xAE, 0xAE, 0xAE, 0xAE };

        public List<byte[]> Parse(byte[] data)
        {
            return Parse(data, data?.Length ?? 0);
        }

        public List<byte[]> Parse(byte[] data, int length)
        {
            List<byte[]> frames = new();

            lock (_bufferLock)
            {
                if (data != null && length > 0)
                {
                    int safeLength = Math.Min(length, data.Length);
                    if (_bufferLength + safeLength > _buffer.Length)
                    {
                        int newSize = Math.Max(_buffer.Length * 2, _bufferLength + safeLength);
                        byte[] newBuffer = new byte[newSize];
                        if (_bufferLength > 0)
                        {
                            Buffer.BlockCopy(_buffer, 0, newBuffer, 0, _bufferLength);
                        }
                        _buffer = newBuffer;
                    }
                    Buffer.BlockCopy(data, 0, _buffer, _bufferLength, safeLength);
                    _bufferLength += safeLength;
                }

                while (true)
                {
                    int start = IndexOf(_buffer, _bufferLength, HEADER, 0);
                    if (start < 0)
                    {
                        _bufferLength = 0;
                        break;
                    }

                    if (start > 0)
                    {
                        _bufferLength -= start;
                        if (_bufferLength > 0)
                        {
                            Buffer.BlockCopy(_buffer, start, _buffer, 0, _bufferLength);
                        }
                    }

                    int end = IndexOf(_buffer, _bufferLength, TAIL, HEADER.Length);
                    if (end < 0)
                        break;

                    int frameLen = end + TAIL.Length;
                    byte[] frame = new byte[frameLen];
                    Buffer.BlockCopy(_buffer, 0, frame, 0, frameLen);
                    frames.Add(frame);

                    _bufferLength -= frameLen;
                    if (_bufferLength > 0)
                    {
                        Buffer.BlockCopy(_buffer, frameLen, _buffer, 0, _bufferLength);
                    }
                }
            }

            return frames;
        }

        private int IndexOf(byte[] src, int srcLength, byte[] pattern, int startIndex = 0)
        {
            int maxIndex = srcLength - pattern.Length;
            for (int i = startIndex; i <= maxIndex; i++)
            {
                if (src[i] == pattern[0])
                {
                    bool match = true;
                    for (int j = 1; j < pattern.Length; j++)
                    {
                        if (src[i + j] != pattern[j])
                        {
                            match = false;
                            break;
                        }
                    }
                    if (match) return i;
                }
            }
            return -1;
        }
    }

}
