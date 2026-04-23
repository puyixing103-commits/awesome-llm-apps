using System;
using System.Buffers;
using System.Collections;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Net.Sockets;
using System.Runtime.InteropServices;
using System.Runtime.Remoting.Channels;
using System.Text;
using System.Threading;
using System.Threading.Channels;
using System.Threading.Tasks;
using System.Xml;

using DataAcquisitionLibrary;
using DataAcquisitionLibrary.Utils;

using DC_0003.Models;

using Newtonsoft.Json;
using Newtonsoft.Json.Linq;

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

    public class SocketClient
    {
        private const int MaxReceiveQueueSize = 50000;
        private Socket _socket;

        public bool IsConnected { get; private set; } = false;



        private readonly Channel<ReceivedData> _recvQueue = Channel.CreateBounded<ReceivedData>(
        new BoundedChannelOptions(20000)
        {
            FullMode = BoundedChannelFullMode.DropOldest,
            SingleWriter = false,
            SingleReader = false
        });
        private int _recvQueueCount;
        private int _droppedRecvFrames;

        private int _queueCount = 0;
        private CancellationTokenSource _cts;

        private FrameParser _parser = new();

        public event Func<DataFrame, Task> OnFrameParsed;

        private readonly ILogServices _logServices = LogServices.Instance;
        private readonly SemaphoreSlim _concurrencyLimit = new(5); // 最多同时跑5个
        private readonly object _sendLock = new object();


        public SocketClient(CancellationTokenSource cts)
        {
            _cts = cts;
            _ = taskt();
        }

        private async Task taskt() 
        {
            while (true)
            {
                DataAcquisitionManager.EnqueueLog($"采集队列数量：{_recvQueueCount}");
                //_recvQueueCount

                    await Task.Delay(1000);
            }
            
        
        }
        public async Task<bool> Start(string ip, int port)
        {
            try
            {
                _socket = new Socket(AddressFamily.InterNetwork, SocketType.Stream, ProtocolType.Tcp);
                await _socket.ConnectAsync(ip, port);
                IsConnected = true;
                DataAcquisitionManager.EnqueueLog($"数据源设备:{ip}:{port}连接成功");
                _logServices.Info($"数据源设备:{ip}:{port}连接成功");
                _ = ProcessLoop();
                _ = ReceiveLoop();
                return true;
            }
            catch (Exception exp)
            {
                IsConnected = false;
               
                DataAcquisitionManager.EnqueueLog($"数据源设备:{ip}:{port}失败,Error:{exp.Message}");
                _logServices.Error($"数据源设备:{ip}:{port}连接失败,Error:{exp.Message}");
                return false;
            }

        }

        private async Task ReceiveLoop()
        {
            byte[] buffer = new byte[4096];
            StringBuilder S = new StringBuilder();
            int recvCount = 0;
            long lastSecond = DateTimeOffset.UtcNow.ToUnixTimeSeconds();
            while (!_cts.Token.IsCancellationRequested)
            {
                try
                {
                    if (!IsConnected)
                    {
                        await Task.Delay(100, _cts.Token);
                        continue;
                    }
                    int len = await _socket.ReceiveAsync(
                            new ArraySegment<byte>(buffer),
                            SocketFlags.None
                        );
                    if (len <= 0) continue;

                    byte[] data = ArrayPool<byte>.Shared.Rent(len);
                    Buffer.BlockCopy(buffer, 0, data, 0, len);
                   // var timestmp = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds();
                    //时间戳
                    var dateNow = DateTime.UtcNow;
                    // 获取纳秒级时间戳（16位）: 毫秒级时间戳(13位) + 纳秒部分(3位)
                    var timestmp = (new DateTimeOffset(dateNow).ToUnixTimeMilliseconds() * 1000) + (dateNow.Ticks % 10000) / 10; // 数据保留到纳秒级保留16位

                    _recvQueue.Writer.TryWrite(new ReceivedData(data, len, timestmp));
                    Interlocked.Increment(ref _recvQueueCount);

                    recvCount++;
                    long currentSecond = DateTimeOffset.UtcNow.ToUnixTimeSeconds();

                    if (currentSecond > lastSecond)
                    {
                        recvCount = 0;
                        lastSecond = currentSecond;
                    }
                }
                catch (Exception ex)
                {
                    _logServices.Error(ex.ToString());
                }

                await Task.Delay(10, _cts.Token);
            }
        }

        private async Task ProcessLoop()
        {
            try
            {
                _recvQueueCount = 0;
                await foreach (var item in _recvQueue.Reader.ReadAllAsync(_cts.Token))
                {
                    if (!IsConnected)
                    {
                        await Task.Delay(100, _cts.Token);
                        continue;
                    }
                    try
                    {

                        var receivedData = item;

                        Interlocked.Decrement(ref _recvQueueCount);
                        long processTimestamp = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds();
                        long delay = processTimestamp - receivedData.ReceiveTimestamp;

                        try
                        {
                            var frames = _parser.Parse(receivedData.Data, receivedData.Length);

                            foreach (var frameData in frames)
                            {
                                DataFrame df = new DataFrame
                                {
                                    Payload = frameData,
                                    DataLength = frameData.Length
                                };
                                df.Add("RECEIVE_TIMESTAMP", receivedData.ReceiveTimestamp);
                                df.Add("PROCESS_TIMESTAMP", processTimestamp);
                                df.Add("QUEUE_DELAY_MS", delay);
                                df.Add("TIMESTAMP", receivedData.ReceiveTimestamp);
                                ParseFrame(frameData, df);
                                await InvokeCallbackAsync(df);
                            }
                        }

                        catch (Exception ex)
                        {
                            _logServices.Error(ex.ToString());
                        }
                        finally
                        {
                            if (receivedData.Data != null)
                            {
                                ArrayPool<byte>.Shared.Return(receivedData.Data);
                            }
                        }
                    }
                    catch (Exception ex)
                    {
                        _logServices.Error(ex.ToString());
                    }
                 // await Task.Delay(1, _cts.Token);
                }
            }
            catch (OperationCanceledException)
            {
                // 正常停止
            }

        }



        async Task InvokeCallbackAsync(DataFrame frame)
        {
            //  await _concurrencyLimit.WaitAsync();
            try { await OnFrameParsed(frame); }
            finally {/* _concurrencyLimit.Release();*/ }
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
            lock (_sendLock)
            {
                _socket.Send(data);
            }
            return Task.CompletedTask;
        }


        public void Stop()
        {
            IsConnected = false;
            _socket?.Close();
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