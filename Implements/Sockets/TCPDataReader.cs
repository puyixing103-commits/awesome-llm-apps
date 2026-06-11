using System;
using System.Linq;
using System.Net;
using System.Net.Sockets;
using System.Threading;
using System.Threading.Tasks;

using DataAcquisitionLibrary;
using DataAcquisitionLibrary.Utils;

using DC_0003.Core.Constants;
using DC_0003.Core.Interfaces;
using DC_0003.Models;

namespace DC_0003.Services.Implements.Sockets;

public class TCPDataReader : ISocketDataReader, IDataReader, IDisposable, INotifyLogOutput
{
    private readonly ILogServices _logServices;

    private Socket _serverSocket;

    private bool _serverRunning;

    private string _lastConnectIp = "";

    private int _lastConnectPort;

    public event Action<string> OnLogOutputEvent;

    public event Action<IDataReader, DataFrame> OnDataRead;

    public TCPDataReader()
    {
        _logServices = LogServices.Instance;
    }

    public TCPDataReader(Socket serverSocket)
        : this()
    {
        _serverSocket = serverSocket;
    }

    public void Dispose()
    {
        Stop();
        _serverSocket = null;
    }

    public virtual Task<string> ReadAsStringAsync(string source)
    {
        if (_serverRunning)
        {
            return Task.FromResult(string.Empty);
        }
        Start(source);
        return Task.FromResult(string.Empty);
    }

    public void Start(string source)
    {
        if (_serverRunning)
        {
            return;
        }
        if (_serverSocket != null)
        {
            Socket serverSocket = _serverSocket;
            if (serverSocket == null || serverSocket.Connected)
            {
                goto IL_00ec;
            }
        }
        string[] array = source.Split(InstructionConstants.INSTRUCTION_SPLIT_CHARS, 2);
        if (array.Length != 2)
        {
            _logServices.Warning("[Socket] 指令集格式错误，当前指令：[" + source + "]，应为 \"ip:port\" 或 \"ip:port\"");
            throw new ArgumentException("指令集格式错误，当前指令：[" + source + "]，应为 \"ip:port\" 或 \"ip:port\"");
        }
        string text = array.ElementAtOrDefault(0);
        if (!int.TryParse(array.ElementAtOrDefault(1), out var result) || result <= 0 || result > 65535)
        {
            _logServices.Warning("[Socket] 配置的端口号[" + array.ElementAtOrDefault(1) + "]无效");
            throw new ArgumentException("配置的端口号[" + array.ElementAtOrDefault(1) + "]无效");
        }
        _lastConnectIp = text;
        _lastConnectPort = result;
        InitServerSocket();
        TryConnect(text, result);
        goto IL_00ec;
    IL_00ec:
        Task.Run(delegate
        {
            Receiving();
        });
        _serverRunning = true;
    }

    private void Receiving()
    {
        if (!_serverRunning)
        {
            return;
        }
        byte[] array = new byte[8192];
        int num = 0;
        while (_serverRunning)
        {
            if (_serverSocket == null)
            {
                InitServerSocket();
            }
            if (!_serverSocket.Connected)
            {
                TryConnect(_lastConnectIp, _lastConnectPort);
            }
            int num2 = _serverSocket.Receive(array, num, array.Length - num, SocketFlags.None);
            if (num2 <= 0)
            {
                continue;
            }
            num += num2;
            // 仅转换实际接收到的数据长度，而非整个8192字节buffer
            byte[] validData = new byte[num];
            Buffer.BlockCopy(array, 0, validData, 0, num);
            foreach (string item in (from p in StringUtil.ByteArrayToHexString(validData).Split(new string[1] { "AE AE AE AE" }, StringSplitOptions.None)
                                     where p.StartsWith("EA EA EA EA")
                                     select (p + "AE AE AE AE").Replace(" ", "")).ToList())
            {
                DataFrame dataFrame = new DataFrame();
                dataFrame.Add("VERSION", item.Substring(8, 8));
                dataFrame.Add("DEVICE_IP", item.Substring(16, 8));
                dataFrame.Add("TYPE_CODE", item.Substring(24, 2));
                dataFrame.Add("FUNC_CODE", item.Substring(26, 2));
                dataFrame.Add("DATA_LEN", item.Substring(28, 8));
                dataFrame.Add("CHANNEL", item.Substring(36, 4));
                dataFrame.Add("DATA_NUM", item.Substring(40, 4));
                dataFrame.Add("HEAD_LEN", dataFrame.AdditionProperties.Values.Sum((object p) => p.ToString().Length) + 8);
                dataFrame.Add("CRC", item.Substring(item.Length - 12, 4));
                dataFrame.Add("TAIL_LEN", dataFrame.Get("CRC").ToString().Length + 8);
                dataFrame.Add("REAL_DATA_LEN", Convert.ToInt32(dataFrame.Get("HEAD_LEN")) + Convert.ToInt32(dataFrame.Get("TAIL_LEN")));
                dataFrame.Payload = StringUtil.HexStringToByteArray(item);
                dataFrame.DataLength = num2;
                this.OnDataRead?.Invoke(this, dataFrame);
            }
            // 解析完成后重置偏移，等待下一批数据
            num = 0;
        }
    }

    private void MovePoint(ref int pos, int num)
    {
        pos += num;
    }

    private int IndexOfPattern(byte[] buffer, int offset, int length, byte[] pattern)
    {
        for (int i = offset; i < length - pattern.Length; i++)
        {
            bool flag = true;
            for (int j = 0; j < pattern.Length; j++)
            {
                if (buffer[i + j] != pattern[j])
                {
                    flag = false;
                    break;
                }
            }
            if (flag)
            {
                return i;
            }
        }
        return -1;
    }

    private void TryConnect(string ip, int port)
    {
        if (_serverSocket.Connected)
        {
            return;
        }
        int retryCount = 0;
        const int maxRetries = 10;
        do
        {
            try
            {
                // 当Socket未连接时，创建一个新的Socket对象
                if (!_serverSocket.Connected)
                {
                    // 释放旧的Socket资源
                    _serverSocket.Dispose();
                    // 重新初始化Socket
                    InitServerSocket();
                }
                
                _serverSocket.Connect(ip, port);
                if (_serverSocket.Connected)
                {
                    _serverRunning = true;
                    _logServices.Info($"[Socket] 成功连接到服务[{ip}:{port}]");
                    OutputLog($"[Socket] 成功连接到服务[{ip}:{port}]{Environment.NewLine}");
                    break;
                }
            }
            catch (SocketException ex)
            {
                retryCount++;
                _logServices.Warning($"[Socket] 连接服务[{ip}:{port}]失败(第{retryCount}次)：{ex.Message}");
                OutputLog($"[Socket] 连接服务[{ip}:{port}]失败(第{retryCount}次)：{ex.Message}{Environment.NewLine}");
            }
            if (retryCount >= maxRetries)
            {
                _logServices.Warning($"[Socket] 连接服务[{ip}:{port}]已达到最大重试次数({maxRetries})，放弃连接");
                break;
            }
            Thread.Sleep(TimeSpan.FromSeconds(10.0));
        }
        while (!_serverSocket.Connected);
    }

    private void InitServerSocket()
    {
        _serverSocket = new Socket(AddressFamily.InterNetworkV6, SocketType.Stream, ProtocolType.Tcp);
        _serverSocket.DualMode = true;
        _serverSocket.ReceiveTimeout = 0;
        _serverSocket.NoDelay = true;
        _serverSocket.Bind(new IPEndPoint(IPAddress.IPv6Any, 0));
        _logServices.Info("[Socket] 服务初始化完成，服务监听在 " + $"{(_serverSocket.LocalEndPoint as IPEndPoint).Address.MapToIPv4()}:{(_serverSocket.LocalEndPoint as IPEndPoint).Port}");
        OutputLog("[Socket] 服务初始化完成，服务监听在[" + $"{(_serverSocket.LocalEndPoint as IPEndPoint).Address.MapToIPv4()}:{(_serverSocket.LocalEndPoint as IPEndPoint).Port}" + "]");
    }

    public void Stop()
    {
        if (_serverSocket != null && _serverSocket.Connected)
        {
            _serverSocket.Disconnect(reuseSocket: false);
            _serverSocket.Close();
        }
        _serverRunning = false;
        _logServices.Info("[Socket] 服务停止");
        OutputLog("[Socket] 服务停止");
    }

    public void OutputLog(string message)
    {
        this.OnLogOutputEvent?.Invoke(message);
    }
}