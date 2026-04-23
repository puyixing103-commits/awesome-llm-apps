using System;
using System.Linq;
using System.Net;
using System.Net.Sockets;
using System.Threading.Tasks;

using DataAcquisitionLibrary;

using DC_0003.Core.Constants;
using DC_0003.Core.Interfaces;

namespace DC_0003.Services.Implements.Sockets;

public class TCPDataSender : IDataSender, INotifyLogOutput
{
    private readonly ILogServices _logServices;

    private Socket _serverSocket;

    public Socket ServerSocket => _serverSocket;

    public event Action<string> OnLogOutputEvent;

    public TCPDataSender()
    {
        _logServices = LogServices.Instance;
        InitServerSocket();
    }

    private void InitServerSocket()
    {
        if (_serverSocket == null)
        {
            Socket serverSocket = _serverSocket;
            if (serverSocket == null || !serverSocket.Connected)
            {
                goto IL_003a;
            }
        }
        _serverSocket.Disconnect(reuseSocket: false);
        _serverSocket.Close();
        _serverSocket = null;
        goto IL_003a;
    IL_003a:
        _serverSocket = new Socket(AddressFamily.InterNetworkV6, SocketType.Stream, ProtocolType.Tcp);
        _serverSocket.DualMode = true;
        _serverSocket.ReceiveTimeout = 0;
        _serverSocket.NoDelay = true;
    }

    public void OutputLog(string message)
    {
        this.OnLogOutputEvent?.Invoke(message);
    }

    public async Task SendAsync(string destination, byte[] payload)
    {
        if (_serverSocket == null)
        {
            return;
        }
        if (!_serverSocket.Connected)
        {
            string[] array = destination.Split(InstructionConstants.INSTRUCTION_SPLIT_CHARS, 2);
            if (array.Length != 2)
            {
                _logServices.Warning("[Socket] 指令集格式错误，当前指令：[" + destination + "]，应为 \"ip:port\" 或 \"ip:port\"");
                throw new ArgumentException("指令集格式错误，当前指令：[" + destination + "]，应为 \"ip:port\" 或 \"ip:port\"");
            }
            string text = array.ElementAtOrDefault(0);
            if (!int.TryParse(array.ElementAtOrDefault(1), out var result) || result <= 0 || result > 65535)
            {
                _logServices.Warning("[Socket] 配置的端口号[" + array.ElementAtOrDefault(1) + "]无效");
                throw new ArgumentException("配置的端口号[" + array.ElementAtOrDefault(1) + "]无效");
            }
            if (IPAddress.TryParse(text, out var _))
            {
                try
                {
                    _serverSocket.Connect(text, result);
                }
                catch (Exception)
                {
                    InitServerSocket();
                    await SendAsync(destination, payload);
                }
            }
        }
        _serverSocket.Send(payload);
    }
}
