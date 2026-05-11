using System;
using System.Net;
using System.Net.Sockets;
using System.Threading;
using System.Threading.Tasks;

using DataAcquisitionLibrary;

public class PortChecker
{
    private readonly int _port;
    private readonly string _host;
    private readonly int _checkIntervalMs;
    private CancellationTokenSource _cts;
    private Task _checkTask;
    private bool _isRunning;
    
    public event Action<bool> OnPortStatusChanged;
    public event Action<string> OnLog;

    private LogServices _logServices = LogServices.Instance;
                                                                                                                                                                                                                                                                               
    public bool IsPortOpen { get; private set; }
    public Func<Task> GetReceiceAsync { get; set; }
    public Func<Task> RestartJavaServiceAsync { get; set; }

    public PortChecker(int port, string host = "127.0.0.1", int checkIntervalMs = 60000)
    {
        _port = port;
        _host = host;
        _checkIntervalMs = checkIntervalMs;
    }
    
    public void Start()
    {
        if (_isRunning)
            return;
            
        _isRunning = true;
        _cts = new CancellationTokenSource();
        _checkTask = Task.Run(() => CheckPortLoopAsync(_cts.Token), _cts.Token);
        _logServices.Info($"端口检测服务已启动，检测 {_host}:{_port}");
        OnLog?.Invoke($"端口检测服务已启动，检测 {_host}:{_port}");
        GetReceiceAsync();
    }
    
    public void Stop()
    {
        if (!_isRunning)
            return;
            
        _isRunning = false;
        _cts?.Cancel();
        try
        {
            _checkTask?.Wait(2000);
            _cts?.Dispose();
            OnLog?.Invoke("端口检测服务已停止");
            _logServices.Info("端口检测服务已停止");
        }
        catch
        {
        }
        
    }
    
    private async Task CheckPortLoopAsync(CancellationToken cancellationToken)
    {
        while (!cancellationToken.IsCancellationRequested)
        {
            try
            {
                bool isOpen = await CheckPortAsync(_host, _port, 2000);
                
                if (isOpen != IsPortOpen)
                {
                    IsPortOpen = isOpen;
                  
                    OnLog?.Invoke($"端口 {_host}:{_port} 状态变更: {(isOpen ? "正常" : "异常")}");
                    _logServices.Info($"端口 {_host}:{_port} 状态变更: {(isOpen ? "正常" : "异常")}");
                }
                if (isOpen == false)
                {
                   await RestartJavaServiceAsync();
                    //OnPortStatusChanged?.Invoke(isOpen);
                }
            }
            catch (Exception ex)
            {
                _logServices.Error($"端口检测异常: {ex.ToString()}");
                OnLog?.Invoke($"端口检测异常: {ex.Message}");
            }
            
            try
            {
                await Task.Delay(_checkIntervalMs, cancellationToken);
            }
            catch (OperationCanceledException)
            {
                break;
            }
           // await Task.Delay(5000,cancellationToken);
        }
    }
    
    private async Task<bool> CheckPortAsync(string host, int port, int timeoutMs)
    {
        try
        {
            using (var client = new TcpClient())
            {
                var connectTask = client.ConnectAsync(host, port);
                var timeoutTask = Task.Delay(timeoutMs);
                
                var completedTask = await Task.WhenAny(connectTask, timeoutTask);
                
                if (completedTask == timeoutTask)
                {
                    return false;
                }
                
                return client.Connected;
            }
        }
        catch
        {
            return false;
        }
    }
    
    public async Task<bool> CheckOnceAsync()
    {
        return await CheckPortAsync(_host, _port, 2000);
    }
}