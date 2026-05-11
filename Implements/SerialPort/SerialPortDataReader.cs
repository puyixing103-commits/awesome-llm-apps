

// DC_1005.Services, Version=1.0.0.0, Culture=neutral, PublicKeyToken=null
// DC_1005.Services.Implements.SerialPort.SerialPortDataReader
using System;
using System.Collections.Concurrent;
using System.IO.Ports;
using System.Threading;
using System.Threading.Tasks;

using DataAcquisitionLibrary;
using DataAcquisitionLibrary.Utils;

using DC_0003.Core.Interfaces;
using DC_0003.Models;
using DC_0003.Services.Implements.Processors;



public class SerialPortDataReader : IDataReader, IDisposable
{
    private readonly SerialPort _port;

    private BlockingCollection<DataFrame> _queue;

    private readonly int _byteLength;

    private readonly byte[] _readBuffer;

    private int _interval;

    private byte[] _commandBytes;

    private CancellationTokenSource _cancellationTokenSource;

    private readonly ILogServices _logService;

    private readonly int _queueSize;

    public BlockingCollection<DataFrame> Queue => _queue;

    public event Action<IDataReader, DataFrame> OnDataRead;

    public SerialPortDataReader(string portName, int byteLength, string command, int interval = 0, int baundRate = 9600, Parity parity = Parity.None, int dataBits = 8, StopBits stopBits = StopBits.One, int readTimeoutMs = 0, int queueSize = 3, CancellationTokenSource cancellationTokenSource = null)
    {
        _cancellationTokenSource = cancellationTokenSource;
        _interval = interval;
        _queueSize = queueSize;
        _logService = LogServices.Instance;
        _byteLength = byteLength;
        _readBuffer = new byte[_byteLength];
        _queue = new BlockingCollection<DataFrame>(new ConcurrentQueue<DataFrame>());
        if (string.IsNullOrWhiteSpace(portName))
        {
            _logService.Error("[" + portName + "] 不是一个有效的串口名称！");
        }
        _logService.Info("[" + portName + "] 一个有效的串口名称！");
        _port = new SerialPort(portName, baundRate, parity, dataBits, stopBits)
        {
            ReadTimeout = readTimeoutMs
        };
        _commandBytes = StringUtil.HexStringToByteArray(command);
        _port.DataReceived += Port_DataReceived;
        TryOpenPort();
        
    }

    private Task SendLoopAsync(int interval)
    {    
        _port.Write(_commandBytes, 0, _commandBytes.Length);
        if (interval <= 0)
        {
            return Task.CompletedTask;
        }
        return Task.Run(async delegate
        {
            while (!_cancellationTokenSource.IsCancellationRequested)
            {
                await Task.Delay(TimeSpan.FromSeconds(interval));
                _port.Write(_commandBytes, 0, _commandBytes.Length);
            }
        }, _cancellationTokenSource.Token);
    }

    private void Port_DataReceived(object sender, SerialDataReceivedEventArgs e)
    {
        //IL_0059: Unknown result type (might be due to invalid IL or missing references)
        //IL_0060: Expected O, but got Unknown
        try
        {
            int bytesToRead = _port.BytesToRead;
            if (bytesToRead <= 0 || bytesToRead <= 0 || _cancellationTokenSource.IsCancellationRequested)
            {
                return;
            }
            byte[] array = new byte[bytesToRead];
            _port.Read(array, 0, bytesToRead);
            byte[] array2 = array;
            foreach (byte b in array2)
            {
                DataFrame item = new DataFrame();
                item.Payload = new byte[1] { b };
                DataFrame item2 = item;
                if (_queue.Count > _queueSize)
                {
                    _queue.TryTake(out item);
                }
                _queue.Add(item2);
            }
        }
        catch (Exception ex)
        {
            _logService.Error("[" + _port.PortName + "] 读取串口数据异常: " + ex.Message);
        }
        finally
        {
            _port.DiscardInBuffer();
        }
    }

    private void TryOpenPort()
    {
        if (_port != null && !_port.IsOpen)
        {
            try
            {
                _port.Open();
                _port.DiscardInBuffer();
                _logService.Info($"串口打开成功!");
            }
            catch {
                _logService.Warning($"串口打开失败!");
            }
            finally
            {
            }
        }
    }

    public async Task<string> ReadAsStringAsync(string source)
    {
        await SendLoopAsync(_interval);
        return "";
    }

    public void Dispose()
    {
        _cancellationTokenSource.Cancel();
        if (_port != null)
        {
            try
            {
                _port.Close();
                _port.DataReceived -= Port_DataReceived;
            }
            finally
            {
                _port.Dispose();
            }
        }
        _queue.Dispose();
    }
}
