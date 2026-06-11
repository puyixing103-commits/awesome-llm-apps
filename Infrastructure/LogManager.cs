using System;
using System.Collections.Concurrent;
using System.IO;
using System.Threading;
using System.Threading.Tasks;

using DataAcquisitionLibrary;

using DC_0003.Core.Interfaces;

namespace DC_0003.Services.Infrastructure
{
    /// <summary>
    /// 日志管理器 — 统一的文件 + 控制台日志服务（模板代码）。
    /// 替代原始 DataAcquisitionManager 中的日志队列、文件刷盘、UI 输出等分散逻辑。
    /// </summary>
    public class LogManager : ILogManager
    {
        private const int MaxLogQueueSize = 20000;

        private readonly ConcurrentQueue<string> _fileLogQueue = new ConcurrentQueue<string>();
        private readonly ConcurrentQueue<string> _outputLogQueue = new ConcurrentQueue<string>();
        private CancellationTokenSource _fileLogCts;
        private Task _fileLogFlushTask;
        private Task _outputLogTask;
        private string _logPath = "数据采集";
        private string _logFile;
        private int _logQueueCount;
        private int _droppedLogs;
        private bool _disposed;

        private readonly object _fileLogLock = new object();
        private readonly IniFileManager _iniFileManager;

        public event EventHandler<string> OnLogEntry;

        public LogManager()
        {
            _iniFileManager = new IniFileManager(
                Path.Combine(AppDomain.CurrentDomain.BaseDirectory, "config", "Config.ini"));
        }

        /// <summary>启动后台刷盘任务</summary>
        public void Start()
        {
            // 文件日志刷盘任务
            _fileLogCts = new CancellationTokenSource();
            _fileLogFlushTask = Task.Run(async () =>
            {
                while (!_fileLogCts.Token.IsCancellationRequested)
                {
                    try
                    {
                        await Task.Delay(500, _fileLogCts.Token);
                        FlushFileLogs();
                    }
                    catch (OperationCanceledException) { break; }
                    catch (Exception ex)
                    {
                        System.Diagnostics.Debug.WriteLine($"文件日志刷新异常: {ex.Message}");
                    }
                }
            }, _fileLogCts.Token);

            // 输出日志分发任务（UI 事件）
            _outputLogTask = Task.Run(async () =>
            {
                while (!_fileLogCts.Token.IsCancellationRequested)
                {
                    try
                    {
                        if (_outputLogQueue.TryDequeue(out string msg))
                        {
                            OnLogEntry?.Invoke(this, msg);
                        }
                        else
                        {
                            await Task.Delay(100, _fileLogCts.Token);
                        }
                    }
                    catch (OperationCanceledException) { break; }
                    catch { }
                }
            }, _fileLogCts.Token);
        }

        /// <summary>停止后台任务并刷盘剩余日志</summary>
        public void Stop()
        {
            try { _fileLogCts?.Cancel(); } catch { }
            try { _fileLogFlushTask?.Wait(2000); } catch { }
            FlushFileLogs();
        }

        // --- 日志级别方法 ---

        public void Info(string message) => WriteLog("INFO", message);

        public void Warn(string message) => WriteLog("WARN", message);

        public void Error(string message, Exception ex = null)
        {
            var fullMsg = ex != null ? $"{message} | {ex.GetType().Name}: {ex.Message}" : message;
            WriteLog("ERROR", fullMsg);
        }

        public void Debug(string message) => WriteLog("DEBUG", message);

        /// <summary>带标志的输出日志（用于 UI 区分标记）</summary>
        public void OutputLog(string message, bool isFlag = false)
        {
            var formatted = $"[LOG]{(isFlag ? "[FLAG]" : "")}{message}";
            EnqueueOutput(formatted);
        }

        public void SetLogPath(string path)
        {
            if (!string.IsNullOrWhiteSpace(path))
                _logPath = path;
        }

        // --- 内部实现 ---

        private void WriteLog(string level, string message)
        {
            string text = $"[{DateTime.Now:yyyy-MM-dd HH:mm:ss.fff}] [{level}] {message}";
            _fileLogQueue.Enqueue(text);
        }

        private void EnqueueOutput(string message)
        {
            if (!message.EndsWith(Environment.NewLine))
                message += Environment.NewLine;

            while (Volatile.Read(ref _logQueueCount) >= MaxLogQueueSize
                   && _outputLogQueue.TryDequeue(out _))
            {
                Interlocked.Decrement(ref _logQueueCount);
                Interlocked.Increment(ref _droppedLogs);
            }

            _outputLogQueue.Enqueue($"{DateTime.Now}:{message}");
            Interlocked.Increment(ref _logQueueCount);
        }

        private void FlushFileLogs()
        {
            var logs = new System.Collections.Generic.List<string>();
            while (_fileLogQueue.TryDequeue(out string log))
                logs.Add(log);

            if (logs.Count == 0) return;

            lock (_fileLogLock)
            {
                try
                {
                    var path = _iniFileManager.GetValue("NEWDATACOLLECTION", "FILEURLDATA");
                    if (!string.IsNullOrEmpty(path))
                        _logPath = path;

                    if (!Directory.Exists(_logPath))
                        Directory.CreateDirectory(_logPath);

                    _logFile = Path.Combine(_logPath, $"dc_{DateTime.Now:yyyy-MM-dd}");
                    File.AppendAllText(_logFile + ".txt",
                        string.Join(Environment.NewLine, logs) + Environment.NewLine);
                }
                catch (Exception ex)
                {
                    System.Diagnostics.Debug.WriteLine($"文件日志写入异常: {ex.Message}");
                }
            }
        }

        public void Dispose()
        {
            if (_disposed) return;
            _disposed = true;
            Stop();
            try { _fileLogCts?.Dispose(); } catch { }
        }
    }
}
