using System;
using System.Threading;

namespace DC_0003.Services.Infrastructure
{
    /// <summary>
    /// 采集会话状态 — 替代原始代码中的 static 字段，
    /// 消除全局状态污染，支持多实例运行。
    /// </summary>
    public class SessionState : IDisposable
    {
        private volatile bool _isDataCollecting;
        private volatile bool _isConfigLoaded;
        private CancellationTokenSource _cts;

        /// <summary>是否正在采集数据</summary>
        public bool IsDataCollecting
        {
            get => _isDataCollecting;
            set => _isDataCollecting = value;
        }

        /// <summary>全局取消令牌源（用于停止所有异步任务）</summary>
        public CancellationTokenSource Cts
        {
            get => _cts;
            private set => _cts = value;
        }

        /// <summary>日志任务取消令牌</summary>
        public CancellationTokenSource FileLogCts { get; set; }

        /// <summary>配置是否已加载</summary>
        public bool IsConfigLoaded
        {
            get => _isConfigLoaded;
            set => _isConfigLoaded = value;
        }

        public SessionState()
        {
            _cts = new CancellationTokenSource();
        }

        /// <summary>重置取消令牌（用于 Init 后重启）</summary>
        public void ResetCancellationToken()
        {
            try { _cts?.Cancel(); } catch { }
            try { _cts?.Dispose(); } catch { }
            _cts = new CancellationTokenSource();
        }

        /// <summary>取消所有任务并释放资源</summary>
        public void Shutdown()
        {
            _isDataCollecting = false;
            try { FileLogCts?.Cancel(); } catch { }
            try { _cts?.Cancel(); } catch { }
            try { _cts?.Dispose(); } catch { }
            try { FileLogCts?.Dispose(); } catch { }
        }

        public void Dispose()
        {
            Shutdown();
        }
    }
}
