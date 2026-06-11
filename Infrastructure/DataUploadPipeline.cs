using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;

using DataAcquisitionLibrary;

using DC_0003.Core.Interfaces;

namespace DC_0003.Services.Infrastructure
{
    /// <summary>
    /// 数据处理管道 — 聚合、加密、上传的管道（模板代码）。
    /// 接收设备驱动的原始帧数据，进行批处理聚合、可选加密后上传。
    /// </summary>
    public class DataUploadPipeline : IDataPipeline
    {
        private readonly LogServices _logServices = LogServices.Instance;
        private IDataUploader _uploader;
        private IDataEncryptor _encryptor;

        private readonly ConcurrentQueue<byte[]> _batchQueue = new ConcurrentQueue<byte[]>();
        private readonly int _batchSize;
        private int _currentCount;
        private readonly object _batchLock = new object();

        private bool _disposed;

        /// <summary>当一批数据准备上传时触发（用于外部处理）</summary>
        public event Func<string, Task> OnBatchReady;

        public DataUploadPipeline(int batchSize = 500)
        {
            _batchSize = batchSize;
        }

        public void SetUploader(IDataUploader uploader) => _uploader = uploader;
        public void SetEncryptor(IDataEncryptor encryptor) => _encryptor = encryptor;

        public async Task ProcessBatchAsync(IEnumerable<byte[]> frames, CancellationToken cancellationToken = default)
        {
            foreach (var frame in frames)
            {
                await ProcessSingleAsync(frame, cancellationToken);
            }
        }

        public async Task ProcessSingleAsync(byte[] frame, CancellationToken cancellationToken = default)
        {
            if (frame == null || frame.Length == 0) return;

            // 加密（如果配置了加密器）
            byte[] processed = _encryptor != null ? _encryptor.Encrypt(frame) : frame;

            // 加入批处理队列
            _batchQueue.Enqueue(processed);

            // 检查是否需要上传
            int current = Interlocked.Increment(ref _currentCount);
            if (current >= _batchSize)
            {
                await FlushAsync(cancellationToken);
            }
        }

        public async Task FlushAsync(CancellationToken cancellationToken = default)
        {
            if (_batchQueue.IsEmpty) return;

            List<byte[]> batch;
            lock (_batchLock)
            {
                Interlocked.Exchange(ref _currentCount, 0);
                batch = new List<byte[]>();
                while (_batchQueue.TryDequeue(out byte[] item))
                {
                    batch.Add(item);
                }
            }

            if (batch.Count == 0) return;

            _logServices.Debug($"管道刷盘: {batch.Count} 条数据准备上传");

            // 如果是异步回调模式，触发事件
            if (OnBatchReady != null)
            {
                // 将批量数据序列化后回调
                var payload = System.Text.Encoding.UTF8.GetString(
                    System.Text.Json.JsonSerializer.SerializeToUtf8Bytes(new { count = batch.Count }));
                await OnBatchReady(payload);
            }

            // 如果配置了上传器，执行上传
            if (_uploader != null)
            {
                bool success = await _uploader.UploadAsync(batch, cancellationToken);
                if (!success)
                    _logServices.Warning("数据上传失败");
            }
        }

        public void Dispose()
        {
            if (_disposed) return;
            _disposed = true;

            // 最后刷盘
            try { FlushAsync().GetAwaiter().GetResult(); } catch { }
        }
    }
}
