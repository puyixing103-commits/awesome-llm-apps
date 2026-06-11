using System;
using System.Net.Http;
using System.Text;
using System.Threading.Tasks;

using DataAcquisitionLibrary;

using DC_0003.Core.Interfaces;

using Newtonsoft.Json;

namespace DC_0003.Services.Implements.Uploaders
{
    /// <summary>
    /// 存储上传器 — 通过 HTTP 上传 JSON 数据到存储服务。
    /// 实现 IJsonUploader 接口，可替换为其他上传方式（MQTT、Kafka 等）。
    /// </summary>
    public class StorageUploader : IJsonUploader
    {
        private readonly LogServices _logServices = LogServices.Instance;
        private readonly HttpClient _client;
        private readonly string _baseUrl;
        private bool _disposed;
        
        public string Name => "HTTP 存储上传器";
        public bool IsAvailable => !string.IsNullOrEmpty(_baseUrl);

        public event EventHandler<bool> OnUploadStateChanged;
        public event EventHandler<string> OnLog;

        public StorageUploader(string baseUrl, string authToken = null)
        {
            _baseUrl = baseUrl ?? throw new ArgumentNullException(nameof(baseUrl));

            _client = new HttpClient();
            _client.Timeout = TimeSpan.FromSeconds(10);

            if (!string.IsNullOrEmpty(authToken))
            {
                _client.DefaultRequestHeaders.Authorization =
                    new System.Net.Http.Headers.AuthenticationHeaderValue("Bearer", authToken);
            }

            _logServices.Info($"存储上传器初始化完成，目标地址: {_baseUrl}");
        }

        public async Task<bool> UploadAsync(string json)
        {
            if (string.IsNullOrEmpty(json))
            {
                _logServices.Warning("上传数据为空");
                return false;
            }

            try
            {
                using (var content = new StringContent(json, Encoding.UTF8, "application/json"))
                using (var response = await _client.PostAsync(_baseUrl, content))
                {
                    if (!response.IsSuccessStatusCode)
                    {
                        var errorBody = await response.Content.ReadAsStringAsync();
                        _logServices.Error($"上传失败 → 状态码:{response.StatusCode} | 响应:{errorBody}");
                        OnUploadStateChanged?.Invoke(this, false);
                        return false;
                    }

                    _logServices.Debug($"上传成功，数据大小: {json.Length} 字节");
                    OnUploadStateChanged?.Invoke(this, true);
                    return true;
                }
            }
            catch (OperationCanceledException)
            {
                _logServices.Warning("上传超时或被取消");
                OnUploadStateChanged?.Invoke(this, false);
                return false;
            }
            catch (Exception ex)
            {
                _logServices.Error($"上传异常: {ex.Message}\n{ex}");
                OnUploadStateChanged?.Invoke(this, false);
                return false;
            }
        }

        public void Dispose()
        {
            if (_disposed) return;
            _disposed = true;
            _client?.Dispose();
        }
    }
}
