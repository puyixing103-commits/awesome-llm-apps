using System;
using System.Net.Http;
using System.Text;
using System.Threading.Tasks;

using DataAcquisitionLibrary;

using Newtonsoft.Json;

using static DC_0003.Services.Implements.Data_Collection.Class2;



public class HttpUploader : IUploader
{
    private readonly HttpClient _client;
    private readonly string _url;
    private readonly string _method;
    private readonly LogServices _logServices = LogServices.Instance;

    public HttpUploader(UpHttpConfig config)
    {
        _url = config.url;
        _method = config.method?.ToUpper() ?? "POST";
        _client = new HttpClient();
        _client.Timeout = TimeSpan.FromSeconds(config.timeout);
        if (config.headers != null)
        {
            foreach (var header in config.headers)
            {
                _client.DefaultRequestHeaders.TryAddWithoutValidation(header.Key, header.Value);
            }
        }
    }

    public async Task<bool> BatchUploadAsync(string message, string topic)
    {
        try
        {
            using (var content = new StringContent(message, Encoding.UTF8, "application/json"))
            {
                // 自动释放 response
                using (HttpResponseMessage response = _method == "POST"
                    ? await _client.PostAsync(_url, content)
                    : await _client.PutAsync(_url, content))
                {
                    string result = await response.Content.ReadAsStringAsync();
                    return response.IsSuccessStatusCode;
                }
            }
        }
            catch (OperationCanceledException)
            {
                return false;
            }
        catch (Exception ex)
        {
            _logServices.Error($"批量上传异常：{ex.ToString()}");
            return false;
        }
    }



    public async Task<bool> UploadAsync(DataMessage message, string topic)
    {
        var json = JsonConvert.SerializeObject(message);
        var content = new StringContent(json, Encoding.UTF8, "application/json");

        HttpResponseMessage response;
        if (_method == "POST")
            response = await _client.PostAsync(_url, content);
        else if (_method == "PUT")
            response = await _client.PutAsync(_url, content);
        else
            throw new NotSupportedException($"不支持的 HTTP 方法: {_method}");

        return response.IsSuccessStatusCode;
        //throw new NotImplementedException();
    }


  

    
    public class TimeRange
    {
        public string start { get; set; }
        public string end { get; set; }
    }

    public class DataMessage
    {
        public DateTime[] time_range { get; set; }
        public string plain { get; set; }
        public string cipher { get; set; }
        public string key { get; set; }

        public string time_Id { get; set; }

        public string sign { get; set; }

        public bool isencrypted { get; set; }

        public string ivBase64 { get; set; }
    }
}