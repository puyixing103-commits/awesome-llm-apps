using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Net.Http;
using System.Text;
using System.Threading.Tasks;

using DataAcquisitionLibrary;

using Newtonsoft.Json;

using static DC_0003.Services.Implements.Data_Collection.Class2;

namespace DC_0003.Services.Implements
{
    public class StorageUploadManager
    {
        private readonly HttpClient _client;
        private readonly string _baseUrl;
        private readonly LogServices _logServices = LogServices.Instance;
        private readonly IniFileManager _iniFileManager = new IniFileManager(Path.Combine(AppDomain.CurrentDomain.BaseDirectory, "config", "Config.ini"));

        public StorageUploadManager(KeyServiceConfig config)
        {
            string url = "http://127.0.0.1:8080/local/storage/payload"; //_iniFileManager.GetValue("NEWDATACOLLECTION", "STORAGEURL");
            _baseUrl = url;
            _client = new HttpClient();
            _client.Timeout = TimeSpan.FromSeconds(3);
            //    ／／ _client.BaseAddress = new Uri(_baseUrl);
            _client.DefaultRequestHeaders.Authorization =
                new System.Net.Http.Headers.AuthenticationHeaderValue("Bearer", "TKN1:XzdTWZmkKm7/1ka2Zdxa1A==:zy9BfghUkgkEIoelo9oeU3+4tElBesdMkWAs2/XdPKUUpeDlooOq9XRKpZjx+Tzg");
        }

       public async Task<KeyResponse> StorageUploadAsync(string json)
        {
            try
            {


                int requestBytes = Encoding.UTF8.GetByteCount(json);

                using (var content = new StringContent(json, Encoding.UTF8, "application/json"))
                using (HttpResponseMessage response = await _client.PostAsync(_baseUrl, content))
                {
                    if (!response.IsSuccessStatusCode)
                    {
                        var errorBody = await response.Content.ReadAsStringAsync();
                        _logServices.Error($"请求失败 → 状态码:{response.StatusCode} | 请求体:{json} | 响应:{errorBody}");
                        return null;
                    }

                    //// 获取响应字节数（最准确的方式）
                    byte[] responseData = await response.Content.ReadAsByteArrayAsync();
                   
                    string responseJson = Encoding.UTF8.GetString(responseData);
                    var result = JsonConvert.DeserializeObject<ResponseModel>(responseJson);
                    return result?.data;
                }
            }
            catch (OperationCanceledException)
            {
                return null;
            }
            catch (Exception e)
            {
                _logServices.Error($"HTTP请求异常 → 信息:{e.Message} | 堆栈:{e.StackTrace}");
                return null;
            }
        }
    }
}
