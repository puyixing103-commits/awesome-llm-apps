using System;
using System.Collections.Generic;
using System.Dynamic;
using System.Net.Http;
using System.Text;
using System.Threading.Tasks;

using DataAcquisitionLibrary;

using Newtonsoft.Json;

using static DC_0003.Services.Implements.Data_Collection.Class2;

public class KeyServiceClient : IKeyServiceClient
{
    private readonly HttpClient _client;
    private readonly string _baseUrl;
    private readonly LogServices _logServices = LogServices.Instance;

    public KeyServiceClient(KeyServiceConfig config)
    {
        _baseUrl = "http://127.0.0.1:8080/api/crypto/encrypt/batch";
        _client = new HttpClient();
        _client.Timeout = TimeSpan.FromSeconds(config.timeout);
    //    ／／ _client.BaseAddress = new Uri(_baseUrl);
        _client.DefaultRequestHeaders.Authorization =
            new System.Net.Http.Headers.AuthenticationHeaderValue("Bearer", "TKN1:XzdTWZmkKm7/1ka2Zdxa1A==:zy9BfghUkgkEIoelo9oeU3+4tElBesdMkWAs2/XdPKUUpeDlooOq9XRKpZjx+Tzg");
    }

    async Task<KeyResponse> IKeyServiceClient.GetKeyAsync(string json)
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
                //int responseBytes = responseData.Length;

                //// 记录统计信息
                //_logServices.Warning($"HTTP流量统计 - 请求:{requestBytes} bytes, 响应:{responseBytes} bytes, 总计:{requestBytes + responseBytes} bytes ({(requestBytes + responseBytes) / 1024.0:F2} KB)");

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

    //async Task<KeyResponse> IKeyServiceClient.GetKeyAsync(long time,string macAddress, IotDataMessageModel iotDataMessageModel)
    //{
    //    try
    //    {
    //        string json = JsonConvert.SerializeObject(new
    //        {
    //            timestamp = time,
    //            deviceMac = macAddress,
    //            plain = iotDataMessageModel
    //        });

    //        using (var content = new StringContent(json, Encoding.UTF8, "application/json"))
    //        {
    //            // 重点：using 自动释放 HttpResponseMessage → 解决连接泄漏！
    //            using (HttpResponseMessage response = await _client.PostAsync(_baseUrl, content))
    //            {
    //                if (!response.IsSuccessStatusCode)
    //                {
    //                    var errorBody = await response.Content.ReadAsStringAsync();
    //                    _logServices.Error($"请求失败 → 状态码:{response.StatusCode} | 请求体:{json} | 响应:{errorBody}");
    //                    return null;
    //                }

    //                //获取返回的字节数



    //                var data = await response.Content.ReadAsStringAsync();
    //                var result = JsonConvert.DeserializeObject<ResponseModel>(data);
    //                return result?.data;
    //            }
    //        }
    //    }
    //       catch (OperationCanceledException)
    //        {
    //            return null;
    //        }
    //    catch (Exception e)
    //    {
    //        _logServices.Error($"HTTP请求异常 → 信息:{e.Message} | 堆栈:{e.StackTrace}");
    //        return null;
    //    }
    //}
}


//加密服务实体类

public class KeyEncryptionDto
{
   

    public long timestamp { get; set; }
   
    public string deviceMac { get; set; }
    public List<items> items { get; set; }
}


public class items {
    public IotDataMessageModel plain { get; set; }

}