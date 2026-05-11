using System;
using System.Collections.Generic;
using System.IO;
using System.Net.Http;
using System.Text;
using System.Text.Json;
using System.Threading;
using System.Threading.Tasks;

using DataAcquisitionLibrary;

using Newtonsoft.Json;

using static DC_0003.Services.Implements.Data_Collection.Class2;


//public class HttpHeartbeatSender : IHeartbeatSender
//{
//    private readonly HttpClient _client;
//    private readonly string _url;
//    private readonly string _method;
//    private CancellationTokenSource _cancellationTokenSource;
//    private readonly HeartbeatHttpConfig heartbeatHttpConfig;

//    private readonly ILogServices logServices;

//    public HttpHeartbeatSender(HeartbeatHttpConfig config)
//    {
//        _cancellationTokenSource = DataAcquisitionManager._cts;
//        heartbeatHttpConfig = config;
//        logServices = LogServices.Instance;
//        _url = config.url;
//        _client = new HttpClient();
//        _method = config.method;
//        _client.Timeout = TimeSpan.FromSeconds(config.timeout);
//        if (config.headers != null)
//        {
//            foreach (var header in config.headers)
//            {
//                _client.DefaultRequestHeaders.TryAddWithoutValidation(header.Key, header.Value);
//            }
//        }
//        LoadConfigToDictionary();
//        _ =SendAsync(JsonConvert.SerializeObject(_configDict));
//    }

//    public async Task SendAsync(string message)
//    {
      
//            while (!_cancellationTokenSource.IsCancellationRequested)
//            {
//                try
//                {
//                    var json = message;
//                    var content = new StringContent(json, Encoding.UTF8, "application/json");
//                    using (HttpResponseMessage response = await _client.PostAsync(_url, content))
//                    {
//                        if (!response.IsSuccessStatusCode)
//                        {
//                            var errorBody = await response.Content.ReadAsStringAsync();
//                            logServices.Error($"请求失败 → 状态码:{response.StatusCode} | 请求体:{json} | 响应:{errorBody}");
                        
//                        }
//                        //logServices.Info($"发送HTTP心跳:{message}");
//                    }
//                }
//                catch (OperationCanceledException)
//                {
//                    //break;
//                }
//                catch (Exception e)
//                {
//                    //DataAcquisitionManager.EnqueueLog($"心跳发送失败,Error:{e.Message}");
//                    logServices.Error($"心跳发送失败 → 信息:{e.Message} | 堆栈:{e.ToString()}");
//                }
//                await Task.Delay(1000*30); // 每5秒发送一次心跳
//            }
            
  
            
    
//}

//    Task<bool> IHeartbeatSender.SendAsync(string message)
//    {
//        return null ;
//    }

//    #region 帮助类
//    // 存放所有子项的总字典
//    Dictionary<string, object> _configDict = new Dictionary<string, object>();

//    private void LoadConfigToDictionary()
//    {
//        _configDict.Clear();
//        // 1. 找到配置文件路径（WinForm 根目录）

//        var path = Path.Combine(AppDomain.CurrentDomain.BaseDirectory, "config", "数采配置文件.json");
//        string json = File.ReadAllText(path);

//        // 2. 解析 JSON
//        using var doc = JsonDocument.Parse(json);
//        var root = doc.RootElement;

//        // 3. 递归把所有嵌套子项 放入字典
//        ParseJsonToDict(root, _configDict, "");

//    }

//    // 递归解析 JSON（自动处理嵌套 { }）
//    void ParseJsonToDict(JsonElement element, Dictionary<string, object> dict, string parentKey)
//    {
//        if (element.ValueKind == JsonValueKind.Object)
//        {
//            foreach (var prop in element.EnumerateObject())
//            {
//                string key = string.IsNullOrEmpty(parentKey) ? prop.Name : $"{parentKey}.{prop.Name}";
//                ParseJsonToDict(prop.Value, dict, key);
//            }
//        }
//        else
//        {
//            dict[parentKey] = element.GetRawText().Trim('"');
//        }
//    }
//    #endregion

//}