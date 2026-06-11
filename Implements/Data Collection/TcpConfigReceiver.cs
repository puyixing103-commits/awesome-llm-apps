using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Net;
using System.Net.Sockets;
using System.Runtime.InteropServices.ComTypes;
using System.Text;
using System.Text.Json;
using System.Threading;
using System.Threading.Tasks;

using ApiModels;

using DataAcquisitionLibrary;

using DC_0003.Core.Interfaces;
using DC_0003.Models;
using DC_0003.Services.Implements.Data_Collection.Dto;

using Newtonsoft.Json;

public class TcpConfigReceiver
{
    private readonly int _port;
    private readonly string _storageFile;
    private readonly Func<bool> _isCollectingProvider;
    public event Action<string> _reloadCallback; // 可选：热加载回调
    private CancellationTokenSource _cts;
    private event Action _closeCallback;
    private LogServices logServices = LogServices.Instance;

    public event Action<string> StartCommade;

    public event Action StopCommade;

    public event Action<string> OnLog;

    public TcpConfigReceiver(int port, string storageFile, Func<bool> isCollectingProvider, Action<string> reloadCallback = null, Action _ctscallback = null)
    {
        _port = port;
        _storageFile = storageFile;
        _isCollectingProvider = isCollectingProvider ?? throw new ArgumentNullException(nameof(isCollectingProvider));
        _reloadCallback = reloadCallback;
        _cts = new CancellationTokenSource();
        LoadConfigToDictionary();
    }
    private HttpListener _listener;
    private Thread _serverThread;
    private void ProcessRequest(HttpListenerContext context)
    {
        var request = context.Request;
        var response = context.Response;

        try
        {
            // ✅ 1. 无论什么请求，先加上 CORS 响应头
            response.Headers.Add("Access-Control-Allow-Origin", "*");
            response.Headers.Add("Access-Control-Allow-Methods", "GET, POST, PUT, DELETE, OPTIONS");
            // ✅ 必须包含 content-type，否则浏览器带 content-type 的 GET/POST 依然会失败
            response.Headers.Add("Access-Control-Allow-Headers", "Content-Type, Authorization, Accept");
           // response.Headers.Add("Access-Control-Max-Age", "1728000");
            // ✅ 2. 处理浏览器的 OPTIONS 预检请求
            if (request.HttpMethod == "OPTIONS")
            {
                response.StatusCode = 204; // No Content
                response.Close();
                return; // 直接返回，不进后续逻辑
            }

            //HeartbeatServiceAsync

            if (request.HttpMethod == "GET")
            {
                response.ContentType = "application/json";
                string results = null;

                switch (request.Url.AbsolutePath)
                {
                    // 返回 HeartbeatServiceAsync 的结果
                    case "/api/datacollection/server/heartbeat":
                        try
                        {
                            // ProcessRequest 为同步方法，阻塞等待异步方法结果
                            results = JsonConvert.SerializeObject(ResponseBuilder.Success(HeartbeatServiceAsync()));
                        }
                        catch (Exception ex)
                        {
                            logServices.Error($"生成心跳信息失败: {ex}");
                            results = "{\"code\":500,\"msg\":\"未知错误\"}";
                        }
                        break;

                    default:
                        results = "{\"code\":404,\"msg\":\"接口不存在\"}";
                        break;
                }

                byte[] buffer = Encoding.UTF8.GetBytes(results ?? "{\"code\":500,\"msg\":\"未知错误\"}");
                response.ContentLength64 = buffer.Length;
                response.OutputStream.Write(buffer, 0, buffer.Length);
                return;
            }

            if (request.HttpMethod == "POST")
            {
                string json = new StreamReader(request.InputStream).ReadToEnd();
                string results = null;

                DispatchCommandDto dispatchCommandDto = JsonConvert.DeserializeObject<DispatchCommandDto>(json);
               
                logServices.Info($"HTPP 接收到消息: {json},请求头:{request.Url.AbsolutePath}");
                OnLog?.Invoke($"HTTP 接收到消息: {request.Url.AbsolutePath}");

                switch (request.Url.AbsolutePath)
                {
                    case "/api/datacollection/server/dispatch-command":
                        switch (dispatchCommandDto.runState)
                        {
                            case "STARTING":
                                LoadConfigToDictionary();
                                if (_isCollectingProvider())
                                {
                                    logServices.Warning("HTTP请求但当前正在采集数据，已拒绝");
                                    var s = ResponseBuilder.AlreadyRunning();

                                    results = JsonConvert.SerializeObject(s);

                                    break;
                                }
                                //OnLogOutput?.Invoke($"HTPP 接收到开始采集指令:{json}");
                               

                                // 处理 /api/config 请求
                                results = HandleConfigRequest(dispatchCommandDto.configPath);
                                
                                break;
                          
                            case "STOPPED"://停止

                                if (!_isCollectingProvider())
                                {
                                    results = JsonConvert.SerializeObject(ResponseBuilder.AlreadyStopped()); break;
                                }
                                StopCommade?.Invoke();//停止命令
                                results = JsonConvert.SerializeObject(ResponseBuilder.StopSuccess());
                                break;

                            default:
                                results = "{\"code\":404,\"msg\":\"\"HTPP 接收到未知指令\"}";
                                //OnLogOutput?.Invoke($"HTPP 接收到未知指令:{mqttMessage.commandCode}");
                                break;
                        }
                        break;

                 

                    default:
                        results = results ?? "{\"code\":404,\"msg\":\"接口不存在\"}";
                        break;
                }

             

                byte[] buffer = Encoding.UTF8.GetBytes(results);
                response.ContentLength64 = buffer.Length;
                response.OutputStream.Write(buffer, 0, buffer.Length);
            }
         
        }
        catch (Exception ex)
        {
            logServices?.Error($"处理请求异常: {ex}");
            try
            {
                string errorJson = JsonConvert.SerializeObject(ResponseBuilder.Error());
                byte[] errorBuffer = Encoding.UTF8.GetBytes(errorJson);
                response.ContentLength64 = errorBuffer.Length;
                response.OutputStream.Write(errorBuffer, 0, errorBuffer.Length);
            }
            catch {
            }
        }
        finally
        {
            response.Close();
        }
    }
    /// <summary>
    /// 获取指定目录下最新的 JSON 文件
    /// </summary>
    private string GetLatestJsonFile(string directory)
    {
        try
        {
            // 获取目录下所有 .json 文件
            var jsonFiles = Directory.GetFiles(directory, "*.json", SearchOption.TopDirectoryOnly);

            if (jsonFiles == null || jsonFiles.Length == 0)
                return null;

            // 按最后修改时间排序，获取最新的文件
            string latestFile = jsonFiles.OrderByDescending(f => File.GetLastWriteTime(f)).FirstOrDefault();

            return latestFile;
        }
        catch (Exception ex)
        {
            logServices?.Error($"获取最新 JSON 文件失败，目录:{directory}，错误:{ex}");
            return null;
        }
    }
    private readonly IniFileManager _iniFileManager = new IniFileManager(Path.Combine(AppDomain.CurrentDomain.BaseDirectory, "config", "Config.ini"));
    private string HandleConfigRequest(string url)
    {

        // 2. 写死的配置文件路径

        //string configDir = _iniFileManager.GetValue("NEWDATACOLLECTION", "FILEURL");//@"C:\Users\shucai\Desktop\config";
        string fullData = url;// GetLatestJsonFile(configDir);
        // 3. 校验文件是否存在
        if (!File.Exists(fullData))
        {
            logServices.Warning($"配置文件不存在: {fullData}");
            OnLog?.Invoke($"配置文件不存在: {fullData}");
            return $"{{\"code\":404,\"msg\":\"配置文件不存在\",\"data\":{{\"message\":\"{fullData}\"}}}}";
        }

        // 4. 处理配置文件
        try
        {
            string jsonStr = File.ReadAllText(fullData, Encoding.UTF8);

            // 触发热加载
            _reloadCallback?.Invoke(jsonStr);
            logServices.Info($"TCP配置接收服务成功更新配置，路径:{fullData}");
            OnLog?.Invoke($"配置已更新，路径:{fullData}");
            StartCommade?.Invoke("");
            //DataAcquisitionManager.EnqueueLog($"TCP配置接收服务成功更新配置，路径:{fullData}");
            logServices.Info($"数据开始采集");
            OnLog?.Invoke("数据开始采集");

            return JsonConvert.SerializeObject(ResponseBuilder.StartSuccess());
        }
        catch (Exception ex)
        {
            logServices?.Error($"处理配置文件失败，路径:{fullData}，错误:{ex}");
            return JsonConvert.SerializeObject(ResponseBuilder.Error());
        }
    }
    //private void ProcessRequest(HttpListenerContext context)
    //{
    //    try
    //    {
    //        var request = context.Request;
    //        var response = context.Response;

    //        response.ContentType = "application/json";

    //      if (request.HttpMethod == "GET")
    //        {
    //            string result = null;

    //            switch (request.Url.AbsolutePath)
    //            {
    //              //  "C:\\Users\\shucai\\Desktop\\config"
    //                case "/api/config":
    //                    // 从查询字符串中获取配置文件地址


    //                    if (DataAcquisitionManager._isDataCollecting)
    //                    {
    //                        DataAcquisitionManager.EnqueueLog($"TCP配置接收服务收到新配置，但当前正在采集数据，已忽略");
    //                        var bytes = Encoding.UTF8.GetBytes("ERROR: 当前正在采集数据，无法更新配置\n");

    //                        return; // 不关闭连接，继续等待下一个请求;
    //                    }
    //                    string fullData = @"C:\\Users\\shucai\\Desktop\\config";


    //                    // 校验路径
    //                    if (!File.Exists(fullData))
    //                    {
    //                        var configResponse = new
    //                        {
    //                            code = 503,
    //                            msg = "文件路径不存在!",
    //                            data = new { filePath = fullData }
    //                        };
    //                        //continue; // 不关闭连接，继续等待下一个请求;
    //                    }
    //                    else
    //                    {
    //                        try
    //                        {
    //                            string jsonStr = File.ReadAllText(fullData);
    //                            // 原子写入暂存文件
    //                            var path = Path.Combine(AppDomain.CurrentDomain.BaseDirectory, "config", _storageFile);
    //                            string dir = Path.GetDirectoryName(path);
    //                            if (!Directory.Exists(dir))
    //                                Directory.CreateDirectory(dir);
    //                            string tempFile = _storageFile;
    //                            File.WriteAllText(path, jsonStr);
    //                            // 触发热加载
    //                            _reloadCallback?.Invoke(jsonStr);
    //                            var configResponse = new
    //                            {
    //                                code = 200,
    //                                msg = "配置文件地址接收成功",
    //                                data = new { filePath = fullData }
    //                            };
    //                            result = JsonConvert.SerializeObject(configResponse);
    //                            DataAcquisitionManager.EnqueueLog($"TCP配置接收服务收到新配置，路径:{fullData}");

    //                        }
    //                        catch (Exception ex)
    //                        {
    //                            logServices.Error($"TCP配置接收服务处理配置失败，路径:{fullData}，错误信息:{ex.ToString()}");

    //                        }
    //                    }



    //                    break;
    //                default:
    //                    result = "{\"code\":404,\"msg\":\"接口不存在\"}";
    //                    break;
    //            }

    //            byte[] buffer = Encoding.UTF8.GetBytes(result);
    //            response.ContentLength64 = buffer.Length;
    //            response.OutputStream.Write(buffer, 0, buffer.Length);
    //        }
    //        else
    //        {
    //            byte[] buffer = Encoding.UTF8.GetBytes("{\"code\":405,\"msg\":\"仅支持POST和GET请求\"}");
    //            response.ContentLength64 = buffer.Length;
    //            response.OutputStream.Write(buffer, 0, buffer.Length);
    //        }


    //        response.Close();
    //    }
    //    catch (Exception e)
    //    {
    //        //OnLogOutput?.Invoke($"处理请求异常：{e.Message}");
    //    }
    //}


    private object HeartbeatServiceAsync()
    {
        //int HBCOUNT = _windowsConfigModel.HBCOUNT > 0 ? _windowsConfigModel.HBCOUNT : 60;
        // 优化点：原代码直接向共享字典 _configDict Add 元素，多次心跳会导致异常（键重复）或内存泄漏
        // 此处每次心跳构建独立的属性字典，避免污染全局配置
        var envProperties = new Dictionary<string, object>(_configDict)
            {
                { "os_name", Environment.OSVersion.ToString() },
                { "os_version", Environment.OSVersion.Version.ToString() },
                { "machine_name", Environment.MachineName }
            };

       
            SystemInfoModel systemInfo = SystemInfoHelper.GetSystemInfo();
            var heartbeatMessage = new
            {
                type = "data_collect",
                content = new
                {
                    dataCollectProgramId = envProperties["DATA_COLLECT_PROGRAM_ID.value"].ToString(),
                    dataCollectDeviceNo = envProperties["DATA_COLLECT_DEVICE_NO.value"].ToString(),
                    roomCode = envProperties["ROOM_CODE.value"].ToString(),
                    deviceCode = envProperties["DEVICE_CODE.value"].ToString(),
                    programVersion = envProperties["PROGRAM_VERSION.value"].ToString(),
                    dataCollectSystemRunParamDTO = systemInfo,
                    envProperties = envProperties
                }
            };

            var d = JsonConvert.SerializeObject(heartbeatMessage);

            return heartbeatMessage;

 


        // 优化11：使用 Task.Delay 代替 Thread.Sleep，并在取消时立即唤醒退出


    }

    private void LoadConfigToDictionary()
    {
        _configDict.Clear();
        var path = Path.Combine(AppDomain.CurrentDomain.BaseDirectory, "config", "数采配置文件.json");
        string json = File.ReadAllText(path);
        using var doc = JsonDocument.Parse(json);
        var root = doc.RootElement;
        ParseJsonToDict(root, _configDict, "");
    }
    private Dictionary<string, object> _configDict = new Dictionary<string, object>();
    void ParseJsonToDict(JsonElement element, Dictionary<string, object> dict, string parentKey)
    {
        if (element.ValueKind == JsonValueKind.Object)
        {
            foreach (var prop in element.EnumerateObject())
            {
                string key = string.IsNullOrEmpty(parentKey) ? prop.Name : $"{parentKey}.{prop.Name}";
                ParseJsonToDict(prop.Value, dict, key);
            }
        }
        else
        {
            dict[parentKey] = element.GetRawText().Trim('"');
        }
    }
    public void Stop()
    {
        _listener?.Stop();
        _listener?.Close();
        //OnLogOutput?.Invoke("HTTP API 服务已停止");
    }

    private void ListenLoop()
    {
        while (_listener.IsListening)
        {
            try
            {
                var context = _listener.GetContext();
                ProcessRequest(context);
            }
            catch { }
        }
    }
    public async Task StartListenAsync()
    {

        try
        {
            _listener = new HttpListener();
            _listener.Prefixes.Add($"http://+:{_port}/");
            _listener.Start();
            logServices.Info($"HTTP API 服务已启动：http://localhost:{_port}/");
            OnLog?.Invoke($"HTTP API 服务已启动：http://localhost:{_port}/");
            _serverThread = new Thread(ListenLoop);
            _serverThread.IsBackground = true;
            _serverThread.Start();
        }
        catch (Exception e)
        {
            logServices.Error($"HTTP API 启动失败：{e.Message}");
            OnLog?.Invoke($"HTTP API 启动失败：{e.Message}");
            throw;
        }
        //try
        //{
        //    listener.Start();
        //}
        //catch (SocketException ex) when (ex.SocketErrorCode == SocketError.AddressAlreadyInUse)
        //{
        //    DataAcquisitionManager.EnqueueLog($"端口 {_port} 被占用，尝试杀掉占用进程...");
        //    listener.Stop();
        //      await Task.Delay(1000); // 等待系统释放端口
        //    return;
        //}
        //catch (Exception ex)
        //{
        //    DataAcquisitionManager.EnqueueLog($"启动监听失败: {ex.Message}");
        //    await Task.Delay(1000);
        //    return;
        //}

        ////Console.WriteLine($"TCP 配置接收服务已启动，监听 0.0.0.0:{_port}");
        //DataAcquisitionManager.EnqueueLog($"TCP 配置接收服务已启动，监听 0.0.0.0:{_port}");
        //    while (!_cts.Token.IsCancellationRequested)
        //    {
        //        var client = await listener.AcceptTcpClientAsync();
        //        _ = Task.Run(() => HandleClientAsync(client), _cts.Token);
        //        await Task.Delay(100, _cts.Token); // 避免过快接受连接导致资源占用过高
        //    }
    }

    public async Task InitConfigAsync()
    {
        var path = Path.Combine(AppDomain.CurrentDomain.BaseDirectory,"config", "数采配置文件.json");
        if (!File.Exists(path))
        {
            return;
        }
        string json = File.ReadAllText(path);
        _reloadCallback?.Invoke(json);

    }

    private async Task HandleClientAsync(TcpClient client)
    {
        using (client)
        {
            
            var stream = client.GetStream();
            var buffer = new byte[1024];
            var sb = new StringBuilder();

            // 读取直到换行
            while (true)
            {
                int bytesRead = await stream.ReadAsync(buffer, 0, buffer.Length);
                if (bytesRead == 0) break;
                string data = Encoding.UTF8.GetString(buffer, 0, bytesRead);
                if (_isCollectingProvider())
                {
                    logServices.Info($"TCP配置接收服务收到新配置，但当前正在采集数据，已忽略");
                    var bytes = Encoding.UTF8.GetBytes("ERROR: 当前正在采集数据，无法更新配置\n");
                    await stream.WriteAsync(bytes,0,bytes.Length);
                    await stream.FlushAsync();
                    continue; // 不关闭连接，继续等待下一个请求;
                }
                string fullData = data;
                string response;

                //Thread.Sleep(2000); // 模拟处理时间，实际可根据需要调整或去掉

                if (string.IsNullOrEmpty(fullData))
                {
                    response = "ERROR: 未收到路径\n";
                    await stream.WriteAsync(Encoding.UTF8.GetBytes(response), 0, Encoding.UTF8.GetBytes(response).Length);
                    await stream.FlushAsync();
                   // continue; // 不关闭连接，继续等待下一个请求;
                }
                else
                {
                    // 校验路径
                    if (!File.Exists(fullData))
                    {
                        response = "ERROR: 配置文件不存在\n";
                        await stream.WriteAsync(Encoding.UTF8.GetBytes(response), 0, Encoding.UTF8.GetBytes(response).Length);
                        await stream.FlushAsync();
                        //continue; // 不关闭连接，继续等待下一个请求;
                    }
                    else
                    {
                        try
                        {
                            await stream.WriteAsync(Encoding.UTF8.GetBytes("SUCCESS:配置更新成功"), 0, Encoding.UTF8.GetBytes("SUCCESS:配置更新成功").Length);
                            await stream.FlushAsync();
                          
                            string jsonStr = File.ReadAllText(data);
                            // 原子写入暂存文件
                            var path = Path.Combine(AppDomain.CurrentDomain.BaseDirectory,"config", _storageFile);
                            string dir = Path.GetDirectoryName(path);
                            if (!Directory.Exists(dir))
                                Directory.CreateDirectory(dir);
                            string tempFile = _storageFile;
                            File.WriteAllText(path, jsonStr);
                            // 触发热加载
                            _reloadCallback?.Invoke(jsonStr);

                            response = "SUCCESS:配置更新成功\n";
                           
                        }
                        catch (Exception ex)
                        {
                            logServices.Error($"TCP配置接收服务处理配置失败，路径:{fullData}，错误信息:{ex.ToString()}");
                            response = $"ERROR: 存储失败 - {ex.Message}\n";
                        }
                    }
                }

                logServices.Info($"TCP配置接收服务收到新配置，路径:{fullData}，结果:{response.Trim()}");
            }

           
        }
    }
}