using System;
using System.IO;
using System.Linq;
using System.Net;
using System.Net.Sockets;
using System.Runtime.InteropServices.ComTypes;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

using DataAcquisitionLibrary;

using DC_0003.Models;

using Newtonsoft.Json;

public class TcpConfigReceiver
{
    private readonly int _port;
    private readonly string _storageFile;
    public event Action<string> _reloadCallback; // 可选：热加载回调
    private CancellationTokenSource _cts;
    private event Action _closeCallback;
    private LogServices logServices = LogServices.Instance;

    public TcpConfigReceiver(int port, string storageFile, Action<string> reloadCallback = null, Action _ctscallback = null)
    {
        _port = port;
        _storageFile = storageFile;
        _reloadCallback = reloadCallback;
        _cts = DataAcquisitionManager._cts;
        _=InitConfigAsync();
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


            // 仅处理 GET 请求的 /api/config
            if (request.HttpMethod != "GET" || request.Url.AbsolutePath.ToLowerInvariant() != "/api/config")
            {
                string errorResult = request.HttpMethod != "GET"
                    ? "{\"code\":405,\"msg\":\"仅支持GET请求\"}"
                    : "{\"code\":404,\"msg\":\"接口不存在\"}";

                logServices.Warning(errorResult);

                byte[] errorBuffer = Encoding.UTF8.GetBytes(errorResult);
                response.ContentLength64 = errorBuffer.Length;
                response.OutputStream.Write(errorBuffer, 0, errorBuffer.Length);
                response.Close();
                return;
            }

            // 处理 /api/config 请求
            string result = HandleConfigRequest();

            byte[] buffer = Encoding.UTF8.GetBytes(result);
            response.ContentLength64 = buffer.Length;
            response.OutputStream.Write(buffer, 0, buffer.Length);
        }
        catch (Exception ex)
        {
            logServices?.Error($"处理请求异常: {ex}");
            try
            {
                string errorJson = "{\"code\":500,\"msg\":\"请检查输入配置项是否正确!\"}";
                byte[] errorBuffer = Encoding.UTF8.GetBytes(errorJson);
                response.ContentLength64 = errorBuffer.Length;
                response.OutputStream.Write(errorBuffer, 0, errorBuffer.Length);
            }
            catch { }
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
    private string HandleConfigRequest()
    {
        // 1. 检查采集状态
        if (DataAcquisitionManager._isDataCollecting)
        {
            logServices.Warning("TCP配置接收服务收到新配置请求，但当前正在采集数据，已拒绝");
            DataAcquisitionManager.EnqueueLog("TCP配置接收服务收到新配置请求，但当前正在采集数据，已拒绝");
            return "{\"code\":409,\"msg\":\"当前正在采集数据，无法更新配置\"}";
        }

        // 2. 写死的配置文件路径
        
        string configDir = _iniFileManager.GetValue("NEWDATACOLLECTION", "FILEURL");//@"C:\Users\shucai\Desktop\config";
        string fullData = GetLatestJsonFile(configDir);
        // 3. 校验文件是否存在
        if (!File.Exists(fullData))
        {
            logServices.Warning($"配置文件不存在: {fullData}");
            DataAcquisitionManager.EnqueueLog($"配置文件不存在: {fullData}");
            return $"{{\"code\":404,\"msg\":\"配置文件不存在\",\"data\":{{\"filePath\":\"{fullData}\"}}}}";
        }

        // 4. 处理配置文件
        try
        {
            string jsonStr = File.ReadAllText(fullData, Encoding.UTF8);

            // 保存到应用目录
            var path = Path.Combine(AppDomain.CurrentDomain.BaseDirectory, "config", _storageFile);
            string dir = Path.GetDirectoryName(path);
            if (!Directory.Exists(dir))
                Directory.CreateDirectory(dir);

            File.WriteAllText(path, jsonStr, Encoding.UTF8);

            // 触发热加载
            _reloadCallback?.Invoke(jsonStr);


            DataAcquisitionManager.EnqueueLog($"TCP配置接收服务成功更新配置，路径:{fullData}");
            logServices.Info($"TCP配置接收服务成功更新配置，路径:{fullData}");

            return $"{{\"code\":200,\"msg\":\"配置文件更新成功\",\"data\":{{\"filePath\":\"{fullData.Replace("\\", "\\\\")}\"}}}}";
        }
        catch (Exception ex)
        {
            logServices?.Error($"处理配置文件失败，路径:{fullData}，错误:{ex}");
            return "{\"code\":500,\"msg\":\"处理配置文件失败\"}";
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
            DataAcquisitionManager.EnqueueLog($"HTTP API 服务已启动：http://localhost:{_port}/");
            logServices.Info($"HTTP API 服务已启动：http://localhost:{_port}/");
            //OnLogOutput?.Invoke($"HTTP API 服务已启动：http://localhost:{_port}/");
            _serverThread = new Thread(ListenLoop);
            _serverThread.IsBackground = true;
            _serverThread.Start();
        }
        catch (Exception e)
        {
            DataAcquisitionManager.EnqueueLog($"HTTP API 启动失败：{e.Message}");
            //OnLogOutput?.Invoke($"HTTP API 启动失败：{e.Message}");
            throw;
        }
        //var listener = new TcpListener(IPAddress.Any, _port); // 监听所有网络接口
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
                if (DataAcquisitionManager._isDataCollecting)
                {
                    DataAcquisitionManager.EnqueueLog($"TCP配置接收服务收到新配置，但当前正在采集数据，已忽略");
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

                DataAcquisitionManager.EnqueueLog($"TCP配置接收服务收到新配置，路径:{fullData}，结果:{response.Trim()}");
            }

           
        }
    }
}