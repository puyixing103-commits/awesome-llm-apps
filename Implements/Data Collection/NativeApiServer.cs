using System;
using System.IO;
using System.Net;
using System.Text;
using System.Threading;

using DataAcquisitionLibrary;

using DC_0003.Models;

using Newtonsoft.Json;

public class NativeApiServer
{
    private HttpListener _listener;
    private Thread _serverThread;
    private readonly int _port;

    public event Action<string> OnStartCommand;
    public event Action<string> OnStopCommand;
    public event Action<string> OnLogOutput;

    private readonly ILogServices logServices = LogServices.Instance;

    public NativeApiServer(int port = 5002)
    {
        _port = port;
    }

    public void Start()
    {
        try
        {
            _listener = new HttpListener();
            _listener.Prefixes.Add($"http://+:{_port}/");
            _listener.Start();
            OnLogOutput?.Invoke($"HTTP API 服务已启动：http://localhost:{_port}/");
            logServices.Info($"HTTP API 服务已启动：http://localhost:{_port}/");
            _serverThread = new Thread(ListenLoop);
            _serverThread.IsBackground = true;
            _serverThread.Start();
        }
        catch (Exception e)
        {
            OnLogOutput?.Invoke($"HTTP API 启动失败：{e.Message}");
            throw;
        }
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

    private void ProcessRequest(HttpListenerContext context)
    {
        try
        {
            var request = context.Request;
            var response = context.Response;
            // ========== 添加 CORS 头 ==========
            // 允许所有来源（生产环境请指定具体域名）
            response.Headers.Add("Access-Control-Allow-Origin", "*");
            response.Headers.Add("Access-Control-Allow-Methods", "GET, POST, PUT, DELETE, OPTIONS");
            response.Headers.Add("Access-Control-Allow-Headers", "Content-Type, Accept, X-Requested-With");
            response.Headers.Add("Access-Control-Allow-Credentials", "true");

            // 处理预检请求（OPTIONS）
            if (request.HttpMethod == "OPTIONS")
            {
                response.StatusCode = 200;
                response.Close();
                return;
            }
            response.ContentType = "application/json";

            if (request.HttpMethod == "POST")
            {
                string json = new StreamReader(request.InputStream).ReadToEnd();
                string result = null;

                MQTTMessage mqttMessage = JsonConvert.DeserializeObject<MQTTMessage>(json);
                if (mqttMessage == null) return;
                logServices.Info($"HTPP 接收到消息: {json},请求头:{request.Url.AbsolutePath}");

                switch (request.Url.AbsolutePath)
                {
                    case "/api/v1/activity":
                        switch (mqttMessage.commandCode)
                        {
                            case "SEND_ENTITY_IDS":
                            case "COMMAND_START":

                                if (DataAcquisitionManager._isDataCollecting)
                                {
                                    logServices.Warning("HTTP请求但当前正在采集数据，已拒绝");
                                    
                                    result = "{\"code\":409,\"msg\":\"当前正在采集数据中,请稍后再试\"}";

                                    break;
                                }
                                OnLogOutput?.Invoke($"HTPP 接收到开始采集指令:{json}");
                                OnStartCommand?.Invoke(json);
                                result = "{\"code\":200,\"msg\":\"启动命令已执行\"}";
                                break;

                            case "SEND_STOP_ENTITY_IDS":
                            case "COMMAND_COMPLETE":
                                OnLogOutput?.Invoke($"HTPP 接收到停止采集指令:{json}");
                                OnStopCommand?.Invoke(json);
                                result = "{\"code\":200,\"msg\":\"停止命令已执行\"}";
                                break;

                            default:
                                result = "{\"code\":404,\"msg\":\"HTPP 接收到未知指令\"}";
                                OnLogOutput?.Invoke($"HTPP 接收到未知指令:{mqttMessage.commandCode}");
                                break;
                        }
                        break;

                    case "/api/stop":
                        OnLogOutput?.Invoke($"收到停止命令：{json}");
                       // OnStopCommand?.Invoke(json);
                        result = "{\"code\":200,\"msg\":\"停止命令已执行\"}";
                        break;

                    case "/api/upload":
                        OnLogOutput?.Invoke($"收到上传数据：{json}");
                        result = "{\"code\":200,\"msg\":\"上传成功\",\"data\":" + json + "}";
                        break;

                    case "/api/datacollection/server/dispatch-command":
                        OnLogOutput?.Invoke($"收到上传数据：{json}");
                        //result = "{\"code\":200,\"msg\":\"上传成功\",\"data\":" + json + "}";
                        break;

                    default:
                        result = result??"{\"code\":404,\"msg\":\"接口不存在\"}";
                        break;
                }

                //{ "commandSerial":"1MBO97FNTI68VXDLJGCS4UPZQW50YKAH","timestamp":"111111111111111","roomCode":"SYZ2","deviceCode":"SY253","commandCode":"COMMAND_COMPLETE","properties":null}

                byte[] buffer = Encoding.UTF8.GetBytes(result);
                response.ContentLength64 = buffer.Length;
                response.OutputStream.Write(buffer, 0, buffer.Length);
            }
            else
            {
                byte[] buffer = Encoding.UTF8.GetBytes("{\"code\":405,\"msg\":\"仅支持POST请求\"}");
                response.ContentLength64 = buffer.Length;
                response.OutputStream.Write(buffer, 0, buffer.Length);
            }

            response.Close();
        }
        catch (Exception e)
        {
            logServices.Error($"处理请求异常 → 信息:{e.Message} | 堆栈:{e.ToString()}");
            OnLogOutput?.Invoke($"处理请求异常：{e.Message}");
        }
    }

    public void Stop()
    {
        _listener?.Stop();
        _listener?.Close();
        OnLogOutput?.Invoke("HTTP API 服务已停止");
    }
}
