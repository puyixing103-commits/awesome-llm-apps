using System;
using System.Text.Json.Serialization;
namespace ApiModels
{
    /// <summary>
    /// 状态码枚举
    /// </summary>
    public enum Statuscode
    {
        Success = 200,
        Failed = 500,
        AlreadyRunning = 501,
        AlreadyStopped = 502,
        IllegalStateTransition = 503
    }
    /// <summary>
    /// 通用响应实体
    /// </summary>
    public class ApiResponse<T>
    {
        [JsonPropertyName("code")]
        public int code { get; set; }
        [JsonPropertyName("msg")]
        public string msg { get; set; }
        [JsonPropertyName("data")]
        public T data { get; set; }
    }
    /// <summary>
    /// 状态变更数据实体
    /// </summary>
    public class StateChangedata
    {
        [JsonPropertyName("currentState")]
        public string currentState { get; set; }
        [JsonPropertyName("previousState")]
        public string previousState { get; set; }
        [JsonPropertyName("allowed")]
        public bool allowed { get; set; }
        [JsonPropertyName("timestamp")]
        public string timestamp { get; set; }
        [JsonPropertyName("message")]
        public string message { get; set; }
    }
    /// <summary>
    /// 响应构造帮助类
    /// </summary>
    public static class ResponseBuilder
    {
        /// <summary>
        /// 启动成功
        /// </summary>
        public static ApiResponse<StateChangedata> StartSuccess(string previousState = "STOPPED")
        {
            return new ApiResponse<StateChangedata>
            {
                code = (int)Statuscode.Success,
                msg = "success",
                data = new StateChangedata
                {
                    currentState = "STARTING",
                    previousState = previousState,
                    allowed = true,
                    timestamp = DateTime.Now.ToString("yyyy-MM-ddTHH:mm:ss"),
                    message = "采集启动成功"
                }
            };
        }


        /// <summary>
        /// 启动成功
        /// </summary>
        public static ApiResponse<object> Success(object s)
        {
            return new ApiResponse<object>
            {
                code = (int)Statuscode.Success,
                msg = "success",
                data = s
            };
        }



        public static ApiResponse<StateChangedata> StopSuccess(string previousState = "STARTING")
        {
            return new ApiResponse<StateChangedata>
            {
                code = (int)Statuscode.Success,
                msg = "success",
                data = new StateChangedata
                {
                    currentState = "STOPPED",
                    previousState = previousState,
                    allowed = true,
                    timestamp = DateTime.Now.ToString("yyyy-MM-ddTHH:mm:ss"),
                    message = "采集关闭成功"
                }
            };
        }
        /// <summary>
        /// 重复启动
        /// </summary>
        public static ApiResponse<StateChangedata> AlreadyRunning()
        {
            return new ApiResponse<StateChangedata>
            {
                code = (int)Statuscode.AlreadyRunning,
                msg = "采集已在运行中",
                data = new StateChangedata
                {
                    currentState = "STARTING",
                    previousState = "STARTING",
                    allowed = false,
                    timestamp = DateTime.Now.ToString("yyyy-MM-ddTHH:mm:ss"),
                    message = "禁止重复启动"
                }
            };
        }
        /// <summary>
        /// 重复停止
        /// </summary>
        public static ApiResponse<StateChangedata> AlreadyStopped()
        {
            return new ApiResponse<StateChangedata>
            {
                code = (int)Statuscode.AlreadyStopped,
                msg = "采集已停止",
                data = new StateChangedata
                {
                    currentState = "STOPPED",
                    previousState = "STOPPED",
                    allowed = false,
                    timestamp = DateTime.Now.ToString("yyyy-MM-ddTHH:mm:ss"),
                    message = "禁止重复停止"
                }
            };
        }
        /// <summary>
        /// 非法状态切换
        /// </summary>
        public static ApiResponse<StateChangedata> IllegalTransition(string currentState)
        {
            return new ApiResponse<StateChangedata>
            {
                code = (int)Statuscode.IllegalStateTransition,
                msg = "非法状态切换",
                data = new StateChangedata
                {
                    currentState = currentState,
                    previousState = currentState,
                    allowed = false,
                    timestamp = DateTime.Now.ToString("yyyy-MM-ddTHH:mm:ss"),
                    message = "当前状态不允许执行该操作"
                }
            };
        }


        public static ApiResponse<StateChangedata> Error()
        {
            return new ApiResponse<StateChangedata>
            {
                code = (int)Statuscode.Failed,
                msg = "异常",
                data = new StateChangedata
                { 
                    allowed = false,
                    timestamp = DateTime.Now.ToString("yyyy-MM-ddTHH:mm:ss"),
                    message = "异常"
                }
            };
        }
    }
}