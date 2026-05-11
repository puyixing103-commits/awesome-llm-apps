using System;
using System.Collections.Generic;
using System.Linq;
using System.Net;
using System.Text;
using System.Threading.Tasks;

using DataAcquisitionLibrary;



namespace DC_0003.Services.Implements.Data_Collection
{
    public class Class2
    {
        /// <summary>
        /// 数据上传器接口
        /// </summary>
        public interface IUploader
        {
            //Task<bool> UploadAsync(DataMessage message,string topic);

            Task<bool> BatchUploadAsync(string message,string topic);
        }

        /// <summary>
        /// 心跳发送器接口
        /// </summary>
        public interface IHeartbeatSender
        {
            Task<bool> SendAsync(string message);
        }

        /// <summary>
        /// 密钥服务客户端接口
        /// </summary>
        public interface IKeyServiceClient
        {
            //public Task<KeyResponse> GetKeyAsync(long time,string macAddress, IotDataMessageModel iotDataMessageModel);

            public Task<KeyResponse> GetKeyAsync(string json);
        }
        public class ResponseModel
        {
            public int code { get; set; }
            public string message { get; set; }
            public KeyResponse data { get; set; }
        }
        public class KeyResponse
        {
         public int total { get; set; }
            public int success { get; set; }

            public int failed { get; set; }

            public List<KeyResponseDetail> items { get; set; }
        }


        public class KeyResponseDetail
        {
            public string key { get; set; }
            public string cipher { get; set; }
            public string plain { get; set; }

            public DateTime[] time_range { get; set; }

            public string cipherSign { get; set; }

            public string plainSign { get; set; }
            // public string ivBase64 { get; set; }//解密时要将这个还原成byte数组
        }
    }
}
