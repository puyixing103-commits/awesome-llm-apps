using System;
using System.Collections.Generic;
using System.Linq;

using DataAcquisitionLibrary;
using DataAcquisitionLibrary.Utils;

using DC_0003.Core.Interfaces;

namespace DC_0003.Services.Implements.Processors
{
    /// <summary>
    /// EA 帧数据处理器 — 处理 EA 协议帧格式的数据。
    /// 帧结构: EA EA EA EA ... AE AE AE AE
    /// 
    /// 替换设备时，可创建新的 IDataFrameProcessor 实现（如 Modbus 处理器、自定义协议处理器等）。
    /// </summary>
    public class EAFrameDataProcessor : IDataFrameProcessor
    {
        private readonly LogServices _logServices = LogServices.Instance;
        private bool _disposed;

        public string Name => "EA 帧数据处理器";

        public EAFrameDataProcessor()
        {
        }

        public object ProcessFrame(byte[] frameData, double voltage, double discharge)
        {
            if (frameData == null || frameData.Length == 0)
                return null;

            try
            {
                // 将 byte[] 转换为十六进制字符串
                string hexString = StringUtil.ByteArrayToHexString(frameData);

                // 解析 EA 帧
                var frames = ParseEAFrames(hexString);
                if (frames.Count == 0)
                {
                    _logServices.Warning($"无法解析 EA 帧，数据长度: {frameData.Length}");
                    return null;
                }

                // 提取十进制数值
                var decimals = ExtractDecimals(frames[0]);
                if (decimals == null || decimals.Count == 0)
                {
                    _logServices.Warning("无法从帧中提取十进制数值");
                    return null;
                }

                // 构建上传数据模型
                var result = new IotDataMessageModelNew
                {
                    timestamp = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds(),
                    timestampType = "MILLISECOND",
                    data = new Dictionary<string, string>
                    {
                        ["voltage"] = voltage.ToString("F2"),
                        ["discharge"] = discharge.ToString("F2"),
                        ["decimals"] = string.Join(",", decimals),
                        ["frameHex"] = hexString
                    }
                };

                return result;
            }
            catch (Exception ex)
            {
                _logServices.Error($"处理 EA 帧失败: {ex.Message}\n{ex}");
                return null;
            }
        }

        public IEnumerable<object> ProcessBatch(IEnumerable<byte[]> frames, Func<double> getVoltage, Func<double> getDischarge)
        {
            var results = new List<object>();

            foreach (var frameData in frames)
            {
                var voltage = getVoltage();
                var discharge = getDischarge();
                var processed = ProcessFrame(frameData, voltage, discharge);

                if (processed != null)
                    results.Add(processed);
            }

            return results;
        }

        /// <summary>
        /// 解析 EA 帧：EA EA EA EA ... AE AE AE AE
        /// </summary>
        private List<string> ParseEAFrames(string hexString)
        {
            var frames = new List<string>();
            if (string.IsNullOrEmpty(hexString))
                return frames;

            // 查找所有 EA EA EA EA 开头的位置
            int startIndex = 0;
            while (startIndex < hexString.Length)
            {
                int frameStart = hexString.IndexOf("EAEAEAEA", startIndex, StringComparison.OrdinalIgnoreCase);
                if (frameStart < 0)
                    break;

                // 查找 AE AE AE AE 结束位置
                int frameEnd = hexString.IndexOf("AEAEAEAE", frameStart, StringComparison.OrdinalIgnoreCase);
                if (frameEnd < 0)
                    break;

                // 提取完整帧（包含尾部 AE AE AE AE）
                int frameLength = frameEnd + 8 - frameStart; // 8 = "AEAEAEAE".Length
                if (frameStart + frameLength <= hexString.Length)
                {
                    string frame = hexString.Substring(frameStart, frameLength);
                    frames.Add(frame);
                }

                startIndex = frameEnd + 8;
            }

            return frames;
        }

        /// <summary>
        /// 从 EA 帧中提取十进制数值
        /// </summary>
        private List<decimal> ExtractDecimals(string frame)
        {
            var decimals = new List<decimal>();

            if (string.IsNullOrEmpty(frame) || frame.Length < 24)
                return decimals;

            try
            {
                // 跳过头部 EA EA EA EA (8 字符)
                int dataStart = 8;

                // 查找尾部 AE AE AE AE
                int dataEnd = frame.IndexOf("AEAEAEAE", dataStart, StringComparison.OrdinalIgnoreCase);
                if (dataEnd < 0)
                    dataEnd = frame.Length;

                // 提取数据部分
                string dataPart = frame.Substring(dataStart, dataEnd - dataStart);

                // 每 4 个字符（2 字节）转换为一个十进制数
                for (int i = 0; i < dataPart.Length; i += 4)
                {
                    if (i + 4 > dataPart.Length)
                        break;

                    string hex = dataPart.Substring(i, 4);
                    if (ushort.TryParse(hex, System.Globalization.NumberStyles.HexNumber, null, out ushort value))
                    {
                        decimals.Add(value);
                    }
                }
            }
            catch (Exception ex)
            {
                _logServices.Error($"提取十进制数值失败: {ex.Message}\n{ex}");
            }

            return decimals;
        }

        public void Dispose()
        {
            if (_disposed) return;
            _disposed = true;
        }
    }

    /// <summary>
    /// IoT 数据消息模型
    /// </summary>
    public class IotDataMessageModelNew
    {
        public long timestamp { get; set; }
        public string timestampType { get; set; }
        public Dictionary<string, string> data { get; set; } = new Dictionary<string, string>();
    }
}
