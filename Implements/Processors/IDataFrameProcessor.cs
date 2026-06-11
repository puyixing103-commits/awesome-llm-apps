using System;
using System.Collections.Generic;

namespace DC_0003.Services.Implements.Processors
{
    /// <summary>
    /// 数据帧处理器接口 — 定义原始帧数据如何转换为上传格式。
    /// 换设备或换数据格式时，只需实现此接口。
    /// </summary>
    public interface IDataFrameProcessor : IDisposable
    {
        /// <summary>处理器名称</summary>
        string Name { get; }

        /// <summary>
        /// 处理原始数据帧，返回序列化后的数据对象。
        /// </summary>
        /// <param name="frameData">原始帧数据（byte[]）</param>
        /// <param name="voltage">当前电压值</param>
        /// <param name="discharge">当前局放值</param>
        /// <returns>处理后的数据对象（通常是 IotDataMessageModelNew）</returns>
        object ProcessFrame(byte[] frameData, double voltage, double discharge);

        /// <summary>
        /// 批量处理多个数据帧
        /// </summary>
        IEnumerable<object> ProcessBatch(IEnumerable<byte[]> frames, Func<double> getVoltage, Func<double> getDischarge);
    }
}
