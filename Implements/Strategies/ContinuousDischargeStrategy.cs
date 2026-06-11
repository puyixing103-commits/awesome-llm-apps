using System;

using DataAcquisitionLibrary;

using DC_0003.Core.Interfaces;

namespace DC_0003.Services.Implements.Strategies
{
    /// <summary>
    /// 持续轮询局放策略 — 在固定区间内三角波递增递减循环。
    ///
    /// 区间: 120 ~ 135（可通过构造函数自定义）
    /// 替换设备时，可创建新的 IDischargeStrategy 实现（如固定值、脉冲等）。
    /// </summary>
    public class ContinuousDischargeStrategy : IDischargeStrategy
    {
        private readonly LogServices _logServices = LogServices.Instance;

        // 局放范围配置
        private readonly double _min;
        private readonly double _max;

        // 运行时状态
        private DateTime _startTime;
        private double _currentValue;
        private bool _isAscending;
        private bool _initialized;

        private readonly object _lock = new object();
        private bool _disposed;

        public string Name => $"持续轮询局放策略({Min}~{Max})";

        // 暴露配置供外部读取
        public double Min => _min;
        public double Max => _max;

        public ContinuousDischargeStrategy(
            double min = 120.0,
            double max = 135.0,
            int stepsPerHalfCycle = 50)
        {
            _min = min;
            _max = max;
            StepsPerHalfCycle = stepsPerHalfCycle;
        }

        /// <summary>每半个周期（递增或递减）的步数</summary>
        public int StepsPerHalfCycle { get; set; } = 50;

        public void Initialize()
        {
            lock (_lock)
            {
                _startTime = DateTime.Now;
                _currentValue = _min;
                _isAscending = true;
                _initialized = true;
                _logServices.Info($"局放策略初始化: 范围 ({_min}~{_max})，持续循环轮询");
            }
        }

        public double GetCurrentValue()
        {
            lock (_lock)
            {
                if (!_initialized)
                {
                    Initialize();
                }

                return GetSequentialValue();
            }
        }

        private double GetSequentialValue()
        {
            double range = _max - _min;
            double step = range / StepsPerHalfCycle;

            if (_isAscending)
            {
                _currentValue += step;
                if (_currentValue >= _max)
                {
                    _currentValue = _max;
                    _isAscending = false;
                }
            }
            else
            {
                _currentValue -= step;
                if (_currentValue <= _min)
                {
                    _currentValue = _min;
                    _isAscending = true;
                }
            }

            if (_currentValue < _min) _currentValue = _min;
            if (_currentValue > _max) _currentValue = _max;

            return _currentValue;
        }

        public void Reset()
        {
            lock (_lock)
            {
                _startTime = DateTime.MinValue;
                _currentValue = _min;
                _isAscending = true;
                _initialized = false;
            }
        }

        public string GetCurrentPhaseDescription()
        {
            lock (_lock)
            {
                if (!_initialized) return "局放策略未启动";
                return $"局放采集中({_min}~{_max})";
            }
        }

        public void Dispose()
        {
            if (_disposed) return;
            _disposed = true;
            Reset();
        }
    }
}
