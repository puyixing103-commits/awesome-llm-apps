using System;

using DataAcquisitionLibrary;

using DC_0003.Core.Interfaces;

namespace DC_0003.Services.Implements.Strategies
{
    /// <summary>
    /// 两阶段电压采集策略 — 每5分钟在两个电压区间之间切换，区间内三角波递增递减。
    ///
    /// 阶段1 (0-5min): 342.54 ~ 418.66
    /// 阶段2 (5-10min): 693 ~ 847
    /// 阶段1 (10-15min): 循环回到阶段1
    ///
    /// 替换设备时，可创建新的 IVoltageStrategy 实现（如固定电压、随机电压等）。
    /// </summary>
    public class TwoPhaseVoltageStrategy : IVoltageStrategy
    {
        private readonly LogServices _logServices = LogServices.Instance;

        // 电压范围配置（通过构造函数可自定义）
        private readonly double _phase1Min;
        private readonly double _phase1Max;
        private readonly double _phase2Min;
        private readonly double _phase2Max;
        private readonly int _phaseDurationMinutes;

        // 运行时状态
        private DateTime _startTime;
        private int _currentPhase; // 1 或 2
        private double _currentValue;
        private bool _isAscending;

        private readonly object _lock = new object();
        private bool _disposed;

        public string Name => $"两阶段电压策略({Phase1Min}~{Phase1Max}, {Phase2Min}~{Phase2Max})";

        // 暴露配置供外部读取
        public double Phase1Min => _phase1Min;
        public double Phase1Max => _phase1Max;
        public double Phase2Min => _phase2Min;
        public double Phase2Max => _phase2Max;

        public TwoPhaseVoltageStrategy(
            double phase1Min = 342.54,
            double phase1Max = 418.66,
            double phase2Min = 693.0,
            double phase2Max = 847.0,
            int phaseDurationMinutes = 5,
            int stepsPerHalfCycle = 50)
        {
            _phase1Min = phase1Min;
            _phase1Max = phase1Max;
            _phase2Min = phase2Min;
            _phase2Max = phase2Max;
            _phaseDurationMinutes = phaseDurationMinutes;
            StepsPerHalfCycle = stepsPerHalfCycle;
        }

        /// <summary>每半个周期（递增或递减）的步数</summary>
        public int StepsPerHalfCycle { get; set; } = 50;

        public void Initialize()
        {
            lock (_lock)
            {
                _startTime = DateTime.Now;
                _currentPhase = 1;
                _currentValue = _phase1Min;
                _isAscending = true;
                _logServices.Info($"电压策略初始化: 阶段1开始 ({_phase1Min}~{_phase1Max})");
            }
        }

        public double GetCurrentValue()
        {
            lock (_lock)
            {
                if (_startTime == DateTime.MinValue)
                    return 0;

                TimeSpan elapsed = DateTime.Now - _startTime;
                int cycleNumber = (int)(elapsed.TotalMinutes / _phaseDurationMinutes);
                int phaseInCycle = cycleNumber % 2 == 0 ? 1 : 2;

                // 阶段切换检测
                if (phaseInCycle != _currentPhase)
                {
                    _currentPhase = phaseInCycle;
                    double min = phaseInCycle == 1 ? _phase1Min : _phase2Min;
                    double max = phaseInCycle == 1 ? _phase1Max : _phase2Max;

                    if (_currentValue < min || _currentValue > max)
                    {
                        _currentValue = min;
                        _isAscending = true;
                    }
                    _logServices.Info($"电压切换到阶段{_currentPhase}: ({min}~{max})");
                }

                double phaseMin = _currentPhase == 1 ? _phase1Min : _phase2Min;
                double phaseMax = _currentPhase == 1 ? _phase1Max : _phase2Max;

                return GetSequentialValue(phaseMin, phaseMax);
            }
        }

        private double GetSequentialValue(double min, double max)
        {
            double range = max - min;
            double step = range / StepsPerHalfCycle;

            if (_isAscending)
            {
                _currentValue += step;
                if (_currentValue >= max)
                {
                    _currentValue = max;
                    _isAscending = false;
                }
            }
            else
            {
                _currentValue -= step;
                if (_currentValue <= min)
                {
                    _currentValue = min;
                    _isAscending = true;
                }
            }

            if (_currentValue < min) _currentValue = min;
            if (_currentValue > max) _currentValue = max;

            return _currentValue;
        }

        public void Reset()
        {
            lock (_lock)
            {
                _startTime = DateTime.MinValue;
                _currentPhase = 0;
                _currentValue = 0;
                _isAscending = true;
            }
        }

        public string GetCurrentPhaseDescription()
        {
            lock (_lock)
            {
                if (_startTime == DateTime.MinValue) return "电压策略未启动";
                return $"电压阶段{_currentPhase}({_phase1Min}~{_phase1Max}/{_phase2Min}~{_phase2Max})";
            }
        }

        public void Dispose()
        {
            if (_disposed) return;
            _disposed = true;
            Reset();
        }

        public string GetCurrentPhaseName()
        {
            throw new NotImplementedException();
        }
    }
}
