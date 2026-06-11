using System;
using System.IO;
using System.Threading.Tasks;

using DataAcquisitionLibrary;

using DC_0003.Core.Interfaces;
using DC_0003.Models;

using Newtonsoft.Json;

namespace DC_0003.Services.Infrastructure
{
    /// <summary>
    /// 配置管理器 — 统一配置加载/访问层（模板代码）。
    /// 从 JSON 字符串反序列化 AppConfig，同时读取 INI 配置补充信息。
    /// </summary>
    public class ConfigManager : IConfigProvider
    {
        private AppConfig _appConfig;
        private readonly IniFileManager _iniFileManager;
        private readonly LogServices _logServices = LogServices.Instance;

        public AppConfig AppConfig => _appConfig;
        public bool IsLoaded => _appConfig != null;

        public ConfigManager()
        {
            _iniFileManager = new IniFileManager(
                Path.Combine(AppDomain.CurrentDomain.BaseDirectory, "config", "Config.ini"));
        }

        /// <summary>从 JSON 字符串加载主配置</summary>
        public void LoadFromJson(string jsonConfig)
        {
            if (string.IsNullOrWhiteSpace(jsonConfig))
            {
                _logServices.Warning("配置 JSON 为空，无法加载");
                return;
            }

            try
            {
                var config = JsonConvert.DeserializeObject<AppConfig>(jsonConfig);
                if (config == null)
                {
                    _logServices.Warning("配置反序列化结果为 null，无法加载");
                    return;
                }
                _appConfig = config;
                _logServices.Info("配置加载成功");
            }
            catch (JsonException ex)
            {
                _logServices.Error($"配置 JSON 格式错误: {ex.Message}");
            }
            catch (Exception ex)
            {
                _logServices.Error($"配置加载异常: {ex.Message}");
            }
        }

        /// <summary>获取 INI 文件中的值</summary>
        public string GetIniValue(string section, string key)
        {
            return _iniFileManager.GetValue(section, key);
        }

        // --- IConfigProvider 实现 ---

        public T GetConfig<T>() where T : class, new()
        {
            if (_appConfig is T t) return t;
            return null;
        }

        public string GetValue(string key, string defaultValue = "")
        {
            // 先从 INI 中找
            var iniVal = _iniFileManager.GetValue("NEWDATACOLLECTION", key);
            if (!string.IsNullOrEmpty(iniVal)) return iniVal;

            return defaultValue;
        }

        public int GetIntValue(string key, int defaultValue = 0)
        {
            var val = GetValue(key, defaultValue.ToString());
            return int.TryParse(val, out int result) ? result : defaultValue;
        }

        public bool GetBoolValue(string key, bool defaultValue = false)
        {
            var val = GetValue(key, defaultValue.ToString());
            return bool.TryParse(val, out bool result) ? result : defaultValue;
        }

        public Task ReloadAsync()
        {
            _logServices.Info("配置重新加载（无操作，需重新调用 LoadFromJson）");
            return Task.CompletedTask;
        }

        public Task SaveAsync()
        {
            _logServices.Info("配置保存（当前未实现持久化）");
            return Task.CompletedTask;
        }
    }
}
