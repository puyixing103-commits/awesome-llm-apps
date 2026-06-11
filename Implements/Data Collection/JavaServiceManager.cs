using System;
using System.Collections.Generic;
using System.Linq;
using System.Net;
using System.Text;
using System.Threading.Tasks;

using DataAcquisitionLibrary;



using static DC_0003.Services.Implements.Data_Collection.Class2;
using System.IO;
using System.Diagnostics;
using Newtonsoft.Json;
public class JavaServiceManager
 {
     private Process _currentJavaProcess;
     private readonly object _processLock = new object();
     
     public Action<string> OnOutputLog;

     private LogServices _logServices = LogServices.Instance;
     public async Task RestartJavaServiceAsync(string java,string jar)
     {
         try
         {
             // 1. 停止现有服务
             await StopJavaServiceAsync(jar);

             // 2. 等待指定时间
             int waitSeconds = 2;
             await Task.Delay(waitSeconds * 1000);

             // 3. 启动新服务
             await StartJavaServiceAsync(java,jar);
         }
         catch (Exception ex)
         {
             //OutputLog($"重启 Java 服务失败: {ex.Message}");
             _logServices.Error(ex.ToString());
         }
     }

     public async Task StopJavaServiceAsync(string jar)
     {
         lock (_processLock)
         {
             if (_currentJavaProcess != null && !_currentJavaProcess.HasExited)
             {
                 //OutputLog("正在停止 Java 服务...");
                 OnOutputLog?.Invoke("正在停止 Java 服务...");
                
                 // 尝试正常关闭
                 _currentJavaProcess.CloseMainWindow();

                 // 等待进程退出（最多 10 秒）
                 if (!_currentJavaProcess.WaitForExit(5000))
                 {
                     // 如果没退出，强制杀死
                      OnOutputLog?.Invoke("Java 服务未响应，强制终止...");
                     _currentJavaProcess.Kill();
                     _currentJavaProcess.WaitForExit(1000);
                 }

                 _currentJavaProcess.Dispose();
                 _currentJavaProcess = null;
                  OnOutputLog?.Invoke("Java 服务已停止");
             }
         }

         // 额外：杀死所有相关 Java 进程（可选）
          KillJavaProcessesByJar(jar);
     }

     public async Task StartJavaServiceAsync(string java,string jar)
     {
         string javaPath = java; //@"C:\Program Files\Common Files\Oracle\Java\javapath";
         string jarPath = jar;// @"D:\myapp\encryptProject-0.0.1-SNAPSHOT.jar";

         //if (!File.Exists(jarPath))
         //{
         //   _logServices.Warning($"错误: JAR 文件不存在: {jarPath}");
         //    //OutputLog($"错误: JAR 文件不存在: {jarPath}");
         //    return;
         //}

         string javaExePath = Path.Combine(javaPath, "java.exe");
         if (!File.Exists(javaExePath))
         {
             javaExePath = "java"; // 使用系统 PATH 中的 Java
         }

         // 检查 Java 版本兼容性
         if (! CheckJavaVersion(javaExePath))
         {
             _logServices.Warning("错误: Java 版本不兼容，需要 Java 17");
             //OutputLog("错误: Java 版本不兼容，需要 Java 17");
             return;
         }

         //OutputLog("正在启动 Java 服务...");

         ProcessStartInfo startInfo = new ProcessStartInfo
         {
             FileName = javaExePath,
             Arguments = $"-jar \"{jarPath}\"",
             WorkingDirectory = Path.GetDirectoryName(jarPath),
             UseShellExecute = false,
             CreateNoWindow = true,
             RedirectStandardOutput = true,
             RedirectStandardError = true
         };

         Process javaProcess = new Process { StartInfo = startInfo };

         javaProcess.OutputDataReceived += (sender, e) =>
         {
             if (!string.IsNullOrEmpty(e.Data))
             {
                  //OnOutputLog?.Invoke($"[Java] {e.Data}");
             }
         };

         javaProcess.ErrorDataReceived += (sender, e) =>
         {
             if (!string.IsNullOrEmpty(e.Data))
             {
                _logServices.Error($"[Java Error] {e.Data}");
                  //OnOutputLog?.Invoke($"[Java Error] {e.Data}");
             }
         };

         lock (_processLock)
         {
             javaProcess.Start();
             _currentJavaProcess = javaProcess;
         }

         javaProcess.BeginOutputReadLine();
         javaProcess.BeginErrorReadLine();

         // 等待一下确认进程启动成功
         await Task.Delay(1000);

         if (!javaProcess.HasExited)
         {
             OnOutputLog?.Invoke($"Java 服务已启动，进程 ID: {javaProcess.Id}");
         }
         else
         {
              OnOutputLog?.Invoke("Java 服务启动失败，进程已退出");
         }
         _logServices.Info($"Java 服务启动详情:{JsonConvert.SerializeObject(javaProcess)}");
     }

     private bool CheckJavaVersion(string javaExePath)
     {
         try
         {
             ProcessStartInfo startInfo = new ProcessStartInfo
             {
                 FileName = javaExePath,
                 Arguments = "-version",
                 UseShellExecute = false,
                 CreateNoWindow = true,
                 RedirectStandardError = true
             };

             using (Process process = new Process { StartInfo = startInfo })
             {
                 process.Start();
                 string output = process.StandardError.ReadToEnd();
                 process.WaitForExit(); // 使用同步版本

                 return output.Contains("version \"17") || output.Contains("version \"21");
             }
         }
         catch
         {
             return false;
         }
     }

     private void KillJavaProcessesByJar(string jarPath) // 改为同步方法
     {
         try
         {
             string jarFileName = Path.GetFileName(jarPath);

             ProcessStartInfo startInfo = new ProcessStartInfo
             {
                 FileName = "taskkill",
                 Arguments = $"/F /IM java.exe /FI \"WINDOWTITLE eq {jarFileName}\"",
                 UseShellExecute = false,
                 CreateNoWindow = true,
                 RedirectStandardOutput = true
             };

             using (Process process = new Process { StartInfo = startInfo })
             {
                 process.Start();
                 process.WaitForExit(); // 使用同步版本
             }
         }
         catch (Exception ex)
         {
              OnOutputLog?.Invoke($"杀死进程失败: {ex.Message}");
              _logServices.Error($"杀死进程失败: {ex.ToString()}");
         }
     }

     // 获取服务状态
     public bool IsJavaServiceRunning()
     {
         lock (_processLock)
         {
             return _currentJavaProcess != null && !_currentJavaProcess.HasExited;
         }
     }
 }
