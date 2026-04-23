using System;
using System.Linq;
using System.Net.Sockets;
using System.Reflection;

using DataAcquisitionLibrary;

using DC_0003.Core.Interfaces;

namespace DC_0003;

public static class Extensions
{
    public static T Get<T>(this object obj, string propertyName, BindingFlags bindingFlags = BindingFlags.Instance | BindingFlags.Public)
    {
        Type type = obj.GetType();
        PropertyInfo property = type.GetProperty(propertyName, bindingFlags);
        if (property != null && property.CanRead)
        {
            return (T)property.GetValue(obj);
        }
        if (property == null)
        {
            FieldInfo field = type.GetField(propertyName, bindingFlags);
            if (field != null)
            {
                return (T)field.GetValue(obj);
            }
        }
        return default(T);
    }

    public static object Set(this object obj, string propertyName, object value, BindingFlags bindingFlags = BindingFlags.Instance | BindingFlags.Public)
    {
        Type type = obj.GetType();
        PropertyInfo property = type.GetProperty(propertyName, bindingFlags);
        if (property != null && property.CanWrite)
        {
            property.SetValue(obj, value, null);
        }
        else if (property == null)
        {
            FieldInfo field = type.GetField(propertyName, bindingFlags);
            if (field != null)
            {
                field.SetValue(obj, value);
            }
        }
        return obj;
    }

    public static byte[] SendAndReadImmediately(this ISocketDataReader reader, byte[] payload, string ip = "", int port = 0)
    {
        Socket socket = reader.Get<Socket>("_serverSocket", BindingFlags.Instance | BindingFlags.NonPublic);
        if (string.IsNullOrWhiteSpace(ip) || port <= 0)
        {
            ip = reader.Get<string>("_lastConnectIp", BindingFlags.Instance | BindingFlags.NonPublic);
            port = reader.Get<int>("_lastConnectPort", BindingFlags.Instance | BindingFlags.NonPublic);
        }
        reader.Set("_lastConnectIp", ip, BindingFlags.Instance | BindingFlags.NonPublic);
        reader.Set("_lastConnectPort", port, BindingFlags.Instance | BindingFlags.NonPublic);
        byte[] array = Array.Empty<byte>();
        try
        {
            // 检查socket是否为null或已关闭
            if (socket == null || !socket.Connected)
            {
                // 释放旧的socket资源
                if (socket != null)
                {
                    socket.Dispose();
                }
                // 创建新的socket
                socket = new Socket(AddressFamily.InterNetwork, SocketType.Stream, ProtocolType.Tcp);
                socket.NoDelay = true;
                // 更新reader中的socket引用
                reader.Set("_serverSocket", socket, BindingFlags.Instance | BindingFlags.NonPublic);
                
                try
                {
                    socket.Connect(ip, port);
                }
                catch (SocketException e)
                {
                    LogServices.Instance.Warning($"[TCP/IP]连接到{ip}:{port}发送异常:{e.WrapExceptionInfo()}");
                }
            }
            if (socket.Connected)
            {
                socket.Send(payload);
                int num = 0;
                byte[] array2 = new byte[4096];
                num = socket.Receive(array2);
                Array.Resize(ref array, array.Length + num);
                Buffer.BlockCopy(array2, 0, array, array.Length - num, num);
            }
        }
        catch (Exception e2)
        {
            LogServices.Instance.Error("通过TCP/IP获取玛康数据异常:" + e2.WrapExceptionInfo());
        }
        return array;
    }

    public static object Invoke(this object obj, string methodName, BindingFlags bindingFlags, params object[] args)
    {
        MethodInfo method = obj.GetType().GetMethod(methodName, bindingFlags, null, args?.Where((object p) => p != null)?.Select((object p) => p.GetType()).ToArray() ?? Type.EmptyTypes, null);
        if (method != null)
        {
            return method.Invoke(obj, args);
        }
        return null;
    }
}

