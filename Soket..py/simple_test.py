import socket
import time
import sys

HOST = '127.0.0.1'
PORT = 8654

# 构建测试数据帧
TEST_FRAME = bytes.fromhex(
    "EAEAEAEA"
    "01000001C0A8100C020100000001E0000000600000000000000000000040290E7E000000000000000000008E1F"
    "AEAEAEAE"
)

print("开始测试...", flush=True)
print(f"尝试连接到 {HOST}:{PORT}", flush=True)

# 建立连接
try:
    conn = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    print("创建socket成功", flush=True)
    
    conn.connect((HOST, PORT))
    print("连接成功", flush=True)
    
    conn.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
    print("设置TCP_NODELAY成功", flush=True)
    
    # 测试1ms内发送的数据帧数量
    start_time = time.time()
    end_time = start_time + 0.001  # 1ms
    
    count = 0
    
    print("开始发送数据帧...", flush=True)
    while time.time() < end_time:
        conn.send(TEST_FRAME)
        count += 1
    
    print(f"1ms内发送了 {count} 个数据帧", flush=True)
    
    # 接收响应
    print("等待响应...", flush=True)
    response = conn.recv(65536)
    print(f"接收到 {len(response)} 字节的响应", flush=True)
    print(f"响应中包含 {response.count(b'EAEAEAEA')} 个数据帧", flush=True)
    print(f"响应内容: {response.hex()}", flush=True)
    
    conn.close()
    print("连接关闭", flush=True)
except Exception as e:
    print(f"错误: {e}", flush=True)
    import traceback
    traceback.print_exc()

print("测试完成", flush=True)
