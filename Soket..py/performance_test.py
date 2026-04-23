import socket
import time

HOST = '127.0.0.1'
PORT = 8654

# 构建测试数据帧
TEST_FRAME = bytes.fromhex(
    "EAEAEAEA"
    "01000001C0A8100C020100000001E0000000600000000000000000000040290E7E000000000000000000008E1F"
    "AEAEAEAE"
)

print("开始性能测试...")

# 建立连接
conn = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
conn.connect((HOST, PORT))
conn.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)

print("连接成功")

# 测试1ms内发送的数据帧数量
start_time = time.time()
end_time = start_time + 0.001  # 1ms

frames_sent = 0

while time.time() < end_time:
    conn.send(TEST_FRAME)
    frames_sent += 1

print(f"1ms内发送了 {frames_sent} 个数据帧")

# 接收所有响应
frames_received = 0
start_recv_time = time.time()

while time.time() - start_recv_time < 1.0:  # 等待1秒接收所有响应
    try:
        data = conn.recv(65536)
        if not data:
            break
        # 计算响应帧数
        frames_received += data.count(b'EAEAEAEA')
    except BlockingIOError:
        break

print(f"接收到 {frames_received} 个响应帧")
print(f"服务器1ms内处理了 {frames_received} 个数据帧")

conn.close()
print("测试完成")
