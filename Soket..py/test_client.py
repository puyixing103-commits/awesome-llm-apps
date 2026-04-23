import socket
import time
import select

HOST = '127.0.0.1'
PORT = 8654

# 构建测试数据帧，使用与服务器响应相同的格式
FRAME_HEADER = bytes([0xEA, 0xEA, 0xEA, 0xEA])
FRAME_FOOTER = bytes([0xAE, 0xAE, 0xAE, 0xAE])
# 使用与服务器响应相同的数据格式
TEST_FRAME = bytes.fromhex(
    "EAEAEAEA"
    "01000001C0A8100C020100000001E0000000600000000000000000000040290E7E000000000000000000008E1F"
    "AEAEAEAE"
)

# 测试函数
def test_performance():
    print("开始测试服务器性能...")
    
    try:
        # 建立连接
        conn = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        conn.connect((HOST, PORT))
        conn.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
        conn.setblocking(False)
        
        print("连接成功")
        
        # 测试1ms内发送的数据帧数量
        start_time = time.time()
        end_time = start_time + 0.001  # 1ms
        
        sent_frames = 0
        
        # 发送数据
        while time.time() < end_time:
            try:
                conn.send(TEST_FRAME)
                sent_frames += 1
            except BlockingIOError:
                break
        
        print(f"1ms内发送帧数: {sent_frames}")
        
        # 接收响应
        received_frames = 0
        start_recv_time = time.time()
        
        while time.time() - start_recv_time < 0.5:  # 等待0.5秒接收响应
            ready, _, _ = select.select([conn], [], [], 0.1)
            if not ready:
                continue
            
            try:
                data = conn.recv(65536)
                if not data:
                    break
                # 计算响应帧数
                received_frames += data.count(b'EAEAEAEA')
            except BlockingIOError:
                break
        
        print(f"接收到的响应帧数: {received_frames}")
        
    except Exception as e:
        print(f"测试失败: {e}")
    finally:
        if 'conn' in locals():
            conn.close()
    
    print("测试完成")

if __name__ == "__main__":
    test_performance()
