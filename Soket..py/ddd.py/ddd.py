import socket
import time
import threading
import select
from datetime import datetime

HOST = '0.0.0.0'
PORT = 8654

RESPONSE = bytes.fromhex(
    "EAEAEAEA01000001C0A8100C02010000001E0000000600000000000000000000000040290E7E00000000000000008E1FAEAEAEAE"
)

FRAME_HEADER = bytes([0xEA, 0xEA, 0xEA, 0xEA])
FRAME_FOOTER = bytes([0xAE, 0xAE, 0xAE, 0xAE])

# ─── 全局统计 ────────────────────────────────────────────────────
_lock = threading.Lock()
_stats = {
    'recv_bytes':  0,   # 本秒收到字节数
    'recv_frames': 0,   # 本秒收到完整帧数
    'send_frames': 0,   # 本秒发送帧数
    'send_bytes':  0,   # 本秒发送字节数
    'clients':     0,   # 当前在线连接数
}

def _add(**kwargs):
    with _lock:
        for k, v in kwargs.items():
            _stats[k] += v

def _now():
    return datetime.now().strftime('%Y-%m-%d %H:%M:%S.%f')[:-3]

# ─── 统计打印线程 ────────────────────────────────────────────────
def stats_printer():
    with open('server_stats.log', 'a', encoding='utf-8') as f:
        while True:
            time.sleep(1)
            with _lock:
                rb = _stats['recv_bytes'];  _stats['recv_bytes']  = 0
                rf = _stats['recv_frames']; _stats['recv_frames'] = 0
                sf = _stats['send_frames']; _stats['send_frames'] = 0
                sb = _stats['send_bytes'];  _stats['send_bytes']  = 0
                cl = _stats['clients']

            line = (
                f"[{_now()}] "
                f"在线连接: {cl:>3}  |  "
                f"收到数据: {rb:>8} B  |  "
                f"解析完整帧: {rf:>6} 帧  |  "
                f"发送帧: {sf:>6} 帧  ({sb:>8} B)  "
                f"[帧头:EAEAEAEA  帧尾:AEAEAEAE]"
            )
            print(line, flush=True)
            f.write(line + '\n')
            f.flush()

threading.Thread(target=stats_printer, daemon=True).start()

# ─── 帧解析器 ────────────────────────────────────────────────────
class FrameParser:
    def __init__(self):
        self._buf = bytearray()

    def feed(self, data: bytes) -> list[bytes]:
        self._buf += data
        frames = []
        while True:
            start = self._buf.find(FRAME_HEADER)
            if start == -1:
                self._buf.clear()
                break
            if start > 0:
                self._buf = self._buf[start:]
            end = self._buf.find(FRAME_FOOTER, len(FRAME_HEADER))
            if end == -1:
                break
            frame_end = end + len(FRAME_FOOTER)
            frames.append(bytes(self._buf[:frame_end]))
            self._buf = self._buf[frame_end:]
        return frames

# ─── 客户端处理 ──────────────────────────────────────────────────
def handle_client(conn, addr):
    tag = f"{addr[0]}:{addr[1]}"
    print(f"[{_now()}] 连接建立: {tag}", flush=True)
    _add(clients=1)

    conn.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
    conn.setblocking(False)

    parser      = FrameParser()
    total_recv  = 0   # 累计收到字节
    total_rf    = 0   # 累计收到完整帧
    total_sf    = 0   # 累计发送帧

    try:
        while True:
            ready, _, _ = select.select([conn], [], [], 0.1)
            if not ready:
                continue

            try:
                data = conn.recv(65536)
            except BlockingIOError:
                continue
            except (ConnectionResetError, OSError):
                break

            if not data:
                break

            nb = len(data)
            total_recv += nb
            _add(recv_bytes=nb)

            frames = parser.feed(data)
            fc = len(frames)
            if fc:
                total_rf += fc
                _add(recv_frames=fc)

                for frame in frames:
                    # 打印每帧摘要（可按需注释掉）
                    # print(f"  帧: {frame[:4].hex()} ... {frame[-4:].hex()}  ({len(frame)}B)")
                    try:
                        conn.send(RESPONSE)
                        total_sf += 1
                        _add(send_frames=1, send_bytes=len(RESPONSE))
                    except OSError as e:
                        print(f"发送错误: {e}", flush=True)
                        return

    finally:
        conn.close()
        _add(clients=-1)
        print(
            f"[{_now()}] 连接断开: {tag}  "
            f"累计收到: {total_recv} B  "
            f"解析帧: {total_rf}  "
            f"发送帧: {total_sf}",
            flush=True
        )

# ─── 主循环（多线程接受连接）───────────────────────────────────────
server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
server.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
server.setsockopt(socket.SOL_SOCKET, socket.SO_RCVBUF, 1024 * 1024)
server.setsockopt(socket.SOL_SOCKET, socket.SO_SNDBUF, 1024 * 1024)
server.bind((HOST, PORT))
server.listen(128)

print(f"✅ 服务端启动: {HOST}:{PORT}", flush=True)

while True:
    try:
        conn, addr = server.accept()
        threading.Thread(target=handle_client, args=(conn, addr), daemon=True).start()
    except Exception as e:
        print(f"Accept错误: {e}", flush=True)
