import socket

HOST = '0.0.0.0'
PORT = 8654

print("Starting test server...")
server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
server.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
server.bind((HOST, PORT))
server.listen(1)

print(f"Server started on {HOST}:{PORT}")
print("Waiting for connection...")

while True:
    try:
        conn, addr = server.accept()
        print(f"Accepted connection from {addr}")
        
        # 接收数据
        data = conn.recv(1024)
        print(f"Received data: {data.hex()}")
        
        # 发送响应
        conn.send(b"Hello from server")
        print("Sent response")
        
        conn.close()
        print("Connection closed")
        break
    except Exception as e:
        print(f"Error: {e}")
        break

server.close()
print("Server closed")
