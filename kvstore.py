import socket


def set_command(conn: socket.socket, key: str, size: int, value: str) -> str:
    set_message = "set {0} {1}\r\n{2}\r\n".format(key,size,value)
    conn.send(set_message.encode())
    message = conn.recv(100)
    return message.decode()

def append_command(conn: socket.socket, key: str, size: int, value: str) -> str:
    append_message = "append {0} {1}\r\n{2}\r\n".format(key,size,value)
    conn.send(append_message.encode())
    message = conn.recv(100)
    return message.decode()

def get_command(conn: socket.socket, key: str):
    getMsg = "get {0}\r\n".format(key)
    conn.send(getMsg.encode())

def get_store_connection() -> socket.socket:
    conn = socket.socket(socket.AF_INET, socket.SOCK_STREAM) 
    host = socket.gethostname()                           
    port = 9248
    conn.connect((host, port))
    return conn

def close_store_connection(conn: socket.socket):
    conn.close()
