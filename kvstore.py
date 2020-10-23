import socket
import configparser

config = configparser.ConfigParser()
config.read('kv.ini')

def set_command(conn: socket.socket, key: str, size: int, value: str) -> str:
    '''
    Sends the SET command over the socket
    '''
    set_message = "set {0} {1}\r\n{2}\r\n".format(key,size,value)
    conn.send(set_message.encode())
    message = conn.recv(config['kv'].getint('buffer'))
    return message.decode(encoding='utf-8', errors='ignore')

def append_command(conn: socket.socket, key: str, size: int, value: str) -> str:
    '''
    Sends the APPEND command over the socket
    '''
    append_message = "append {0} {1}\r\n{2}\r\n".format(key,size,value)
    conn.send(append_message.encode())
    message = conn.recv(config['kv'].getint('buffer'))
    return message.decode(encoding='utf-8', errors='ignore')

def delete_command(conn: socket.socket, key: str) -> str:
    '''
    Sends the DELETE command over the socket
    '''
    delete_message = "delete {0}\r\n".format(key)
    conn.send(delete_message.encode())
    message = conn.recv(config['kv'].getint('buffer'))
    return message.decode(encoding='utf-8', errors='ignore')

def get_command(conn: socket.socket, key: str):
    '''
    Sends the GET command over the socket
    '''
    get_message = "get {0}\r\n".format(key)
    conn.send(get_message.encode())

def read_store(conn: socket.socket, key: str) -> str:
    '''
    Sends the GET command over the socket
    Reads from the stream based on buffer size
    Returns the formatted output message
    '''
    get_command(conn, key)
    bufferSize = config['kv'].getint('buffer')
    # Get the first line of the GET message
    message = conn.recv(bufferSize)
    meta_data = message.decode(encoding='utf-8', errors='ignore').split('\n')[0]
    meta_data_size =  len(meta_data.encode())
    value_size = int(meta_data.split().pop()) + meta_data_size + 3
    while len(message) <= value_size:
        message += conn.recv(bufferSize)
    # Format message to strip meta data and END
    message = message.decode(encoding='utf-8', errors='ignore').split('\n')[1:-2]
    return '\n'.join(message)

def get_store_connection() -> socket.socket:
    '''
    Opens a socket connection to the KeyValue store
    Returns the socket object
    '''
    conn = socket.socket(socket.AF_INET, socket.SOCK_STREAM) 
    host = config['kv']['host']                           
    port = config['kv'].getint('port')
    conn.connect((host, port))
    return conn

def close_store_connection(conn: socket.socket):
    '''
    Closes the connection for the given socket
    '''
    conn.close()
