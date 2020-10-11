import marshal, types
import kvstore as kv

kv_conn = kv.get_store_connection()

def read_store() -> bytes:
    kv.get_command(kv_conn,"hello")
    bufferSize = 100
    # Get the first line of the GET message
    message = kv_conn.recv(bufferSize)
    meta_data = message.decode().split('\n')[0]
    meta_data_size =  len(meta_data.encode())
    value_size = int(meta_data.split().pop()) + meta_data_size + 3
    while len(message) <= value_size:
        message += kv_conn.recv(bufferSize)
    return message

def run_map(map_func: bytes,key: str,value: str) -> list:
    mapper = marshal.loads(map_func)
    mapper = types.FunctionType(mapper,{})
    return mapper(key, value)

def main():
    message = read_store().decode()
    # Strip meta data and END
    message = '\n'.join(message.split('\n')[1:-2])
    map_result = run_map(b'\xe3\x02\x00\x00\x00\x00\x00\x00\x00\x06\x00\x00\x00\x04\x00\x00\x00C\x00\x00\x00sB\x00\x00\x00|\x01\xa0\x00d\x01\xa1\x01}\x02g\x00}\x03x.|\x02D\x00]&}\x04|\x04\xa0\x01\xa1\x00}\x04|\x04\xa0\x00\xa1\x00}\x05|\x03d\x02d\x03\x84\x00|\x05D\x00\x83\x01\x17\x00}\x03q\x14W\x00|\x03S\x00)\x04N\xda\x01\nc\x01\x00\x00\x00\x00\x00\x00\x00\x02\x00\x00\x00\x04\x00\x00\x00S\x00\x00\x00s\x14\x00\x00\x00g\x00|\x00]\x0c}\x01|\x01d\x00f\x02\x91\x02q\x04S\x00)\x01\xe9\x01\x00\x00\x00\xa9\x00)\x02\xda\x02.0\xda\x04wordr\x03\x00\x00\x00r\x03\x00\x00\x00\xfa\tmap_wc.py\xfa\n<listcomp>\x0c\x00\x00\x00s\x02\x00\x00\x00\x06\x00z\x1eword_count.<locals>.<listcomp>)\x02\xda\x05split\xda\x05strip)\x06\xda\tfile_name\xda\rfile_contents\xda\x05lines\xda\x03res\xda\x04line\xda\x05wordsr\x03\x00\x00\x00r\x03\x00\x00\x00r\x06\x00\x00\x00\xda\nword_count\x03\x00\x00\x00s\x0e\x00\x00\x00\x00\x01\n\x01\x04\x01\n\x02\x08\x02\x08\x02\x16\x03'
    , "hello", message)
    res = kv.set_command(kv_conn, "MapperResult", len(str(map_result).encode()), str(map_result))
    print(res)
    kv.close_store_connection(kv_conn)


if __name__ == "__main__":
    main()