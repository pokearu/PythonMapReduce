import marshal, types
import kvstore as kv
import sys

kv_conn = kv.get_store_connection()

job_id = sys.argv[1]

def partition_intermediate_results(map_result: list) -> dict:
    partition_fn = lambda x : int(''.join([str(ord(c)) for c in x])) % 3
    partition_map = {}
    for result in map_result:
        key = "partition_{0}".format(partition_fn(result[0]))
        # Reformat the tuples fpr storage
        result = '{0}\t{1}'.format(result[0],result[1])
        if key in partition_map:
            partition_map[key] += '{0}\r,'.format(result)
        else:
            partition_map[key] = '{0}\r,'.format(result)
    return partition_map

def store_intermediate_results(partition_map: dict):
    for key in partition_map:
        res = kv.append_command(kv_conn, key, len(str(partition_map[key]).encode()), str(partition_map[key]))
        if res != "STORED\r\n":
            print("Error: " + res)
            exit()    
    kv.set_command(kv_conn, job_id + '_status',len("DONE".encode()),"DONE")

def run_map(map_func: bytes,key: str,value: str) -> list:
    mapper = marshal.loads(map_func)
    mapper = types.FunctionType(mapper,{})
    return mapper(key, value)

def main():
    res = kv.set_command(kv_conn, job_id + '_status',len("STARTED".encode()),"STARTED")
    if res != "STORED\r\n":
        print(res)
        exit()

    message = kv.read_store(kv_conn, job_id + '_input')
    map_result = run_map(b'\xe3\x02\x00\x00\x00\x00\x00\x00\x00\x02\x00\x00\x00\x03\x00\x00\x00C\x00\x00\x00s\x12\x00\x00\x00d\x01d\x02\x84\x00|\x01\xa0\x00\xa1\x00D\x00\x83\x01S\x00)\x03Nc\x01\x00\x00\x00\x00\x00\x00\x00\x02\x00\x00\x00\x04\x00\x00\x00S\x00\x00\x00s\x14\x00\x00\x00g\x00|\x00]\x0c}\x01|\x01d\x00f\x02\x91\x02q\x04S\x00)\x01\xe9\x01\x00\x00\x00\xa9\x00)\x02\xda\x02.0\xda\x04wordr\x02\x00\x00\x00r\x02\x00\x00\x00\xfa\x0fmapreduce_wc.py\xfa\n<listcomp>\x0c\x00\x00\x00s\x02\x00\x00\x00\x06\x00z\x1eword_count.<locals>.<listcomp>)\x01\xda\x05split)\x02\xda\tfile_name\xda\rfile_contentsr\x02\x00\x00\x00r\x02\x00\x00\x00r\x05\x00\x00\x00\xda\nword_count\x0b\x00\x00\x00s\x02\x00\x00\x00\x00\x01'
    , job_id + '_input', message)
    partition_map = partition_intermediate_results(map_result)
    store_intermediate_results(partition_map)
    kv.close_store_connection(kv_conn)


if __name__ == "__main__":
    main()