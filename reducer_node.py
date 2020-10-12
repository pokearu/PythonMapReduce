import marshal, types, sys
import kvstore as kv

kv_conn = kv.get_store_connection()

job_id = sys.argv[1]
partition_key = sys.argv[2]

def store_reduce_output(reduce_output: list):
    output_contents = '\r,'.join(["{0}\t{1}".format(output[0], output[1]) 
                                for output in reduce_output])
    res = kv.set_command(kv_conn, job_id + '_result', len(output_contents.encode('utf-8')), output_contents)
    if res != "STORED\r\n":
        print("Error: " + res)
        exit()    
    kv.set_command(kv_conn, job_id + '_status',len("DONE".encode()),"DONE")

def sort_intermediate_results(value: str):
    sort_results = {}
    for result in value.split('\r,'):
        data = result.split('\t')
        if len(data) < 2:
            continue
        key, value = data[0].strip(),data[1]
        if key in sort_results:
            sort_results[key].append((key,value))
        else:
            sort_results[key] = [(key,value)]
    return sort_results

def run_reduce(reduce_func: bytes, key:str, value: list):
    reducer = marshal.loads(reduce_func)
    reducer = types.FunctionType(reducer,globals())
    return reducer(key, value)

def main():
    res = kv.set_command(kv_conn, job_id + '_status',len("STARTED".encode()),"STARTED")
    if res != "STORED\r\n":
        print(res)
        exit()

    message = kv.read_store(kv_conn, partition_key)
    sorted_results = sort_intermediate_results(message)
    reduce_output = []
    for key in sorted_results:
        output = run_reduce(b'\xe3\x02\x00\x00\x00\x00\x00\x00\x00\x04\x00\x00\x00\x05\x00\x00\x00C\x00\x00\x00s,\x00\x00\x00d\x01}\x02x\x1e|\x01D\x00]\x16}\x03|\x02t\x00|\x03d\x02\x19\x00d\x03\x83\x02\x17\x00}\x02q\nW\x00|\x00|\x02f\x02S\x00)\x04N\xe9\x00\x00\x00\x00\xe9\x01\x00\x00\x00\xe9\n\x00\x00\x00)\x01\xda\x03int)\x04\xda\x03key\xda\x05words\xda\x05count\xda\x04word\xa9\x00r\t\x00\x00\x00\xfa\x0fmapreduce_wc.py\xda\x0creduce_words\x0e\x00\x00\x00s\x08\x00\x00\x00\x00\x01\x04\x01\n\x02\x16\x01',
        key, sorted_results[key])
        reduce_output.append(output)
    store_reduce_output(reduce_output)


if __name__ == "__main__":
    main()