import marshal, types, sys, uuid, json
import kvstore as kv
import logging

kv_conn = kv.get_store_connection()

job_id = sys.argv[1]

def wait_for_config():
    logging.info("{0} waiting for config".format(job_id))
    while True:
        config = kv.read_store(kv_conn, job_id + '_config')
        if config != '\r':
            return config
            # break
        else:
            continue

def store_reduce_output(reduce_output: list):
    output_contents = '\r,'.join(["{0}\t{1}".format(output[0], output[1]) 
                                for output in reduce_output])
    res = kv.set_command(kv_conn, job_id + '_result', len(output_contents.encode('utf-8')), output_contents)
    if res != "STORED\r\n":
        logging.error("Final results set failure : %s",res)
        logging.critical("ABORTING")
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

    if job_id == None:
        logging.critical("Job Initialization Error! ABORTING")
        exit()
    
    config = json.loads(wait_for_config())
    partition_key = config['partition_key']
    res = kv.set_command(kv_conn, job_id + '_status',len("STARTED".encode()),"STARTED")
    if res != "STORED\r\n":
        logging.error("Status set failure : %s",res)
        logging.critical("ABORTING")
        exit()

    message = kv.read_store(kv_conn, partition_key)
    sorted_results = sort_intermediate_results(message)
    reduce_output = []
    reduce_fn_serialized = bytes(config['reduce_fn'])
    for key in sorted_results:
        output = run_reduce(reduce_fn_serialized, key, sorted_results[key])
        reduce_output.append(output)
    store_reduce_output(reduce_output)


if __name__ == "__main__":
    logging.basicConfig(filename='reducer.log', format='%(asctime)s %(levelname)s %(message)s', 
        level=logging.DEBUG)
    main()