import marshal, types, sys, uuid, json
import kvstore as kv
import logging

kv_conn = kv.get_store_connection()

job_id = sys.argv[1]

def wait_for_config():
    '''
    Waits for the master to set the Job config
    '''
    logging.info("{0} waiting for config".format(job_id))
    while True:
        config = kv.read_store(kv_conn, job_id + '_config')
        if config != '\r':
            return config
        else:
            continue

def store_reduce_output(reduce_output: list):
    '''
    Sets reducer output
    '''
    output_contents = '\r,'.join(["{0}\t{1}".format(output[0], output[1]) 
                                for output in reduce_output])
    res = kv.set_command(kv_conn, job_id + '_result', len(output_contents.encode('utf-8')), output_contents)
    if res != "STORED\r\n":
        logging.error("Final results set failure : %s",res)
        logging.critical("ABORTING")
        exit()
    kv.set_command(kv_conn, job_id + '_status',len("DONE".encode()),"DONE")

def sort_intermediate_results(value: str):
    '''
    Sorts the Intermediate results based on key
    '''
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
    '''
    Runs the Reduce function based on the user provided reduce function
    '''
    reducer = marshal.loads(reduce_func)
    reducer = types.FunctionType(reducer,globals())
    return reducer(key, value)

def main():
    '''
    The driver function that runs the Reduce job
    '''
    try:
        if job_id == None:
            logging.critical("Job Initialization Error! ABORTING")
            exit()
        # Step 1 : Wait for Reduce Job config
        config = json.loads(wait_for_config())
        partition_key = config['partition_key']
        # Step 2 : Update status as started
        res = kv.set_command(kv_conn, job_id + '_status',len("STARTED".encode()),"STARTED")
        if res != "STORED\r\n":
            logging.error("Status set failure : %s",res)
            logging.critical("ABORTING")
            exit()
        # Step 3 : Read partition data from mapper
        message = kv.read_store(kv_conn, partition_key)
        # Step 4 : Sort intermediate data
        sorted_results = sort_intermediate_results(message)
        reduce_output = []
        # Step 5 : Run Reduce on the sorted data
        reduce_fn_serialized = bytes(config['reduce_fn'])
        for key in sorted_results:
            output = run_reduce(reduce_fn_serialized, key, sorted_results[key])
            reduce_output.append(output)
        # Step 6 : Store reduce results
        store_reduce_output(reduce_output)
    except Exception as e:
        logging.critical("JOB FAILED : %s",e)
        res = kv.set_command(kv_conn, job_id + '_status',len("FAILED".encode()),"FAILED")
    finally:
         kv.close_store_connection(kv_conn)

if __name__ == "__main__":
    logging.basicConfig(filename='reducer.log', format='%(asctime)s %(levelname)s %(message)s', 
        level=logging.DEBUG)
    main()