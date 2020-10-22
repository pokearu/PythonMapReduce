import marshal, types, uuid, json, os
import kvstore as kv
import sys
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


def partition_intermediate_results(map_result: list, reducers: int, reducer_jobids: list) -> dict:
    partition_fn = lambda x : int(''.join([str(ord(c)) for c in x])) % reducers
    partition_map = {}
    for result in map_result:
        key = "partition_{0}".format(reducer_jobids[partition_fn(result[0])])
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
            logging.error("Intermediate results append failure : %s",res)
            logging.critical("ABORTING")
            exit()
    kv.set_command(kv_conn, job_id + '_status',len("DONE".encode()),"DONE")

def run_map(map_func: bytes,key: str,value: str) -> list:
    mapper = marshal.loads(map_func)
    mapper = types.FunctionType(mapper,{})
    return mapper(key, value)

def main():

    if job_id == None:
        logging.critical("Job Initialization Error! ABORTING")
        exit()
    config = json.loads(wait_for_config())
    reducer_node = config['reducer_node']

    res = kv.set_command(kv_conn, job_id + '_status',len("STARTED".encode()),"STARTED")
    if res != "STORED\r\n":
        logging.error("Status set failure : %s",res)
        logging.critical("ABORTING")
        exit()

    message = kv.read_store(kv_conn, job_id + '_input')
    message_list = message.split('#\r#')[1:]
    map_result = []
    for i in range(0, len(message_list), 2):
        map_result = map_result + run_map(bytes(config['map_fn']),message_list[i], message_list[i+1])
    partition_map = partition_intermediate_results(map_result, len(reducer_node), reducer_node)
    store_intermediate_results(partition_map)
    kv.close_store_connection(kv_conn)


if __name__ == "__main__":
    logging.basicConfig(filename='mapper.log', format='%(asctime)s %(levelname)s %(message)s', 
        level=logging.DEBUG)
    main()