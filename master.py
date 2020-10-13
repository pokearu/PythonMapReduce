import kvstore as kv                         
import subprocess
import configparser
import uuid
import json

config = configparser.ConfigParser()
config.read('master.ini')

kv_conn = kv.get_store_connection()

def process_data_file(file_path: str, mapper_jobids: list):
    split_input = {}
    nodes = len(mapper_jobids)
    data_input = open(file_path, 'r', encoding = "utf8", errors='ignore')
    count = 0
    while True:
        line = data_input.readline()
        if not line:
            # end of file is reached
            break
        line = line.strip()
        # size = len(line.encode('utf-8'))
        key = mapper_jobids[count % nodes] + '_input'
        if key in split_input:
            split_input[key] = split_input[key] + " " + line
        else:
            split_input[key] = line
        # kv.append_command(kv_conn,key,size,line)
        count += 1
    status = [kv.append_command(kv_conn,key,len(split_input[key].strip().encode()),split_input[key].strip()) 
                    for key in split_input]
    data_input.close()
    return status

def get_mapper_jobids():
    nodes_count = config['mapper'].getint('nodes')
    mapper_jobids = []
    while True:
        mapper_jobids = kv.read_store(kv_conn, 'mapper_jobids').split()
        if len(mapper_jobids) == nodes_count:
            print("All Mappers have started.")
            break
        else:
            continue
    
    return mapper_jobids
    # return [str(uuid.uuid1()) for i in range(nodes_count)]

def start_mapper_jobs():
    nodes_count = config['mapper'].getint('nodes')
    status = [subprocess.Popen(['python', 'mapper_node.py']) for i in range(nodes_count)]
    return status

def get_reducer_jobids():
    nodes_count = config['reducer'].getint('nodes')
    reducer_jobids = []
    while True:
        reducer_jobids = kv.read_store(kv_conn, 'reducer_jobids').split()
        if len(reducer_jobids) == nodes_count:
            print("All Reducers have started.")
            break
        else:
            continue
    
    return reducer_jobids
    # return [str(uuid.uuid1()) for i in range(nodes_count)]

def start_reducer_jobs():
    nodes_count = config['reducer'].getint('nodes')
    status = [subprocess.Popen(['python', 'reducer_node.py']) for i in range(nodes_count)]
    return status

def wait_for_mappers(mapper_jobids: list):
    while True:
        statuses = [kv.read_store(kv_conn, job_id + '_status') for job_id in mapper_jobids]
        if all(status == "DONE\r" for status in statuses):
            print("Mappers Completed - status\n{0}".format(statuses))
            break
        else:
            continue

def update_mapper_config(mapper_jobids: list):
    nodes_count = config['reducer'].getint('nodes')
    for job_id in mapper_jobids:
        mapper_config = {}
        mapper_config['reducer_node'] = nodes_count
        mapper_config['map_fn'] = get_user_map()
        mapper_config = json.dumps(mapper_config)
        res = kv.set_command(kv_conn, job_id + '_config',len(mapper_config.encode()), mapper_config)
        if res != "STORED\r\n":
            print(res)
            exit()

def get_user_map() -> list:
    with open(config['mapper'].get('map_fn'), 'rb') as f:
        output = f.read()
        return list(output)

def get_user_reduce() -> list:
    with open(config['reducer'].get('reduce_fn'), 'rb') as f:
        output = f.read()
        return list(output)

def update_reducer_config(reducer_jobids: list):
    for i in range(len(reducer_jobids)):
        reducer_config = {}
        reducer_config['partition_key'] = "partition_{0}".format(i)
        reducer_config['reduce_fn'] = get_user_reduce()
        reducer_config = json.dumps(reducer_config)
        res = kv.set_command(kv_conn, reducer_jobids[i] + '_config',len(str(reducer_config).encode()), str(reducer_config))
        if res != "STORED\r\n":
            print(res)
            exit()

def wait_for_reducers(reducer_jobids: list):
    while True:
        statuses = [kv.read_store(kv_conn, job_id + '_status') for job_id in reducer_jobids]
        if all(status == "DONE\r" for status in statuses):
            print("Reducers Completed - status\n{0}".format(statuses))
            break
        else:
            continue

def consolidate_output(reducer_jobids: list, output_file_path: str):
    reducer_output = [kv.read_store(kv_conn, job_id + '_result').replace('\r,','\n')
                        for job_id in reducer_jobids]
    with open(output_file_path, 'w', encoding = "utf8", errors='ignore') as output:
        output.writelines(reducer_output)

def clean_up(mapper_jobids: list, reducer_jobids: list):
    kv.delete_command(kv_conn,"mapper_jobids")
    kv.delete_command(kv_conn,"reducer_jobids")
    nodes_count = config['reducer'].getint('nodes')
    [kv.delete_command(kv_conn,"partition_{0}".format(i)) for i in range(nodes_count)]
    [kv.delete_command(kv_conn,"{0}_input".format(job_id)) for job_id in mapper_jobids]
    [kv.delete_command(kv_conn,"{0}_result".format(job_id)) for job_id in reducer_jobids]

def main():
    try:    
        # Step 1 : Start the Mapper Nodes
        status = start_mapper_jobs()
        mapper_jobids = get_mapper_jobids()
        # Step 2 : Distribute the Input files
        input_files = json.loads(config.get('master','input_file'))
        split_status = [process_data_file(file_path, mapper_jobids) for file_path in input_files]
        # Step 3 : Update the config for mappers
        update_mapper_config(mapper_jobids)
        # Step 4 : Wait for mappers to finish
        wait_for_mappers(mapper_jobids)
        # Step 5 : Start the Reducer Nodes
        status = start_reducer_jobs()
        reducer_jobids = get_reducer_jobids()
        # Step 6 : Update the config for reducers
        update_reducer_config(reducer_jobids)
        # Step 7 : Wait for reducers to finish
        wait_for_reducers(reducer_jobids)
        # Step 8 : Consolidate output file
        consolidate_output(reducer_jobids,config.get('master','output_file'))
    except Exception as e:
        print(e)
    finally:
        # Step 9 : Clean up intermediate data
        clean_up(mapper_jobids, reducer_jobids)
        kv.close_store_connection(kv_conn)


if __name__ == "__main__":
    main()
