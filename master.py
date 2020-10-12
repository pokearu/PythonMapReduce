import kvstore as kv                         
import subprocess
import configparser
import uuid
import json

config = configparser.ConfigParser()
config.read('master.ini')

kv_conn = kv.get_store_connection()

def process_data_file(file_path: str, mapper_jobids: list):
    data_input = open(file_path, 'r')
    count = 0
    while True:
        line = data_input.readline()
        if not line:
            # end of file is reached
            break
        line = line.strip()
        print("Line {}: {}".format(count, line))
        size = len(line.encode())
        key = mapper_jobids[count % len(mapper_jobids)] + '_input'
        kv.append_command(kv_conn,key,size,line)
        count += 1
    data_input.close()

def get_mapper_jobids():
    nodes_count = config['mapper'].getint('nodes')
    return [str(uuid.uuid1()) for i in range(nodes_count)]

def start_mapper_jobs(mapper_jobids: list):
    status = [subprocess.run(['python', 'mapper_node.py', job_id]) for job_id in mapper_jobids]
    return status

def get_reducer_jobids():
    nodes_count = config['reducer'].getint('nodes')
    return [str(uuid.uuid1()) for i in range(nodes_count)]

def start_reducer_jobs(reducer_jobids: list):
    status = [subprocess.run(['python', 'reducer_node.py', reducer_jobids[i], 'partition_{}'.format(i)]) 
                        for i in range(len(reducer_jobids))]
    return status

def wait_for_mappers(mapper_jobids: list):
    while True:
        statuses = [kv.read_store(kv_conn, job_id + '_status') for job_id in mapper_jobids]
        if all(status == "DONE\r" for status in statuses):
            print("Mappers Completed - status\n{0}".format(statuses))
            break
        else:
            continue

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
    with open(output_file_path, 'w') as output:
        output.writelines(reducer_output)

def main():
    mapper_jobids = get_mapper_jobids()
    input_files = json.loads(config.get('master','input_file'))
    process_data_file(input_files[0], mapper_jobids)
    status = start_mapper_jobs(mapper_jobids)
    wait_for_mappers(mapper_jobids)
    reducer_jobids = get_reducer_jobids()
    status = start_reducer_jobs(reducer_jobids)
    wait_for_reducers(reducer_jobids)
    consolidate_output(reducer_jobids,config.get('master','output_file'))
    kv.close_store_connection(kv_conn)


if __name__ == "__main__":
    main()
