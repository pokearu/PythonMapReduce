import kvstore as kv                         
import subprocess
import configparser
import uuid
import json
import logging
import sys
import gcloud_cli as gcli

job_id = sys.argv[1]

config = configparser.ConfigParser()
config.read('master_{0}.ini'.format(job_id))

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
        key = mapper_jobids[count % nodes] + '_input'
        if key in split_input:
            split_input[key] = split_input[key] + " " + line
        else:
            split_input[key] = "#\r#{0}#\r#{1}".format(file_path,line)
        count += 1
    status = [kv.append_command(kv_conn,key,len(split_input[key].strip().encode()),split_input[key].strip()) 
                    for key in split_input]
    data_input.close()
    return status

def get_mapper_jobids():
    nodes_count = config['mapper'].getint('nodes')
    # mapper_jobids = []
    # while True:
    #     mapper_jobids = kv.read_store(kv_conn, 'mapper_jobids').split()
    #     if len(mapper_jobids) == nodes_count:
    #         print("All Mappers have started.")
    #         break
    #     else:
    #         continue
    
    # return mapper_jobids

    return [str(uuid.uuid1()) for i in range(nodes_count)]

def start_mapper_jobs(mapper_jobids: list):
    start_up_script = '''
        #! /bin/bash
        sudo apt update
        sudo apt -y install git
        sudo apt -y python3.8
        git clone https://github.com/pokearu/PythonMapReduce.git
        cd PythonMapReduce
        sudo python3 mapper_node.py {0}

    '''
    status = [gcli.create_vm("mapper-{0}".format(job_id), "us-east1-b", start_up_script.format(job_id)) for job_id in mapper_jobids]
    # gcloud_command = "gcloud compute instances create mapper-{0} --zone=us-east1-b --metadata startup-script='{1}'"
    # status = [subprocess.run(gcloud_command.format(job_id, start_up_script.format(job_id)), shell=True) for job_id in mapper_jobids]
    return status

def get_reducer_jobids():
    nodes_count = config['reducer'].getint('nodes')
    # reducer_jobids = []
    # while True:
    #     reducer_jobids = kv.read_store(kv_conn, 'reducer_jobids').split()
    #     if len(reducer_jobids) == nodes_count:
    #         print("All Reducers have started.")
    #         break
    #     else:
    #         continue
    # return reducer_jobids
    return [str(uuid.uuid1()) for i in range(nodes_count)]

def start_reducer_jobs(reducer_jobids: list):
    # nodes_count = config['reducer'].getint('nodes')
    start_up_script = '''
        #! /bin/bash
        sudo apt update
        sudo apt -y install git
        sudo apt -y python3.8
        git clone https://github.com/pokearu/PythonMapReduce.git
        cd PythonMapReduce
        sudo python3 reducer_node.py {0}

    '''
    status = [gcli.create_vm("reducer-{0}".format(job_id), "us-east1-b", start_up_script.format(job_id)) for job_id in reducer_jobids]
    # gcloud_command = "gcloud compute instances create reducer-{0} --zone=us-east1-b --metadata startup-script='{1}'"
    # status = [subprocess.run(gcloud_command.format(job_id,start_up_script.format(job_id)), shell=True) for job_id in reducer_jobids]
    return status

def wait_for_mappers(mapper_jobids: list):
    while True:
        statuses = [kv.read_store(kv_conn, job_id + '_status') for job_id in mapper_jobids]
        if all(status == "DONE\r" for status in statuses):
            logging.debug("Mappers Completed - status\n{0}".format(statuses))
            break
        else:
            continue

def update_mapper_config(mapper_jobids: list, reducer_jobids: list):
    for job_id in mapper_jobids:
        mapper_config = {}
        mapper_config['reducer_node'] = reducer_jobids
        mapper_config['map_fn'] = get_user_map()
        mapper_config = json.dumps(mapper_config)
        res = kv.set_command(kv_conn, job_id + '_config',len(mapper_config.encode()), mapper_config)
        if res != "STORED\r\n":
            logging.error("Update mapper config failed : %s", res)
            logging.critical("ABORTING JOB")
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
        reducer_config['partition_key'] = "partition_{0}".format(reducer_jobids[i])
        reducer_config['reduce_fn'] = get_user_reduce()
        reducer_config = json.dumps(reducer_config)
        res = kv.set_command(kv_conn, reducer_jobids[i] + '_config',len(str(reducer_config).encode()), str(reducer_config))
        if res != "STORED\r\n":
            logging.error("Update reducer config failed : %s", res)
            logging.critical("ABORTING JOB")
            exit()

def wait_for_reducers(reducer_jobids: list):
    while True:
        statuses = [kv.read_store(kv_conn, job_id + '_status') for job_id in reducer_jobids]
        if all(status == "DONE\r" for status in statuses):
            logging.debug("Reducers Completed - status\n{0}".format(statuses))
            break
        else:
            continue

def consolidate_output(reducer_jobids: list, output_file_path: str):
    reducer_output = [kv.read_store(kv_conn, job_id + '_result').replace('\r,','\n')
                        for job_id in reducer_jobids]
    with open(output_file_path, 'w', encoding = "utf8", errors='ignore') as output:
        output.writelines(reducer_output)

def clean_up(mapper_jobids: list, reducer_jobids: list):
    # Delete mapper VMs
    # nodes_count = config['mapper'].getint('nodes')
    # gcloud_command = "gcloud compute instances delete mapper-{0} --zone=us-east1-b --quiet"
    status = [gcli.delete_vm("mapper-{0}".format(job_id), "us-east1-b") for job_id in mapper_jobids]
    if all(vm == "DELETED\r" for vm in status):
        logging.debug("All Mappers have been deleted")
    # Delete reducer VMs
    # nodes_count = config['reducer'].getint('nodes')
    # gcloud_command = "gcloud compute instances delete reducer-{0} --zone=us-east1-b --quiet"
    status = [gcli.delete_vm("reducer-{0}".format(job_id), "us-east1-b") for job_id in reducer_jobids]
    if all(vm == "DELETED\r" for vm in status):
        logging.debug("All Reducers have been deleted")
    # Delete intermediate data
    [kv.delete_command(kv_conn,"partition_{0}".format(job_id)) for job_id in reducer_jobids]
    [kv.delete_command(kv_conn,"{0}_input".format(job_id)) for job_id in mapper_jobids]
    [kv.delete_command(kv_conn,"{0}_result".format(job_id)) for job_id in reducer_jobids]
    # Delete Job Configs
    [kv.delete_command(kv_conn,"{0}_config".format(job_id)) for job_id in mapper_jobids]
    [kv.delete_command(kv_conn,"{0}_config".format(job_id)) for job_id in reducer_jobids]

def main():
    try:
        logging.info("Starting MapReduce Job : %s", job_id)
        logging.info("Step 0 : Initializing Mapper and Reducer Job IDs")
        mapper_jobids = get_mapper_jobids()
        reducer_jobids = get_reducer_jobids()
        logging.debug("Mapper Job IDs : %s Reducer Job IDs : %s", mapper_jobids, reducer_jobids)

        logging.info("Step 1 : Starting the Mapper Nodes")
        status = start_mapper_jobs(mapper_jobids)
        if all(vm == "RUNNING\r" for vm in status):
            logging.debug("All Mappers have started")
        
        logging.info("Step 2 : Distribute the Input files")
        input_files = json.loads(config.get('master','input_file').replace("'", '"'))
        split_status = [process_data_file(file_path, mapper_jobids) for file_path in input_files]
        
        logging.info("Step 3 : Update the mapper configs")
        update_mapper_config(mapper_jobids, reducer_jobids)

        logging.info("Step 4 : Waiting for mappers to finish")
        wait_for_mappers(mapper_jobids)

        logging.info("Step 5 : Starting the Reducer Nodes")
        status = start_reducer_jobs(reducer_jobids)
        if all(vm == "RUNNING\r" for vm in status):
            logging.debug("All Reducers have started")

        logging.info("Step 6 : Update the reducer config")
        update_reducer_config(reducer_jobids)

        logging.info("Step 7 : Waiting for reducers to finish")
        wait_for_reducers(reducer_jobids)

        logging.info("Step 8 : Consolidating output file")
        consolidate_output(reducer_jobids,config.get('master','output_file'))
    except Exception as e:
        logging.critical("JOB FAILED : %s",e)
    finally:
        logging.info("Step 9 : Clean up VMs and intermediate data")
        clean_up(mapper_jobids, reducer_jobids)
        kv.close_store_connection(kv_conn)


if __name__ == "__main__":
    logging.basicConfig(filename='master.log', format='%(asctime)s %(levelname)s %(message)s', 
        level=logging.DEBUG)
    main()
