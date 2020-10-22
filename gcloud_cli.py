import subprocess
import logging


def create_vm(instance_name:str, zone:str, startup_script:str) -> str:
    '''
    This function runs a gcloud cli command to create compute VM
    '''
    gcloud_command = "gcloud compute instances create {0} --zone={1} --format=csv[no-heading](status) --metadata startup-script='{2}'".format(instance_name,
        zone, startup_script)
    process = subprocess.run(gcloud_command, shell=True, capture_output=True)
    stdout, stderr = process.stdout.decode(encoding='utf-8'), process.stderr.decode(encoding='utf-8')
    if 'ERROR' in stderr:
        logging.error(stderr)
        raise Exception("gcloud VM creation failed", instance_name)
    if 'RUNNING' in stdout:
        logging.info("%s CREATED with status RUNNING", instance_name)
        return 'RUNNING\r'
    raise Exception("gcloud VM creation failed", instance_name)

def delete_vm(instance_name:str, zone:str):
    '''
    This function runs a gcloud cli command to delete compute VM
    '''
    gcloud_command = "gcloud compute instances delete {0} --zone={1} --format=josn --quiet".format(instance_name, zone)
    process = subprocess.run(gcloud_command, shell=True, capture_output=True)
    stdout, stderr = process.stdout.decode(encoding='utf-8'), process.stderr.decode(encoding='utf-8')
    if 'ERROR' in stderr:
        logging.error(stderr)
        raise Exception("gcloud VM deleteion failed", instance_name)
    if 'Deleted' in stderr:
        logging.info("%s DELETED", instance_name)
        return 'DELETED\r'
    raise Exception("gcloud VM deleteion failed", instance_name)