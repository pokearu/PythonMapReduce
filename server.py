from flask import Flask, request, Response, send_from_directory
import subprocess
import logging, uuid
import configparser
import kvstore as kv

config = configparser.ConfigParser()

app = Flask(__name__, static_url_path='/output')


@app.route('/')
def hello():
    '''
    Welcome Route
    '''
    return "Welcome to MapReduce"

@app.route('/getjoblog', methods = ['GET'])
def get_job_log():
    '''
    This API route is used to get the logs of the MapReduce Job ID 
    '''
    try:
        job_id = request.args.get('jobid')
        logging.info("Getting log for job : {0}".format(job_id))
        return send_from_directory('output',filename='masterlog_{}.txt'.format(job_id))
    except Exception as e:
        logging.error("Job log fetch failed : %s", e)
        kv.close_store_connection(conn)
        return "ERROR : Job log fetch failed"


@app.route('/getjobstatus', methods = ['GET'])
def get_job_status():
    '''
    This API route is used to get the current Job status given a Job ID
    Returns Job output if the Job has completed
    '''
    try:
        job_id = request.args.get('jobid')
        logging.info("Getting status for job : {0}".format(job_id))
        conn = kv.get_store_connection()
        status = kv.read_store(conn,"{0}_status".format(job_id))
        if status == "COMPLETED\r":
            kv.close_store_connection(conn)
            return send_from_directory('output',filename='output_{}.txt'.format(job_id))
        else:
            kv.close_store_connection(conn)
            return status
    except Exception as e:
        logging.error("Job status check failed : %s", e)
        kv.close_store_connection(conn)
        return "ERROR : Job status check failed"


@app.route('/mapreduce', methods = ['POST'])
def map_reduce():
    '''
    This API route is used to trigger a MapReduce Job and returns the Job ID for tracking
    '''
    try:
        mapreduce_job_id = str(uuid.uuid1())
        map_reduce_config = request.json
        logging.info("Job Config : %s", request.json)
        config['master'] = map_reduce_config['master']
        config['master']['output_file'] = './output/output_{0}.txt'.format(mapreduce_job_id)
        config['mapper'] = map_reduce_config['mapper']
        config['reducer'] = map_reduce_config['reducer']
        with open('master_{0}.ini'.format(mapreduce_job_id), 'w') as configfile:
            config.write(configfile)
        subprocess.Popen(['python3', 'master.py', mapreduce_job_id])
        return str({ "job_id" : mapreduce_job_id })
    except Exception as e:
        logging.error("Job Initilization failed : %s", e)
        return "ERROR : Job Initilization failed"


if __name__ == '__main__':
    logging.basicConfig(filename='server.txt', format='%(asctime)s %(levelname)s %(message)s', 
        level=logging.DEBUG)
    app.run(port=9248)