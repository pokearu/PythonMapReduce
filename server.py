from flask import Flask, request, Response, send_from_directory
import subprocess
import logging, uuid
import configparser
import kvstore as kv

config = configparser.ConfigParser()

app = Flask(__name__, static_url_path='/output')


@app.route('/')
def hello():
    return "Hello World!"


# @app.route('/getjoboptions', methods = ['GET'])
# def get_job_options():
#     try:

#     except Exception as e:

@app.route('/getjobstatus', methods = ['GET'])
def get_job_status():
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
        logging.error("Job Initilization failed : %s", e)
        kv.close_store_connection(conn)
        return "ERROR : Job Initilization failed"


@app.route('/mapreduce', methods = ['POST'])
def map_reduce():
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
    logging.basicConfig(filename='server.log', format='%(asctime)s %(levelname)s %(message)s', 
        level=logging.DEBUG)
    app.run('10.142.0.7', port=9248)