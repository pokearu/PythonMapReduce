from flask import Flask, request, Response
import subprocess
import logging, uuid
import configparser

config = configparser.ConfigParser()

app = Flask(__name__)


@app.route('/')
def hello():
    return "Hello World!"


# @app.route('/getjoboptions', methods = ['GET'])
# def get_job_options():
#     try:

#     except Exception as e:



@app.route('/mapreduce', methods = ['POST'])
def map_reduce():
    try:
        mapreduce_job_id = str(uuid.uuid1())
        map_reduce_config = request.json
        print("POST data : %s", request.json)
        config['master'] = map_reduce_config['master']
        config['master']['output_file'] = './output/output_{0}'.format(mapreduce_job_id)
        config['mapper'] = map_reduce_config['mapper']
        config['reducer'] = map_reduce_config['reducer']
        with open('master_{0}.ini'.format(mapreduce_job_id), 'w') as configfile:
            config.write(configfile)
        subprocess.Popen(['python3', 'master.py', mapreduce_job_id])
        return str({ "job_id" : mapreduce_job_id })
    except Exception as e:
        print(e)
        return "ERROR : Job Initilization failed"


if __name__ == '__main__':
    app.run('10.142.0.7', port=80)