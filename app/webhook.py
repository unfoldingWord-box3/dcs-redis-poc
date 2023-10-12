import time
import requests
import redis
from rq import Queue, get_current_job
import time
from datetime import timedelta
import sys
import atexit
import os
from pathlib import Path
import safe_exit
import signal

r = redis.Redis(host='redis', port=6379, db=0)
tjh_queue = Queue('tx-job-handler', connection=r)

def job(delay, payload):
    atexit.register(clean_shut)
    myJob = get_current_job(r)
    print(f"JOB: GOT THIS JOBID: {myJob.id}\nDELAY: {delay}\n", file=sys.stderr)
    converter = Converter(myJob.id, delay)
    signal.signal(signal.SIGTERM, converter.__del__)
    url = converter.run()
    tjh_queue.enqueue("webhook.job2", delay, payload, job_id=myJob.id+"-tjh")
    return url

def job2(delay, payload):
    atexit.register(clean_shut)
    myJob = get_current_job(r)
    print(f"JOB2: GOT THIS JOBID: {myJob.id}\nDELAY: {delay}\n", file=sys.stderr)
    converter = Converter(myJob.id, delay)
    signal.signal(signal.SIGTERM, converter.__del__)
    url = converter.run()
    return url

class Converter():
    def __init__(self, job_id, delay):
        self.job_id = job_id
        self.delay = delay
        signal.signal(signal.SIGTERM, self.__del__)

    def run(self):
        Path(f'/tmp/{self.job_id}.txt').touch()
        time.sleep(self.delay)
        url = requests.get(
            "https://api.thecatapi.com/v1/images/search").json()[0]['url']
        return url
    
    def __del__(self):
        print(f"IN __del___!!! Deleting /tmp/{self.job_id}.txt...", file=sys.stderr)
        os.remove(f'/tmp/{self.job_id}.txt')

@atexit.register
def clean_shut():
    myJob = get_current_job(r)
    print(f"PERFORMING CLEAN SHUT DOWN ON JOB {myJob.id}!!!!!!", file=sys.stderr)
    try:
        os.remove(f'/tmp/{myJob.id}.txt')
        print("REMOVED FILE", file=sys.stderr)
    except:
        print("UNABLE TO REMOVE FILE", file=sys.stderr)
        pass

safe_exit.register(clean_shut)
