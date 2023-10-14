import time
import requests
import redis
from rq import Queue, get_current_job
import time
from datetime import timedelta
import sys
from pathlib import Path
import signal

redis_connection = redis.Redis(host='redis', port=6379, db=0)
tjh_queue = Queue('tx_job_handler', connection=redis_connection)

def job(payload):
    myJob = get_current_job(redis_connection)
    print("JOB ID IN JOB: "+myJob.id, file=sys.stderr)
    delay = payload['delay'] if 'delay' in payload else 10
    print(f"JOB: GOT THIS JOBID: {myJob.id}\nDELAY: {delay}\n", file=sys.stderr)
    converter = Converter(myJob.id, delay)
    url = converter.run()
    tjh_queue.enqueue("webhook.job2", payload, job_id=myJob.id)
    return url

def job2(payload):
    myJob = get_current_job(redis_connection)
    print("JOB ID IN JOB2: "+myJob.id, file=sys.stderr)
    delay = payload['delay'] if 'delay' in payload else 10
    print(f"JOB2: GOT THIS JOBID: {myJob.id}\nDELAY: {delay}\n", file=sys.stderr)
    converter = Converter(myJob.id, delay)
    return converter.run()

class Converter():
    def __init__(self, job_id, delay):
        self.job_id = job_id
        self.delay = delay

    def run(self):
        time.sleep(self.delay)
        url = requests.get(
            "https://api.thecatapi.com/v1/images/search").json()[0]['url']
        return url
