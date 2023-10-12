import time
import requests
import redis
from rq import Queue, get_current_job
import time
from datetime import timedelta
import sys

r = redis.Redis(host='redis', port=6379, db=0)

def job(delay, payload):
    myJob = get_current_job(r)
    time.sleep(delay)
    tjh_queue = Queue('tx-job-handler', connection=r)
    print(f"GOT THIS JOBID: {myJob.id}\nDELAY: {delay}\n", file=sys.stderr)
    tjh_queue.enqueue("webhook.job2", delay, payload, job_id=myJob.id+"-tjh")    
    url = requests.get(
        "https://api.thecatapi.com/v1/images/search").json()[0]['url']
    return url

def job2(delay, payload):
    time.sleep(delay)
    url = requests.get(
        "https://api.thecatapi.com/v1/images/search").json()[0]['url']
    return url
