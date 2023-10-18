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
priority_queue = Queue('tx_job_handler_priority', connection=redis_connection)
pdf_queue = Queue('tx_job_handler', connection=redis_connection)
callback_queue = Queue('door43_job_handler_callback', connection=redis_connection)


def job(payload):
    myJob = get_current_job(redis_connection)
    converter = Converter(myJob.id, 10)
    url = converter.run()
    if "ref" in payload and ("master" in payload["ref"] or "tags" in payload["ref"]):
        priority_queue.enqueue("webhook.job2", payload, job_id="tx_job_handler_priority_"+myJob.id, result_ttl=(60*60*24))
    elif "DCS_event" in payload and "pdf" in payload["DCS_event"]:
        pdf_queue.enqueue("webhook.job2", payload, job_id="tx_job_handler_pdf_"+myJob.id, result_ttl=(60*60*24))
    else:
        tjh_queue.enqueue("webhook.job2", payload, job_id="tx_job_handler_"+myJob.id, result_ttl=(60*60*24))
    return url


def job2(payload):
    myJob = get_current_job(redis_connection)
    converter = Converter(myJob.id, 10)
    url = converter.run()
    callback_queue.enqueue("webhook.job3", payload, job_id="door43_job_handler_callback_"+myJob.id, result_ttl=(60*60*24))
    return url


def job3(payload):
    myJob = get_current_job(redis_connection)
    converter = Converter(myJob.id, 10)
    url = converter.run()
    return url


class Converter():
    def __init__(self, job_id, delay):
        self.job_id = job_id
        self.delay = delay

    def run(self, extra=""):
        time.sleep(self.delay)
        url = requests.get(f"https://api.thecatapi.com/v1/images/search{extra}").json()[0]['url']
        return url
