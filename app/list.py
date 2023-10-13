import redis
from rq import Queue, command, cancel_job
import sys
import datetime
import json
from functools import reduce

r = redis.Redis(port=6379, db=0)
djh_queue = Queue('door43-job-handler', connection=r)
tjh_queue = Queue('tx-job-handler', connection=r)

def run():
    print(djh_queue, tjh_queue, file=sys.stderr)

    f = open('./payload.json')
    default_payload = f.read()
    f.close()

    # delayStr = request.form.get('delay', 10)
    # is_user_branch = request.form.get('is_user_branch', False)
    # schedule_seconds = request.form.get('schedule_seconds', 10)
    # payload = json.loads(request.form.get('payload', default_payload))

    # try:
    #     delay = int(delayStr)
    # except:
    #     delay = 10

    # if request.method == 'POST':
    #     job = queue_new_job(delay, payload, is_user_branch, schedule_seconds)
    #     if job:
    #         print(f"Status: {'Scheduled' if is_user_branch else 'Queued'} new job: {job.id}") 

    for queue in [djh_queue, tjh_queue]:
        print(f"{queue.name} Registries:")
        
        print(f"Queued Jobs:")
        n = len(queue.get_jobs())
        for job in queue.get_jobs():
            if job:
                print(f"{job.id}")
        print(f"Total {n} Jobs in queue\n")

        print(f"Scheduled Jobs:")
        n = len(queue.scheduled_job_registry.get_job_ids())
        for id in queue.scheduled_job_registry.get_job_ids():
            job = queue.fetch_job(id)
            if job:
                print(f"{id}: {job.is_scheduled}")
        print(f"Total {n} Jobs scheduled\n")

        print(f"Started Jobs:")
        n = len(queue.started_job_registry.get_job_ids())
        for id in queue.started_job_registry.get_job_ids():
            job = queue.fetch_job(id)
            if job:
                print(f"{id}: {job.is_started}")
        print(f"Total {n} Jobs started\n")

        print(f"Finished Jobs:")
        n = len(queue.finished_job_registry.get_job_ids())
        for id in queue.finished_job_registry.get_job_ids():
            job = queue.fetch_job(id)
            if job:
                print(f"{id}: {job.is_finished}")
        print(f"Total {n} Jobs finished\n")

        print(f"Canceled Jobs:")
        n = len(queue.canceled_job_registry.get_job_ids())
        for id in queue.canceled_job_registry.get_job_ids():
            job = queue.fetch_job(id)
            if job:
                print(f"{id}: {job.is_canceled}")
        print(f"Total {n} Jobs canceled\n")

        print(f"Failed Jobs:")
        n = len(queue.failed_job_registry.get_job_ids())
        for id in queue.failed_job_registry.get_job_ids():
            job = queue.fetch_job(id)
            if job:
                print(f"{id}: {job.is_failed}")
        print(f"Total {n} Jobs failed\n")

        print(f"ALL Jobs:")
        job_ids = queue.scheduled_job_registry.get_job_ids() + queue.get_job_ids() + queue.started_job_registry.get_job_ids() + queue.finished_job_registry.get_job_ids() + queue.deferred_job_registry.get_job_ids() + queue.canceled_job_registry.get_job_ids() + queue.failed_job_registry.get_job_ids()
        n = len(job_ids)
        for id in job_ids:
            job = queue.fetch_job(id)
            if job:
                print(id)
        print(f"Total {n} Jobs\n")


def getJob(job_id):
    djh_queue = Queue("door43-job-handler", connection=r)
    tjh_queue = Queue("tx-job-handler", connection=r)
    res = djh_queue.fetch_job(job_id)
    if res == None:
        res = tjh_queue.fetch_job(job_id)
    print(res.args, file=sys.stderr)
    if not res.result:
        return f'<center><br /><br /><h3>The job is still pending</h3><br /><br />ID:{job_id}<br />Queued at: {res.enqueued_at}<br />Status: {res._status}</center><b>{res.args[1]}</b>'
    return f'<center><br /><br /><img src="{res.result}" height="200px"><br /><br />ID:{job_id}<br />Queued at: {res.enqueued_at}<br />Finished at: {res.ended_at}</center><b>{res.args[1]}</b>'

def queue_new_job(delay, payload, is_user_branch, schedule_seconds):
    remove_similar_jobs(payload, ["ref", "repo.full_name"])
    try:
        print(f"DELAY: {delay}", file=sys.stderr)
        if is_user_branch:
            job = djh_queue.enqueue_in(datetime.timedelta(seconds=schedule_seconds), "webhook.job", delay, payload)
        else:
            job = djh_queue.enqueue("webhook.job", delay, payload)
    except Exception as e:
        print("Failed to queue", file=sys.stderr)
        print(e, file=sys.stderr)
    return job

def remove_similar_jobs(payload, matching_fields):
    for queue in [djh_queue, tjh_queue]:
        job_ids = queue.scheduled_job_registry.get_job_ids() + queue.get_job_ids() + queue.started_job_registry.get_job_ids()
        print("JOB IDS: ", job_ids, file=sys.stderr)
        for job_id in job_ids:
            print("JOB ID "+job_id, file=sys.stderr)
            job = queue.fetch_job(job_id)
            if job:
                print("GOT JOB", file=sys.stderr)
                old_payload = job.args[1]
                if payload:
                    similar = []
                    for field in matching_fields:
                        try:
                            new_value = reduce(lambda x,y : x[y],field.split("."),payload)
                            old_value = reduce(lambda x,y : x[y],field.split("."),old_payload)
                            similar += [new_value == old_value]
                            print(f'{job.id}: {field} is {"NOT" if new_value != old_value else ""} similar!!', file=sys.stderr)
                        except KeyError:
                            pass
                    if all(similar):
                        job.cancel()
                        print(f"CANCELLED JOB {job.id} ({job.get_status()}) DUE TO BEING SIMILAR: ", payload, old_payload, file=sys.stderr)


if __name__ == "__main__":
    run()