from flask import Flask, request
import redis
from rq import Queue, command, cancel_job
import sys
import datetime
import json
from functools import reduce

app = Flask(__name__)

r = redis.Redis(host='redis', port=6379, db=0)
djh_queue = Queue('door43-job-handler', connection=r)
tjh_queue = Queue('tx-job-handler', connection=r)

@app.route('/', methods = ['POST', 'GET'])
def index():
    print(djh_queue, tjh_queue, file=sys.stderr)

    f = open('./payload.json')
    default_payload = f.read()
    f.close()

    delayStr = request.form.get('delay', 10)
    is_user_branch = request.form.get('is_user_branch', False)
    schedule_seconds = request.form.get('schedule_seconds', 10)
    payload = json.loads(request.form.get('payload', default_payload))

    try:
        delay = int(delayStr)
    except:
        delay = 10

    html = f'''
<form method="POST">Time Working: <input type="text" name="delay" value="{delayStr}" />
    <br/>
    <input type="checkbox" name="is_user_branch" checked="{is_user_branch}"/> Is user branch, schedule for <input type="text" name="schedule_seconds" value="{schedule_seconds}" /> seconds
    <br/>
    Payload:<br/><textarea name="payload" rows=5 cols="50">{json.dumps(payload, indent=2)}</textarea>
    <br/>
    <input type="submit" value="Queue Job"/>
</form>'''

    if request.method == 'POST':
        job = queue_new_job(delay, payload, is_user_branch, schedule_seconds)
        if job:
            html += f"<p><b>Status:</b> {'Scheduled' if is_user_branch else 'Queued'} new job: {job.id}</p>" 

    for queue in [djh_queue, tjh_queue]:
        html += f'<div style="float:left; padding-right: 10px"><center><h3>{queue.name} Registries:</h3></center>'
        
        html += '<p style="min-height: 100px"><b>Queued Jobs:</b><br/><br/>'
        n = len(queue.get_jobs())
        for job in queue.get_jobs():
            if job:
                html += f'<a href="job/{job.id}">{job.id}</a><br /><br />'
        html += f'Total {n} Jobs in queue</p><hr /><br />'

        html += '<p style="min-height: 100px"><b>Scheduled Jobs:</b><br /><br />'
        n = len(queue.scheduled_job_registry.get_job_ids())
        for id in queue.scheduled_job_registry.get_job_ids():
            job = queue.fetch_job(id)
            if job:
                html += f'<a href="job/{id}">{id}: {job.is_scheduled}</a><br /><br />'
        html += f'Total {n} Jobs scheduled </center><p/><hr /><br />'

        html += '<p style="min-height: 100px"><b>Started Jobs:</b><br /><br />'
        n = len(queue.started_job_registry.get_job_ids())
        for id in queue.started_job_registry.get_job_ids():
            job = queue.fetch_job(id)
            if job:
                html += f'<a href="job/{id}">{id}: {job.is_started}</a><br /><br />'
        html += f'Total {n} Jobs started </p><hr /><br />'

        html += '<p style="min-height: 100px"><b>Finished Jobs:</b><br /><br />'
        n = len(queue.finished_job_registry.get_job_ids())
        for id in queue.finished_job_registry.get_job_ids():
            job = queue.fetch_job(id)
            if job:
                html += f'<a href="job/{id}">{id}: {job.is_finished}</a><br /><br />'
        html += f'Total {n} Jobs finished</p><hr /><br />'

        html += '<p style="min-height: 100px"><b>Canceled Jobs:</b><br /><br />'
        n = len(queue.canceled_job_registry.get_job_ids())
        for id in queue.canceled_job_registry.get_job_ids():
            job = queue.fetch_job(id)
            if job:
                html += f'<a href="job/{id}">{id}: {job.is_canceled}</a><br /><br />'
        html += f'Total {n} Jobs canceled</p><hr /><br />'

        html += '<p style="min-height: 100px"><b>Failed Jobs:</b><br /><br />'
        n = len(queue.failed_job_registry.get_job_ids())
        for id in queue.failed_job_registry.get_job_ids():
            job = queue.fetch_job(id)
            if job:
                html += f'<a href="job/{id}">{id}: {job.is_failed}</a><br /><br />'
        html += f'Total {n} Jobs failed</p><hr /><br />'

        html += '<p style="min-height: 100px"><b>ALL Jobs:</b><br /><br />'
        job_ids = queue.scheduled_job_registry.get_job_ids() + queue.get_job_ids() + queue.started_job_registry.get_job_ids() + queue.finished_job_registry.get_job_ids() + queue.deferred_job_registry.get_job_ids() + queue.canceled_job_registry.get_job_ids() + queue.failed_job_registry.get_job_ids()
        n = len(job_ids)
        html += "<table><tr><th>ID</th><th>Schedule</th><th>Queued</th><th>Started</th><th>Finished</th><th>Canceled</th><th>Deffered</th><th>Failed</th></tr>"
        for id in job_ids:
            job = queue.fetch_job(id)
            if job:
                html += f'<tr><td><a href="job/{id}">{id}</a>:</td><td>{job.is_scheduled}</td><td>{job.is_queued}</td><td>{job.is_started}</td><td>{job.is_finished}</td><td>{job.is_canceled}</td><td>{job.is_deferred}</td><td>{job.is_failed}</td></tr>'
        html += "</table><br/>"
        html += f'Total {n} Jobs</p><hr /><br />'

        html += '</div>'

    return f"{html}"


@app.route('/job/<job_id>')
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
    app.run(host="0.0.0.0", debug=True)