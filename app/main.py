from flask import Flask, request
import redis
from rq import Queue, command, cancel_job
import sys
import datetime
import json
from functools import reduce

app = Flask(__name__)

redis_connection = redis.Redis(host='redis', port=6379, db=0)
djh_queue = Queue('door43_job_handler', connection=redis_connection)
tjh_queue = Queue('tx_job_handler', connection=redis_connection)

@app.route('/', methods = ['POST', 'GET'])
def status():
    print(djh_queue, tjh_queue, file=sys.stderr)

    f = open('./payload.json')
    default_payload = f.read()
    f.close()

    delayStr = request.form.get('delay', 10)
    is_user_branch = request.form.get('is_user_branch', False)
    scheduleSecondsStr = request.form.get('schedule_seconds', 10)
    payload = json.loads(request.form.get('payload', default_payload))

    try:
        delay = int(delayStr)
    except:
        delay = 10

    try:
        schedule_seconds = int(scheduleSecondsStr)
    except:
        schedule_seconds = 10

    html = f'''
<form method="POST">Time Working: <input type="text" name="delay" value="{delayStr}" />
    <br/>
    <input type="checkbox" name="is_user_branch" checked="{is_user_branch}"/> Is user branch, schedule for <input type="text" name="schedule_seconds" value="{schedule_seconds}" /> seconds
    <br/>
    Payload:<br/><textarea name="payload" rows=5 cols="50">{json.dumps(payload, indent=2) if payload else ""}</textarea>
    <br/>
    <input type="submit" value="Queue Job"/>
</form>'''

    if request.method == 'POST':
        payload['delay'] = delay
        job = queue_new_job(payload, is_user_branch, schedule_seconds)
        if job:
            html += f"<p><b>Status:</b> {'Scheduled' if is_user_branch else 'Queued'} new job: {job.id}</p>" 

    queue_names = ["door43_job_handler", "tx_job_handler"]
    status_order = ["scheduled", "enqueued", "started", "finished", "failed", 'canceled']
    rows = {}
    for q_name in queue_names:
        queue = Queue(q_name, connection=redis_connection)
        rows[q_name] = {}
        for status in status_order:
            rows[q_name][status] = {}
        for id in queue.scheduled_job_registry.get_job_ids():
            job = queue.fetch_job(id)
            if job:
                rows[q_name]["scheduled"][job.created_at.strftime(f'%Y-%m-%d %H:%M:%S {job.id}')] = get_job_list_html(q_name, job)
        for job in queue.get_jobs():
            rows[q_name]["enqueued"][job.created_at.strftime(f'%Y-%m-%d %H:%M:%S {job.id}')] = get_job_list_html(q_name, job)
        for id in queue.started_job_registry.get_job_ids():
            job = queue.fetch_job(id)
            if job:
                rows[q_name]["started"][job.created_at.strftime(f'%Y-%m-%d %H:%M:%S {job.id}')] = get_job_list_html(q_name, job)
        for id in queue.finished_job_registry.get_job_ids():
            job = queue.fetch_job(id)
            if job:
                rows[q_name]["finished"][job.created_at.strftime(f'%Y-%m-%d %H:%M:%S {job.id}')] = get_job_list_html(q_name, job)
        for id in queue.failed_job_registry.get_job_ids():
            job = queue.fetch_job(id)
            if job:
                rows[q_name]["failed"][job.created_at.strftime(f'%Y-%m-%d %H:%M:%S {job.id}')] = get_job_list_html(q_name, job)
        for id in queue.canceled_job_registry.get_job_ids():
            job = queue.fetch_job(id)
            if job:
                rows[q_name]["canceled"][job.created_at.strftime(f'%Y-%m-%d %H:%M:%S {job.id}')] = get_job_list_html(q_name, job)
    html += '<table cellpadding=10 colspacing=10 border=2><tr>'
    for q_name in queue_names:
        html += f'<th>{q_name} Queue</th>'
    html += '</tr>'
    for status in status_order:
        html += '<tr>'
        for q_name in queue_names:
            html += f'<td style="vertical-align:top"><h3>{status.capitalize()} Registery</h3>'
            keys = rows[q_name][status].keys()
            sorted(keys)
            for key in keys:
                html += rows[q_name][status][key]
            html += '</td>'
        html += '</tr>'
    html += '</table><br/><br/>'
    return html


@app.route("/job/<queue_name>/<job_id>", methods=['GET'])
def getJob(queue_name, job_id):
    queue = Queue(queue_name, connection=redis_connection)
    job = queue.fetch_job(job_id)
    if not job or not job.args:
        return f"<h1>JOB {job_id} NOT FOUND IN QUEUE {queue_name}</h1>"
    repo = get_repo_from_job(job)
    type = get_ref_type_from_job(job)
    ref = get_ref_from_job(job)
    html = f'<p><a href="../../" style="text-decoration:none"><- Go back</a></p>' 
    html += f'<h1>JOB ID: {job_id.split("_")[-1]} ({queue_name})</h1>'
    html += f'<h2><b>Repo:</b> <a href="https://git.door43.org/{repo}/src/{type}/{ref}" target="_blank">{repo}</a></h2>'
    html += f'<h3>{get_ref_type_from_job(job)}: {get_ref_from_job(job)}</h3>'
    html += f'<p>Status: {job.get_status()}<br/>'
    if job.enqueued_at:
        html += f'Enqued at: {job.enqueued_at}{f" ({job.get_position()})" if job.is_queued else ""}<br/>'
    if job.started_at:
        html += f'Started: {job.started_at}<br/>'
    if job.ended_at:
        html += f'Ended: {job.ended_at} {round((job.ended_at-job.enqueued_at).total_seconds() / 60)}'
    if job.is_failed:
        html += f"<div><b>Latest Result</b><p><pre>{job.exc_info}</pre></p></div>"
    html += f'<div><p><b>Payload:</b>'
    html += f'<form method="POST" action"../../">'
    html += f'<textarea cols=200 rows=20>'
    try:
        html += json.dumps(job.args[0], indent=2)
    except:
        pass
    html += f'</textarea>'
    html += f'<br/><br/><input type="submit" value="Queue again" />'
    html += f'</form></p></div>'
    return html


def get_job_list_html(queue_name, job):
    html = f'<a href="/job/{queue_name}/{job.id}">{job.id.split("_")[-1][:5]}</a>: {get_dcs_link(job)}<br/>'
    times = []
    if job.created_at:
        times.append(f'created {job.created_at.strftime("%Y-%m-%d %H:%M:%S")}')
    if job.enqueued_at:
        times.append(f'enqued {job.enqueued_at.strftime("%Y-%m-%d %H:%M:%S")}')
    if job.started_at:
        times.append(f'started {job.started_at.strftime("%Y-%m-%d %H:%M:%S")}')
    if job.ended_at:
        times.append(f'ended {job.started_at.strftime("%Y-%m-%d %H:%M:%S")} ({round((job.ended_at-job.enqueued_at).total_seconds() / 60)})')
    if len(times) > 0:
        html += '<div style="font-style: italic; color: #929292">'
        html += ';<br/>'.join(times)
        html += '</div>'
    return html


def get_repo_from_job(job):
    if not job or not job.args:
        return None
    payload = job.args[0]
    if "repo_name" in payload and "repo_owner" in payload:
        return f'{payload["repo_owner"]}/{payload["repo_name"]}'
    elif "repository" in payload and "full_name" in payload["repository"]:
        return payload["repository"]["full_name"]

  
def get_ref_from_job(job):  
    if not job or not job.args:
        return None
    payload = job.args[0]
    if "repo_ref" in payload and "repo_ref_type" in payload:
        return payload["repo_ref"]
    elif "ref" in payload:
        ref_parts = payload["ref"].split("/")
        return ref_parts[-1]


def get_ref_type_from_job(job):
    if not job or not job.args:
        return None
    payload = job.args[0]
    if "repo_ref" in payload and "repo_ref_type" in payload:
        return payload["repo_ref_type"]
    elif "ref" in payload:
        ref_parts = payload["ref"].split("/")
        if ref_parts[1] == "tags":
            return "tag"
        else:
            return "branch"


def get_dcs_link(job):
    repo = get_repo_from_job(job)
    ref = get_ref_from_job(job)
    type = get_ref_type_from_job(job)
    if not repo or not ref:
        return 'INVALID'
    return f'<a href="https://git.door43.org/{repo}/src/{type}/{ref}" target="_blank">{repo.split("/")[-1]}=>{ref}</a>'


def queue_new_job(payload, is_user_branch, schedule_seconds):
    remove_similar_jobs(payload, ["ref", "repo.full_name"])
    try:
        if is_user_branch:
            job = djh_queue.enqueue_in(datetime.timedelta(seconds=schedule_seconds), "webhook.job", payload, result_ttl=(60*60*24))
        else:
            job = djh_queue.enqueue("webhook.job", payload, result_ttl=(60*60*24))
        return job
    except Exception as e:
        print("Failed to queue", file=sys.stderr)
        print(e, file=sys.stderr)


def remove_similar_jobs(payload, matching_fields):
    for queue in [djh_queue, tjh_queue]:
        job_ids = queue.scheduled_job_registry.get_job_ids() + queue.get_job_ids() + queue.started_job_registry.get_job_ids()
        print("JOB IDS: ", job_ids, file=sys.stderr)
        for job_id in job_ids:
            print("JOB ID "+job_id, file=sys.stderr)
            job = queue.fetch_job(job_id)
            if job:
                print("GOT JOB", file=sys.stderr)
                old_payload = job.args[0]
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