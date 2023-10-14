from flask import Flask, request, jsonify
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

@app.route('/', methods=['POST'])
def job_receiver():
    if not request.data:
        print("Received request but no payload found", file=sys.stderr)
        return jsonify({'error': 'No payload found. You must submit a POST request via a DCS webhook notification.'}), 400

    # Bail if this is not from DCS
    if 'X-Gitea-Event' not in request.headers:
        print(f"No 'X-Gitea-Event' in {request.headers}", file=sys.stderr)
        return jsonify({'error': 'This does not appear to be from DCS.'}), 400

    event_type = request.headers['X-Gitea-Event']
    print(f"Got a '{event_type}' event from DCS", file=sys.stderr)

    # Get the json payload and check it
    payload = request.get_json()
    job = queue_new_job(payload)
    if job:
        return_dict = {'success': True,
                       'job_id': job.id,
                        'status': 'queued',
                        'queue_name': "door43_job_handler",
                       'door43_job_queued_at': datetime.datetime.utcnow()}
        return jsonify(return_dict)
    else:
        return jsonify({'error': 'Failed to queue job. See logs'}), 400    


@app.route('/', methods = ['GET'])
def status():
    f = open('./payload.json')
    payload = f.read()
    f.close()

    html = f'''
<form>
    Payload:<br/><textarea id="payload" rows="5" cols="50">{payload}</textarea>
    <br/>
    <input type="button" value="Queue Job" onClick="submitForm()"/>
</form>
'''
    html += '''<script type="text/javascript">
    function submitForm() {
        var xhr = new XMLHttpRequest();
        xhr.open("POST", "/", true);
        xhr.setRequestHeader('Content-Type', 'application/json; charset=UTF-8');
        xhr.setRequestHeader('X-Gitea-Event', 'push');
        xhr.setRequestHeader('X-Gitea-Event-Type', 'push')
        var input = document.getElementById("payload");
        xhr.onreadystatechange = () => {
            if (xhr.readyState === 4) {
                alert(xhr.response);
                console.log(xhr.response);
            }
        };
        console.log(xhr.send(payload.value));
    }
</script>
'''

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
    html += '<table cellpadding="10" colspacing="10" border="2"><tr>'
    for q_name in queue_names:
        html += f'<th>{q_name} Queue</th>'
    html += '</tr>'
    for status in status_order:
        html += '<tr>'
        for q_name in queue_names:
            html += f'<td style="vertical-align:top"><h3>{status.capitalize()} Registery</h3>'
            keys = sorted(rows[q_name][status].keys(), reverse=True)
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
    html += f'<form>'
    html += f'<textarea id="payload" cols="200" rows="20" id="payload">'
    try:
        html += json.dumps(job.args[0], indent=2)
    except:
        pass
    html += f'</textarea>'
    if queue_name == "door43_job_handler":
        html += "<br/><br/>"
        html += '''<input type="button" value="Re-Queue Job" onClick="submitForm()"/>
<script type="text/javascript">
    function submitForm() {
        var xhr = new XMLHttpRequest();
        xhr.open("POST", "../..", true);
        xhr.setRequestHeader('Content-Type', 'application/json; charset=UTF-8');
        xhr.setRequestHeader('X-Gitea-Event', 'push');
        xhr.setRequestHeader('X-Gitea-Event-Type', 'push')
        var input = document.getElementById("payload");
        xhr.send(payload.value);
    }
</script>
'''
    html += f'</form></p></div>'
    return html


def get_relative_time(start=None, end=None):
    if not end:
        end = datetime.datetime.utcnow()
    if not start:
        start = end
    print(start, file=sys.stderr)
    print(end, file=sys.stderr)
    ago = round((end - start).total_seconds())
    print(f"ago: {ago}", file=sys.stderr)
    t = "s"
    if ago > 120:
        ago = round(ago / 60)
        t = "m"
        if ago > 120:
            ago = round(ago / 60)
            t = "h"
            if ago > 48:
                ago = round(ago / 24)
                t = "d"
    return f"{ago}{t}"


def get_job_list_html(queue_name, job):
    orig_job_id = job.id.split('_')[-1]
    html = f'<a href="/job/{queue_name}/{job.id}">{orig_job_id[:5]}</a>: {get_dcs_link(job)}<br/>'
    if job.ended_at:
        timeago = f'{get_relative_time(job.ended_at)} ago'
        runtime = get_relative_time(job.started_at, job.ended_at)
        end_word = "canceled" if job.is_canceled else "failed" if job.is_failed else "finished"
        html += f'<div style="padding-left:5px;font-style:italic;" title="started: {job.started_at.strftime("%Y-%m-%d %H:%M:%S")}; {end_word}: {job.ended_at.strftime("%Y-%m-%d %H:%M:%S")}">ran for {runtime}, {end_word} {timeago}</div>'
    elif job.is_started:
        timeago = f'{get_relative_time(job.started_at)} ago'
        html += f'<div style="padding-left:5px;font-style:italic"  title="started: {job.started_at.strftime("%Y-%m-%d %H:%M:%S")}">started {timeago}</div>'
    elif job.is_queued:
        timeago = f'{get_relative_time(job.queued_at)}'
        html += f'<div style="padding-left:5px;font-style:italic;" title="queued: {job.queued_at.strftime("%Y-%m-%d %H:%M:%S")}">queued for {timeago}</div>'
    elif job.get_status(True) == "scheduled":
        timeago = f'{get_relative_time(job.created_at)}'
        html += f'<div style="padding-left:5px;font-style:italic;" title="scheduled: {job.created_at.strftime("%Y-%m-%d %H:%M:%S")}">schedued for {timeago}</div>'
    else:
        timeago = f'{get_relative_time(job.created_at)} ago'
        html += f'<div style="padding-left:5px;font-style:italic;" title="created: {job.created_at.strftime("%Y-%m-%d %H:%M:%S")}">created {timeago}, status: {job.get_status()}</div>'
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


def queue_new_job(payload):
    remove_similar_jobs(payload, ["ref", "repo.full_name"])
    if not payload or "ref" not in payload or "repository" not in payload:
        return None

    try:
        if 'ref' in payload and "refs/tags" not in payload['ref'] and "master" not in payload['ref']:
            job = djh_queue.enqueue_in(datetime.timedelta(seconds=10), "webhook.job", payload, result_ttl=(60*60*24))
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