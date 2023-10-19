import os
import redis
import logging
import json
from datetime import datetime, timedelta
from flask import Flask, render_template, url_for, request, jsonify, session
from flask_sqlalchemy import SQLAlchemy
from rq import Queue
from rq.registry import FailedJobRegistry

PREFIX = ""
WEBHOOK_URL_SEGMENT = ""
LOGGING_NAME = 'door43_enqueue_job' # Used for logging
DOOR43_JOB_HANDLER_QUEUE_NAME = 'door43_job_handler' # The main queue name for generating HTML, PDF, etc. files (and graphite name) -- MUST match setup.py in door43-job-handler. Will get prefixed for dev
DOOR43_JOB_HANDLER_CALLBACK_QUEUE_NAME = 'door43_job_handler_callback'
PREFIXED_LOGGING_NAME = PREFIX + LOGGING_NAME


logger = logging.getLogger(PREFIXED_LOGGING_NAME)
basedir = os.path.abspath(os.path.dirname(__file__))
redis_connection = redis.Redis(host='redis', port=6379, db=0)

app = Flask(__name__)

# app.config['SQLALCHEMY_DATABASE_URI'] = 'sqlite:///' + os.path.join(basedir, 'status_data.db')
# app.config['SQLALCHEMY_COMMIT_ON_TEARDOWN'] = True
# db = SQLAlchemy(app)
# class StoreSearchData(db.Model):
#   __tablename__ = 'status_data'
#   id = db.Column('id', db.Integer, primary_key = True)
#   timestamp = db.Column('timestamp', db.DateTime)
#   repo = db.Column('repo', db.String(50))
#   ref = db.Column('ref', db.String(50))
#   event = db.Column('event', db.String(50))
#   job_id = db.Column('job_id', db.String(50))

#   def __init__(self, repo, ref, event, job_id):
#     self.timestamp = datetime.now()
#     self.repo = repo
#     self.ref = ref
#     self.event = event
#     self.job_id = job_id


@app.route('/'+WEBHOOK_URL_SEGMENT, methods=['POST'])
def job_receiver():
    if not request.data:
        logger.error("Received request but no payload found")
        return jsonify({'error': 'No payload found. You must submit a POST request via a DCS webhook notification.'}), 400

    # Bail if this is not from DCS
    if 'X-Gitea-Event' not in request.headers:
        logger.error(f"No 'X-Gitea-Event' in {request.headers}")
        return jsonify({'error': 'This does not appear to be from DCS.'}), 400

    event_type = request.headers['X-Gitea-Event']
    logger.error(f"Got a '{event_type}' event from DCS")

    # Get the json payload and check it
    payload = request.get_json()
    payload["DCS_event"] = event_type
    job = queue_new_job(payload)
    if job:
        return_dict = {'success': True,
                       'job_id': job.id,
                        'status': 'queued',
                        'queue_name': "door43_job_handler",
                       'door43_job_queued_at': datetime.now()}
        return jsonify(return_dict)
    else:
        return jsonify({'error': 'Failed to queue job. See logs'}), 400    


def queue_new_job(response_dict):
    if not response_dict or "ref" not in response_dict or "repository" not in response_dict:
        return None

    cancel_similar_jobs(response_dict)

    try:
        queue = Queue("door43_job_handler", connection=redis_connection)
        if 'ref' in response_dict and "refs/tags" not in response_dict['ref'] and "master" not in response_dict['ref'] and response_dict["DCS_event"] == "push":
            job = queue.enqueue_in(timedelta(seconds=10), "webhook.job", response_dict, result_ttl=(60*60*24))
        else:
            job = queue.enqueue("webhook.job", response_dict, result_ttl=(60*60*24))
        return job
    except Exception as e:
        logger.error("Failed to queue")
        logger.error(e)
        logger.error("HI")


#### COPY THIS AND BELOW!!!!! #####

queue_names = ["door43_job_handler", "tx_job_handler", "tx_job_handler_priority", "tx_job_handler_pdf", "door43_job_handler_callback"]
registry_names = ["scheduled", "enqueued", "started", "finished", "failed", 'canceled']
reg_colors = {
    "scheduled": "primary",
    "enqueued": "secondary",
    "started": "success",
    "finished": "info",
    "failed": "danger",
    "canceled": "light"
}
basedir = os.path.abspath(os.path.dirname(__file__))


@app.route('/'+WEBHOOK_URL_SEGMENT, methods = ['GET'])
def homepage():
    return 'Go to the  <a href="status/">status page</a>'


@app.route('/' + WEBHOOK_URL_SEGMENT + 'status/', methods = ['GET'])
def status_page():
#   if not os.path.exists(os.path.join(basedir, 'status_data.db')):
#     db.create_all()
  f = open(os.path.join(basedir, 'payload.json'))
  payload = f.read()
  f.close()
  
  repo = request.args.get("repo", "")
  ref = request.args.get("ref", "")
  event = request.args.get("event", "")
  job_id = request.args.get("job_id", "")
  return render_template('index.html', payload=payload, repo=repo, ref=ref, event=event, job_id=job_id)


@app.route('/get_status_table_rows', methods=['POST'])
def get_status_table():
    status_data = request.get_json()
    repo_filter = status_data['repo']
    ref_filter = status_data['ref']
    event_filter = status_data['event']
    job_id_filter = status_data['job_id']

    logger.error(status_data)      

    # db.session.add(StoreSearchData(repo, ref, event, job_id))
    # db.session.commit()
    # numJobs = db.session.query(StoreSearchData).count()

    table_rows = {}
    for r_name in registry_names:
        table_rows[r_name] = {
            "name": r_name,
            "color": reg_colors[r_name],
            "rows": [],
        }   
        r_data = {}
        job_created = {}
        for q_name in queue_names:
            r_data[q_name] = {}
            queue = Queue(PREFIX+q_name, connection=redis_connection)

            if r_name == "scheduled":
                job_ids = queue.scheduled_job_registry.get_job_ids()
            elif r_name == "enqueued":
                job_ids = queue.get_job_ids()
            elif r_name == "started":
                job_ids = queue.started_job_registry.get_job_ids()
            elif r_name == "finished":
                job_ids = queue.finished_job_registry.get_job_ids()
            elif r_name == "failed":
                job_ids = queue.failed_job_registry.get_job_ids()
            elif r_name == "canceled":
                job_ids = queue.canceled_job_registry.get_job_ids()

            for job_id in job_ids:
                orig_job_id = job_id.split('_')[-1]
                job = queue.fetch_job(job_id)
                if not job or not job.args:
                    continue
                if job_id_filter and job_id_filter != orig_job_id:
                    continue
                repo = get_repo_from_payload(job.args[0])
                ref_type = get_ref_type_from_payload(job.args[0])
                ref = get_ref_from_payload(job.args[0])
                event = get_event_from_payload(job.args[0])
                if (repo_filter and repo_filter != repo) \
                    or (ref_filter and ref_filter != ref) \
                    or (event_filter and event_filter != event):
                    continue
                if orig_job_id not in job_created:
                    job_created[orig_job_id] = job.created_at
                r_data[q_name][orig_job_id] = {
                    "job_id": orig_job_id,
                    "created_at": job.created_at,
                    "enqueued_at": job.enqueued_at,
                    "started_at": job.started_at,
                    "ended_at": job.ended_at,
                    "is_scheduled": job.is_scheduled,
                    "is_queued": job.is_queued,
                    "is_started": job.is_started,
                    "is_finished": job.is_finished,
                    "is_failed": job.is_failed,
                    "is_canceled": job.is_canceled,
                    "status": job.get_status(),
                    "repo": repo,
                    "ref_type": ref_type,
                    "ref": ref,
                    "event": event,
                }
        reverse_ordered_job_ids = sorted(job_created.keys(), key=lambda id: job_created[id], reverse=True)
        if len(reverse_ordered_job_ids) == 0:
            continue
        for orig_job_id in reverse_ordered_job_ids:
            row_html = f'<tr class="table-{reg_colors[r_name]} {r_name}Row" aria-labelledby="{r_name}HeaderRow" data-parent="#{r_name}HeaderRow"><td scope="row">&nbsp;</th>'
            for q_name in queue_names:
                row_html += '<td style="vertical-align:top">'
                if orig_job_id in r_data[q_name]:
                    row_html += get_job_list_html(r_data[q_name][orig_job_id])
                else:
                    row_html += "&nbsp;"
                row_html += '</td>'
            row_html += '</tr>'
            table_rows[r_name]["rows"].append(row_html)
    results = {'table_rows': table_rows}
    return jsonify(results)


@app.route('/'+WEBHOOK_URL_SEGMENT+"status/job/<job_id>", methods=['GET'])
def getJob(job_id):
    job_datas = []
    for q_name in queue_names:
        queue = Queue(PREFIX+q_name, connection=redis_connection)
        prefix = ""
        if q_name != DOOR43_JOB_HANDLER_QUEUE_NAME:
            prefix = f'{PREFIX}{q_name}_'
        job = queue.fetch_job(f'{prefix}{job_id}')
        if not job or not job.args:
            continue
        job_data = {
            "queue_name": q_name,
            "created_at": job.created_at,
            "enqueued_at": job.enqueued_at,
            "started_at": job.started_at,
            "ended_at": job.ended_at,
            "is_scheduled": job.is_scheduled,
            "is_queued": job.is_queued,
            "is_started": job.is_started,
            "is_finished": job.is_finished,
            "is_failed": job.is_failed,
            "is_canceled": job.is_canceled,
            "status": job.get_status(),
            "repo": get_repo_from_payload(job.args[0]),
            "ref_type": get_ref_type_from_payload(job.args[0]),
            "ref": get_ref_from_payload(job.args[0]),
            "event": get_event_from_payload(job.args[0]),
            "canceled": job.args[0]["canceled"] if "canceled" in job.args[0] else [],
            "canceled_by": None,
            "result": job.result,
            "error": job.exc_info,
            "payload": job.args[0],
        }
        for r_name in registry_names:
            if r_name == "scheduled":
                job_ids = queue.scheduled_job_registry.get_job_ids()
            if r_name == "enqueued":
                job_ids = queue.get_job_ids()
            if r_name == "started":
                job_ids = queue.started_job_registry.get_job_ids()
            if r_name == "finished":
                job_ids = queue.finished_job_registry.get_job_ids()
            if r_name == "failed":
                job_ids = queue.failed_job_registry.get_job_ids()
            if r_name == "canceled":
                job_ids = queue.canceled_job_registry.get_job_ids()
            for id in job_ids:
                j = queue.fetch_job(id)
                if not j or not j.args:
                    continue
                orig_id = id.split('_')[-1]
                if "canceled" in j.args[0] and job_id in j.args[0]["canceled"]:
                    job_data["canceled_by"] = orig_id
        job_datas.append(job_data)

    html = f'<p><a href="../" style="text-decoration:none">&larr; Go back</a></p>'

    if len(job_datas) == 0:
        return f'{html}<h1>JOB {job_id} NOT FOUND!</h1>'

    jobs_html = ""
    for job_data in job_datas:
        jobs_html += get_queue_job_info_html(job_data)
    
    first_job = job_datas[0]
    last_job = job_datas[-1]

    html += f'<p><a href="../?job_id={job_id}" style="text-decoration:none">&larr; See only this job in queues</a></p>'
    html += f'<h1>JOB ID: {job_id}</h1>'
    html += "<p>"
    html += f'<b>Repo:</b> <a href="https://git.door43.org/{first_job["repo"]}/src/{first_job["ref_type"]}/{first_job["ref"]}" target="_blank">{first_job["repo"]}</a><br/>'
    html += f'<b>{first_job["ref_type"].capitalize()}:</b> {first_job["ref"]}<br/>'
    html += f'<b>Event:</b> {first_job["event"]}'
    html += f'</p>'
    html += "<p>"
    html += "<h2>Overall Stats</h2>"
    html += f'<b>Status:</b> {last_job["status"]}<br/>'
    html += f'<b>Final Queue:</b> {last_job["queue_name"]}<br/><br/>'
    html += f'<b>Created at:</b>{first_job["created_at"].strftime("%Y-%m-%d %H:%M:%S")}<br/>'
    if first_job["started_at"]:
        html += f'<b>Started at:</b> {first_job["started_at"].strftime("%Y-%m-%d %H:%M:%S")}<br/>'
    html += f'{get_job_final_status_and_time(first_job["created_at"], last_job)}<br/>'
    if last_job["canceled_by"]:
        html += f'<b>Canceled by a similar job:</b> <a href="{last_job["canceled_by"]}">{last_job["canceled_by"]}</a><br/>'
    if len(first_job["canceled"]) > 0:
        jobs_canceled = []
        for id in first_job["canceled"]:
            id = id.split('_')[-1]
            jobs_canceled.append(f'<a href="{id}">{id}</a>')
        html += f'<b>This job canceled previous jobs(s):</b> {", ".join(jobs_canceled)}<br/>'
    html += "</p>"
    if last_job["is_failed"] or last_job["error"]:
        html += f'<div><b>ERROR:</b><p><pre>{last_job["error"]}</pre></p></div>'
    elif last_job["result"]:
        html += f'<div><b>Result:</b><p>{last_job["result"]}</p></div>'
    html += jobs_html

    return html


@app.route('/'+WEBHOOK_URL_SEGMENT+"status/clear/failed", methods=['GET'])
def clearFailed():
    hours = request.args.get("hours", 24)
    for queue_name in queue_names:
        queue = Queue(queue_name, connection=redis_connection)
        for job_id in queue.failed_job_registry.get_job_ids():
            job = queue.fetch_job(job_id)
            if job and (datetime.now() - job.ended_at) >= timedelta(hours=hours):
                job.delete()
    return "Failed jobs cleared"


@app.route('/'+WEBHOOK_URL_SEGMENT+"status/clear/canceled", methods=['GET'])
def clearCanceled():
    hours = request.args.get("hours", 3)
    for queue_name in queue_names:
        queue = Queue(queue_name, connection=redis_connection)
        for job_id in queue.canceled_job_registry.get_job_ids():
            job = queue.fetch_job(job_id)
            if job and (datetime.now() - job.created_at) >= timedelta(hours=hours):
                job.delete()
    return "Canceled jobs cleared"


#### FUNCS FOR GENERATING TABLE

def get_queue_job_info_html(job_data):
    html = f'<h2>Queue: {PREFIX+job_data["queue_name"]}</h2>'
    html += f'<div><b>Status:</b> {job_data["status"]}<br/>'
    if job_data["created_at"]:
        html += f'<b>Created at:</b> {job_data["created_at"].strftime("%Y-%m-%d %H:%M:%S")}<br/>'
    if job_data["enqueued_at"]:
        html += f'<b>Enqued at:</b> {job_data["enqueued_at"].strftime("%Y-%m-%d %H:%M:%S")}<br/>'
    if job_data["started_at"]:
        html += f'<b>Started:</b> {job_data["started_at"].strftime("%Y-%m-%d %H:%M:%S")}<br/>'
    if job_data["ended_at"]:
        if job_data["status"] == "failed":
            html += f'<b>Failed:</b> '
        else:
            html += f'<b>Ended:</b> '
        html += f'{job_data["ended_at"].strftime("%Y-%m-%d %H:%M:%S")} ({get_relative_time(job_data["started_at"], job_data["ended_at"])})<br/>'
    html += "</div>"
    if job_data["result"]:
        html += f'<div style="padding-top: 10px"><b>Result:</b><br/>{job_data["result"]}</div>'
    if job_data["payload"]:
        html += f'<div><p><b>Payload:</b>'
        html += f'<form>'
        html += f'<textarea id="payload" cols="200" rows="10" id="payload">'
        try:
            html += json.dumps(job_data["payload"], indent=2)
        except:
            html += f'{job_data["payload"]}'
        html += f'</textarea>'
    if job_data["queue_name"] == DOOR43_JOB_HANDLER_QUEUE_NAME:
        html += '''<br/><br/>
    <input type="button" value="Re-Queue Job" onClick="submitForm()"/>
<script type="text/javascript">
    function submitForm() {
        var payload = document.getElementById("payload");
        var payloadJSON = JSON.parse(payload.value);
        console.log(payloadJSON);
        console.log(payloadJSON["DCS_event"]);
        delete payloadJSON["canceled"];
        var xhr = new XMLHttpRequest();
        xhr.open("POST", "../../", true);
        xhr.setRequestHeader('Content-Type', 'application/json; charset=UTF-8');
        xhr.setRequestHeader('X-Gitea-Event', payloadJSON["DCS_event"]);
        xhr.setRequestHeader('X-Gitea-Event-Type', payloadJSON["DCS_event"]);
        xhr.onreadystatechange = () => {
            if (xhr.readyState === 4) {
                alert(xhr.response);
                console.log(xhr.response);
            }
        };
        xhr.send(JSON.stringify(payloadJSON));
    }
</script>
'''
    html += f'</form></p></div>'
    return html


def get_job_final_status_and_time(created_time, job_data):
    if job_data["is_scheduled"]:
        return f'<b>Scheduled at:</b> {job_data["created_at"].strftime("%Y-%m-%d %H:%M:%S")}<br/>'+get_elapsed_time(created_time, job_data["created_at"])
    if job_data["is_queued"]:
        return f'<b>Enqeued at:</b> {job_data["enqueued_at"].strftime("%Y-%m-%d %H:%M:%S")}<br/>'+get_elapsed_time(created_time, job_data["enqueued_at"])
    if job_data["ended_at"]:
        if job_data["status"] == "failed":
            html = f'<b>Failed at:</b> '
        else:
            html = f'<b>Ended at:</b> '
        return f'{html}{job_data["ended_at"].strftime("%Y-%m-%d %H:%M:%S")}<br/>'+get_elapsed_time(created_time, job_data["ended_at"])
    if job_data["is_canceled"]:
        end_time = job_data["ended_at"] if job_data["ended_at"] else job_data["started_at"] if job_data["started_at"] else job_data["enqueued_at"] if job_data["enqueued_at"] else job_data["created_at"]
        return f'<b>Canceled at:</b> {job_data["created_at"].strftime("%Y-%m-%d %H:%M:%S")}<br/>'+get_elapsed_time(created_time, end_time)
    return ""


def get_elapsed_time(start, end):
    if not start or not end or start >= end:
        return ""
    return "<b>Elapsed Time:</b> "+get_relative_time(start, end)


def get_relative_time(start=None, end=None):
    if not end:
        end = datetime.now()
    if not start:
        start = end
    ago = round((end - start).total_seconds())
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


def get_job_list_html(job_data):
    job_id = job_data["job_id"]
    html = f'<a href="job/{job_id}">{job_id[:5]}</a>: {get_job_list_filter_link(job_data)}<br/>'
    if job_data["ended_at"]:
        timeago = f'{get_relative_time(job_data["ended_at"])} ago'
        runtime = get_relative_time(job_data["started_at"], job_data["ended_at"])
        end_word = "canceled" if job_data["is_canceled"] else "failed" if job_data["is_failed"] else "finished"
        html += f'<div style="padding-left:5px;font-style:italic;" title="started: {job_data["started_at"].strftime("%Y-%m-%d %H:%M:%S")}; {end_word}: {job_data["ended_at"].strftime("%Y-%m-%d %H:%M:%S")}">ran for {runtime}, {end_word} {timeago}</div>'
    elif job_data["is_started"]:
        timeago = f'{get_relative_time(job_data["started_at"])} ago'
        html += f'<div style="padding-left:5px;font-style:italic"  title="started: {job_data["started_at"].strftime("%Y-%m-%d %H:%M:%S")}">started {timeago}</div>'
    elif job_data["is_queued"]:
        timeago = f'{get_relative_time(job_data["enqueued_at"])}'
        html += f'<div style="padding-left:5px;font-style:italic;" title="queued: {job_data["enqueued_at"].strftime("%Y-%m-%d %H:%M:%S")}">queued for {timeago}</div>'
    elif job_data["is_scheduled"]:
        timeago = f'{get_relative_time(job_data["created_at"])}'
        html += f'<div style="padding-left:5px;font-style:italic;" title="scheduled: {job_data["created_at"].strftime("%Y-%m-%d %H:%M:%S")}">schedued for {timeago}</div>'
    else:
        timeago = f'{get_relative_time(job_data["created_at"])} ago'
        html += f'<div style="padding-left:5px;font-style:italic;" title="created: {job_data["created_at"].strftime("%Y-%m-%d %H:%M:%S")}">created {timeago}, status: {job_data["status"]}</div>'
    return html


def get_repo_from_payload(payload):
    if not payload:
        return None
    if "repo_name" in payload and "repo_owner" in payload:
        return f'{payload["repo_owner"]}/{payload["repo_name"]}'
    elif "repository" in payload and "full_name" in payload["repository"]:
        return payload["repository"]["full_name"]

  
def get_ref_from_payload(payload):
    if not payload:
        return None
    if "repo_ref" in payload and "repo_ref_type" in payload:
        return payload["repo_ref"]
    elif "ref" in payload:
        ref_parts = payload["ref"].split("/")
        return ref_parts[-1]
    elif "release" in payload and "tag_name" in payload["release"]:
        return payload["release"]["tag_name"]
    elif "DCS_event" in payload and payload["DCS_event"] == "fork":
        return "master"


def get_ref_type_from_payload(payload):
    if not payload:
        return None
    if "repo_ref" in payload and "repo_ref_type" in payload:
        return payload["repo_ref_type"]
    elif "ref" in payload:
        ref_parts = payload["ref"].split("/")
        if len(ref_parts) > 1:
            if ref_parts[1] == "tags":
                return "tag"
            elif ref_parts[1] == "heads":
                return "branch"
    elif "DCS_event" in payload:
        if payload["DCS_event"] == "fork":
            return "branch"
        elif payload["DCS_event"] == "release":
            return "tag"
    return "branch"


def get_event_from_payload(payload):
    if not payload:
        return None
    if 'DCS_event' in payload:
        return payload['DCS_event']
    else:
        return 'push'


def get_job_list_filter_link(job_data):
    repo = job_data["repo"]
    ref = job_data["ref"]
    event = job_data["event"]
    return f'<a href="javascript:void(0)" onClick="filterTable(\'{job_data["repo"]}\')" title="{job_data["repo"]}">'+ \
            f'{job_data["repo"].split("/")[-1]}&#128172;</a>'+ \
            f'=><a href="javascript:void(0)" onClick="filterTable(\'{job_data["repo"]}\', \'{ref}\')">'+ \
            f'{ref}'+ \
            f'</a>'+ \
            f'=><a href="javascript:void(0)" onClick="filterTable(\'{job_data["repo"]}\', \'{job_data["ref"]}\', \'{job_data["event"]}\')">'+ \
            f'{job_data["event"]}'+ \
            f'</a>'


def get_dcs_link(job_data):
    if not job_data["repo"]:
        return 'INVALID'
    text = f'{job_data["repo"].split("/")[-1]}=>{job_data["repo"]}=>{job_data["ref"]}'
    if job_data["event"] != "delete":
        return f'<a href="https://git.door43.org/{job_data["repo"]}/src/{job_data["ref_type"]}/{job_data["ref"]}" target="_blank" title="{job_data["repo"]}/src/{job_data["type"]}/{job_data["ref"]}">{text}</a>'
    else:
        return text


# If a job has the same repo.full_name and ref that is already scheduled or queued, we cancel it so this one takes precedence
def cancel_similar_jobs(incoming_payload):
    if not incoming_payload or 'repository' not in incoming_payload or 'full_name' not in incoming_payload['repository'] or 'ref' not in incoming_payload:
        return
    logger.info("Checking if similar jobs already exist further up the queue to cancel them...")
    logger.info(incoming_payload)
    my_repo = get_repo_from_payload(incoming_payload)
    my_ref = get_ref_from_payload(incoming_payload)
    my_event = get_event_from_payload(incoming_payload)
    if not my_repo or not my_ref or my_event != "push":
        return
    for queue_name in queue_names:
        # Don't want to cancel anything being called back - let it happen
        if queue_name == DOOR43_JOB_HANDLER_CALLBACK_QUEUE_NAME:
            continue
        queue = Queue(PREFIX + queue_name, connection=redis_connection)
        job_ids = queue.scheduled_job_registry.get_job_ids() + queue.get_job_ids() + queue.started_job_registry.get_job_ids()
        for job_id in job_ids:
            job = queue.fetch_job(job_id)
            if job and len(job.args) > 0:
                pl = job.args[0]
                old_repo = get_repo_from_payload(pl)
                old_ref = get_ref_from_payload(pl)
                old_event = get_event_from_payload(pl)
                if my_repo == old_repo and my_ref == old_ref and old_event == "push":
                        logger.info(f"Found older job for repo: {old_repo}, ref: {old_ref}")
                        try:
                            job.cancel()
                            # stats_client.incr(f'{enqueue_job_stats_prefix}.canceled')
                            logger.info(f"CANCELLED JOB {job.id} ({job.get_status()}) IN QUEUE {queue.name} DUE TO BEING SIMILAR TO NEW JOB")
                            if "canceled" not in incoming_payload:
                                incoming_payload["canceled"] = []
                            incoming_payload["canceled"].append(job.id)
                        except:
                            pass
# end of cancel_similar_jobs function


#### COPY THIS AND ABOVE ONLY!!!! ######


if __name__ == "__main__":
    app.run(host="0.0.0.0", debug=True)

