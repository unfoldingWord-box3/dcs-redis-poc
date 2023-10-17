from flask import Flask, request, jsonify
import redis
from rq import Queue, command, cancel_job
from rq.registry import FailedJobRegistry
import sys
from datetime import datetime, timedelta
import json
from functools import reduce
import logging

app = Flask(__name__)

redis_connection = redis.Redis(host='redis', port=6379, db=0)

PREFIX = ""
WEBHOOK_URL_SEGMENT = ""

LOGGING_NAME = 'door43_enqueue_job' # Used for logging
DOOR43_JOB_HANDLER_QUEUE_NAME = 'door43_job_handler' # The main queue name for generating HTML, PDF, etc. files (and graphite name) -- MUST match setup.py in door43-job-handler. Will get prefixed for dev
DOOR43_JOB_HANDLER_CALLBACK_QUEUE_NAME = 'door43_job_handler_callback'
PREFIXED_LOGGING_NAME = PREFIX + LOGGING_NAME


logger = logging.getLogger(PREFIXED_LOGGING_NAME)

######
###### COPY FROM HERE TO DO NOT COPY
######

queue_names = ["door43_job_handler", "tx_job_handler", "tx_job_handler_priority", "tx_job_handler_pdf", "door43_job_handler_callback"]
queue_desc = {
    DOOR43_JOB_HANDLER_QUEUE_NAME: "Lints files & massages files for tX, uploads to cloud, sends work request to tx_job_handler",
    "tx_job_handler": "Handles branches that are not master (user branches), converts to HTML, uploads result to cloud, sends a work request to door43 callback",
    "tx_job_handler_priority": "Handles master branch and tags (releass), converting to HTML, uploads result to cloud, sends a work request to door43 callback",
    "tx_job_handler_pdf": "Handles PDF requests, converting to PDF, uploads result to cloud, sends a work request door43 callback",
    DOOR43_JOB_HANDLER_CALLBACK_QUEUE_NAME: "Fetches coverted files from cloud, deploys to Door43 Preview (door43.org) for HTML and PDF jobs",
}
registry_names = ["scheduled", "enqueued", "started", "finished", "failed", 'canceled']

@app.route('/' + WEBHOOK_URL_SEGMENT + 'sbmit/', methods = ['GET'])
def getSubmitForm():
    f = open('./payload.json')
    payload = f.read()
    f.close()

    html += f'''
<form>
    <b>Payload:</b><br/><textarea id="payload" rows="5" cols="50">{payload}</textarea>
    <br/>
    <b>DCS_event:</b> <input type="text" id="DCS_event" value="push" />
    <br/>
    <input type="button" value="Queue Job" onClick="submitForm()"/>
</form>
'''
    html += '''<script type="text/javascript">
    function submitForm() {
        var payload = document.getElementById("payload");
        var dcs_event = document.getElementById("DCS_event");
        var xhr = new XMLHttpRequest();
        xhr.open("POST", "/", true);
        xhr.setRequestHeader('Content-Type', 'application/json; charset=UTF-8');
        xhr.setRequestHeader('X-Gitea-Event', dcs_event.value);
        xhr.setRequestHeader('X-Gitea-Event-Type', dcs_event.value)
        xhr.onreadystatechange = () => {
            if (xhr.readyState === 4) {
                alert(xhr.response);
                console.log(xhr.response);
            }
        };
        console.log(payload.value);
        console.log(event.value);
        xhr.send(payload.value);
    }
</script>
'''
    return html


@app.route('/' + WEBHOOK_URL_SEGMENT + "status/", methods = ['GET'])
def getStatusTable():
    html = ""
    job_id_filter = request.args.get("job_id")
    repo_filter = request.args.get("repo")
    ref_filter = request.args.get("ref")
    event_filter = request.args.get("event")
    show_canceled = request.args.get("show_canceled",  default=False, type=bool)

    f = open('./payload.json')
    payload = f.read()
    f.close()

    html += f'''
<form>
    <b>Payload:</b><br/><textarea id="payload" rows="5" cols="50">{payload}</textarea>
    <br/>
    <b>DCS_event:</b> <input type="text" id="DCS_event" value="push" />
    <br/>
    <input type="button" value="Queue Job" onClick="submitForm()"/>
</form>
'''
    html += '''<script type="text/javascript">
    function submitForm() {
        var payload = document.getElementById("payload");
        var dcs_event = document.getElementById("DCS_event");
        var xhr = new XMLHttpRequest();
        xhr.open("POST", "/", true);
        xhr.setRequestHeader('Content-Type', 'application/json; charset=UTF-8');
        xhr.setRequestHeader('X-Gitea-Event', dcs_event.value);
        xhr.setRequestHeader('X-Gitea-Event-Type', dcs_event.value)
        xhr.onreadystatechange = () => {
            if (xhr.readyState === 4) {
                alert(xhr.response);
                console.log(xhr.response);
            }
        };
        console.log(payload.value);
        console.log(event.value);
        xhr.send(payload.value);
    }
</script>
'''

    if len(request.args) > (1 if show_canceled else 0):
        html += f'<p>Table is filtered. <a href="?{"show_canceled=true" if show_canceled else ""}">Click to Show All Jobs</a></p>'

    html += f'<form METHOD="GET">'
    if repo_filter:
        html += f'<input type="hidden" name="repo" value="{repo_filter}"/>'
    if ref_filter:
        html += f'<input type="hidden" name="ref" value="{ref_filter}"/>'
    if event_filter:
        html += f'<input type="hidden" name="event" value="{event_filter}"/>'
    html += f'<input type="checkbox" name="show_canceled" value="true" onChange="this.form.submit()"  {"checked" if show_canceled else ""}/> Show canceled</form>'

    job_map = get_job_map(job_id_filter=job_id_filter, repo_filter=repo_filter, ref_filter=ref_filter, event_filter=event_filter, show_canceled=show_canceled)

    html += '<table cellpadding="10" colspacing="10" border="2"><tr>'
    for i, q_name in enumerate(queue_names):
        html += f'<th style="vertical-align:top">{q_name}{"&rArr;tx" if i==0 else "&rArr;callback" if i<(len(queue_names)-1) else ""}</th>'
    html += '</tr><tr>'
    for q_name in queue_names:
        html += f'<td style="font-style: italic;font-size:0.8em;vertical-align:top">{queue_desc[q_name]}</td>'
    html += '</tr>'
    for r_name in registry_names:
        if r_name == "canceled" and not show_canceled:
            continue
        html += '<tr>'
        for q_name in queue_names:
            html += f'<td style="vertical-align:top"><h3>{r_name.capitalize()} Registery</h3>'
            sorted_ids = sorted(job_map[q_name][r_name].keys(), key=lambda id: job_map[q_name][r_name][id]["job"].created_at, reverse=True)
            for id in sorted_ids:
                html += get_job_list_html(job_map[q_name][r_name][id]["job"])
            html += '</td>'
        html += '</tr>'
    html += '</table><br/><br/>'
    return html

def get_job_map(job_id_filter=None, repo_filter=None, ref_filter=None, event_filter=None, show_canceled=False):
    job_map = {}

    def add_to_job_map(queue_name, registry_name, job):
        if not job or not job.args:
            return
        repo = get_repo_from_payload(job.args[0])
        ref = get_ref_from_payload(job.args[0])
        event = get_event_from_payload(job.args[0])
        job_id = job.id.split('_')[-1]

        if (job_id_filter and job_id_filter != job_id) \
            or (repo_filter and repo_filter != repo) \
            or (ref_filter and ref_filter != ref) \
            or (event_filter and event_filter != event):
            return

        canceled = job.args[0]["canceled"] if "canceled" in job.args[0] else []
        if job_id not in job_map[queue_name][registry_name]:
            job_map[queue_name][registry_name][job_id] = {}
        job_map[queue_name][registry_name][job_id]["job"] = job
        job_map[queue_name][registry_name][job_id]["canceled"] = canceled

        for qn in job_map:
            for rn in job_map[qn]:
                for id in job_map[qn][rn]:
                    j = job_map[qn][rn][id]["job"]
                    c = job_map[qn][rn][id]["canceled"] if "canceled" in job_map[qn][rn][id]["job"].args[0] else []
                    if id != job_id and (job.is_canceled or j.is_canceled):
                        if id in canceled:
                            job_map[qn][rn][id]["canceled_by"] = job_id
                        elif job_id in c:
                            job_map[queue_name][registry_name][job_id]["canceled_by"] = id

    for queue_name in queue_names:
        queue = Queue(PREFIX + queue_name, connection=redis_connection)
        job_map[queue_name] = {}
        for registry_name in registry_names:
            job_map[queue_name][registry_name] = {}
        for id in queue.scheduled_job_registry.get_job_ids():
            if not job_id_filter or job_id_filter == id.split('_')[-1]:
                add_to_job_map(queue_name, "scheduled", queue.fetch_job(id))
        for job in queue.get_jobs():
            if not job_id_filter or job_id_filter == id.split('_')[-1]:
                add_to_job_map(queue_name, "enqueued", job)
        for id in queue.started_job_registry.get_job_ids():
            if not job_id_filter or job_id_filter == id.split('_')[-1]:
               add_to_job_map(queue_name, "started", queue.fetch_job(id))
        for id in queue.finished_job_registry.get_job_ids():
            if not job_id_filter or job_id_filter == id.split('_')[-1]:
                add_to_job_map(queue_name, "finished", queue.fetch_job(id))
        for id in queue.failed_job_registry.get_job_ids():
            if not job_id_filter or job_id_filter == id.split('_')[-1]:
                add_to_job_map(queue_name, "failed", queue.fetch_job(id))
        if show_canceled:
            if not job_id_filter or job_id_filter == id:
                for id in queue.canceled_job_registry.get_job_ids():
                    add_to_job_map(queue_name, "canceled", queue.fetch_job(id))
    return job_map


@app.route('/'+WEBHOOK_URL_SEGMENT+"status/job/<job_id>", methods=['GET'])
def getJob(job_id):
    html = f'<p><a href="../" style="text-decoration:none">&larr; Go back</a></p>'
    html += f'<p><a href="../?job_id={job_id}" style="text-decoration:none">&larr; See only this job in queues</a></p>'

    job = None
    for q_name in queue_names:
        logger.error(q_name)
        queue = Queue(PREFIX+q_name, connection=redis_connection)
        prefix = f'{q_name}_' if q_name != DOOR43_JOB_HANDLER_QUEUE_NAME else ""
        job = queue.fetch_job(prefix+job_id)
        break
    if not job or not job.args:
        return f"<h1>JOB NOT FOUND: {job_id}</h1>"

    job_map = get_job_map(repo_filter=get_repo_from_payload(job.args[0]), ref_filter=get_ref_from_payload(job.args[0]))

    jobs_html = ""
    job_infos = []
    last_queue = None
    last_job_info = None

    for queue_name in job_map:
        for registry_name in job_map[queue_name]:
            if job_id in job_map[queue_name][registry_name]:
                job_info = job_map[queue_name][registry_name][job_id]
                job_infos.append(job_info)
                last_queue = queue_name
                last_job_info = job_info
                jobs_html += get_queue_job_info_html(queue_name, registry_name, job_info)

    if len(job_infos) < 1:
        return html+"<h1>NO JOB FOUND!</h1>"
    
    job = job_infos[0]["job"]
    last_job = last_job_info["job"]
    repo = get_repo_from_payload(job.args[0])
    ref_type = get_ref_type_from_payload(job.args[0])
    ref = get_ref_from_payload(job.args[0])
    event = get_event_from_payload(job.args[0])

    html += f'<h1>JOB ID: {job_id}</h1>'
    html += "<p>"
    html += f'<b>Repo:</b> <a href="https://git.door43.org/{repo}/src/{ref_type}/{ref}" target="_blank">{repo}</a><br/>'
    html += f'<b>{ref_type.capitalize()}:</b> {ref}<br/>'
    html += f'<b>Event:</b> {event}'
    html += f'</p>'
    html += "<p>"
    html += "<h2>Overall Stats</h2>"
    html += f'<b>Status:</b> {last_job.get_status()}<br/>'
    html += f'<b>Final Queue:</b> {last_queue}<br/><br/>'
    html += f'<b>Created at:</b>{job.created_at.strftime("%Y-%m-%d %H:%M:%S")}<br/>'
    if job.started_at:
        html += f'<b>Started at:</b> {job.started_at.strftime("%Y-%m-%d %H:%M:%S")}<br/>'
    html += f'{get_job_final_status_and_time(job.created_at, last_job)}<br/>'
    if "canceled_by" in last_job_info:
        html += f'<b>Canceled by a similar job:</b> <a href="{last_job_info["canceled_by"]}">{last_job_info["canceled_by"]}</a><br/>'
    if "canceled" in job_info and len(job_info["canceled"]) > 0:
        jobs_canceled = []
        for id in job_info["canceled"]:
            jobs_canceled.append(f'<a href="{id}">{id}</a>')
        html += f'<b>This job canceled previous jobs(s):</b> {", ".join(jobs_canceled)}<br/>'
    html += "</p>"
    if last_job.is_failed or last_job.exc_info:
        html += f'<div><b>ERROR:</b><p><pre>{last_job.exc_info}</pre></p></div>'
    elif last_job.result:
        html += f'<div><b>Result:</b><p>{last_job.result}</p></div>'

    html += jobs_html

    return html


@app.route('/'+WEBHOOK_URL_SEGMENT+"status/clear/failed", methods=['GET'])
def clearFailed():
    for queue_name in queue_names:
        queue = Queue(queue_name, connection=redis_connection)
        for job_id in queue.failed_job_registry.get_job_ids():
            job = queue.fetch_job(job_id)
            job.delete()
    return "Failed jobs cleared"


@app.route('/'+WEBHOOK_URL_SEGMENT+"status/clear/canceled", methods=['GET'])
def clearCanceled():
    for queue_name in queue_names:
        queue = Queue(queue_name, connection=redis_connection)
        for job_id in queue.canceled_job_registry.get_job_ids():
            job = queue.fetch_job(job_id)
            job.delete()
    return "Canceled jobs cleared"


def get_queue_job_info_html(queue_name, registry_name, job_info):
    if not job_info:
        return ""
    job = job_info["job"]

    html = f"<h2>Queue: {PREFIX+queue_name}</h2>"
    html += f'<div><b>Status:</b> {job.get_status()}<br/>'
    if job.created_at:
        html += f'<b>Created at:</b> {job.created_at.strftime("%Y-%m-%d %H:%M:%S")}<br/>'
    if job.enqueued_at:
        html += f'<b>Enqued at:</b> {job.enqueued_at.strftime("%Y-%m-%d %H:%M:%S")}{f" (Position: {job.get_position()+1})" if job.is_queued else ""}<br/>'
    if job.started_at:
        html += f'<b>Started:</b> {job.started_at.strftime("%Y-%m-%d %H:%M:%S")}<br/>'
    if job.ended_at:
        if job.get_status() == "failed":
            html += f'<b>Failed:</b> '
        else:
            html += f'<b>Ended:</b> '
        html += f'{job.ended_at.strftime("%Y-%m-%d %H:%M:%S")} ({get_relative_time(job.started_at, job.ended_at)})<br/>'
    html += "</div>"
    if job.result:
        html += f'<div style="padding-top: 10px"><b>Result:</b><br/>{job.result}</div>'
    if job.args and job.args[0]:
        html += f'<div><p><b>Payload:</b>'
        html += f'<form>'
        html += f'<textarea id="payload" cols="200" rows="10" id="payload">'
        try:
            html += json.dumps(job.args[0], indent=2)
        except:
            html += f"{job.args[0]}"
        html += f'</textarea>'
    if queue_name == DOOR43_JOB_HANDLER_QUEUE_NAME:
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


def get_job_final_status_and_time(created_time, job):
    if job.is_scheduled:
        return f'<b>Scheduled at:</b> {job.created_at.strftime("%Y-%m-%d %H:%M:%S")}<br/>'+get_elapsed_time(created_time, job.created_at)
    if job.is_queued:
        return f'<b>Enqeued at:</b> {job.enqueued_at.strftime("%Y-%m-%d %H:%M:%S")}<br/>'+get_elapsed_time(created_time, job.enqueued_at)
    if job.ended_at:
        if job.get_status() == "failed":
            html = f'<b>Failed at:</b> '
        else:
            html = f'<b>Ended at:</b> '
        return f'{html}{job.ended_at.strftime("%Y-%m-%d %H:%M:%S")}<br/>'+get_elapsed_time(created_time, job.ended_at)
    if job.is_canceled:
        end_time = job.ended_at if job.ended_at else job.started_at if job.started_at else job.enqueued_at if job.enqueued_at else job.created_at
        return f'<b>Canceled at:</b> {job.created_at.strftime("%Y-%m-%d %H:%M:%S")}<br/>'+get_elapsed_time(created_time, end_time)
    return ""


def get_elapsed_time(start, end):
    if not start or not end or start >= end:
        return ""
    return "<b>Elapsed Time:</b> "+get_relative_time(start, end)


def get_relative_time(start=None, end=None):
    if not end:
        end = datetime.utcnow()
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


def get_job_list_html(job):
    job_id = job.id.split('_')[-1]
    html = f'<a href="job/{job_id}">{job_id[:5]}</a>: {get_job_list_filter_link(job)}<br/>'
    if job.ended_at:
        timeago = f'{get_relative_time(job.ended_at)} ago'
        runtime = get_relative_time(job.started_at, job.ended_at)
        end_word = "canceled" if job.is_canceled else "failed" if job.is_failed else "finished"
        html += f'<div style="padding-left:5px;font-style:italic;" title="started: {job.started_at.strftime("%Y-%m-%d %H:%M:%S")}; {end_word}: {job.ended_at.strftime("%Y-%m-%d %H:%M:%S")}">ran for {runtime}, {end_word} {timeago}</div>'
    elif job.is_started:
        timeago = f'{get_relative_time(job.started_at)} ago'
        html += f'<div style="padding-left:5px;font-style:italic"  title="started: {job.started_at.strftime("%Y-%m-%d %H:%M:%S")}">started {timeago}</div>'
    elif job.is_queued:
        timeago = f'{get_relative_time(job.enqueued_at)}'
        html += f'<div style="padding-left:5px;font-style:italic;" title="queued: {job.enqueued_at.strftime("%Y-%m-%d %H:%M:%S")}">queued for {timeago}</div>'
    elif job.get_status(True) == "scheduled":
        timeago = f'{get_relative_time(job.created_at)}'
        html += f'<div style="padding-left:5px;font-style:italic;" title="scheduled: {job.created_at.strftime("%Y-%m-%d %H:%M:%S")}">schedued for {timeago}</div>'
    else:
        timeago = f'{get_relative_time(job.created_at)} ago'
        html += f'<div style="padding-left:5px;font-style:italic;" title="created: {job.created_at.strftime("%Y-%m-%d %H:%M:%S")}">created {timeago}, status: {job.get_status()}</div>'
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
        if ref_parts[1] == "tags":
            return "tag"
        elif ref_parts[1] == "heads":
            return "branch"
    elif "DCS_event" in payload:
        if payload["DCS_event"] == "fork":
            return "branch"
        elif payload["DCS_event"] == "release":
            return "tag"


def get_event_from_payload(payload):
    if not payload:
        return None
    if 'DCS_event' in payload:
        return payload['DCS_event']
    else:
        return 'push'


def get_job_list_filter_link(job):
    repo = get_repo_from_payload(job.args[0])
    ref = get_ref_from_payload(job.args[0])
    event = get_event_from_payload(job.args[0])
    return f'<a href="?repo={repo}" title="{repo}">{repo.split("/")[-1]}&#128172;</a>=><a href="?repo={repo}&ref={ref}">{ref}</a>=><a href="?repo={repo}&ref={ref}&event={event}">{event}</a>'


def get_dcs_link(job):
    repo = get_repo_from_payload(job.args[0]) if job.args else None
    ref_type = get_ref_type_from_payload(job.args[0]) if job.args else 'branch'
    ref = get_ref_from_payload(job.args[0]) if job.args else 'master'
    event = get_event_from_payload(job.args[0]) if job.args else 'push'
    if not repo:
        return 'INVALID'
    text = f'{repo.split("/")[-1]}=>{event}=>{ref}'
    if event != "delete":
        return f'<a href="https://git.door43.org/{repo}/src/{ref_type}/{ref}" target="_blank" title="{repo}/src/{type}/{ref}">{text}</a>'
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


#####
##### DO NOT COPY BELOW HERE
#####

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
        print("Failed to queue", file=sys.stderr)
        print(e, file=sys.stderr)


@app.route('/'+WEBHOOK_URL_SEGMENT, methods=['POST'])
def job_receiver():
    if not request.data:
        print("Received request but no payload found", file=sys.stderr)
        return jsonify({'error': 'No payload found. You must submit a POST request via a DCS webhook notification.'}), 400

    # Bail if this is not from DCS
    if 'X-Gitea-Event' not in request.headers:
        print(f"No 'X-Gitea-Event' in {request.headers}", file=sys.stderr)
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
                       'door43_job_queued_at': datetime.utcnow()}
        return jsonify(return_dict)
    else:
        return jsonify({'error': 'Failed to queue job. See logs'}), 400    


@app.route('/'+WEBHOOK_URL_SEGMENT, methods=['GET'])
def homepage():
    return 'Go to the  <a href="status/">status page</a>'


if __name__ == "__main__":
    app.run(host="0.0.0.0", debug=True)