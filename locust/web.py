# -*- coding: utf-8 -*-

import csv
import json
import logging
import os.path
from collections import defaultdict
from itertools import chain
from time import time

try:
    # >= Py3.2
    from html import escape
except ImportError:
    # < Py3.2
    from cgi import escape

import six
from flask import Flask, make_response, jsonify, render_template, request
from gevent import pywsgi

from locust import __version__ as version
from six.moves import StringIO, xrange

from . import runners
from .cache import memoize
# from .runners import MasterLocustRunner
from .stats import distribution_csv, median_from_dict, requests_csv, sort_stats
from .runners import MasterLocustRunner
from .stats import failures_csv, median_from_dict, requests_csv, sort_stats, stats_history_csv
from .util.cache import memoize
from .util.rounding import proper_round
from .util.timespan import parse_timespan


logger = logging.getLogger(__name__)

DEFAULT_CACHE_TIME = 2.0

app = Flask(__name__)
app.debug = True
app.root_path = os.path.dirname(os.path.abspath(__file__))


@app.route('/')
def index():
    if runners.main.host:
        host = runners.main.host
    elif len(runners.main.locust_classes) > 0:
        host = runners.main.locust_classes[0].host
    else:
        host = None
    
    return render_template("index.html",
        state=runners.main.state,
        slave_count=runners.main.slave_count,
        worker_count=runners.main.worker_count,
        user_count=runners.main.user_count,
        version=version,
        host=host
    )

@app.route('/swarm', methods=["POST"])
def swarm():
    assert request.method == "POST"
    is_step_load = runners.locust_runner.step_load
    locust_count = int(request.form["locust_count"])
    hatch_rate = float(request.form["hatch_rate"])
    runners.main.start_hatching(locust_count, hatch_rate)
    response = make_response(json.dumps({'success': True, 'message': 'Swarming started'}))
    response.headers["Content-type"] = "application/json"
    return response

@app.route('/stop')
def stop():
    runners.main.stop()
    response = make_response(json.dumps({'success':True, 'message': 'Test stopped'}))
    response.headers["Content-type"] = "application/json"
    return response

@app.route("/stats/reset")
def reset_stats():
    runners.main.stats.reset_all()
    return "ok"
    
@app.route("/stats/requests/csv")
def request_stats_csv():
    response = make_response(requests_csv())
    file_name = "requests_{0}.csv".format(time())
    disposition = "attachment;filename={0}".format(file_name)
    response.headers["Content-type"] = "text/csv"
    response.headers["Content-disposition"] = disposition
    return response

@app.route("/stats/stats_history/csv")
def stats_history_stats_csv():
    response = make_response(stats_history_csv(False, True))
    file_name = "stats_history_{0}.csv".format(time())
    disposition = "attachment;filename={0}".format(file_name)
    response.headers["Content-type"] = "text/csv"
    response.headers["Content-disposition"] = disposition
    return response

@app.route("/stats/failures/csv")
def failures_stats_csv():
    response = make_response(failures_csv())
    file_name = "failures_{0}.csv".format(time())
    disposition = "attachment;filename={0}".format(file_name)
    response.headers["Content-type"] = "text/csv"
    response.headers["Content-disposition"] = disposition
    return response

@app.route('/stats/requests')
@memoize(timeout=DEFAULT_CACHE_TIME, dynamic_timeout=True)
def request_stats():
    stats = []
    for s in chain(sort_stats(runners.main.request_stats), [runners.main.stats.aggregated_stats("Total")]):
        stats.append({
            "method": s.method,
            "name": s.name,
            "safe_name": escape(s.name, quote=False),
            "num_requests": s.num_requests,
            "num_failures": s.num_failures,
            "avg_response_time": s.avg_response_time,
            "min_response_time": 0 if s.min_response_time is None else proper_round(s.min_response_time),
            "max_response_time": proper_round(s.max_response_time),
            "current_rps": s.current_rps,
            "current_fail_per_sec": s.current_fail_per_sec,
            "median_response_time": s.median_response_time,
            "ninetieth_response_time": s.get_response_time_percentile(0.9),
            "avg_content_length": s.avg_content_length,
        })

    errors = [e.to_dict() for e in six.itervalues(runners.main.errors)]

    # Truncate the total number of stats and errors displayed since a large number of rows will cause the app
    # to render extremely slowly. Aggregate stats should be preserved.
    report = {"stats": stats[:500], "errors": errors[:500]}
    if len(stats) > 500:
        report["stats"] += [stats[-1]]

    if stats:
        report["total_rps"] = stats[len(stats)-1]["current_rps"]
        report["fail_ratio"] = runners.main.stats.aggregated_stats("Total").fail_ratio

        # since generating a total response times dict with all response times from all
        # urls is slow, we make a new total response time dict which will consist of one
        # entry per url with the median response time as key and the number of requests as
        # value
        response_times = defaultdict(int) # used for calculating total median
        for i in xrange(len(stats)-1):
            response_times[stats[i]["median_response_time"]] += stats[i]["num_requests"]

        # calculate total median
        stats[len(stats)-1]["median_response_time"] = median_from_dict(stats[len(stats)-1]["num_requests"], response_times)

    report["slave_count"] = runners.main.slave_count

    report["state"] = runners.main.state
    report["user_count"] = runners.main.user_count
    report["worker_count"] = runners.main.worker_count
    
    return json.dumps(report)

@app.route("/exceptions")
def exceptions():
    return jsonify({
        'exceptions': [
            {
                "count": row["count"],
                "msg": row["msg"],
                "traceback": row["traceback"],
                "nodes" : ", ".join(row["nodes"])
            } for row in six.itervalues(runners.locust_runner.exceptions)
        ]
    })

@app.route("/exceptions/csv")
def exceptions_csv():
    data = StringIO()
    writer = csv.writer(data)
    writer.writerow(["Count", "Message", "Traceback", "Nodes"])
    for exc in six.itervalues(runners.locust_runner.exceptions):
        nodes = ", ".join(exc["nodes"])
        writer.writerow([exc["count"], exc["msg"], exc["traceback"], nodes])

    data.seek(0)
    response = make_response(data.read())
    file_name = "exceptions_{0}.csv".format(time())
    disposition = "attachment;filename={0}".format(file_name)
    response.headers["Content-type"] = "text/csv"
    response.headers["Content-disposition"] = disposition
    return response

def start(locust, options):
    pywsgi.WSGIServer((options.web_host, options.web_port),
                      app, log=None).serve_forever()
