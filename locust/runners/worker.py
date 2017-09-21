"""Worker Locust runner"""
import logging
import warnings
import random
import socket
import uuid
import sys
import traceback
from time import time

import six
import gevent
from gevent import GreenletExit
from gevent.pool import Group

from .distributed import STATE
from locust import events
from locust import stats
from locust.stats import global_stats
from locust.rpc import Message, rpc

logger = logging.getLogger(__name__)

WORKER_STATS_INTERVAL = 2

class LocustRunner(object):
    def __init__(self, locust_classes, options):
        self.options = options
        self.locust_classes = locust_classes
        self.hatch_rate = options.hatch_rate
        self.num_clients = options.num_clients
        self.num_requests = options.num_requests
        self.host = options.host
        self.locusts = Group()
        self.state = STATE.INIT
        self.hatching_greenlet = None
        self.exceptions = {}
        self.stats = global_stats
        
        # register listener that resets stats when hatching is complete
        def on_hatch_complete(user_count):
            self.state = STATE.RUNNING
            if not self.options.no_reset_stats:
                logger.info("Resetting stats\n")
                self.stats.reset_all()
        events.hatch_complete += on_hatch_complete

    @property
    def request_stats(self):
        return self.stats.entries

    @property
    def errors(self):
        return self.stats.errors

    @property
    def user_count(self):
        return len(self.locusts)

    def weight_locusts(self, amount, stop_timeout=None):
        """
        Distributes the amount of locusts for each WebLocust-class according to it's weight
        returns a list "bucket" with the weighted locusts
        """
        bucket = []
        weight_sum = sum((locust.weight for locust in self.locust_classes if locust.task_set))
        for locust in self.locust_classes:
            if not locust.task_set:
                warnings.warn(
                    "Notice: Found Locust class (%s) got no task_set. Skipping" % locust.__name__
                )
                continue

            if self.host is not None:
                locust.host = self.host
            if stop_timeout is not None:
                locust.stop_timeout = stop_timeout

            # create locusts depending on weight
            percent = locust.weight / float(weight_sum)
            num_locusts = int(round(amount * percent))
            bucket.extend([locust for x in xrange(0, num_locusts)])
        return bucket

    def spawn_locusts(self, spawn_count=None, stop_timeout=None, wait=False):
        if spawn_count is None:
            spawn_count = self.num_clients

        if self.num_requests is not None:
            self.stats.max_requests = self.num_requests

        bucket = self.weight_locusts(spawn_count, stop_timeout)
        spawn_count = len(bucket)
        if self.state == STATE.INIT or self.state == STATE.STOPPED:
            self.state = STATE.HATCHING
            self.num_clients = spawn_count
        else:
            self.num_clients += spawn_count

        logger.info(
            "Hatching and swarming %i clients at the rate %g clients/s...",
            spawn_count,
            self.hatch_rate
        )
        occurence_count = dict([(l.__name__, 0) for l in self.locust_classes])

        def hatch():
            sleep_time = 1.0 / self.hatch_rate
            while True:
                if not bucket:
                    locusts = [
                        "%s: %d" % (name, count) for name, count in six.iteritems(occurence_count)
                    ]
                    logger.info("All locusts hatched: %s", ", ".join(locusts))
                    events.hatch_complete.fire(user_count=self.num_clients)
                    return

                locust = bucket.pop(random.randint(0, len(bucket)-1))
                occurence_count[locust.__name__] += 1
                def start_locust(_):
                    try:
                        locust().run()
                    except GreenletExit:
                        pass
                new_locust = self.locusts.spawn(start_locust, locust)
                if len(self.locusts) % 10 == 0:
                    logger.debug("%i locusts hatched" % len(self.locusts))
                gevent.sleep(sleep_time)

        hatch()
        if wait:
            self.locusts.join()
            logger.info("All locusts dead\n")

    def kill_locusts(self, kill_count):
        """
        Kill a kill_count of weighted locusts from the Group() object in self.locusts
        """
        bucket = self.weight_locusts(kill_count)
        kill_count = len(bucket)
        self.num_clients -= kill_count
        logger.info("Killing %i locusts", kill_count)
        dying = []
        for g in self.locusts:
            for l in bucket:
                if l == g.args[0]:
                    dying.append(g)
                    bucket.remove(l)
                    break
        for g in dying:
            self.locusts.killone(g)
        events.hatch_complete.fire(user_count=self.num_clients)

    def start_hatching(self, locust_count=None, hatch_rate=None, wait=False):
        if self.state != STATE.RUNNING and self.state != STATE.HATCHING:
            self.stats.clear_all()
            self.stats.start_time = time()
            self.exceptions = {}
            events.locust_start_hatching.fire()

        # Dynamically changing the locust count
        if self.state != STATE.INIT and self.state != STATE.STOPPED:
            self.state = STATE.HATCHING
            if self.num_clients > locust_count:
                # Kill some locusts
                kill_count = self.num_clients - locust_count
                self.kill_locusts(kill_count)
            elif self.num_clients < locust_count:
                # Spawn some locusts
                if hatch_rate:
                    self.hatch_rate = hatch_rate
                spawn_count = locust_count - self.num_clients
                self.spawn_locusts(spawn_count=spawn_count)
            else:
                events.hatch_complete.fire(user_count=self.num_clients)
        else:
            if hatch_rate:
                self.hatch_rate = hatch_rate
            if locust_count is not None:
                self.spawn_locusts(locust_count, wait=wait)
            else:
                self.spawn_locusts(wait=wait)

    def stop(self):
        # if we are currently hatching locusts we need to kill the hatching greenlet first
        if self.hatching_greenlet and not self.hatching_greenlet.ready():
            self.hatching_greenlet.kill(block=True)
        self.locusts.kill(block=True)
        self.state = STATE.STOPPED
        events.locust_stop_hatching.fire()


class WorkerLocustRunner(LocustRunner):
    """Locust runner with communication layer"""

    class WorkerClientHandler(object):
        """Handler for SlaveClient zmq rpc client"""

        def __init__(self, worker):
            self.worker = worker

        def on_hatch(self, msg):
            self.worker.client.send_all(Message("hatching", None, self.worker.worker_id))
            job = msg.data
            self.worker.hatch_rate = job["hatch_rate"]
            self.worker.num_requests = job["num_requests"]
            self.worker.host = job["host"]
            self.hatching_greenlet = gevent.spawn(
                lambda: self.worker.start_hatching(
                    locust_count=job["num_clients"],
                    hatch_rate=job["hatch_rate"]
                )
            )

        def on_stop(self, msg):
            events.quitting.fire()

        def on_quit(self, msg):
            events.quitting.fire()

        def on_ping(self, msg):
            self.worker.client.send_all(Message("pong", None, self.worker.worker_id))


    @classmethod
    def spawn(self, locust_classes, options, parent):
        parent.server.close()
        parent.client.close()
        try:
            parent.greenlet.kill(block=True)
        except GreenletExit:
            pass
        gevent.reinit()
        events.clear_events_handlers()
        stats.subscribe_stats()
        runner = WorkerLocustRunner(locust_classes, options)
        runner.greenlet.join()
        sys.exit(0)

    def __init__(self, locust_classes, options):
        super(WorkerLocustRunner, self).__init__(locust_classes, options)
        self.master_port = options.master_port
        self.worker_id = socket.gethostname().replace(' ', '_') + "_" + uuid.uuid4().hex

        self.greenlet = Group()

        self.client = rpc.WorkerClient(self.master_port, self.worker_id)
        self.client.bind_handler(self.WorkerClientHandler(self))

        def noop(*args, **kwargs):
            pass

        # register listener for when all locust users have hatched, and report it to the master node
        def on_hatch_complete(user_count):
            self.client.send_all(Message("hatch_complete", {"count": user_count}, self.worker_id))
        events.hatch_complete += on_hatch_complete

        # register listener that adds the current number of spawned locusts to
        # the report that is sent to the master node
        def on_report_to_master(node_id, data):
            data["user_count"] = self.user_count
        events.report_to_master += on_report_to_master

        # register listener that sends quit message to master
        def on_quitting():
            self.quit()
        events.quitting += on_quitting

        # register listener thats sends locust exceptions to master
        def on_locust_error(locust_instance, exception, tb):
            formatted_tb = "".join(traceback.format_tb(tb))
            data = {"msg" : str(exception), "traceback" : formatted_tb}
            self.client.send_all(Message("exception", data, self.worker_id))
        events.locust_error += on_locust_error

        self.greenlet.spawn(self.slave_listener).link_exception(callback=noop)
        gevent.sleep(0.5)
        self.client.send_all(Message("worker_ready", None, self.worker_id))
        self.greenlet.spawn(self.stats_reporter).link_exception(callback=noop)

    def quit(self):
        self.client.send_all(Message("quit", None, self.worker_id))
        self.client.close()
        self.greenlet.kill(block=True)

    def slave_listener(self):
        while True:
            self.client.recv()

    def stats_reporter(self):
        while True:
            data = {}
            events.report_to_master.fire(node_id=self.worker_id, data=data)
            self.client.send_all(Message("stats", data, self.worker_id))
            gevent.sleep(WORKER_STATS_INTERVAL)
