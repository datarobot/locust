"""Master Locust runner. Represent master for distributed worker sceduler process"""
import os
import sys
import time
import pickle
import logging
import subprocess

import gevent
from gevent.pool import Group

from locust.rpc import Message, rpc
from locust import events
from .distributed import DistributedLocustRunner, Node, NodesDict, STATE
from .slave import SlaveLocustRunner

logger = logging.getLogger(__name__)

HEARTBEAT_INTERVAL = 3
SLAVE_INIT_TIMEOUT = 600

class MasterLocustRunner(DistributedLocustRunner):
    """Master Locust runner. Represent high level sceduler process"""

    class MasterServerHandler(object):
        """Handler for MasterServer zmq rpc client"""

        def __init__(self, master):
            self.master = master

        def _validate_msg(fun):
            def wraper(inst, msg):
                msg.node_id = msg.node_id.encode('ascii')
                if msg.node_id in inst.master.slaves.keys():
                    fun(inst, msg)
                else:
                    logger.warn('Unknown Slave. Reply unknown: %s.', msg.node_id)
                    inst.master.server.send_to(msg.node_id, Message("unknown", None, None))
            return wraper

        def on_slave_ready(self, msg):
            id = msg.node_id.encode('ascii')
            self.master.slaves[id] = Node(id)
            logger.info(
                "Slave %r reported as ready. Currently %i Slaves ready to swarm.",
                id,
                len(self.master.slaves.ready)
            )
            options = self.master.options.to_dict()
            self.master.server.send_to(id, Message("new_config", options, None))

        @_validate_msg
        def on_slave_stopped(self, msg):
            del self.master.slaves[msg.node_id]
            if len(self.master.slaves.hatching + self.master.slaves.running) == 0:
                self.master.state = STATE.STOPPED
                logger.info("Removing %s slave from running slaves", msg.node_id)

        @_validate_msg
        def on_stats(self, msg):
            events.node_report.fire(node_id=msg.node_id, data=msg.data)

        @_validate_msg
        def on_hatching(self, msg):
            self.master.slaves[msg.node_id].state = STATE.HATCHING

        @_validate_msg
        def on_hatch_complete(self, msg):
            self.master.slaves[msg.node_id].state = STATE.RUNNING
            self.master.slaves[msg.node_id].user_count = msg.data["count"]
            if len(self.master.slaves.hatching) == 0:
                count = sum(c.user_count for c in self.master.slaves.itervalues())
                events.hatch_complete.fire(user_count=count)
                self.master.state = STATE.RUNNING

        @_validate_msg
        def on_quit(self, msg):
            if msg.node_id in self.master.slaves:
                del self.master.slaves[msg.node_id]
                logger.info(
                    "slave %r quit. Currently %i slaves connected.",
                    msg.node_id,
                    len(self.master.slaves.ready)
                )

        @_validate_msg
        def on_exception(self, msg):
            self.master.log_exception(msg.node_id, msg.data["msg"], msg.data["traceback"])

        @_validate_msg
        def on_pong(self, msg):
            self.master.slaves[msg.node_id].ping_answ = True
            self.master.slaves[msg.node_id].ping_missed = 0


    def __init__(self, locust_classes, options):
        super(MasterLocustRunner, self).__init__(locust_classes, options)

        self.state = STATE.INIT
        self.exceptions = {}
        self.server = rpc.MasterServer(self.master_bind_host, self.master_bind_port)
        self.server.bind_handler(self.MasterServerHandler(self))
        self.greenlet = Group()
        self.slaves = NodesDict()

        # listener that gathers info on how many locust users the slaves has spawned
        def on_slave_report(node_id, data):
            if node_id not in self.slaves:
                logger.info("Discarded report from unrecognized slave %s", node_id)
                return
            self.slaves[node_id].user_count = data["user_count"]
            self.slaves[node_id].worker_count = data["worker_count"]
        events.node_report += on_slave_report

        # register listener that sends quit message to slave nodes
        def on_quitting():
            self.quit()
        events.quitting += on_quitting

        self.spawn_slave()
        self.greenlet.spawn(self.slaves_listener).link_exception(callback=self.noop)
        self.wait_for_slaves(1)
        self.greenlet.spawn(self.heartbeat).link_exception(callback=self.noop)

    @property
    def user_count(self):
        return sum([c.user_count for c in self.slaves.itervalues()])

    @property
    def worker_count(self):
        return sum([c.worker_count for c in self.slaves.itervalues()])

    @property
    def slave_count(self):
        return len(self.slaves)

    @property
    def request_stats(self):
        return self.stats.entries

    @property
    def task_stats(self):
        return self.stats.tasks

    @property
    def errors(self):
        return self.stats.errors

    def spawn_slave(self):
        path = os.path.dirname(os.path.abspath(__file__)) + '/subrunner.py'
        options = pickle.dumps(self.options)
        command = "{} {} --process slave --options \"{}\"".format(
            sys.executable, path, options
        )
        locusts = [sys.modules[lc.__module__].__file__ for lc in self.locust_classes]
        command += ' ' + ' '.join(locusts)
        subprocess.Popen([command], shell=True)

    def wait_for_slaves(self, n=0):
        counter = 0
        while counter <= SLAVE_INIT_TIMEOUT:
            if self.slave_count >= n:
                return
            gevent.sleep(2)
            counter += 2
        raise Exception("Only {} slaves connected from expected {}".format(
            self.slave_count, n
        ))

    def propagate_config(self, updates=None):
        if updates is not None:
            self.options.update_config(updates)
        self.server.send_all(Message("new_config", self.options.to_dict(), None))

    def stop(self):
        for _slave in self.slaves.hatching + self.slaves.running:
            self.server.send_all(Message("stop", None, None))
        events.master_stop_hatching.fire()
        counter = 0
        while counter <= SLAVE_INIT_TIMEOUT:
            if len(self.slaves.hatching + self.slaves.running) == 0:
                return
            gevent.sleep(1)
            counter += 1
        raise Exception("Some slaves were not stopped")

    def quit(self):
        self.server.send_all(Message("quit", None, None))
        self.greenlet.kill(block=True)
        self.server.close()

    def heartbeat(self):
        while True:
            gen = (w for w in self.slaves.copy().itervalues() if not w.ping_answ)
            for dead_slave in gen:
                if dead_slave.ping_missed > 3:
                    self.server.send_to(dead_slave.id, Message("unknown", None, None))
                    del self.slaves[dead_slave.id]
                    logger.warn("Connection to %s slave was lost", dead_slave.id)
                else:
                    dead_slave.ping_missed += 1

            for slave in self.slaves.itervalues():
                slave.ping_answ = False
            self.server.send_all(Message("ping", None, None))
            gevent.sleep(HEARTBEAT_INTERVAL)

    def slaves_listener(self):
        while True:
            self.server.recv()
            gevent.sleep()

    def start_hatching(self, locust_count, hatch_rate):
        worker_num = self.slave_count

        if self.state != STATE.INIT and self.state != STATE.STOPPED:
            self.stop()

        self.stats.clear_all()
        self.stats.reset_all()
        self.exceptions = {}
        events.master_start_hatching.fire()
        self.state = STATE.HATCHING

        leftover = locust_count - (locust_count / worker_num) * worker_num
        adjust = lambda x: 1 if x <= leftover else 0
        calc = lambda x: locust_count / worker_num + adjust(x)
        slave_locust_count = [calc(x) for x in range(1, worker_num + 1)]
        slave_hatch_rate = hatch_rate / float(worker_num)
        if self.options.num_requests:
            slave_num_requests = int(self.options.num_requests / self.slave_count)
        else:
            slave_num_requests = None

        logger.info("Sending hatch jobs to %d ready slave(s)", worker_num)

        for client_rate, slave_id in zip(slave_locust_count, self.slaves.keys()):
            data = {
                "hatch_rate": slave_hatch_rate,
                "num_clients": client_rate,
                "num_requests": slave_num_requests
            }
            self.server.send_to(slave_id, Message("hatch", data, None))
            self.slaves[slave_id].task = data

        self.stats.start_time = time.time()
        self.state = STATE.HATCHING

    def log_exception(self, node_id, msg, formatted_tb):
        key = hash(formatted_tb)
        row = self.exceptions.setdefault(
            key,
            {"count": 0, "msg": msg, "traceback": formatted_tb, "nodes": set()}
        )
        row["count"] += 1
        row["nodes"].add(node_id)
        self.exceptions[key] = row
