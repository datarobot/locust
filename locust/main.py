import logging
import signal
import socket
import sys
import time

import gevent

import locust

from runners import MasterLocustRunner, SlaveLocustRunner
from . import events, runners, web
from .core import WebLocust, Locust
from .inspectlocust import get_task_ratio_dict, print_task_ratio
from .log import console_logger, setup_logging
from .stats import (print_error_report, print_percentile_stats, print_stats,
                    stats_printer, stats_writer, write_stat_csvs)
from util import time

from . import config

from runners import MasterLocustRunner, SlaveLocustRunner

_internals = [Locust, WebLocust]
version = locust.__version__

main_greenlet = None

def launch(options, locusts):
    """
    Locust entrypoint, could be called for programmatical launch:
        * options - Any object which implements field access by attribute
                    Recommended to use extended locust.config.LocustConfig object
        * locusts - list of locust classes inherited from locust.core.Locust
    """
    logger = logging.getLogger(__name__)

    if options.show_version:
        console_logger.info("Locust %s", version)
        sys.exit(0)

    if options.list_commands:
        console_logger.info("Available Locusts:")
        for locust_class in locusts:
            console_logger.info("    " + locust_class.__name__)
        sys.exit(0)

    if options.show_task_ratio:
        console_logger.info("\n Task ratio per locust class")
        console_logger.info("-" * 80)
        print_task_ratio(locusts)
        console_logger.info("\n Total task ratio")
        console_logger.info("-" * 80)
        print_task_ratio(locusts, total=True)
        sys.exit(0)

    if options.show_task_ratio_json:
        from json import dumps
        task_data = {
            "per_class": get_task_ratio_dict(locusts),
            "total": get_task_ratio_dict(locusts, total=True)
        }
        console_logger.info(dumps(task_data))
        sys.exit(0)

    if options.run_time:
        if not options.no_web:
            logger.error("The --run-time argument can only be used together with --no-web")
            sys.exit(1)
        try:
            options.run_time = time.parse_timespan(options.run_time)
        except ValueError:
            logger.error("Valid --time-limit formats are: 20, 20s, 3m, 2h, 1h20m, 3h30m10s, etc.")
            sys.exit(1)
        def spawn_run_time_limit_greenlet():
            logger.info("Run time limit set to %s seconds" % options.run_time)
            def timelimit_stop():
                logger.info("Time limit reached. Stopping Locust.")
                shutdown()
            gevent.spawn_later(options.run_time, timelimit_stop)

    # Master / Slave init
    if options.slave:
        logger.info(
            "Starting slave node. Connecting to %s:%s",
            options.master_host,
            options.master_port
        )
        slave = SlaveLocustRunner(locusts, options)
        runners.main = slave
        main_greenlet = runners.main.greenlet
    else:
        logger.info("Starting master node")
        master = MasterLocustRunner(locusts, options)
        runners.main = master
        main_greenlet = runners.main.greenlet

    # Headful / headless init
    if options.slave:
        if options.run_time:
            logger.error("--run-time should be specified on the master node, and not on slave nodes")
            sys.exit(1)
        logger.info("Slave connected in headless mode")
    elif options.no_web and not options.slave:
        if options.run_time:
                spawn_run_time_limit_greenlet()
        logger.info("Starting headless execution")
        runners.main.wait_for_slaves(options.expect_slaves)
        runners.main.start_hatching(options.num_clients, options.hatch_rate)
    else:
        logger.info(
            "Starting web monitor at %s:%s",
            options.web_host or "localhost",
            options.web_port
        )
        gevent.spawn(web.start, locusts, options)

    #### Stats, etc
    if options.print_stats and not options.slave:
        gevent.spawn(stats_printer)
    if options.csvfilebase and not options.slave:
        gevent.spawn(stats_writer, options.csvfilebase)

    def shutdown(code=0):
        """
        Shut down locust by firing quitting event, printing/writing stats and exiting
        """
        logger.info("Shutting down (exit code %s), bye." % code)

        events.quitting.fire()
        print_stats(runners.main.request_stats)
        print_percentile_stats(runners.main.request_stats)
        print_error_report()
        sys.exit(code)

    # install SIGTERM handler
    def sig_term_handler():
        logger.info("Got SIGTERM signal")
        shutdown(0)
    gevent.signal(signal.SIGTERM, sig_term_handler)

    try:
        logger.info("Starting Locust %s" % version)
        main_greenlet.join()
        code = 0
        if len(runners.main.errors):
            code = 1
        shutdown(code=code)
    except KeyboardInterrupt:
        shutdown(0)

def main():
    options, locusts = config.process_options()
    launch(options, locusts)

if __name__ == '__main__':
    main()
