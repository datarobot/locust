"""Module for creation subrunner through subprocess to avoid main process fork"""
import pickle
from optparse import OptionParser

from gevent import monkey

from locust.config import LocustConfig, load_locustfile
from locust.runners.slave import SlaveLocustRunner
from locust.runners.worker import WorkerLocustRunner

def main():
    monkey.patch_all()

    parser = OptionParser()

    parser.add_option(
        '--process',
        dest="process"
    )

    parser.add_option(
        '--options',
        dest="options"
    )

    opts, args = parser.parse_args()
    options = pickle.loads(opts.options)
    locusts = [l for f in args for l in load_locustfile(f)[1].values()]

    if opts.process == 'slave':
        runner = SlaveLocustRunner(locusts, options)
        runner.greenlet.join()
    elif opts.process == 'worker':
        runner = WorkerLocustRunner(locusts, options)
        runner.greenlet.join()

if __name__ == '__main__':
    main()
