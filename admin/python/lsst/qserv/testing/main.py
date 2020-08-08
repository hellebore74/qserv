"""Command line script for running KPM tests.

To run it just make a small wrapper script (or use Python
setup process to define an entry point).
"""

import argparse
import logging

from .config import Config
from . import mock_db
from .monitor import AddTagsMonitor, InfluxDBFileMonitor, LogMonitor
from .runner_mgr import RunnerManager

_LOG = logging.getLogger(__name__)


def _logConfig(level):
    levels = {0: logging.WARNING, 1: logging.INFO, 2: logging.DEBUG}
    simple_format = "%(asctime)s %(levelname)7s %(name)s -- %(message)s"
    level = levels.get(level, logging.DEBUG)
    logging.basicConfig(format=simple_format, level=level)


def main():

    parser = argparse.ArgumentParser(description="Test harness to generate load for QServ")

    parser.add_argument('-v', '--verbose', default=0, action='count',
                        help='More verbose output, can use several times.')

    parser.add_argument("-n", "--num-slots", type=int, default=None, metavar="NUMBER",
                        help="Number of slots to divide the whole workload into.")
    parser.add_argument("-s", "--slot", type=int, default=None, metavar="NUMBER",
                        help="Slot number for this process, in range [0, num-slots)."
                        " --num-slots and --slot must be specified together.")
    parser.add_argument("-t", "--time-limit", type=int, default=None, metavar="SECONDS",
                        help="Run for maximum number of seconds.")

    parser.add_argument("--dummy-db", action="store_true", default=False,
                        help="Use dummy implementation of database connection, for testing.")

    parser.add_argument("-m", "--monitor", choices=["log", "influxdb-file"],
                        help="Type for monitoring output, one of %(choices)s, default is no output.")
    parser.add_argument("-r", "--monitor-rollover", type=int, default=3600, metavar="SECONDS",
                        help="Number of seconds between rollovers for influxdb-file monitor,"
                        " default is %(default)s")
    parser.add_argument("--influxdb-file-name", default="qserv-kraken-mon-%T.dat", metavar="PATH",
                        help="File name for influxdb-file monitor, default is %(default)s.")
    parser.add_argument("--influxdb-db", default="qserv_kraken", metavar="DATABASE",
                        help="InfluxDB database name, default is %(default)s.")

    parser.add_argument("config", nargs="+", type=argparse.FileType(),
                        help="Configuration file name, at least one is required.")
    args = parser.parse_args()

    if (args.num_slots, args.slot).count(None) == 1:
        parser.error("options --num-slots and --slot must be specified together")

    _logConfig(args.verbose)

    # build/split config
    cfg = Config.from_yaml(args.config)
    if args.num_slots is not None:
        cfg = cfg.split(args.num_slots, args.slot)
    print("Configuration for this process (n_slots={args.num_slots}, slot={args.slot}):")
    print(cfg.to_yaml())

    # connection factory
    if args.dummy_db:
        connFactory = mock_db.connect
    else:
        raise NotImplementedError()

    # monitor
    monitor = None
    if args.monitor == "log":
        monitor = LogMonitor(logging.getLogger("metrics"))
    elif args.monitor == "influxdb-file":
        monitor = InfluxDBFileMonitor(
            args.influxdb_file_name,
            periodSec=args.monitor_rollover,
            dbname=args.influxdb_db
        )
    # add a tag for slot number
    if args.slot is not None:
        monitor = AddTagsMonitor(monitor, tags={"slot": args.slot})

    mgr = RunnerManager(cfg, connFactory, args.slot,
                        runTimeLimit=args.time_limit, monitor=monitor)
    mgr.run()


if __name__ == "__main__":
    main()
