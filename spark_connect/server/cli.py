import os
import signal

import click

from spark_connect.log import logger
from spark_connect.server.daemon import Daemon


@click.command()
@click.option("--mode", default=None)
@click.option("--root_dir", default="./")
def start(mode, root_dir):
    if mode:
        Daemon(mode=mode, root_dir=root_dir).start_and_daemon()
    else:
        Daemon(root_dir=root_dir).start_and_daemon()


@click.command()
@click.option("--root_dir", default="./")
def stop(root_dir="./"):
    logger.disable("spark-connect")
    print(f"Stop daemon as root_dir:{root_dir}")
    pid_file = Daemon(root_dir=root_dir).daemon_pid_file
    if pid_file.exists():
        with open(pid_file) as f:
            pid = int(f.read())
        # Stop the daemon by pid
        try:
            print("Kill daemon by pid, send SIGTERM")
            os.kill(pid, signal.SIGTERM)
        except ProcessLookupError:
            pass
    else:
        print(f"No daemon pid file found at {pid_file.as_posix()}, daemon is not running")


@click.group()
def cli():
    pass


cli.add_command(start)
cli.add_command(stop)

if __name__ == "__main__":
    cli()
