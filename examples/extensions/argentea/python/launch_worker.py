#!/usr/bin/env python3
"""Use Sail's existing supervised worker launcher with native quota settings."""
import argparse
import json
import os
from pathlib import Path
import signal
import sys

sys.path.insert(0, str(Path(__file__).resolve().parents[2] / 'scripts'))
from two_host_worker import launch, stop


def worker_environment(target, environment):
    forwarded = ('SAIL_EXPERIMENTAL_EXTENSIONS', 'SAIL_ARGENTEA_MEMORY_BYTES',
                 'SAIL_NUTMEG_MEMORY_BYTES', 'RUST_LOG', 'TOKIO_WORKER_THREADS', 'RAYON_NUM_THREADS')
    env = {key: value for key, value in environment.items()
           if key.startswith(('SAIL_CLUSTER__', 'SAIL_EXECUTION__', 'SAIL_RUNTIME__')) or key in forwarded}
    env.update(SAIL_CLUSTER__WORKER_LISTEN_HOST='0.0.0.0',
               SAIL_CLUSTER__WORKER_EXTERNAL_HOST=target['advertise'],
               SAIL_CLUSTER__WORKER_LISTEN_PORT=str(target.get('port', 0)),
               SAIL_CLUSTER__WORKER_EXTERNAL_PORT=str(target.get('port', 0)))
    return env


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--targets-json', required=True)
    args = parser.parse_args()
    targets = json.loads(args.targets_json)
    if not isinstance(targets, list) or not targets:
        raise ValueError('at least one worker target is required')
    target = targets[int(os.environ['SAIL_CLUSTER__WORKER_ID']) % len(targets)]
    env = worker_environment(target, os.environ)
    process = launch(target, [target['sail'], 'worker'], env)
    try:
        return process.wait()
    finally:
        stop(process)


def interrupted(_number, _frame):
    raise KeyboardInterrupt


if __name__ == '__main__':
    signal.signal(signal.SIGTERM, interrupted)
    try:
        sys.exit(main())
    except KeyboardInterrupt:
        sys.exit(130)
