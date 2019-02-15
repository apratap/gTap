#!/bin/env python

import argparse
import datetime as dt
from multiprocessing import Pipe, Process
import os
from pytz import timezone as tz
import socket
from ssl import SSLError
import sys
import time


import synapseclient
from synapseclient import Column, Schema, Table
from synapseclient.exceptions import SynapseHTTPError

import app.config as secrets
import app.model as model
from app.scripts import TakeOutExtractor

syn = secrets.syn


def log(message):
    rmeow = dt.datetime.now(tz("US/Pacific")).strftime(secrets.DTFORMAT).upper()

    path = os.path.join(secrets.ARCHIVE_AGENT_TMP_DIR.replace('tmp', ''), 'ArchiveLog.txt')
    with open(path, 'a+') as f:
        f.write(f'{rmeow}: {message}\n')


def get_wait_time_from_env():
    if 'ARCHIVE_AGENT_WAIT_TIME' in os.environ:
        return float(os.environ['ARCHIVE_AGENT_WAIT_TIME'])
    else:
        return None


def main():
    parser = argparse.ArgumentParser(description='--')
    parser.add_argument(
        '--wait',
        type=int,
        help='interval in minutes between poll to task db',
        required=False
    )
    parser.add_argument(
        '--conn',
        type=str,
        help='optional database connection',
        required=False
    )
    parser.add_argument(
        '--k',
        type=int,
        help='optional. a value > 0 will keep the agent running forever',
        required=False
    )

    wait_time = parser.parse_args().wait
    if wait_time is None:
        wait_time = 600
    else:
        wait_time *= 60

    conn = parser.parse_args().conn
    if conn is None:
        conn = secrets.DATABASE

    keep_alive = parser.parse_args().k
    if keep_alive is None or keep_alive == 0:
        keep_alive = False
    else:
        keep_alive = True

    agent = ArchiveAgent(
        conn=conn,
        keep_alive=keep_alive,
        wait_time=wait_time
    )

    agent.start_async()
    return f'agent started on pid {agent.get_pid()}'


def empty_tmp_dir():
    files = os.listdir(secrets.ARCHIVE_AGENT_TMP_DIR)
    for f in files:
        os.remove(os.path.join(secrets.ARCHIVE_AGENT_TMP_DIR, f))


class ArchiveAgent(object):
    def __init__(self, conn, keep_alive=True, wait_time=None):
        if wait_time is None:
            self.wait_time = get_wait_time_from_env()
        else:
            self.wait_time = wait_time

        if self.wait_time is None:
            self.wait_time = 600.

        self.conn = conn
        self.keep_alive = keep_alive

        self.__sigkill, self.__done = Pipe()
        self.__agent = Process(
            target=self.__run_agent,
            args=(self.wait_time, self.conn, self.keep_alive, self.__sigkill, self.__done)
        )
        self.__agent.name = 'ArchiveAgent'

        if not os.path.exists(secrets.ARCHIVE_AGENT_TMP_DIR):
            os.mkdir(secrets.ARCHIVE_AGENT_TMP_DIR)

    def get_pid(self):
        return self.__agent.pid

    def get_status(self):
        return f'archive agent <pid={self.get_pid()}> is {"" if self.__agent.is_alive() else "not"} running'

    def start_async(self):
        if not self.__agent.is_alive():
            self.__agent.start()
        else:
            pass

    def start(self):
        self.start_async()
        self.terminate()

    def terminate(self):
        if self.__agent.is_alive():
            self.__sigkill.send(True)
            log('signalled agent to terminate')
        else:
            pass

        self.__done.recv()
        log('agent terminated gracefully')

        self.__agent.terminate()
        self.__agent.join()

    @staticmethod
    def __run_agent(wait_time, conn, keep_alive, sigkill, done):
        terminate = False

        while not terminate:
            try:
                start = time.time()

                with model.session_scope(conn) as s:
                    pending = model.get_all_pending(session=s)

                    n = len(pending)
                    if n > 0:
                        log(f'found {n} task{"s" if n > 1 else ""} to process')

                    while len(pending) > 0:
                        consent = pending.pop()
                        log(f'starting task for {str(consent)}')

                        task = TakeOutExtractor(consent)
                        result = task.run()

                        if isinstance(result, str):
                            log(f'{str(task)}. {result}')

                        else:
                            search, location = False, False

                            if not isinstance(result['search'], str):
                                consent = model.add_search_error(s, consent, result['search'])
                            else:
                                consent.search_eid = None
                                consent.search_err = []
                                consent.search_sid = result['search']
                                search = True

                            if not isinstance(result['location'], str):
                                consent = model.add_location_error(s, consent, result['location'])
                            else:
                                consent.location_eid = None
                                consent.location_err = []
                                consent.location_sid = result['location']
                                location = True

                            model.update_synapse_sids(consent)

                            if search and location:
                                consent.data = None
                            else:
                                pass

                            log(f'archival task for {str(consent)} completed '
                                f'{"with errors" if not search or not location else "successfully"}')

                        del task

                    empty_tmp_dir()

                terminate = sigkill.poll(1)
                if not terminate:
                    remaining = wait_time - (time.time() - start)
                    # log(f'sleeping for {int(remaining)} seconds')
                    time.sleep(remaining if remaining > 0 else 0)
                else:
                    pass

            except Exception as e:
                log(f'agent terminated unexpectedly. {str(e.__class__)}: {", ".join([a for a in e.args])}')

                if not keep_alive:
                    log('agent shutting down')
                    break
                else:
                    log('agent restarting')

        done.send(True)


if __name__ == '__main__':
    # build_synapse_log()
    main()
