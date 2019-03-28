#!/bin/env python

import argparse
import datetime as dt
from multiprocessing import Pipe, Process
import os
import time

from botocore.exceptions import ClientError
import boto3
from jinja2 import Template
import numpy as np

import app.config as secrets
import app.context as ctx
from app.xtractor import TakeOutExtractor


class ArchiveAgent(object):
    """class for managing archive tasks"""

    def __init__(self, conn, keep_alive=True, wait_time=None):
        """constructor

        Args:
            conn: (dict) connection parameters for database
            keep_alive: (bool) optional. restart agent if failure occurs
            wait_time: (int) optional. seconds to wait between polling for new tasks. default=3600
        """
        if wait_time is None:
            self.wait_time = get_wait_time_from_env()
        else:
            self.wait_time = wait_time

        if self.wait_time is None:
            self.wait_time = 600.

        self.conn = conn
        self.keep_alive = keep_alive

        self.__digest_date = dt.date.today()

        self.__sigkill, self.__done = Pipe()
        self.__agent = Process(
            name=secrets.ARCHIVE_AGENT_PROC_NAME,
            target=self.__run_agent,
            args=(self.wait_time, self.conn, self.keep_alive, self.__sigkill, self.__done)
        )

        if not os.path.exists(secrets.ARCHIVE_AGENT_TMP_DIR):
            os.mkdir(secrets.ARCHIVE_AGENT_TMP_DIR)

    def get_pid(self):
        """get the process id from the running agent"""
        return self.__agent.pid

    def get_status(self):
        """get the status of the agent"""
        return f'archive agent <pid={self.get_pid()}> is{" " if self.__agent.is_alive() else "not "}running'

    def start_async(self):
        """start the agent as a forked process"""
        if not self.__agent.is_alive():
            self.__agent.start()
        else:
            pass

    def start(self):
        """run the agent for one task polling iteration"""
        self.start_async()

        time.sleep(5)
        self.terminate()

    def terminate(self):
        """signal the agent to terminate. block until current round of tasks is completed"""
        if self.__agent.is_alive():
            self.__sigkill.send(True)
            ctx.add_log_entry('signalled agent to terminate')
        else:
            pass

        self.__done.recv()
        self.__agent.join()
        ctx.add_log_entry('agent terminated gracefully')

    def send_digest(self):
        """send the daily digest if one has not already been sent today"""
        # check for digest send
        now = dt.date.today()
        if (now - self.__digest_date).days > 0:
            send_daily_digest()
            self.__digest_date = now
        else:
            pass

    def __run_agent(self, wait_time, conn, keep_alive, sigkill, done):
        """code to run on forked agent process"""
        terminate = False

        # continue to process until told to terminate
        while not terminate:
            try:
                start = time.time()
                current_id = np.nan

                with ctx.session_scope(conn) as s:
                    pending = ctx.get_pending(session=s)

                    while len(pending) > 0:
                        p = pending.pop()

                        current_id = p.internal_id
                        ctx.add_log_entry(f'starting task', cid=p.internal_id)

                        try:
                            task = TakeOutExtractor(p).run()

                            # make sure all updates have been persisted to backend
                            ctx.commit(s)

                            # final call to update Synapse consents table
                            task.consent.update_synapse()
                        except Exception as e:
                            p.set_status(ctx.ConsentStatus.FAILED)
                            raise e

                # check for termination signal (blocking for one second)
                terminate = sigkill.poll(1)

                # figure out how long we need to wait to keep task polling interval
                if not terminate:
                    remaining = wait_time - (time.time() - start)
                    time.sleep(remaining if remaining > 0 else 0)
                else:
                    pass

                self.send_digest()
            except Exception as e:
                ctx.mark_as_permanently_failed(current_id)
                ctx.add_log_entry(
                    f'agent terminated unexpectedly. {str(e.__class__)}: {", ".join([a for a in e.args])}',
                    cid=current_id
                )

                if not keep_alive:
                    ctx.add_log_entry('agent shutting down')
                    break
                else:
                    ctx.add_log_entry('agent restarting')

        done.send(True)


def get_wait_time_from_env():
    """get task polling wait time

    Notes:
        return from environment variables, application config, or 3600 seconds in that order

    Returns:
        int
    """
    if 'ARCHIVE_AGENT_WAIT_TIME' in os.environ:
        return float(os.environ['ARCHIVE_AGENT_WAIT_TIME'])
    elif hasattr(secrets, 'ARCHIVE_AGENT_WAIT_TIME'):
        return secrets.ARCHIVE_AGENT_WAIT_TIME
    else:
        return 3600


def send_daily_digest(conn=None):
    """send the daily digest email"""
    try:
        digest = ctx.daily_digest(conn)
        template = Template(secrets.DIGEST_TEMPLATE)

        client = boto3.client('ses', region_name=secrets.REGION_NAME)
        response = client.send_email(
            Source=secrets.FROM_STUDY_EMAIL,
            Destination={
                'ToAddresses': secrets.ADMIN_EMAILS
            },
            Message={
                'Subject': {
                    'Data': secrets.DIGEST_SUBJECT.format(today=digest['today']),
                    'Charset': secrets.CHARSET
                },
                'Body': {
                    'Html': {
                        'Data': template.render(x=digest),
                        'Charset': secrets.CHARSET
                    }
                }
            },
            ReplyToAddresses=[secrets.FROM_STUDY_EMAIL]
        )
    except ClientError as e:
        raise Exception(f'email failed with <{str(e.response["Error"]["Message"])}>')
    else:
        return response


def main():
    """start the archive agent from command line

    Command line arguments:
        wait: seconds between poll to task db. default=3600
        conn: database connection parameters. default defined in application config
        k: keep alive. default=False

    Examples:
        The following will start the agent with default options
        >>> python3 archive_agent.py

        The following starts with a 30 minute poll interval and keep alive
        >>> python3 archive_agent.py --wait 108000 --k 1
    """
    parser = argparse.ArgumentParser(description='--')
    parser.add_argument(
        '--wait',
        type=int,
        help='interval in seconds between poll to task db',
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
        wait_time = 3600

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

    agent.start()
    return f'agent started on process {agent.get_pid()}'


if __name__ == '__main__':
    main()
