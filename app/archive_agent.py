#!/bin/env python

import argparse
import datetime as dt
import gc
from io import BytesIO
import json
from multiprocessing import Pipe, Process
from multiprocessing.dummy import Pool as TPool
import os
import time
from zipfile import ZipFile

from google.oauth2.credentials import Credentials
from google.auth.transport.requests import AuthorizedSession
from jinja2 import Template
import pandas as pd
import numpy as np
from sendgrid import Email, SendGridAPIClient
from sendgrid.helpers.mail import Content, Mail, Personalization
from synapseclient import File, Activity

import app.config as secrets
import app.context as ctx

syn = secrets.syn


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

    def __del__(self):
        self.terminate()

    def get_pid(self):
        return self.__agent.pid

    def get_status(self):
        return f'archive agent <pid={self.get_pid()}> is{" " if self.__agent.is_alive() else "not "}running'

    def start_async(self):
        if not self.__agent.is_alive():
            self.__agent.start()
        else:
            pass

    def start(self):
        self.start_async()

        time.sleep(5)
        self.terminate()

    def terminate(self):
        if self.__agent.is_alive():
            self.__sigkill.send(True)
            ctx.add_log_entry('signalled agent to terminate')
        else:
            pass

        self.__done.recv()
        ctx.add_log_entry('agent terminated gracefully')

        self.__agent.join()

    @staticmethod
    def __run_agent(wait_time, conn, keep_alive, sigkill, done):
        terminate, digest_date = False, dt.date.today()

        while not terminate:
            try:
                start = time.time()

                # process tasks
                with ctx.session_scope(conn) as s:
                    pending = ctx.get_all_pending(session=s)

                    n = len(pending)
                    if n > 0:
                        ctx.add_log_entry(f'found {n} task{"s" if n > 1 else ""} to process')

                    while len(pending) > 0:
                        consent = pending.pop()
                        ctx.add_log_entry(f'starting task for {str(consent)}', cid=consent.eid)

                        task = TakeOutExtractor(consent)
                        task.run()

                        consent.update_synapse()

                        del task, consent
                        gc.collect()

                # check for termination
                terminate = sigkill.poll(1)
                if not terminate:
                    remaining = wait_time - (time.time() - start)
                    time.sleep(remaining if remaining > 0 else 0)
                else:
                    pass

                # check for digest send
                now = dt.date.today()
                if (now-digest_date).days > 0:
                    send_daily_digest()
                    digest_date = now
                else:
                    pass

            except Exception as e:
                ctx.add_log_entry(f'agent terminated unexpectedly. {str(e.__class__)}: {", ".join([a for a in e.args])}')

                if not keep_alive:
                    ctx.add_log_entry('agent shutting down')
                    break
                else:
                    ctx.add_log_entry('agent restarting')

        done.send(True)


class TakeOutExtractor(object):
    def __init__(self, consent):
        self.consent = consent

        self.__authorized_session = self.authorize_user_session()
        self.__zip_stream = None
        self.__tmp_files = []
        self.__tid = None

    def __repr__(self):
        return f'<TakeOutExtractor({str(self.consent)})>'

    def __del__(self):
        if hasattr(self, '__zip_stream') and self.__zip_stream is not None:
            self.__zip_stream.close()
            del self.__zip_stream

        for tmp in self.__tmp_files:
            if os.path.exists(tmp['path']):
                os.remove(tmp['path'])

        del self.__tmp_files
        gc.collect()

    @property
    def takeout_id(self):
        if self.__tid is not None:
            return self.__tid
        else:
            to_file = self.__authorized_session.get(secrets.TAKEOUT_URL)
            df = pd.DataFrame.from_records(json.loads(to_file.content)['files'])

            if len(df) > 0:
                df['timeStamp'] = df.name.str.split('-', 3).apply(lambda x: x[1])
                df['timeStamp'] = pd.to_datetime(df['timeStamp'])

                self.__tid = df.id[df.timeStamp.idxmax()]
                return self.__tid
            else:
                return False

    @property
    def zipped(self):
        return ZipFile(self.__zip_stream)

    def authorize_user_session(self):
        try:
            jdata = json.loads(self.consent.credentials)

            credentials = Credentials(
                token=jdata['access_token'],
                refresh_token=jdata['refresh_token'],
                token_uri=jdata['token_uri'],
                client_id=jdata['client_id'],
                client_secret=jdata['client_secret']
            )

            return AuthorizedSession(credentials)
        except TypeError as e:
            if any(['NoneType' in a for a in e.args]):
                self.__log_it('cannot authorize session without credentials')
                return None
            else:
                self.__log_it('failed to authorize participant http session')

    def __log_it(self, s):
        ctx.add_log_entry(s, cid=self.consent.eid)

    def download_takeout_data(self):
        try:
            url = f'https://www.googleapis.com/drive/v3/files/{self.takeout_id}?alt=media'
            response = self.__authorized_session.get(url)

            if response.status_code == 200:
                self.__zip_stream = BytesIO(response.content)
                return True
            else:
                return False
        except Exception as e:
            self.__log_it(
                f'downloading takeout data for eid={self.consent.eid} failed with error={str(e)}'
            )
            return False

    def extract_searches(self):
        try:
            search_files = [f for f in self.zipped.namelist() if 'Search' in f]

            if len(search_files) > 0:
                dfs = []
                for fn in search_files:
                    with self.zipped.open(fn) as f:
                        s = f.read().decode('utf-8')
                        df = pd.DataFrame(json.loads(s))
                        dfs.append(df)

                search_queries = pd.concat(dfs, sort=False)

                def tx(x):
                    try:
                        t = dt.datetime.strptime(x, '%Y-%m-%dT%H:%M:%S.%fZ')
                    except ValueError:
                        t = dt.datetime.strptime(x, '%Y-%m-%dT%H:%M:%SZ')
                    except Exception as e:
                        raise e

                    return t

                search_queries['time'] = search_queries.time.apply(tx)
                search_queries = search_queries.sort_values(by='time')

                search_queries = search_queries.drop(columns=[
                    'details', 'products', 'titleUrl'
                ], errors='ignore')

                actions, titles = [], []
                for q in search_queries.title:
                    a = q.split(' ')

                    action = a[0]
                    if action == 'Searched':
                        title = ' '.join(a[2:])
                    else:
                        title = ' '.join(a[1:])

                    actions.append(action)
                    titles.append(title)

                search_queries['action'] = actions
                search_queries['title'] = titles

                filename = os.path.join(
                    secrets.ARCHIVE_AGENT_TMP_DIR,
                    f'search_{str(self.consent.eid)}.csv'
                )
                search_queries.to_csv(filename, index=None)

                self.__tmp_files.append({
                    'type': 'search',
                    'path': filename
                })
                self.__log_it(f'searches for eid={self.consent.eid} downloaded successfully')
                return True
            else:
                self.consent.add_search_error(
                    f'search file for {self.consent.eid} not found in takeout data'
                )
                return False
        except Exception as e:
            self.consent.add_search_error(
                f'downloading searches for eid={self.consent.eid} failed with error={str(e)}'
            )
            return False

    def extract_gps(self):
        try:
            gps_files = [
                f for f in self.zipped.namelist()
                if 'Location History' in f
            ]

            if len(gps_files) > 0:
                def write_json(count, f):
                    filename = '%s_%s_%s_GPS.json' % (
                        str(self.consent.pid), self.consent.eid, count + 1
                    )
                    filename = os.path.join(secrets.ARCHIVE_AGENT_TMP_DIR, filename)

                    with open(filename, 'wb') as out:
                        out.write(self.zipped.open(f).read())
                        out.close()

                    return filename

                location_files = [
                    write_json(count, f)
                    for count, f in enumerate(gps_files)
                ]

                self.__tmp_files += [{
                        'type': 'gps_part',
                        'path': l
                    } for l in location_files
                ]
                self.__log_it(
                    f'{len(location_files)} location part(s) for eid={self.consent.eid} downloaded successfully'
                )

                return self.clean_gps()
            else:
                self.consent.add_location_error(
                    f'location files for eid={self.consent.eid} not found in takeout data'
                )
                return False
        except Exception as e:
            self.consent.add_location_error(
                f'downloading location parts for eid={self.consent.eid} failed with error={str(e)}'
            )
            return False

    def clean_gps(self):
        try:
            dfs = []
            for fn in [f for f in self.__tmp_files if f['type'] == 'gps_part']:
                dfs.append(parse_google_location_data(fn['path']))

            df = pd.concat(dfs, sort=False).sort_values(by='ts')

            filename = os.path.join(
                secrets.ARCHIVE_AGENT_TMP_DIR,
                f'GPS_{self.consent.pid}_{self.consent.eid}.csv'
            )
            df.to_csv(filename, index=None)

            self.__tmp_files.append({
                'type': 'gps',
                'path': filename
            })
            self.__log_it(f'parsing location data for eid={self.consent.eid} completed successfully')
            return True
        except Exception as e:
            self.consent.add_location_error(
                f'parsing location data for eid={str(self.consent.eid)} failed with error={str(e)}'
            )
            return False

    def push_to_synapse(self, force=False):
        cnt = 0

        for tmp in self.__tmp_files:
            t, path = tmp['type'], tmp['path']

            if t not in ['gps', 'gps_part', 'search']:
                continue

            if t in ['gps', 'gps_part']:
                parent = secrets.LOCATION_SYNID
                setter = self.consent.set_location_sid
            elif t == 'search':
                parent = secrets.SEARCH_SYNID
                setter = self.consent.set_search_sid
            else:
                raise Exception('there\'s a snake in my boot')

            if force or not does_exist(parent, path):
                try:
                    result = syn.store(File(path, parentId=parent))

                    synid = result.properties['id']
                    setter(synid)

                    syn.setProvenance(
                        synid,
                        activity=Activity(
                            name='gTap Archive Manager',
                            description='This file was created by gTap',
                        )
                    )

                    cnt += 1
                    self.__log_it(f'uploaded {t} data as {synid} for eid={self.consent.eid}')
                except Exception as e:
                    self.__log_it(
                        f'uploading {t} data for eid={self.consent.eid} failed with error={str(e)}'
                    )
        return cnt

    def run(self):
        if self.takeout_id is not None:
            if self.download_takeout_data() and any([
                self.extract_searches(),
                self.extract_gps()
            ]):
                cnt = self.push_to_synapse()

                self.__log_it(f'task for eid={self.consent.eid} completed. {cnt} files uploaded to Synapse')

                self.consent.clear_credentials()
                self.consent.notify_participant()
            else:
                pass
        else:
            self.__log_it(f'Google Drive for eid={self.consent.eid} not ready')


def get_wait_time_from_env():
    if 'ARCHIVE_AGENT_WAIT_TIME' in os.environ:
        return float(os.environ['ARCHIVE_AGENT_WAIT_TIME'])
    else:
        return None


def parse_google_location_data(filename):
    def arow(args):
        idx, row = args

        try:
            j_ = js.activity[idx]

            if isinstance(j_, float):
                return np.nan

            if len(j_) > 0:
                result = j_[0]['activity'][0]['type']
                return result
            else:
                return np.nan
        except:
            return np.nan

    with open(filename, 'r') as f:
        js = json.load(f)

    js = pd.DataFrame(js['locations'])

    if 'verticalAccuracy' in js.columns:
        js.drop(columns='verticalAccuracy', inplace=True)

    if 'altitude' in js.columns:
        js.drop(columns='altitude', inplace=True)

    if 'heading' in js.columns:
        js.drop(columns='heading', inplace=True)

    if 'velocity' in js.columns:
        js.drop(columns='velocity', inplace=True)

    js.timestampMs = pd.to_datetime(js.timestampMs, unit='ms')

    js.latitudeE7 = np.round(js.latitudeE7 / 10e6, 5)
    js.longitudeE7 = np.round(js.longitudeE7 / 10e6, 5)

    js['date'] = js.timestampMs.apply(dt.datetime.date)

    if 'activity' in js.columns:
        pool = TPool(secrets.CLEANING_THREADS)

        js.activity = list(pool.map(arow, list(js.iterrows())))

        pool.close()
        pool.join()

    js.rename(columns={'latitudeE7': 'lat', 'longitudeE7': 'lon', 'timestampMs': 'ts'}, inplace=True)

    js = js.sort_values(by='ts')
    return js


def does_exist(synid, name):
    children = [child['name'] for child in list(syn.getChildren(synid))]
    return any([name in c for c in children])


def send_daily_digest(conn=None):
    digest = ctx.daily_digest(conn)

    template = Template(secrets.DIGEST_TEMPLATE)
    content = Content(
        "text/html",
        template.render(x=digest)
    )

    from_ = Email(secrets.FROM_STUDY_EMAIL)
    to_ = [Email(e) for e in secrets.ADMIN_EMAILS]

    mail = Mail(
        from_email=from_,
        subject=secrets.DIGEST_SUBJECT.format(today=digest['today']),
        content=content
    )

    p = Personalization()
    for to in to_:
        p.add_to(to)

    mail.add_personalization(p)

    sg = SendGridAPIClient(apikey=secrets.SENDGRID_API_KEY)
    response = sg.client.mail.send.post(request_body=mail.get())

    return response.status_code


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

    agent.start()
    return f'agent started on pid {agent.get_pid()}'


if __name__ == '__main__':
    # build_synapse_log()
    main()
