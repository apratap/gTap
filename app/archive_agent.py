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

from botocore.exceptions import ClientError
import boto3
from google.oauth2.credentials import Credentials
from google.auth.transport.requests import AuthorizedSession
import google.cloud.dlp as dlp
from jinja2 import Template
import pandas as pd
import numpy as np
from synapseclient import File, Activity

import app.config as secrets
import app.context as ctx

syn = secrets.syn

"""generate a single authorized client for all tasks"""
__dlp = dlp.DlpServiceClient()

DRIVE_NOT_READY = 'drive not ready'


class TakeOutExtractor(object):
    """class for processing takeout data"""

    def __init__(self, consent):
        """constructor

        Args:
            consent: (gtap.context.Consent) consent to process
        """
        self.consent = consent

        self.__authorized_session = self.authorize_user_session()
        self.__zip_stream = None
        self.__tmp_files = []
        self.__tid = None

    def __repr__(self):
        return f'<TakeOutExtractor({str(self.consent)})>'

    def __del__(self):
        """make sure we don't leave any streams leaking or tmp files in the OS"""
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
        """get the takeout id

        Returns:
            (str) to represent the id if the takeout data is ready else DRIVE_NOT_READY
        """
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
                return DRIVE_NOT_READY

    @property
    def zipped(self):
        return ZipFile(self.__zip_stream)

    def authorize_user_session(self):
        """authorize the HTTP session with consent credentials

        Returns:
            AuthorizedSession
        """
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
                self.consent.add_search_error()
                self.consent.add_location_error()

                self.__log_it('cannot authorize session without credentials')
                return None
            else:
                self.__log_it('failed to authorize participant http session')

    def __log_it(self, s):
        """add log message for associated consent. Synapse consents table is updated."""
        ctx.add_log_entry(s, cid=self.consent.internal_id)
        self.consent.update_synapse()

    def download_takeout_data(self):
        """download takeout archive from Google Drive

        Returns:
            success flag as bool
        """
        try:
            url = f'https://www.googleapis.com/drive/v3/files/{self.takeout_id}?alt=media'
            response = self.__authorized_session.get(url)

            if response.status_code == 200:
                self.__zip_stream = BytesIO(response.content)
                return True
            else:
                return False
        except Exception as e:
            self.__log_it(f'downloading takeout data failed with <{str(e)}>')
            return False

    def extract_searches(self):
        """extract search data from takeout archive

        Returns:
            success flag as bool
        """
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
                    except Exception as ex:
                        raise ex

                    return t

                search_queries['time'] = search_queries.time.apply(tx)
                search_queries = search_queries.sort_values(by='time')

                # for future studies, the 'locations' column contains true labels for locations (home, work, etc)
                search_queries = search_queries.drop(columns=[
                    'header', 'details', 'products', 'locations'
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
                    f'search_raw_{str(self.consent.internal_id)}.csv'
                )
                search_queries.to_csv(filename, index=None)

                self.__tmp_files.append({
                    'type': 'search_raw',
                    'path': filename
                })
                self.__log_it(f'searches downloaded')

                return self.clean_search()
            else:
                self.consent.add_search_error(f'search data not found in archive')
                return False
        except Exception as e:
            self.consent.add_search_error(f'downloading searches failed with <{str(e)}>')
            return False

    def clean_search(self):
        """perform a tiny bit of pre-processing on search data, and redact through DLP

        Notes: All individual search files are processed into one. Only unique search entries are redacted. Web visits
        are not sent to the DLP API.

        Returns:
            success flag as bool
        """
        try:
            dfs = [
                pd.read_csv(f['path'])
                for f in self.__tmp_files if f['type'] == 'search_raw'
            ]

            if len(dfs) == 1:
                df = dfs[0]
            elif len(dfs) > 1:
                df = pd.concat(dfs, axis=0, sort=False)
            else:
                return False

            # only process unique searches, build a reference set of uniques to the dup rows
            def fx(x):
                idx = x.index.tolist()[0]

                if len(x) > 1:
                    ref = ','.join([str(int(i)) for i in x.index.tolist()[1:]])
                    return pd.Series([idx, ref])
                else:
                    return pd.Series([idx, np.nan])

            idx_map = df.loc[df.action == 'Searched']\
                .groupby('title')\
                .apply(fx)\
                .reset_index(drop=True)\
                .rename(columns={0: 'src', 1: 'ref'})

            idx_map.src = idx_map.src.astype(int)

            redacted = make_dlp_request(df.loc[idx_map.src]).reset_index()

            cols_to_update = []
            for c in redacted.columns:
                if c == 'index':
                    continue

                if c not in df.columns:
                    df[c] = ''
                    cols_to_update.append(c)
                    df.loc[redacted.index, c] = redacted[c]

            df.loc[redacted.index, 'title'] = redacted.title

            idx_map = idx_map.loc[redacted.info_type.apply(lambda x: len(x) > 0), :]

            def fx(x):
                to_set = [int(i) for i in x.ref.split(',')]
                df.loc[to_set, 'title'] = redacted.loc[x.src, 'title']

                for c in cols_to_update:
                    df.loc[to_set, c] = redacted.loc[x.src, c]

            [fx(x) for x in idx_map.loc[idx_map.ref.notna()].itertuples()]

            filename = os.path.join(
                secrets.ARCHIVE_AGENT_TMP_DIR,
                secrets.SYNAPSE_SEARCH_NAMING_CONVENTION.format(self.consent.study_id, self.consent.internal_id)
            )
            self.__tmp_files.append({
                'type': 'search_redacted',
                'path': filename
            })
            df.to_csv(filename, index=None)

            self.__log_it(f'searches redacted')
            return True
        except Exception as e:
            self.consent.add_search_error(f'redaction failed with <{str(e)}>')
            return False

    def extract_gps(self):
        """extract GPS data from takeout archive

        Returns:
            success flag as bool
        """
        try:
            gps_files = [
                f for f in self.zipped.namelist()
                if 'Location History' in f
            ]

            if len(gps_files) > 0:
                def write_json(count, f):
                    filename = '%s_%s_%s_GPS.json' % (
                        str(self.consent.study_id), self.consent.internal_id, count + 1
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
                self.__log_it(f'{len(location_files)} location part(s) downloaded')

                return self.clean_gps()
            else:
                self.consent.add_location_error('location data not found in archive')
                return False
        except Exception as e:
            self.consent.add_location_error(f'downloading location parts failed with <{str(e)}>')
            return False

    def clean_gps(self):
        """clean GPS data

        Returns:
            success flag as bool
        """
        try:
            parts, dfs = [f for f in self.__tmp_files if f['type'] == 'gps_part'], []

            if len(parts) > 0:
                for fn in parts:
                    dfs.append(parse_google_location_data(fn['path']))

                df = pd.concat(dfs, sort=False).sort_values(by='ts')

                filename = os.path.join(
                    secrets.ARCHIVE_AGENT_TMP_DIR,
                    secrets.SYNAPSE_LOCATION_NAMING_CONVENTION.format(self.consent.study_id, self.consent.internal_id)
                )
                df.to_csv(filename, index=None)

                self.__tmp_files.append({
                    'type': 'gps_processed',
                    'path': filename
                })
                self.__log_it(f'parsing location data completed successfully')

                return True
            else:
                return False
        except Exception as e:
            self.consent.add_location_error(f'parsing location data failed with <{str(e)}>')
            return False

    def push_to_synapse(self, force=False):
        """upload all processed files to Synapse

        Args:
            force: (bool) optional flag to force overwrite if file exists

        Returns:
            (int) as number of files uploaded
        """
        cnt = 0

        for tmp in self.__tmp_files:
            t, path = tmp['type'], tmp['path']

            if t not in ['gps_processed', 'search_redacted']:
                continue

            if t in ['gps_processed']:
                parent = secrets.LOCATION_SYNID
                setter = self.consent.set_location_sid
            elif t in ['search_redacted']:
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
                    syn.setAnnotations(
                        synid,
                        annotations={
                            'study_id': self.consent.study_id,
                            'internal_id': self.consent.internal_id
                        }
                    )

                    cnt += 1
                    self.__log_it(f'uploaded {t} data as {synid}')
                except Exception as e:
                    self.__log_it(f'uploading {t} data failed with <{str(e)}>')
                    return 0
        return cnt

    def run(self):
        """perform the extraction process"""
        if self.takeout_id != DRIVE_NOT_READY:
            if self.download_takeout_data() and any([
                self.extract_searches(),
                self.extract_gps()
            ]):
                try:
                    cnt = self.push_to_synapse()

                    self.consent.clear_credentials()
                    self.consent.notify_admins()

                    self.__log_it(f'task complete. {cnt} file {"s" if cnt > 1 else ""} put to Synapse')
                    self.consent.set_status(ctx.ConsentStatus.COMPLETE)
                except Exception as e:
                    ctx.add_log_entry(str(e), self.consent.internal_id)
                    self.consent.set_status(ctx.ConsentStatus.FAILED)
            else:
                pass
        else:
            self.consent.set_status(ctx.ConsentStatus.DRIVE_NOT_READY)
            self.__log_it(f'Google Drive for {self.consent.study_id} not ready')

        return self


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


def parse_google_location_data(filename):
    """parse GPS data from Takeout archive"""
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
        except Exception:
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
    """determine whether a file exists in a Synapse entity

    Args:
        synid: (str) parent id to look within
        name: (str) name of file to look for

    Returns:
        bool
    """
    children = [child['name'] for child in list(syn.getChildren(synid))]
    return any([name in c for c in children])


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


def make_dlp_request(df):
    """redact a dataframe through DLP
    
    Notes: each request is processed in parallel according to the number of threads defined by CLEANING_THREADS in 
    the application config.
    
    Args:
        df: (pandas.DataFrame)

    Returns:
        pandas.DataFrame with redacted data and added columns
    """""
    df = df.copy()

    def inspect_wrapper(args):
        idx, x = args

        response = __dlp.inspect_content(
            parent=parent,
            inspect_config=secrets.DLP_INSPECT_CONFIG,
            item={'value': x}
        )
        return idx, x, response

    def process_results(args):
        idx, x, response = args

        findings = response.result.findings
        info_types, likelihoods, redactions = [], [], []

        if findings is not None and len(findings) > 0:
            for finding in findings:
                info_type = finding.info_type.name

                x = x.replace(finding.quote, info_type)
                info_types.append(info_type)

                lik = str(finding)
                lik = lik[lik.find('likelihood') + 11:]
                lik = lik[:lik.find('\n')].strip()
                likelihoods.append(lik)

                redactions.append('partial')

        return idx, x, ', '.join(info_types), ', '.join(likelihoods), ', '.join(redactions)

    parent = __dlp.project_path(secrets.DLP_PROJECT_ID)

    # run each query through DLP
    pool = TPool(secrets.CLEANING_THREADS)
    results = pool.map(inspect_wrapper, [(idx, q.title) for idx, q in df.iterrows()])

    # process the results
    redacted = list(pool.map(process_results, results))
    pool.close()

    new_cols = ['info_type', 'likelihood', 'redact']
    for c in new_cols:
        df[c] = ''

    for item in redacted:
        df.loc[item[0], 'title'] = item[1]
        df.loc[item[0], new_cols] = item[2:]

    return df


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
