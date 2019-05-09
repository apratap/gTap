#!/bin/env python

import argparse
import datetime as dt
import gc
import re
from io import BytesIO
import json
import numpy as np
from multiprocessing.dummy import Pool as TPool
import os
from pytz import timezone as tz
import sys
from zipfile import ZipFile

import google.cloud.dlp as dlp
from google.oauth2.credentials import Credentials
from google.auth.transport.requests import AuthorizedSession
import numpy as np
import pandas as pd
from synapseclient import File, Activity

import app.config as secrets
import app.context as ctx

syn = secrets.syn

"""generate a single authorized client for all tasks"""
__dlp = dlp.DlpServiceClient()

"""takeout errors"""
DRIVE_NOT_READY = 'drive not ready'
ARCHIVE_STRUCTURE_FAILURE = 'takeout archive has no content'
TAKEOUT_URL_FAILURE = 'takeout url could not be found'


class TakeOutExtractor(object):
    """class for processing takeout data"""

    def __init__(self, consent, **kwargs):
        """constructor

        Args:
            consent: (gtap.context.Consent) consent to process
        """
        self.consent = consent

        self.__archive_path = None
        self.__authorized_session = None
        self.__local = False

        if 'archive_path' in kwargs.keys():
            self.__archive_path = kwargs['archive_path']
            self.__local = True
        else:
            self.__authorized_session = self.authorize_user_session()

        self.__zip_stream = None
        self.__tmp_files = []
        self.__tid = None
        self.__search_queries = None
        self.__gps_queries = None
        self.cleaned_search_file = None
        self.cleaned_gps_file = None

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

        elif self.__archive_path is not None:
            self.__tid = 'local'
            return self.__tid

        else:
            response = self.__authorized_session.get(secrets.TAKEOUT_URL)

            if response.status_code == 200:
                content = json.loads(response.content).get('files')

                if content is not None:
                    df = pd.DataFrame.from_records(content)

                    if len(df) > 0:
                        df['timeStamp'] = df.name.str.split('-', 3).apply(lambda x: x[1])
                        df['timeStamp'] = pd.to_datetime(df['timeStamp'])

                        self.__tid = df.id[df.timeStamp.idxmax()]
                        return self.__tid
                    else:
                        return DRIVE_NOT_READY
                else:
                    self.consent.mark_as_failure(ARCHIVE_STRUCTURE_FAILURE)
                    return ARCHIVE_STRUCTURE_FAILURE
            else:
                self.consent.mark_as_failure(TAKEOUT_URL_FAILURE)
                return TAKEOUT_URL_FAILURE

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

    def __filename(self, p):
        if self.__local:
            filename = os.path.join(os.getcwd(), p)
        else:
            filename = os.path.join(secrets.ARCHIVE_AGENT_TMP_DIR, p)

        return filename

    def __log_it(self, s):
        """add log message for associated consent. Synapse consents table is updated."""
        if self.__local:
            print(f'{dt.datetime.now(tz(secrets.TIMEZONE)).strftime(secrets.DTFORMAT).upper()}: {s}')

        ctx.add_log_entry(s, cid=self.consent.internal_id)
        self.consent.update_synapse()

    def download_takeout_data(self):
        """download takeout archive from Google Drive

        Returns:
            success flag as bool
        """
        if self.__authorized_session is None:
            return False

        try:
            url = f'https://www.googleapis.com/drive/v3/files/{self.takeout_id}?alt=media'
            response = self.__authorized_session.get(url)

            if response.status_code == 200:
                self.__zip_stream = BytesIO(response.content)
                self.__log_it(f'takeout archive downloaded')
                return True
            else:
                return False
        except Exception as e:
            self.__log_it(f'downloading takeout data failed with <{str(e)}>')
            return False

    def load_from_local(self):
        """load takeout archive from local filesystem"""
        if self.__archive_path is None:
            return False
        try:
            with open(self.__archive_path, 'rb') as f:
                self.__zip_stream = BytesIO(f.read())
            
            self.__log_it('takeout archive loaded from filesystem')
            return True
        except Exception as e:
            self.__log_it(f'loading takeout data from filesystem failed with <{str(e)}>')
            return False

    def extract_searches(self):
        """extract search data from takeout archive

        Returns:
            success flag as bool
        """
        try:
            search_files = [f for f in self.zipped.namelist() if 'Search' in f]

            if len(search_files) > 0:
                self.__log_it(f'Found <{len(search_files)}> search files')

                dfs = []
                for fn in search_files:
                    prefix, suffix = str(fn).split('.')
                    self.__log_it(f'Processing file {fn} with suffix {suffix}')

                    ## Process JSON search file
                    if suffix == 'json':                        
                        with self.zipped.open(fn) as f:
                            s = f.read().decode('utf-8')
                            df = pd.DataFrame(json.loads(s))
                            df['action'] = df.title.str.extract(r'(?P<action>Visited|Searched)')
                            df.title = df.title.str.replace('Visited ', '')
                            df.title = df.title.str.replace('Searched for ', '')
                            dfs.append(df)

                    #Process HTML search file
                    elif suffix == 'html':
                        with open('tmp.html', 'wb') as out:
                            out.write(self.zipped.open(fn).read())
                            out.close()
                            df, numTotalBlocks, numErrorBlocks = process_userSearchQueries_in_htmlFormat('tmp.html')
                            self.__log_it(f'HTML File had {numTotalBlocks} blocks with {numErrorBlocks} blocks failed parsing')
                            dfs.append(df)
                        os.remove('tmp.html')

                search_queries = pd.concat(dfs, sort=False)

                # for future studies, the 'locations' column contains true labels for locations (home, work, etc)
                # search_queries = search_queries.drop(columns=[ 'header', 'details', 'products', 'locations'], 
                #     errors='ignore')
                search_queries = search_queries.loc[:,('time', 'title', 'titleUrl', 'action')]

                self.__search_queries = search_queries
                self.__log_it(f'{search_queries.shape[0]} search queries found and extracted')
                return self.clean_searches()
            else:
                self.consent.add_search_error(f'search data not found in archive')
                return False
        except Exception as e:
            self.consent.add_search_error(f'downloading searches failed with <{str(e)}>')
            return False

    def clean_searches(self):
        """perform a tiny bit of pre-processing on search data, and redact through DLP

        Notes: All individual search files are processed into one. Only unique search entries are redacted. Web visits
        are not sent to the DLP API.

        Returns: success flag as bool
        """
        try:
            df = self.__search_queries
            uniqueSearchQueries = df.title.to_list()
            ## Run the text queries through DLP api
            dlp_results = run_dlp_api(uniqueSearchQueries)
            #merge the DLP results
            df = df.merge(dlp_results, how='left', left_on='title', right_on='title')
            toRedact = ~df.info_type.isnull()
            df.title[toRedact] = 'REDACTED'
            df['redact'] = toRedact
            filename = self.__filename(
                secrets.SYNAPSE_SEARCH_NAMING_CONVENTION.format(
                    studyId=self.consent.study_id, internalID=self.consent.internal_id))
            df.to_csv(filename, index=None)
            self.cleaned_search_file = filename
            self.__log_it(f'searches redacted')
            return True
        except Exception as e:
            self.__log_it(f'redaction failed with <{str(e)}>')
            self.consent.add_search_error(f'redaction failed with <{str(e)}>')
            return False

    def extract_gps(self):
        """extract GPS data from takeout archive
        Returns: success flag as bool
        """
        try:
            gps_files = [ f for f in self.zipped.namelist() if 'Location History' in f ]

            if len(gps_files) > 0:
                dfs = []
                for fn in gps_files:
                    with open('tmp.json', 'wb') as out:
                        out.write(self.zipped.open(fn).read())
                        out.close()
                        dfs.append(parse_google_location_data('tmp.json'))
                os.remove('tmp.json')
                df = pd.concat(dfs, sort=False)
                filename = self.__filename(
                    secrets.SYNAPSE_LOCATION_NAMING_CONVENTION.format(studyId=self.consent.study_id, 
                        internalID=self.consent.internal_id)
                )
                df.to_csv(filename, index=None)
                self.cleaned_gps_file = filename
                self.__log_it(f'location data extracted')
                return True
            else:
                self.__log_it(f'location data not found in archive')
                self.consent.add_location_error('location data not found in archive')
                return False

        except Exception as e:
            self.__log_it(f'Either downloading/parsing location parts failed with <{str(e)}>')
            self.consent.add_location_error(f'Either downloading/parsing location parts failed with <{str(e)}>')
            return False


    def push_to_synapse(self):
        """upload all processed files to Synapse
        """
        count = 0
        def tmp(self, path, setter, parent):        
            try:
                result = syn.store(File(path, parentId=parent))
                synid = result.properties['id']
                setter(synid)
                syn.setProvenance(synid, activity=Activity( name='gTap Archive Manager'))
                syn.setAnnotations( synid, annotations={    
                    'study_id': self.consent.study_id,
                    'internal_id': self.consent.internal_id
                    }
                )
                self.__log_it(f'uploaded {path} data as {synid}')
                os.remove(path)
            except Exception as e:
                self.__log_it(f'uploading {path} data failed with <{str(e)}>')
                return 0

        if self.cleaned_search_file is not None:
            parent = secrets.SEARCH_SYNID
            setter = self.consent.set_search_sid
            tmp(self, self.cleaned_search_file, setter, parent)
            count += 1

        if self.cleaned_gps_file is not None:
            parent = secrets.LOCATION_SYNID
            setter = self.consent.set_location_sid
            tmp(self, self.cleaned_gps_file, setter, parent)
            count += 1
        return count


    def run(self):
        """perform the extraction process"""
        if self.takeout_id in [ARCHIVE_STRUCTURE_FAILURE, TAKEOUT_URL_FAILURE]:
            self.consent.mark_as_failure(self.takeout_id)

        elif self.takeout_id == DRIVE_NOT_READY:
            self.consent.set_status(ctx.ConsentStatus.DRIVE_NOT_READY)
            self.__log_it(f'Google Drive for {self.consent.study_id} not ready')

        else:
            if (self.download_takeout_data() or self.load_from_local()) and any([
                self.extract_searches(),
                self.extract_gps()
            ]):
                try:
                    count = self.push_to_synapse()
                    self.consent.clear_credentials()
                    self.consent.notify_admins()
                    self.__log_it(f'task complete. {count} file {"s" if count > 1 else ""} put to Synapse')
                    self.consent.set_status(ctx.ConsentStatus.COMPLETE)
                except Exception as e:
                    ctx.add_log_entry(str(e), self.consent.internal_id)
                    self.consent.set_status(ctx.ConsentStatus.FAILED)
            else:
                pass
        return self



def process_userSearchQueries_in_htmlFormat(html_file):
    '''
    html_file_content - is the content (string) of the HTML file
    '''
    textSearches = []
    webVisits = []
    errorBlock = []
    
    def __processSearchBlock(block):
        webVisit       = re.match('^.*Visited.*href="(.*)">(.*)</a><br>(.*?)</div.*$', block)
        textSearch     = re.match('^.+?Searched for.+?">(.+?)</a><br>(.*?)</div>.+$', block)
        #location       = re.match('^.*Locations.*">(.*)</a><br></div></div></div>$', block)
        if textSearch:
            textSearch = textSearch.groups()
        else:
            textSearch = None
        if webVisit:
            webVisit = webVisit.groups()
        else:
            webVist = None
        return([textSearch,webVisit])

    with open(html_file, "r") as f:
        contents = f.read()
    #this should be a unique search block in HTML file
    blocks = re.findall('<div class="outer-cell.+?mdl-shadow--2dp">.+?</div></div></div>',contents)

    #process each block    
    for b in blocks:
        textSearch,webVisit = __processSearchBlock(b)
        if textSearch is None and webVisit is None:
            errorBlock.append(b)
        if textSearch: textSearches.append(textSearch) 
        if webVisit: webVisits.append(webVisit)
    webVisits_df = pd.DataFrame.from_records(webVisits,columns=('titleUrl', 'title', 'time' ))
    textSearches_df = pd.DataFrame.from_records(textSearches,columns=('title', 'time'))
    textSearches_df['titleUrl'] = 'NA'
    textSearches_df['action'] = 'Searched'
    webVisits_df['action'] = 'Visited'
    
    df = pd.concat([textSearches_df,webVisits_df], sort=False)
    df = df.loc[:, ('time', 'title', 'titleUrl', 'action')]
    numTotalBlocks = len(blocks)
    numErrorBlocks = len(errorBlock)
    return([df, numTotalBlocks, numErrorBlocks])


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


def run_dlp_api(queryList):
    """redact a dataframe through DLP

    Args: queryList

    Returns:pandas.DataFrame 
    """""

    def buildQueryTable(searchQueries):
        """
        ref - https://github.com/GoogleCloudPlatform/python-docs-samples/blob/master/dlp/inspect_content.py
        """
        searchQueries = np.unique(searchQueries)
        rows = []
        for query in searchQueries:
            rows.append({ "values": [{"string_value": query} ]})
        table = {}
        table['headers'] = [ {'name' : 'userSearchQueries' }]
        table['rows'] = rows
        item = {"table" : table}
        return item

    def chunks(l, n):
        """Yield successive n-sized chunks from l"""
        for i in range(0, len(l), n):
            yield l[i:i + n]
        
    def make_dlp_request(dlpServiceObject, searchQueries, DLP_PROJECT_ID, DLP_INSPECT_CONFIG):
        parent = dlpServiceObject.project_path(DLP_PROJECT_ID)
        
        #pre-process the text list into JSON
        items = buildQueryTable(searchQueries)
        
        #Run the actual query
        response = dlpServiceObject.inspect_content(
                parent=parent,
                inspect_config=DLP_INSPECT_CONFIG,
                item=items)
        tmp = []
        for f in response.result.findings:
            tmp.append([f.quote, f.info_type.name, f.likelihood])
        df = pd.DataFrame.from_records(tmp, columns=('title', 'info_type', 'likelihood'))
        return df

    ### __dlp is the global authorized object to make queries using DLP service account creds
    ### parent = sets the project under which DLP queries are run
    parent = __dlp.project_path(secrets.DLP_PROJECT_ID)    
    queryList = np.unique(queryList)
    ##Process the queryList in chunks - Max CHUNK_SIZE = 2000 (can be changed)
    CHUNK_SIZE= 2000
    DLP_results = [ ]
    for chunk in chunks(queryList, CHUNK_SIZE):
        DLP_results.append(make_dlp_request(__dlp, chunk, secrets.DLP_PROJECT_ID, secrets.DLP_INSPECT_CONFIG))
    DLP_results = pd.concat(DLP_results, ignore_index=True)

    return DLP_results


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

    # if 'altitude' in js.columns:
    #     js.drop(columns='altitude', inplace=True)

    if 'heading' in js.columns:
        js.drop(columns='heading', inplace=True)

    # if 'velocity' in js.columns:
    #     js.drop(columns='velocity', inplace=True)

    js.timestampMs = pd.to_datetime(js.timestampMs, unit='ms')
    js.latitudeE7 = np.round(js.latitudeE7 / 10e6, 5)
    js.longitudeE7 = np.round(js.longitudeE7 / 10e6, 5)

    if 'activity' in js.columns:
        pool = TPool(secrets.CLEANING_THREADS)
        js.activity = list(pool.map(arow, list(js.iterrows())))
        pool.close()
        pool.join()

    js.rename(columns={'latitudeE7': 'lat', 'longitudeE7': 'lon', 'timestampMs': 'ts'}, inplace=True)
    return js


def process_from_local(study_id, consent_dt, path):
    """process a takeout archive located in the local filesystem

    Args:
        study_id: (str) participant's study id
        consent_dt: (datetime) datetime the participant consented 
        path: (str) path to takeout archive
    """
    args = {
        'study_id': study_id,
        'consent_dt': consent_dt,
    }

    with ctx.session_scope(secrets.DATABASE) as s:
        consent = ctx.add_entity(s, ctx.Consent(**args))

        consent.set_status(ctx.ConsentStatus.PROCESSING)
        ctx.add_log_entry(f'starting task', cid=consent.internal_id)

        try:
            task = TakeOutExtractor(consent, archive_path=path).run()
            # make sure all updates have been persisted to backend
            ctx.commit(s)
            # final call to update Synapse consents table
            task.consent.update_synapse()
        except Exception as e:
            consent.set_status(ctx.ConsentStatus.FAILED)
            print(e)
            return 1
    return 0


def main():
    """run the takeout extractor from the command line

    Command line arguments:
        studyid: (str) study id for participant
        dt: (str) datetime as a string in the format '%m/%d/%Y-%Z-%H:%M:%S'
        path: (str) path to takeout archive

    Examples:
        >>> python3 xtractor.py --studyid testcase --dt "03/28/2019-UTC-11:07:00" --path "/home/luke/to.zip"
    """
    parser = argparse.ArgumentParser(description='--')
    parser.add_argument(
        '--studyid',
        type=str,
        help='study id',
        required=True
    )
    parser.add_argument(
        '--dt',
        type=str,
        help="""
        dd/mon/year 
            dd:   2 digit day
            mon:  2 digit month
            year: 4 digit year
        """,
        required=True
    )
    parser.add_argument(
        '--path',
        type=str,
        help='file path to takeout zipfile',
        required=True
    )

    args = parser.parse_args()

    # verify consent date is in correct format
    fmt = '%m/%d/%Y'
    try:
        consent_dt = dt.datetime.strptime(args.dt, fmt)
    except ValueError as e:
        print(e)
        return 1

    # verify takeout zipfile exists
    if not os.path.exists(args.path):
        print(f'takeout archive does not exist at {args.path}')
        return 1
    else:
        path = args.path

    exit_code = process_from_local(args.studyid, consent_dt, path)
    return exit_code


if __name__ == '__main__':
    sys.exit(main())
