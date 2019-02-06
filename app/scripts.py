from io import BytesIO
import json
import logging
import os
from zipfile import ZipFile

from google.oauth2.credentials import Credentials
from google.auth.transport.requests import AuthorizedSession
import pandas as pd
import sendgrid
from sendgrid.helpers.mail import *
from synapseclient import File, Activity

import app.config as config

syn = config.syn


class TakeOutExtractor(object):
    def __init__(self, consent):
        self.consent = consent

        self.authorized_session = None
        self.response = None
        self.to_file_id = None
        self.bio = None
        self.to_zipfile = None
        self.to_search_file = None
        self.to_location_file = None
        self.error = None
        self.errorMessage = None
        self.logger = logging.getLogger('Takeout Processing log')

    def __str__(self):
        return ('prefix: %s' % str(self.consent) + '\n' +
                'takeOutFileId: %s' % self.to_file_id + '\n' +
                'takeOutSearchQueryFile: %s' % self.to_search_file + '\n' +
                'takeOutLocationQueryFile: %s' % self.to_location_file)

    def __del__(self):
        if self.bio is not None:
            self.bio.close()
            del self.bio

    def authorize_user_session(self):
        jdata = json.loads(self.consent.credentials)
        credentials = Credentials(
            token=jdata['access_token'],
            refresh_token=jdata['refresh_token'],
            token_uri=jdata['token_uri'],
            client_id=jdata['client_id'],
            client_secret=jdata['client_secret']
        )

        self.authorized_session = AuthorizedSession(credentials)

    def locate_takeout_file(self):
        self.response = self.authorized_session.get(
            'https://www.googleapis.com/drive/v3/files?q=name+contains+%22takeout%22+and+mimeType+contains+%22zip%22')

    def get_takeout_id(self):
        df = pd.DataFrame.from_records(json.loads(self.response.content)['files'])

        if len(df) > 0:
            df['timeStamp'] = df.name.str.split('-', 3).apply(lambda x: x[1])
            df['timeStamp'] = pd.to_datetime(df['timeStamp'])
            self.to_file_id = df.id[df.timeStamp.idxmax()]

            return True
        else:
            return False

    def download_takeout_data(self):
        url = 'https://www.googleapis.com/drive/v3/files/%s?alt=media' % self.to_file_id
        response = self.authorized_session.get(url)

        if response.status_code == 200:
            self.bio = BytesIO(response.content)
            self.to_zipfile = ZipFile(self.bio)
            return True
        else:
            return False

    def get_searches_file(self):
        search_files = [f for f in self.to_zipfile.namelist() if 'Searches' in f]

        if len(search_files) > 0:
            search_queries = pd.concat([
                pd.read_json(self.to_zipfile.open(search_file).readlines()[0])
                for search_file in search_files
            ], sort=False).reset_index(drop=True)

            def get_ts(row):
                timestamp = row['query']['id'][0]['timestamp_usec']
                return timestamp

            def get_query(row):
                query_text = row['query']['query_text']
                return query_text

            tss = search_queries.event.map(get_ts)
            search_queries = search_queries.event.map(get_query)

            search_results = pd.concat([tss, search_queries], axis=1)
            search_results.columns = ['timeStamp', 'searchQuery']
            search_results.timeStamp = pd.to_datetime(search_results.timeStamp, unit='us')
            search_results.sort_values(by='timeStamp', inplace=True)
            search_results.reset_index(inplace=True, drop=True)

            filename = '%s_searchQueries.feath' % str(self.consent)
            search_results.to_feather(filename)

            self.to_search_file = filename
            return self.to_search_file
        else:
            raise Exception('search file not found in takeout data')

    def get_locations_file(self):
        location_files = [f for f in self.to_zipfile.namelist() if 'Location History' in f]

        if len(location_files) > 0:
            def write_json(count, f):
                filename = '%s_%s_%s_LocationQueries.json' % (
                    str(self.consent.pid), self.consent.ext_id, count + 1)

                with open(filename, 'wb') as out:
                    out.write(self.to_zipfile.open(f).read())
                    out.close()

                return filename

            location_files = [write_json(count, f) for count, f in enumerate(location_files)]

            self.to_location_file = location_files
            return location_files
        else:
            raise Exception('location file not found in takeout data')

    @staticmethod
    def upload_search_data(filename):
        result = syn.store(File(filename, parentId=config.SEARCH_SYNID))
        synid = result.properties['id']

        syn.SetProvenance(
            synid,
            activity=Activity(
                name='AutoDownload Participant Search Data',
                description='This file was created by gTAP',
            )
        )
        os.remove(filename)

        return synid

    @staticmethod
    def upload_location_data(filenames):
        results = []
        for f in filenames:
            result = syn.store(File(f, parentId=config.LOCATION_SYNID))
            synid = result.properties['id']

            syn.setProvenance(
                synid,
                activity=Activity(
                    name='AutoDownload Participant Location Data',
                    description='This file was created by gTAP'
                )
            )
            results.append(synid)
            os.remove(f)

        return ', '.join(results)

    def send_email(self):
        sg = sendgrid.SendGridAPIClient(apikey=config.SENDGRID_API_KEY)
        from_email = Email(config.FROM_STUDY_EMAIL)
        to_email = Email(self.consent.email)
        subject = config.EMAIL_SUBJECT
        content = Content("text/plain", config.EMAIL_BODY)
        mail = Mail(from_email, subject, to_email, content)
        response = sg.client.mail.send.post(request_body=mail.get())
        return response.status_code

    def run(self):
        self.authorize_user_session()
        self.locate_takeout_file()

        ready = self.get_takeout_id()
        if ready:
            downloaded = self.download_takeout_data()

            if downloaded:
                try:
                    if self.consent.search_sid is None or len(self.consent.search_sid) == 0:
                        searches_file = self.get_searches_file()
                        e1 = self.upload_search_data(searches_file)
                    else:
                        e1 = self.consent.search_sid
                except Exception as e:
                    e1 = e

                try:
                    if self.consent.location_sid is None or len(self.consent.location_sid) == 0:
                        locations_files = self.get_locations_file()
                        e2 = self.upload_location_data(locations_files)
                    else:
                        e2 = self.consent.location_sid
                except Exception as e:
                    e2 = e

                self.send_email()

                return {
                    'search': e1,
                    'location': e2
                }
            else:
                return 'session authorized and takeout data ready but failed to download'
        else:
            pass

        return 'drive_not_ready'


def parse_takeout_data():
    pass
