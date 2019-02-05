from io import BytesIO
import httplib2
import json
import logging
import os
from zipfile import ZipFile

from google.oauth2.credentials import Credentials
from google.auth.transport.requests import AuthorizedSession
import pandas as pd
import sendgrid
from sendgrid.helpers.mail import *
import synapseclient

import app.config as config

syn = synapseclient.login()


class TakeOutExtractor(object):
    def __init__(self, consent):
        self.consent = consent

        self.authorized_session = None
        self.response = None
        self.to_file_id = None
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
            self.to_file_id = df.id[df.timeStamp.argmax]

            return True
        else:
            return False

    def download_takeout_data(self):
        url = 'https://www.googleapis.com/drive/v3/files/%s?alt=media' % self.to_file_id
        file = self.authorized_session.get(url)
        self.to_zipfile = ZipFile(BytesIO(file.content))

    def get_searches_file(self):
        # search files
        search_files = [f for f in self.to_zipfile.namelist() if 'Searches' in f]

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

    def get_locations_file(self):
        location_files = [f for f in self.to_zipfile.namelist() if 'Location History' in f]

        def write_json(count, f):
            filename = '%s_%s_LocationQueries.json' % (str(self.consent), count + 1)

            with open(filename, 'wb') as out:
                out.write(self.to_zipfile.open(f).read())
                out.close()

            return filename

        location_files = [write_json(count, f) for count, f in enumerate(location_files)]

        self.to_location_file = location_files
        return location_files

    @staticmethod
    def upload_search_data(filename):
        syn.SetProvenance(
            syn.store(
                synapseclient.File(filename, parentId=config.SEARCH_SYNID)
            ),
            activity=synapseclient.Activity(
                name='AutoDownload Participant Search Data',
                description='This file was created by a background service',
            )
        )
        os.remove(filename)

        return filename

    @staticmethod
    def upload_location_data(filenames):
        for f in filenames:
            syn.SetProvenance(
                syn.store(
                    synapseclient.File(f, parentId=config.LOCATION_SYNID)
                ),
                activity=synapseclient.Activity(
                    name='AutoDownload Participant Location Data',
                    description='This file was created by a background service'
                )
            )
            os.remove(f)

        return filenames

    def send_email(self):
        sg = sendgrid.SendGridAPIClient(apikey=config.SENDGRID_API_KEY)
        from_email = Email(config.FROM_STUDY_EMAIL)
        to_email = Email(self.consent.google_user_id)
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
            self.download_takeout_data()

            e1, e2 = None, None
            try:
                searches_file = self.get_searches_file()
                self.upload_search_data(searches_file)
            except Exception as e1:
                pass

            try:
                locations_files = self.get_locations_file()
                self.upload_location_data(locations_files)
            except Exception as e2:
                pass

            self.send_email()

            return {
                'search': e1,
                'location': e2
            }
        else:
            return 'drive_not_ready'


def parse_takeout_data():
    pass
