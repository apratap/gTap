#!/bin/env python

import json
import logging
from google.cloud import error_reporting
from flask import current_app, Flask, redirect, request, session, url_for,render_template
import httplib2
import urllib
import google.auth
from dateutil import parser
import pandas as pd
import logging

import os
import sys
sys.path.append("search_consent/")
from storedata import upload_file, list_blobs
import config

from io import BytesIO
from zipfile import ZipFile
import urllib.request

import datetime
import tempfile

import synapseclient
syn = synapseclient.login()

import sendgrid
from sendgrid.helpers.mail import *


## GLOBAL VARS
SEARCH_DATA_SYNID = 'syn11377377'
LOCATION_DATA_SYNID = 'syn11377380'


def loadConfiguration(config):
    app = Flask(__name__)
    app.config.from_object(config)
    
loadConfiguration(config)


class TakeOutUserData(object):
    
    def __init__(self, blob):
        self.blob = blob
        self.stringCred=self.blob.download_as_string().decode("utf-8")
        self.prefix = self.blob.name.replace('consentPending/', '').replace('.json', '').replace(':', '_')
        self.authed_session = None
        self.response = None
        self.takeOutFileId = None
        self.takeOutZipFile = None
        self.takeOutSearchQueryFile=None
        self.takeOutLocationQueryFile=None
        self.error=None
        self.errorMessage=None
        self.logger=logging.getLogger('Takeout Processing log')
    
    def __str__(self):
        return('prefix: %s' % self.prefix + '\n' + 
        'takeOutFileId: %s' % self.takeOutFileId + '\n' +
        'takeOutSearchQueryFile: %s' % self.takeOutSearchQueryFile +  '\n' +
        'takeOutLocationQueryFile: %s' % self.takeOutLocationQueryFile)
    
    def authorize_user_session(self):
        try:
            credDict=json.loads(self.stringCred)
            credentials = google.oauth2.credentials.Credentials(credDict['access_token'],
                                                    refresh_token=credDict['refresh_token'],
                                                    token_uri=credDict['token_uri'],
                                                    client_id=credDict['client_id'],
                                                    client_secret=credDict['client_secret'])
            from google.auth.transport.requests import AuthorizedSession
            self.authed_session = AuthorizedSession(credentials)
        except Exception as e:
            pass
        
        
    def locateTakeOutFile(self):
        self.response = self.authed_session.get( 'https://www.googleapis.com/drive/v3/files?q=name+contains+%22takeout%22+and+mimeType+contains+%22zip%22')
    
    def get_TakeOutFileID_to_download(self):
        df = pd.DataFrame.from_records(json.loads(self.response.content)['files'])
        df['timeStamp'] = df.name.str.split('-',3).apply(lambda x: x[1])
        df['timeStamp'] = pd.to_datetime(df['timeStamp'])
        self.takeOutFileId = df.id[df.timeStamp.argmax]
        
    def download_takeOut_data(self):
        url = 'https://www.googleapis.com/drive/v3/files/%s?alt=media' % self.takeOutFileId
        takeOutFile = self.authed_session.get(url)
        self.takeOutZipFile = ZipFile(BytesIO(takeOutFile.content))


    def get_searchQueries_file(self):
        #search files
        search_files = [f for f in self.takeOutZipFile.namelist() if 'Searches' in f]
        search_queries = [ pd.read_json(self.takeOutZipFile.open(search_file).readlines()[0])  for search_file in search_files]
        search_queries = pd.concat(search_queries).reset_index(drop=True)
        
        def __get_timeStamp__(row):
            timestamp = row['query']['id'][0]['timestamp_usec']
            return(timestamp)

        def __get_query__(row):
            query_text = row['query']['query_text']
            return(query_text)
        
        timeStamps = search_queries.event.map(__get_timeStamp__)
        searchQueries = search_queries.event.map(__get_query__)

        search_results = pd.concat([timeStamps, searchQueries], axis=1)
        search_results.columns = ['timeStamp', 'searchQuery']
        search_results.timeStamp = pd.to_datetime(search_results.timeStamp, unit='us')
        search_results.sort_values(by='timeStamp', inplace=True)
        search_results.reset_index(inplace=True, drop=True)

        searchFileName = '%s_searchQueries.feath' % self.prefix
        search_results.to_feather(searchFileName)
    
        self.takeOutSearchQueryFile = searchFileName
        return(self.takeOutSearchQueryFile)
    
    
    
    def get_locationQueries_file(self):
        def __cread_dataFrame__(self, f):
            fp = tempfile.TemporaryFile()
            fp.write(self.takeOutZipFile.open(f).read())
            fp.seek(0)
            tmp_df =  pd.read_json(fp.read())
            fp.close()
            return(tmp_df)
        # read all the files into a pandas df (dirty trick)
        #location_queries = [__cread_dataFrame__(self, f) for f in location_files]
        #concat // if there are more than one location files (not likely)
        #location_queries = pd.concat(location_queries).reset_index(drop=True)
        
        #find all the location files
        location_files = [f for f in self.takeOutZipFile.namelist() if 'Location History' in f]

        def __temp__write_jsonFile__(self, count, f):
            locationFileName = '%s_%s_LocationQueries.json' % (self.prefix, count+1)
            with open(locationFileName, 'wb') as out:
                out.write(self.takeOutZipFile.open(f).read())
                out.close()
            return(locationFileName)
        location_files = [__temp__write_jsonFile__(self, count, f) for count, f in enumerate(location_files)]
        
        self.takeOutLocationQueryFile = location_files
        return(location_files)
    
    def delete_blob(self):
        self.blob.delete()




def sendEmail(user):
	sg = sendgrid.SendGridAPIClient(apikey=config.SENDGRID_API_KEY)
	from_email = Email(config.FROM_STUDY_EMAIL)
	to_email = Email(config.TO_EMAIL)
	subject =  config.EMAIL_SUBJECT
	content = Content("text/plain", config.EMAIL_BODY)
	mail = Mail(from_email, subject, to_email, content)
	response = sg.client.mail.send.post(request_body=mail.get())
	print(response.status_code)




def processBlob(blob):
	"""	
	Process each blob
	"""

	user = TakeOutUserData(blob)
	user.authorize_user_session()
	user.locateTakeOutFile()
	user.get_TakeOutFileID_to_download()
	user.download_takeOut_data()
	searchQueriesFile = user.get_searchQueries_file()
	locationQueriesFiles = user.get_locationQueries_file()
    
    ##### UPLOAD DATA TO SYNAPSE
	try:
		# Search queries
		syn.store(synapseclient.File(searchQueriesFile, parentId = SEARCH_DATA_SYNID))
		os.unlink(searchQueriesFile)

		#location queries
		for f in locationQueriesFiles:
			syn.store(synapseclient.File(f, parentId = LOCATION_DATA_SYNID))
			os.unlink(f)
	except Exception as e:
		print(e)
	else:
		sendEmail(user)
		#user.delete_blob()



####
#Main
####

def main():
	#get all the pending consents
	blobs = [b for b in list_blobs("consentPending", config.CLOUD_STORAGE_BUCKET_PRIVATE)]
	
	for blob in blobs:
		processBlob(blob)


if __name__ == '__main__':
	main()

