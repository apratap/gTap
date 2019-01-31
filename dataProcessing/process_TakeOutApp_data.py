#!/usr/bin/env python

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
import datetime
import re
from google.auth.transport.requests import AuthorizedSession

import os
import sys
SCRIPT_DIR = os.path.abspath(__file__)
SCRIPT_DIR = os.path.dirname(SCRIPT_DIR)
sys.path.append(SCRIPT_DIR + "/search_consent/")
from storedata import upload_file, list_blobs
import config
import run_google_DLP_API

from io import BytesIO
from zipfile import ZipFile
import urllib.request


import tempfile
from datetime import datetime
from pytz import timezone

import synapseclient
syn = synapseclient.login()

import sendgrid
from sendgrid.helpers.mail import *


## GLOBAL VARS
SEARCH_DATA_SYNID = 'syn11377377'
LOCATION_DATA_SYNID = 'syn11377380'
WEBVISTIS_SYNID = 'syn11977096'
ENROLLMENT_SYNTABLE = 'syn11384434'
ENROLLMENT_SYNTABLE_SCHEMA = syn.get(ENROLLMENT_SYNTABLE)


def loadConfiguration(config):
    app = Flask(__name__)
    app.config.from_object(config)


loadConfiguration(config)




DONT_REDACT_THESE = ['FEMALE_NAME', 'FIRST_NAME', 'LAST_NAME', 'MALE_NAME', 'US_TOLLFREE_PHONE_NUMBER',
                     'US_CENSUS_NAME', 'US_FEMALE_NAME', 'US_MALE_NAME', 'US_STATE', 
                     'PERSON_NAME', 'LOCATION', 'FDA_CODE', 'ICD9_CODE', 'ICD10_CODE']
infoTypes = run_google_DLP_API.get_ALL_DLP_API_InfoTypes()
infoTypes['REDACT'] = True
infoTypes.REDACT.loc[infoTypes.name.isin(DONT_REDACT_THESE)] = False




def __get_logger(logFile, logName, debug):
        logging_level = logging.INFO if debug == False else logging.DEBUG   
        logging.basicConfig(level=logging_level,
                    format='%(asctime)s %(name)-12s %(funcName)s %(message)s',
                    datefmt='%m-%d-%Y %H:%M',
                    filename='%s.log' % logFile,
                    filemode='w')
        logger = logging.getLogger(logName)

        # define a Handler which writes INFO messages or higher to the sys.stderr
        consoleHandler = logging.StreamHandler()
        consoleHandler.setLevel(logging_level)
        # set a format which is simpler for console use
        formatter = logging.Formatter('%(asctime)s %(name)-12s %(funcName)s %(message)s')
        # tell the handler to use this format
        consoleHandler.setFormatter(formatter)
        logger.addHandler(consoleHandler)
        return(logger)



def processSearchBlock(block):
    #website_visits_match = re.match('^.+Visited.+?">(.*)/</a><br>(.*?)</div>.+$', block)
    webVisit       = re.match('^.*Visited.*">(.*)</a><br>(.*?)</div.*$', block)
    textSearch     = re.match('^.+?Searched for.+?">(.+?)</a><br>(.*?)</div>.+$', block)
    location       = re.match('^.*Locations.*">(.*)</a><br></div></div></div>$', block)
    
    if location:
        location = location.groups()
    else:
        location = ('NA',)
    
    if textSearch:
        textSearch = textSearch.groups()
        textSearch = textSearch + location
    else:
        textSearch = None
    
    if webVisit:
        webVisit = webVisit.groups()
        webVisit = webVisit + location
    else:
        webVist = None
    return([textSearch,webVisit])




def process_userSearchQueries_in_htmlFormat(html_file_content):
    '''
    html_file_content - is the content (string) of the HTML file
    '''

    textSearches = []
    webVisits = []
    errorBlock = []
    #this should be a unique search block in HTML file
    blocks = re.findall('<div class="outer-cell.*?</div></div></div>',html_file_content)
    #process each block    
    for b in blocks:
        textSearch,webVisit = processSearchBlock(b)
        if textSearch is None and webVisit is None:
            errorBlock.append(b)
        if textSearch: textSearches.append(textSearch) 
        if webVisit: webVisits.append(webVisit)
    webVisits_df = pd.DataFrame.from_records(webVisits,columns=('weblink', 'localTimestamp', 'location'))
    textSearches_df = pd.DataFrame.from_records(textSearches,columns=('query', 'localTimestamp', 'location'))
    return(webVisits_df, textSearches_df)


class TakeOutUserData(object):
    
    def __init__(self, blob, logger):
        self.blob = blob
        self.stringCred=self.blob.download_as_string().decode("utf-8")
        tmp = self.blob.name.replace('consentPending/', '').replace('.json', '')
        re_match = re.match('^extID(.*?)_afsID(.*?)_time(.*?)$', tmp).groups()
        self.extID = re_match[0]
        self.afsID = re_match[1]
        consentTime = re_match[2]
        consentTime = datetime.strptime(consentTime, '%Y:%m:%d-%H:%M:%S')
        localTz = timezone('America/Los_Angeles')
        self.consentTime = localTz.localize(consentTime)
        tmp_date = consentTime.strftime('%Y-%m-%d')
        tmp_time = consentTime.strftime('%H-%M-%S')
        self.prefix = 'extID%s_afsID%s_date%s_time%s' % (self.extID, self.afsID, tmp_date, tmp_time)
        self.authed_session = None
        self.response = None
        self.fileId = None
        self.takeOutZipFile = None
        self.searchQueries = None
        self.webVisits = None
        self.locationsFile = None
        self.status = None
        self.error = None
        self.logger = logger
        self.searchQueries_file_synid = None
        self.webVisits_file_synid = None
        self.location_file_synid = None
    
    def __str__(self):
        return('prefix: %s' % self.prefix + '\n' + 
        'AFS ID: %s' % self.afsID + '\n' +
        'ext ID: %s' % self.extID + '\n' +
        'Conset time: %s' % self.consentTime + '\n' +
        'FileId: %s' % self.fileId + '\n' +
        'error: %s' % self.error + '\n')
    
    def authorize_user_session(self):
        credDict=json.loads(self.stringCred)
        credentials = google.oauth2.credentials.Credentials(credDict['access_token'],
                                                    refresh_token=credDict['refresh_token'],
                                                    token_uri=credDict['token_uri'],
                                                    client_id=credDict['client_id'],
                                                    client_secret=credDict['client_secret'])
        try:
            self.authed_session = AuthorizedSession(credentials)
        except Exception as e:
            error  = 'unable to authorize user session'
            self.error = error
            self.logger.error(error)
        
        
    def locateTakeOutFile(self):
        try:
            self.response = self.authed_session.get( 'https://www.googleapis.com/drive/v3/files?q=name+contains+%22takeout%22+and+mimeType+contains+%22zip%22')
        except Exception as e:
            errorMessage = str(e)
            self.error = errorMessage
            self.logger.error(errorMessage)
    
    def get_TakeOutFileID_to_download(self):
        try:
            df = pd.DataFrame.from_records(json.loads(self.response.content)['files'])
            if df.shape[0] == 0:
                raise Exception
            df['timeStamp'] = df.name.str.split('-',3).apply(lambda x: x[1])
            df['timeStamp'] = pd.to_datetime(df['timeStamp'])
            self.fileId = df.id[df.timeStamp.idxmax]
        except Exception as e:
            errorMessage = str(e)
            self.error = 'Could not locate the takeout file to download'
            self.logger.error(errorMessage)
        
    def download_takeOut_data(self):
        try:
            url = 'https://www.googleapis.com/drive/v3/files/%s?alt=media' % self.fileId
            takeOutFile = self.authed_session.get(url)
            self.takeOutZipFile = ZipFile(BytesIO(takeOutFile.content))
        except Exception as e:
            errorMessage = str(e)
            self.error = errorMessage
            self.logger.error(errorMessage)



    '''
    def __process_searchQueries_inJSON_format(search_files):
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
    
    '''


    def get_searchQueries(self):
        
        #Loctate search files
        #previous JSON format
        #search_files = [f for f in self.takeOutZipFile.namelist() if 'Searches' in f]
        # In case in future JSON comes back processing code moved to -- processing moved to __process_searchQueries_inJSON_format
       

        #Current HTML based output
        search_files = [f for f in self.takeOutZipFile.namelist() if 'Search' in f]
        if(len(search_files) == 0):
            errorMessage = 'Unable to find any file in downloaded search queries takeout file'
            self.error = errorMessage
            self.logger.error(errorMessage)
            return(None)
        elif(len(search_files) > 1 ):
            errorMessage = 'Found more than 1 HTML file for a single user -- unexpected'
            self.error = errorMessage
            self.logger.error(errorMessage)
            return(None)
        else:            
            #hack - temp file to read in HTML content from zipfile and pass to process_userSearchQueries_in_htmlFormat function
            #f = tempfile.NamedTemporaryFile(delete=False)
            #f.write(self.takeOutZipFile.read(search_file))
            #f.close
            #process HTML File
            search_file = search_files[0]
            html_file_content = self.takeOutZipFile.read(search_file).decode('utf-8')
            webVisits, searchQueries = process_userSearchQueries_in_htmlFormat(html_file_content)
            self.searchQueries = searchQueries
            self.webVisits = webVisits


    def get_locationQueries_file(self):

        #find all the location files
        location_files = [f for f in self.takeOutZipFile.namelist() if 'Location History' in f]

        if(len(location_files) == 0):
            infoMessage = 'Did not find optional location history will skip'
            self.logger.info(infoMessage)
            return(None)
        elif(len(location_files) > 1):
            errorMessage = 'Found more than 1 Location file for a single user -- unexpected'
            self.error = errorMessage
            self.logger.error(errorMessage)
            return(None)
        else:
            locationFileName =  '%s_LocationQueries.json' % self.prefix
            with open(locationFileName, 'wb') as out:
                out.write(self.takeOutZipFile.open(location_files[0]).read())
                out.close()    
            self.location_file = locationFileName

        '''
        def __cread_dataFrame__(self,f):
            fp = tempfile.TemporaryFile()
            fp.write(self.takeOutZipFile.open(f).read())
            fp.seek(0)
            tmp_df =  pd.read_json(fp.read())
            fp.close()
            return(tmp_df

        # read all the files into a pandas df (dirty trick)
        #location_queries = [__cread_dataFrame__(self, f) for f in location_files]
        #concat // if there are more than one location files (not likely)
        #location_queries = pd.concat(location_queries).reset_index(drop=True)
        
        '''
    
    def delete_blob(self):
        self.blob.delete()




def sendEmail(user):
    sg = sendgrid.SendGridAPIClient(apikey=config.SENDGRID_API_KEY)
    from_email = Email(config.FROM_STUDY_EMAIL)
    to_email = Email(config.TO_EMAIL)
    subject =  config.EMAIL_SUBJECT.format(afs_id=user.afsID, status=user.status)

    link_prefix = 'https://www.synapse.org/#!Synapse:'
    search_queries_link = link_prefix + user.searchQueries_file_synid
    location_queries_link = link_prefix  +  user.location_file_synid if user.location_file_synid is not None else 'None'
    webVisits_link = link_prefix  +  user.webVisits_file_synid if user.webVisits_file_synid is not None else 'None'

    EMAIL_BODY = config.EMAIL_BODY.format(afs_id=user.afsID,status=user.status,
        search_queries=search_queries_link,
        location_queries=location_queries_link,
        error_message = user.error)

    content = Content("text/plain", EMAIL_BODY)
    mail = Mail(from_email, subject, to_email, content)
    response = sg.client.mail.send.post(request_body=mail.get())
    print(response.status_code)


def updateEnrollmentTable(user):
    searchFile = user.searchQueries_file_synid if user.searchQueries_file_synid is not None else 'NA'
    locationFile = user.location_file_synid if user.location_file_synid is not None else 'NA'
    new_enrollment_row = [user.afsID, user.extID, str(user.consentTime.isoformat()),searchFile,
                          locationFile, user.status]
    
    syn.store(synapseclient.Table(ENROLLMENT_SYNTABLE_SCHEMA, [new_enrollment_row]))


        

def processBlob(blob, logger):
    """ 
    Process each blob
    """

    user = TakeOutUserData(blob, logger)
    logger.info('Authorizing user session for download: AFS ID- %s extID-%s' % (user.afsID, user.extID))
    user.authorize_user_session()

    logger.info('Locating takeout files')
    user.locateTakeOutFile()
    if user.error is not None:
        user.logger.error('ERROR MESSAGE - %s' % user.error)
        return(user)

    user.get_TakeOutFileID_to_download()
    if user.error is not None:
        user.logger.error('ERROR MESSAGE - %s' % user.error)
        return(user)

    logger.info('Downloading takeout files')
    user.download_takeOut_data()
    user.get_searchQueries()
    user.get_locationQueries_file()
    if user.error is not None:
        user.logger.error('ERROR MESSAGE - %s' % user.error)
        return(user)

    

    # Run Google DLP API
    logger.info('running Google DLP pipeline on search queries')
    try:
        searchQueries = user.searchQueries
        searchQueriesList = user.searchQueries['query'].unique()
        DLP_results = [ run_google_DLP_API.processQueriesList(chunk) for chunk in run_google_DLP_API.chunks(searchQueriesList, 1000) ]
        DLP_results = pd.concat(DLP_results, axis=0, ignore_index=True)
        DLP_results = DLP_results.drop_duplicates()
        searchQueries = searchQueries.merge(DLP_results, how='left', left_on='query', right_on='searchQuery')
        searchQueries = searchQueries.merge(infoTypes, left_on='infoType', right_on='name', how='left')
        searchQueries['searchQuery'][searchQueries['REDACT'] == True] = 'REDACTED'
        searchQueries['quote'][searchQueries['REDACT'] == True] = 'REDACTED'
        searchQueries.drop(['query', 'name'], axis=1, inplace=True)
        user.searchQueries = searchQueries
    except Exception as e:
        user.error = 'Unable to fully run Google DLP pipeline \n error %s'  % str(e)
        user.logger.error('ERROR MESSAGE - %s' % user.error)
        return(user)

    

    ##### UPLOAD DATA TO SYNAPSE
    
    try:
        # Search queries
        logger.info("uploading Search queries file to synapse")
        searchQueriesFile = '%s_searchQueries.feath' % user.prefix
        user.searchQueries.to_feather(searchQueriesFile)
        logger.info('Uploading search queries file - %s' % searchQueriesFile)
        tmp = syn.store(synapseclient.File(searchQueriesFile, parentId = SEARCH_DATA_SYNID))
        os.unlink(searchQueriesFile)
        user.searchQueries_file_synid = tmp.id


        # Web visits
        logger.info("uploading Web visits file to synapse")
        webVisitsFile = '%s_webVisits.feath' % user.prefix
        user.webVisits.to_feather(webVisitsFile)
        logger.info('Uploading Web Visits queries file - %s' % webVisitsFile)
        tmp = syn.store(synapseclient.File(webVisitsFile, parentId = WEBVISTIS_SYNID))
        os.unlink(webVisitsFile)
        user.webVisits_file_synid = tmp.id


        #location queries
        if user.locationsFile is None:
            logger.info('No location queries found')
        else:
            logger.info('Uploading location queries file ...')
            tmp = syn.store(synapseclient.File(user.locationsFile, parentId = LOCATION_DATA_SYNID))
            os.unlink(user.locationsFile)
            user.location_file_synid = tmp.id

    except Exception as e:
        print(e)
        user.status='FAILED'
        user.error = e
    else:
        user.status='SUCCESS'
        updateEnrollmentTable(user)
        sendEmail(user)
        logger.info("Deleting user credentials")
        user.delete_blob()
    return(user)


#Main
def main():
    logFilePrefix = datetime.now().strftime("%Y-%m-%d_%H:%M")
    logger = __get_logger(logFilePrefix, 'AFS-Takeout-Log', debug=True)

    #get all the pending consents
    blobs = [b for b in list_blobs("consentPending", config.CLOUD_STORAGE_BUCKET_PRIVATE)]
    logger.info('START: Got %s blobs from google bucket' % len(blobs))

    tmp_counter = 0
    for count, blob in enumerate(blobs):
        count += 1
        logger.info('Processing blob - %s' % count)
        user = processBlob(blob, logger)
        if user.error is not None:
            logger.error('Unable to process takeout data for AFS ID-%s / extID-%s ... Will skip' % (user.afsID, user.extID))
            logger.error('ERROR MESSAGE - %s' % user.error)
            tmp_counter += 1
            user.status = str(user.error)
            updateEnrollmentTable(user)
    logger.info('END: Of %s blobs %s had errors' % (len(blobs), tmp_counter))

if __name__ == '__main__':
    main()

