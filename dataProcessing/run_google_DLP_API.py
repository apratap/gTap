import sys
import os
import json
import requests
import synapseclient
import pandas as pd
import synapseclient
import itertools
from oauth2client.service_account import ServiceAccountCredentials
from httplib2 import Http

import config

syn = synapseclient.login()

SCRIPT_DIR = os.path.abspath(__file__)
SCRIPT_DIR = os.path.dirname(SCRIPT_DIR)

GOOGLE_DLP_API_URL = 'https://dlp.googleapis.com/v2/content:inspect'
SERVICE_JSON_FILE = SCRIPT_DIR + '/' + config.SERVICE_JSON_FILE


#Authenticate for using API usage

credentials = ServiceAccountCredentials.from_json_keyfile_name(SERVICE_JSON_FILE,
                                                              scopes = ['https://www.googleapis.com/auth/cloud-platform'])
HTTP_AUTH = credentials.authorize(Http())


def run_googleDLPAPI(requests):
    body = json.dumps(requests)
    resp, content = HTTP_AUTH.request(GOOGLE_DLP_API_URL, method="POST", body=body)
    return json.loads(content)



def process_DLP_rootCategory(category):
    resp, content = HTTP_AUTH.request(
        'https://dlp.googleapis.com/v2beta1/rootCategories/%s/infoTypes' % category,
         method='GET')
    tmp = json.loads(content)
    tmp = pd.DataFrame.from_records(tmp['infoTypes'])
    tmp['root'] = tmp.categories.map(lambda x:  x[0]['name'])
    #tmp['root'] = tmp.categories.map(lambda x:  x[1]['name'] if(len(x) == 2) else None)
    tmp = tmp.drop(['categories'],axis=1)
    return(tmp)
    

def get_ALL_DLP_API_InfoTypes():
    resp, content = HTTP_AUTH.request('https://dlp.googleapis.com/v2beta1/rootCategories', method='GET')
    rootCategories = json.loads(content)
    rootCategories = [ categ['name'] for categ in rootCategories['categories']]
    DLP_infoTypes = [process_DLP_rootCategory(categ) for categ in rootCategories ]
    infoTypes = pd.concat(DLP_infoTypes).reset_index(drop=True)
    infoTypes = infoTypes.drop_duplicates()
    return(infoTypes)

 #infoTypes = get_ALL_DLP_API_InfoTypes()
 #infoTypes = [{'name':i} for i in infoTypes.name.tolist()]

def buildRequest(searchQueries):
    requestItems  = [ { "values": [ {  "stringValue": searchQuery } ]  } for searchQuery in searchQueries ]    
        
    return {'inspectConfig':{'infoTypes': [], 
                             'minLikelihood': 'LIKELY',  
                            'includeQuote': True
                            }, 
            'items': [ {
                         'table' : {
                                     "headers": [ {'columnName' : 'userSearchQueries' }],
                                     "rows"   :  requestItems 
                                   }
                       } 
                    ]
           }
                
            
def process_googleDLP_Result(result):  
    if result.get('findings', None) is None:
        return(None)
    else:
        df= pd.DataFrame.from_records(result.get('findings', None))
        df.infoType = df.infoType.map(lambda x: x.get('name', None))
        
        def __tmp_to_manage_missing_rowIndex_0_from_google_DLP_API(x):
            try:
                return(x['tableLocation']['rowIndex'])
            except:
                return(0)
        df['listIndex'] = df.location.map(__tmp_to_manage_missing_rowIndex_0_from_google_DLP_API)
        #df['codepointRange'] = df.location.map( lambda x: x.get('codepointRange', None))
        #df['matchStart'] = df.codepointRange.map(lambda x: x.get('start', None))
        #df['matchEnd'] = df.codepointRange.map(lambda x: x.get('end', None))
        df = df.drop(['createTime', 'location'], axis=1)
        df = df.drop_duplicates()
        grpd = df.groupby(['listIndex'])
        df = grpd.aggregate(lambda x: ','.join(x.unique())).reset_index()
        df.listIndex = df.listIndex.astype('int64')
        return(df)


def chunks(l, n):
    """Yield successive n-sized chunks from l"""
    for i in range(0, len(l), n):
        yield l[i:i + n]

       

    
def link_DLPresults_to_searchQueryList(df, searchQueriesList):
    tmp_df = pd.DataFrame({'listIndex': range(0,len(searchQueriesList)),
                      'searchQuery':searchQueriesList})
    tmp_df.listIndex = tmp_df.listIndex.astype('int64')
    df = pd.merge(tmp_df, df, left_on='listIndex', right_on='listIndex')
    df = df.drop(['listIndex'], axis=1)
    return(df) 

def processQueriesList(searchQueriesList):
    searchQueryRequests = buildRequest(searchQueriesList)
    DLP_results = run_googleDLPAPI(searchQueryRequests)
    DLP_results = DLP_results['results'][0]
    DLP_results = process_googleDLP_Result(DLP_results)
    if DLP_results is not None:
        DLP_results = link_DLPresults_to_searchQueryList(DLP_results, searchQueriesList)
    return(DLP_results)



#EXAMPLE
#queries = pd.read_feather(syn.get('syn11415149').path)
#searchQueries = queries.searchQuery.tolist()
#DLP_results = [ processChunk(chunk) for chunk in chunks(searchQueries, 100) ]
#DLP_results = pd.concat(DLP_results, ignore_index=True)
#DLP_results = DLP_results.drop_duplicates()
#queries = queries.merge(DLP_results, how='left', left_on='searchQuery', right_on='searchQuery')
#queries.searchQuery[~queries.type.isnull()] = 'REDACTED'


