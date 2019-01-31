# The secret key is used by Flask to encrypt session cookies.
SECRET_KEY = 'secret'

# Synapse
PROJECT_SYNID = 'syn18133868'
LOCATION_SYNID = 'syn18133890'
SEARCH_SYNID = 'syn18133891'
CONSENTS_SYNID = 'syn18133870'

CIPHER_KEY = b'G#RXk/^+v{4FKD*r'

DTFORMAT = '%Y%m%d %H:%M:%S'

DATABASE = {
   'drivername': 'postgres',
   'host': '127.0.0.1',
   'port': '5432',
   'username': 'svcuser',
   'password': 'clownSack12',
   'database': 'archivetasks'
}
# MAX_CONTENT_LENGTH = 8 * 1024 * 1024
# ALLOWED_EXTENSIONS = set(['png', 'jpg', 'jpeg', 'gif'])

# OAuth2 configuration.
# This can be generated from the Google Developers Console at
# https://console.developers.google.com/project/_/apiui/credential.
# Note that you will need to add all URLs that your application uses as
# authorized redirect URIs. For example, typically you would add the following:
#
#  * http://localhost:8080/oauth2callback
#
# If you receive a invalid redirect URI error review you settings to ensure
# that the current URI is allowed.p
GOOGLE_OAUTH2_CLIENT_ID = '1034180928402-7u756f3f2goaprtg5ocqs01hag6rkdac.apps.googleusercontent.com'
GOOGLE_OAUTH2_CLIENT_SECRET = 'UeIJ0qJbr4qi2cLy5SWphhzf'

GOOGLE_DLP_API_URL = 'https://dlp.googleapis.com/v2/content:inspect'
SERVICE_JSON_FILE = 'uw-afs-study-36d403472b2d.json'

# EMAIL SETTINGS
SENDGRID_API_KEY='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'

FROM_STUDY_EMAIL="uw.afs.study@gmail.com"
TO_EMAIL='afs_takeout@u.washington.edu'
EMAIL_SUBJECT = """AFS Study Participant ID:{afs_id} Takeout data download status: {status}"""


EMAIL_BODY = """

AFS Study participant ID: {afs_id} 

Status: {status}


Data Location:
* Search Queries
{search_queries}

* Location Queries
{location_queries}

---------

Error(if any)
{error_message}



For any questions/concerns contact apratap@uw.edu
"""
