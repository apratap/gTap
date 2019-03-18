import os
from pathlib import Path

import synapseclient

"""use only one Synapse connection in the application"""
try:
    syn
except NameError:
    syn = synapseclient.Synapse()
    syn.login(
        email='',
        apiKey=''
    )

"""use a consistent datetime formatting across application"""
DTFORMAT = '%d%b%Y %Z %H:%M:%S'

# ----------------------------------------------------------------------------------------------------------------------
# Synapse
CONSENTS_TABLE_NAME = ''
PROJECT_SYNID  = ''
LOCATION_SYNID = ''
SEARCH_SYNID   = ''
CONSENTS_SYNID = ''
SYNAPSE_RETRIES = 0
SYNAPSE_NAMING_CONVENTION = 'test_%s-text_%s.csv'

# ----------------------------------------------------------------------------------------------------------------------
# SSL
"""the S3 bucket where we backup the certificates issued by LetsEncrypt"""
EBS_BUCKET = ''

"""download everything in the bucket in this folder"""
EBS_CERT_PREFIX = ''

"""put certficicates directory (letsencrypt) into this directory on the beanstalk-ec2 instance"""
LEDIR = ''

"""force a certificate refresh every n days"""
LE_REFRESH_RATE = 0

"""AWS S3 key to the certbot software package"""
CERTBOT_KEY = ''

"""use this email and FQDN for ticket issuance"""
CERTBOT_EMAIL = ''
FQDN = ''

# ----------------------------------------------------------------------------------------------------------------------
# Encryption
"""key used for both encrypting session cookies and oauth keys"""
SECRET_KEY = b''

"""number of iterations for oauth encryption. https://github.com/furritos/flask-simple-crypt"""
FSC_EXPANSION_COUNT = 0

"""where to store the session cookies: https://pythonhosted.org/Flask-Session/"""
SESSION_TYPE = ''

# ----------------------------------------------------------------------------------------------------------------------
# Archive Agent
"""name the archiving process to make it identifiable in the beanstalk-ec2 instance"""
ARCHIVE_AGENT_PROC_NAME = ''

"""where one beanstalk-ec2 instance to store tmp files for data processing"""
ARCHIVE_AGENT_TMP_DIR = ''

"""how long to wait between Google Drive queries if the last attempt was not ready. (seconds)"""
WAIT_TIME_BETWEEN_DRIVE_NOT_READY = 0

"""maximum time for DRIVE attempts. (seconds)"""
MAX_TIME_FOR_DRIVE_WAIT = 0

"""working directory on the beanstalk-ec2 instance"""
WORKING_DIR = ''

"""Takeout URL query"""
TAKEOUT_URL = ''

"""number of threads to consume on the beanstaalk-ec2 instance for cleaning"""
CLEANING_THREADS = 0

"""the Postgres database connection for logging and task management"""
DATABASE = {
    'drivername': 'postgres',
    'host': '',
    'port': '',
    'username': '',
    'password': '',
    'database': ''
}

# ----------------------------------------------------------------------------------------------------------------------
# Data Loss Prevention
"""set the environment variable for google-cloud-dlp access"""
os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = os.path.join(Path.home(), 'dlp-credentials.json')

"""Google Project ID"""
DLP_PROJECT_ID = ''

"""info types to use for content inspection. https://cloud.google.com/dlp/docs/infotypes-reference"""
DLP_INSPECT_CONFIG = {
    'info_types': [
        {'name': t} for t in [
            'INFO_TYPE_1',
            'INFO_TYPE_2'
        ]
    ],
    'min_likelihood': '',
    'include_quote': True
}

# ----------------------------------------------------------------------------------------------------------------------
# Google OAUTH
GOOGLE_OAUTH2_CLIENT_ID = ''
GOOGLE_OAUTH2_CLIENT_SECRET = ''

# ----------------------------------------------------------------------------------------------------------------------
# Email settings
"""AWS region to send from"""
REGION_NAME = ''

"""AWS SES domain ARN"""
EMAIL_SRC_ARN = ''

"""character encoding"""
CHARSET = 'UTF-8'

"""email address that will appear as 'sent from'"""
FROM_STUDY_EMAIL = ''

"""subject to use for emailing participants"""
PARTICIPANT_EMAIL_SUBJECT = ''

"""Jinja template for participant notification email body"""
PARTICIPANT_EMAIL_BODY = """


"""

"""emails to receive daily digests"""
ADMIN_EMAILS = []

"""subject line for daily digest email to admins"""
DIGEST_SUBJECT = ''

"""Jinja template for daily digest email body"""
DIGEST_TEMPLATE = """


"""
