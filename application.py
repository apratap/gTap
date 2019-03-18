import datetime as dt
import subprocess
import os

import boto3

from app.archive_agent import ArchiveAgent, get_wait_time_from_env
from app.context import create_database, add_log_entry
import app.search_consent as search_consent
import app.config as config

s3_resource = boto3.resource('s3')
s3_client = boto3.client('s3')


def start_archive_agent():
    add_log_entry('starting archive agent')
    agent = ArchiveAgent(conn=config.DATABASE, wait_time=get_wait_time_from_env())
    agent.start_async()
    add_log_entry(agent.get_status())


def is_current():
    """determine whether existing SSL certificates are current

    Notes:
        'current' is defined by the application config var LE_REFRESH_RATE

    Returns:
        success flag as bool
    """
    path = os.path.join(config.LEDIR, config.EBS_CERT_PREFIX)

    if not os.path.exists(path):
        return False
    else:
        files = os.listdir(path)

        mx = lambda x: dt.datetime.fromtimestamp(os.path.getmtime(x))
        now = dt.datetime.now()

        if any([(now-mx(os.path.join(path, f))).days >= config.LE_REFRESH_RATE for f in files]):
            return False

        return True


def get_certs_from_s3():
    """download certificates that have been backed up to S3"""
    cert_files = s3_client.list_objects_v2(
        Bucket=config.EBS_BUCKET, Delimiter=',', Prefix=config.EBS_CERT_PREFIX
    )

    bucket = s3_resource.Bucket(config.EBS_BUCKET)

    for f in cert_files['Contents'][1:]:
        local_path = os.path.join(config.LEDIR, f['Key'])

        local_dir = os.path.join('/', *local_path.split('/')[:-1])
        if not os.path.exists(local_dir):
            os.makedirs(local_dir, exist_ok=True)

        bucket.download_file(f['Key'], local_path)

    add_log_entry('ssl certificates downloaded from S3')


def backup_certs():
    """backup the issued SSL certificates to S3"""
    root = os.path.join(config.LEDIR, config.EBS_CERT_PREFIX)

    for path, dirs, files in os.walk(root):
        for f in files:
            fn = os.path.join(path, f)
            key = '/'.join(fn.split('/')[2:])
            s3_client.upload_file(fn, config.EBS_BUCKET, key)

    add_log_entry('ssl certificates backed up to S3')


def configure_ssl_certs():
    """get a new set of SSL certificates from LetsEncrypt"""
    os.chdir(config.WORKING_DIR)

    if not is_current():
        get_certs_from_s3()

        code = subprocess.call(['service', 'httpd', 'stop'])
        add_log_entry(f'stopping http service finished with return code {code}')

        dirs = ['config', 'log']
        for d in dirs:
            if not os.path.exists(os.path.join('.', d)):
                os.makedirs(d, exist_ok=True)

        # get the bot from s3, make it executable
        bucket = s3_resource.Bucket(config.EBS_BUCKET)
        bucket.download_file(config.CERTBOT_KEY, './certbot-auto')

        code = subprocess.call(['chmod', 'a+x', 'certbot-auto'])
        add_log_entry(f'changing certbot mode finished with return code {code}')

        cli_args = [
            '--non-interactive',
            f'--email={config.CERTBOT_EMAIL}',
            '--debug',
            '--agree-tos',
            '--standalone',
            f'--domains {config.FQDN}',
            '--keep-until-expiring',
            '--logs-dir ./logs',
            '--config-dir ./config',
        ]
        code = subprocess.call(['sudo', './certbot-auto', 'certonly'] + cli_args)
        add_log_entry(f'running certbot finished with return code {code}')

        backup_certs()

        code = subprocess.call(['service httpd start'])
        add_log_entry(f'attempt to start httpd service finished with code {code}')


create_database(config.DATABASE)


if __name__ == '__main__':
    application = search_consent.create_app(config, ssl=False, debug=True)
    application.run(host='localhost', port=8080)
else:
    configure_ssl_certs()
    start_archive_agent()
    application = search_consent.create_app(config)
