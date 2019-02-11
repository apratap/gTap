from datetime import datetime as dt
import os
import sys
from pytz import timezone as tz

from app.aa import ArchiveAgent
from app.aa import log as log_to_synapse
from app.model import create_database
import app.search_consent as search_consent
import app.config as config


def log(s):
    rmeow = dt.now(tz("US/Pacific"))
    log_to_synapse(s)
    sys.stdout.write(f'{rmeow}: {s}\n')


if not os.path.exists(config.DATABASE['path']):
    log('creating task db')
    create_database(config.DATABASE)
else:
    log('found existing task db')


log('starting archive agent')
agent = ArchiveAgent(conn=config.DATABASE)
agent.start_async()

application = search_consent.create_app(config)
