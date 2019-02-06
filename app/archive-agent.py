#!/bin/env python

import argparse
import datetime as dt
import time

import synapseclient
from synapseclient import Column, Schema, Table

import app.config as secrets
import app.model as model
from app.scripts import TakeOutExtractor

syn = synapseclient.Synapse()
syn.login()

LOG_SCHEMA = Schema(
    name='Archive Log',
    columns=[
        Column(name='archival_dt',     columnType='STRING', maximumSize=61),
        Column(name='task',     columnType='STRING', maximumSize=255),
        Column(name='search',   columnType='STRING', maximumSize=1000),
        Column(name='location', columnType='STRING', maximumSize=1000),
    ],
    parent=secrets.PROJECT_SYNID
)
BLANK_LOG = (
    'blank', 'blank', 'blank', 'blank'
)


def log(message):
    syn.store(Table(LOG_SCHEMA, [message]))


def build_synapse_log():
    table = Table(LOG_SCHEMA, values=[BLANK_LOG])
    table = syn.store(table)

    results = syn.tableQuery("select * from %s where archival_dt='%s'" % (table.tableId, BLANK_LOG[0]))
    syn.delete(results)

    syn.setProvenance(
        entity=table.tableId,
        activity=synapseclient.Activity(
            name='Created',
            description='Table generated by gTap.'
        )
    )


def main():
    parser = argparse.ArgumentParser(description='--')
    parser.add_argument(
        '--wait',
        type=int,
        help='interval in minutes between poll to task db',
        required=False
    )
    parser.add_argument(
        '--conn',
        type=str,
        help='optional database connection',
        required=False
    )

    wait_time = parser.parse_args().wait
    if wait_time is None:
        wait_time = 600
    else:
        wait_time *= 60

    conn = parser.parse_args().conn
    if conn is None:
        conn = secrets.DATABASE

    while True:
        with model.session_scope(conn) as s:
            pending = model.get_all_pending(session=s)

            while len(pending) > 0:
                consent = pending.pop()
                task = TakeOutExtractor(consent)

                result = task.run()

                if isinstance(result, str):
                    log((
                        dt.datetime.now().strftime(secrets.DTFORMAT),
                        str(task),
                        result,
                        result
                    ))
                else:
                    search, location = False, False

                    if not isinstance(result['search'], str):
                        consent = model.add_search_error(s, consent, result['search'])
                    else:
                        consent.search_eid = None
                        consent.search_err = []
                        consent.search_sid = result['search']
                        search = True

                    if not isinstance(result['location'], str):
                        consent = model.add_location_error(s, consent, result['location'])
                    else:
                        consent.location_eid = None
                        consent.location_err = []
                        consent.location_sid = result['location']
                        location = True

                    model.update_synapse_sids(consent)

                    if search and location:
                        consent.data = None
                    else:
                        pass

                    log([
                        dt.datetime.now().strftime(secrets.DTFORMAT),
                        str(consent),
                        str(consent.search_err) if consent.search_err is not None else result['search'],
                        str(consent.location_err) if consent.location_err is not None else result['location']
                    ])

        time.sleep(wait_time)


if __name__ == '__main__':
    build_synapse_log()
    main()
