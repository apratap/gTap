#!/bin/env python

import argparse
import datetime as dt
import time

import app.config as secrets
import app.model as model
from app.scripts import TakeOutExtractor


def log(message):
    with open('archive_manager_log.csv', 'w') as f:
        f.write(message)


def main():
    parser = argparse.ArgumentParser(description='--')
    parser.add_argument(
        '--wait',
        type=int,
        help='interval in seconds between poll to task db',
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
        wait_time = 60

    conn = parser.parse_args().conn
    if conn is None:
        conn = secrets.DATABASE

    with model.session_scope(conn) as s:
        while True:
            pending = model.get_all_pending(session=s)

            while len(pending) > 0:
                consent = pending.pop()
                task = TakeOutExtractor(consent)

                result = task.run()

                # ts, task, data
                log('%s,%s,%s,%s' % (
                    dt.datetime.now().strftime(secrets.DTFORMAT),
                    str(task),
                    str(result['has_search_data']),
                    str(result['has_location_data'])
                ))

                for k, v in result.items():
                    if v is None:
                        model.update_synapse_flag(consent, k)
                    elif not isinstance(v, str):
                        model.add_error(v, consent)
                    else:
                        pass

                model.mark_completed(consent)

            time.sleep(wait_time)


if __name__ == '__main__':
    main()
