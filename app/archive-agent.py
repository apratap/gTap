#!/bin/env python

import argparse
import datetime as dt
import time

import app.config as secrets
import app.model as model
from app.scripts import TakeOutExtractor


def main():
    parser = argparse.ArgumentParser(description='--')
    parser.add_argument('wait', type=int, help='interval in seconds between poll to task db', required=False)

    if hasattr(parser.parse_args(), 'wait'):
        wait_time = parser.parse_args().wait
    else:
        wait_time = 60

    while True:
        pending = model.get_all_pending(secrets.DATABASE)

        for consent in pending:
            task = TakeOutExtractor(consent)

            try:
                success = task.run()
                print('consent: %s %s at %s' % (
                    str(task),
                    'completed' if success else 'failed',
                    dt.datetime.now().strftime(secrets.DTFORMAT)
                ))

                if success:
                    model.remove_consent(consent)
                else:
                    pass

            except Exception as e:
                model.add_error(model.TakeoutError(
                    cid=consent.cid,
                    error=str(type(e)),
                    message=str(e.args)
                ))

        time.sleep(wait_time)


if __name__ == '__main__':
    main()
