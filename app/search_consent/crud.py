import datetime as dt
from flask import Blueprint, render_template, request, session
from multiprocessing import Process
from pytz import timezone

import app.context as ctx
from app.search_consent import oauth2
import app.config as secrets

crud = Blueprint('crud', __name__)


@crud.route('/download', methods=['GET', 'POST'])
@oauth2.required
def download():
    if request.method == 'GET':
        study_id = request.args.get('study_id')
        return render_template('download.html', consent={'study_id': study_id})

    if request.method == 'POST':
        data = request.form.to_dict(flat=True)

        try:
            data['email'] = session['profile']['emails'][0]['value']
        except KeyError:
            data['email'] = 'na'

        try:
            data['first_name'] = session['profile']['name']['givenName']
        except KeyError:
            data['first_name'] = 'na'

        try:
            data['last_name'] = session['profile']['name']['familyName']
        except KeyError:
            data['last_name'] = 'na'

        now = dt.datetime.now(timezone(secrets.TIMEZONE))
        data['consent_dt'] = now

        data['credentials'] = session['google_oauth2_credentials']

        # this is bad, i'm leaving a zombie process for the os to cleanup.
        # it does, but ideally we would have a application level task manager
        # do this... i.e. celery
        Process(target=ctx.add_task, args=(data,)).start()

        return 'task submitted'
