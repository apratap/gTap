import datetime as dt

from flask import Blueprint, redirect, render_template, request, session, url_for
from pytz import timezone

import app.config as secrets
import app.model as model
from app.search_consent import oauth2

crud = Blueprint('crud', __name__)


@crud.route('/download', methods=['GET', 'POST'])
@oauth2.required
def download():
    if request.method == 'GET':
        participant_id = request.args.get('pid')
        return render_template('download.html', consent={'participant_id': participant_id})

    if request.method == 'POST':
        data = request.form.to_dict(flat=True)

        data['guid'] = session['profile']['id']
        data['guid_counter'] = model.get_next_guid_counter(data['guid'])

        data['email'] = session['profile']['emails'][0]['value']

        now = dt.datetime.now(timezone('US/Pacific'))
        data['consent_dt'] = now.strftime(secrets.DTFORMAT)

        data['credentials'] = session['google_oauth2_credentials']

        model.put_consent_to_synapse(data)
        model.add_task(data)

        return redirect(url_for('.thanks'))


@crud.route('/thanks', methods=['GET'])
@oauth2.required
def thanks():
    if request.method == 'GET':
        return render_template('thanks.html')
