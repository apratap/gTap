import datetime as dt
from functools import wraps
from flask import Blueprint, current_app, redirect, render_template, request, session, url_for
from pytz import timezone

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

        data['eid'] = model.get_next_eid()

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

        now = dt.datetime.now(timezone('US/Pacific'))
        data['consent_dt'] = now

        data['credentials'] = session['google_oauth2_credentials']

        model.put_consent_to_synapse(data)
        model.add_task(data)

        return redirect('https://takeout.google.com')


@crud.route('/thanks', methods=['GET'])
@oauth2.required
def thanks():
    if request.method == 'GET':
        return render_template('thanks.html')
