import datetime as dt

from flask import Blueprint, redirect, render_template, request, \
    session, url_for
from pytz import timezone

import app.model as model
from app.search_consent import oauth2

crud = Blueprint('crud', __name__)


@crud.route('/add', methods=['GET', 'POST'])
@oauth2.required
def add():
    if request.method == 'GET':
        consent = {
            'first':  session['profile']['name']['givenName'],
            'last':   session['profile']['name']['familyName'],
            'email':  session['profile']['emails'][0]['value'],
            'gender': session['profile']['gender'],
            'guid':   session['profile']['id']
        }

        return render_template("form.html", action="Add", consent=consent)

    elif request.method == 'POST':
        pacific_timezone = timezone('US/Pacific')
        pacific_time_now = dt.datetime.now(pacific_timezone)

        data = request.form.to_dict(flat=True)
        data['consent_dt'] = pacific_time_now
        data['guid'] = session['profile']['id']

        model.put_consent_to_synapse(data)

        return render_template('download.html')

    else:
        return render_template("error.html")


@crud.route('/download', methods=['GET', 'POST'])
@oauth2.required
def download():
    if request.method == 'POST':
        data = request.form.to_dict(flat=True)

        data['guid'] = session['profile']['id']
        data['consent_dt'] = dt.datetime.now()
        data['credentials'] = session['google_oauth2_credentials']

        model.add_consent(data)

        return redirect(url_for('.thanks'))
