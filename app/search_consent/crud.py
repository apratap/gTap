import datetime as dt
from functools import wraps
from flask import Blueprint, current_app, redirect, render_template, request, session, url_for
from pytz import timezone

import app.model as model
from app.search_consent import oauth2

crud = Blueprint('crud', __name__)


def ssl_required(fn):
    @wraps(fn)
    def decorated_view(*args, **kwargs):
        if current_app.config.get('SSL'):
            if request.is_secure:
                return fn(*args, **kwargs)
            else:
                return redirect(request.url.replace('http://', 'https://'))

        return fn(*args, **kwargs)

    return decorated_view


@ssl_required
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
        data['first_name'] = session['profile']['name']['givenName']
        data['last_name'] = session['profile']['name']['familyName']
        data['gender'] = session['profile']['gender']
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
