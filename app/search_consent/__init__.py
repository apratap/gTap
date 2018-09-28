import json
import logging
from flask import current_app, Flask, redirect, request, session, url_for,render_template
from google.cloud import error_reporting
import google.cloud.logging
import httplib2
import urllib
from oauth2client.contrib.flask_util import UserOAuth2
#from apiclient.discovery import build
from .storedata import upload_file

oauth2 = UserOAuth2()


def create_app(config, debug=False, testing=False, config_overrides=None):
    app = Flask(__name__)
    app.config.from_object(config)

    app.debug = debug
    app.testing = testing

    if config_overrides:
        app.config.update(config_overrides)

    # Configure logging
    if not app.testing:
        client = google.cloud.logging.Client(app.config['PROJECT_ID'])
        # Attaches a Google Stackdriver logging handler to the root logger
        client.setup_logging(logging.INFO)
    # Setup the data model.
    with app.app_context():
        model = get_model()
        model.init_app(app)

    # Initalize the OAuth2 helper.
    additional_kwargs={
    'include_granted_scopes':'true', 
    'access_type':'offline', #ref - https://developers.google.com/identity/protocols/OAuth2WebServer?hl=en#incrementalAuth
    'ApprovalPrompt':'force'}  # ref- https://github.com/google/google-api-php-client/issues/1064

    oauth2.init_app(
        app,
        scopes=['https://www.googleapis.com/auth/drive.readonly',
                'https://www.googleapis.com/auth/userinfo.email',
                'https://www.googleapis.com/auth/userinfo.profile'],
        authorize_callback=_request_user_info,**additional_kwargs)

    # Add a logout handler.
    @app.route('/logout')
    def logout():
        # Delete the user's profile and the credentials stored by oauth2.
        del session['profile']
        session.modified = True
        oauth2.storage.delete()
        #return redirect(request.referrer or '/')
        return redirect('https://mail.google.com/mail/u/0/?logout&hl=en')

    # Register the Consent CRUD blueprint.
    from .crud import crud
    app.register_blueprint(crud, url_prefix='/consent')

    # Add a default root route.
    @app.route("/")
    @app.route("/index")
    def index():
        return render_template('index.html')

    # Add an error handler that reports exceptions to Stackdriver Error
    # Reporting. Note that this error handler is only used when debug
    # is False
    @app.errorhandler(500)
    def server_error(e):
        client = error_reporting.Client(app.config['PROJECT_ID'])
        client.report_exception(
            http_context=error_reporting.build_flask_context(request))
        return """
        An internal error occurred.
        """, 500

    return app


def get_model():
    model_backend = current_app.config['DATA_BACKEND']
    if model_backend == 'cloudsql':
        from . import model_cloudsql
        model = model_cloudsql
    elif model_backend == 'datastore':
        from . import model_datastore
        model = model_datastore
    elif model_backend == 'mongodb':
        from . import model_mongodb
        model = model_mongodb
    else:
        raise ValueError(
            "No appropriate databackend configured. "
            "Please specify datastore, cloudsql, or mongodb")

    return model


def _request_user_info(credentials):
    """
    Makes an HTTP request to the Google+ API to retrieve the user's basic
    profile information, including full name and photo, and stores it in the
    Flask session.
    """
    http = httplib2.Http()
    credentials.authorize(http)
    resp, content = http.request('https://www.googleapis.com/plus/v1/people/me')

    if resp.status != 200:
        current_app.logger.error(
            "Error while obtaining user profile: %s" % resp)
        return None
    current_app.logger.debug(json.loads(content.decode('utf-8')))
    current_app.logger.debug(resp)
    
    session['profile'] = json.loads(content.decode('utf-8'))

