import json
from flask import current_app, Flask, redirect, session, render_template
from flask_sslify import SSLify
import httplib2
from oauth2client.contrib.flask_util import UserOAuth2
import os

oauth2 = UserOAuth2()


def create_app(config, debug=False, testing=False, config_overrides=None):
    app = Flask(__name__, static_folder=os.path.abspath('./static'))

    app.config.from_object(config)

    if not debug:
        sslify = SSLify(app)

    app.debug = debug
    app.testing = testing

    if config_overrides:
        app.config.update(config_overrides)

    # Configure logging
    if not app.testing:
        pass

    # Initialize the OAuth2 helper.
    # ref - https://developers.google.com/identity/protocols/OAuth2WebServer?hl=en#incrementalAuth
    # ref - https://github.com/google/google-api-php-client/issues/1064
    additional_kwargs = {
        'include_granted_scopes': 'true',
        'access_type': 'offline',
        'ApprovalPrompt': 'force'
    }

    oauth2.init_app(
        app,
        scopes=[
            'https://www.googleapis.com/auth/drive.readonly',
            'https://www.googleapis.com/auth/userinfo.email',
            'https://www.googleapis.com/auth/userinfo.profile'
        ],
        authorize_callback=request_user_info, **additional_kwargs
    )

    # Add a logout handler.
    @app.route('/logout')
    def logout():
        # Delete the user's profile and the credentials stored by oauth2.
        del session['profile']
        session.modified = True
        oauth2.storage.delete()

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
    def server_error():
        return render_template('error.html')

    return app


def request_user_info(credentials):
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

