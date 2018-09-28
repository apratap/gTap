from pytz import timezone    

from search_consent import get_model, oauth2
from flask import Blueprint, current_app, redirect, render_template, request, \
    session, url_for
import json
from .storedata import upload_file, list_blobs
import datetime
crud = Blueprint('crud', __name__)



@crud.route('/<id>')
@oauth2.required
def view(id):
    consent = get_model().read(id)
    return render_template("view.html", consent=consent)


@crud.route('/add', methods=['GET', 'POST'])
@oauth2.required
def add():
    def uniqid():
        from time import time
        # return 5 digit random code
        return hex(int(time()*10000)).upper()[-5:]
        #return hex(int(time()*10000000))[2:]
    
    googleCredentials=json.loads(session['google_oauth2_credentials'])
    print(googleCredentials)
    print('\n\n\n 20171122 -------\n\n')
    if request.method == 'GET':
      #current_app.logger.debug(session)
      #print(session)
      #consent = {'firstName':session['profile']['name']['givenName'], 'lastName':session['profile']['name']['familyName'], 'googleUserId': session['profile']['id'],'generatedConsentId':uniqid(),'externalSurveyId':""}
      consent = {'firstName':session['profile']['name']['givenName'], 
                 'lastName':session['profile']['name']['familyName'], 
                 'googleUserId': session['profile']['id'],
                 'generatedConsentId':uniqid(),'externalSurveyId':""}
      
      #print(consent)

    if request.method == 'POST':

        pacific_timezone = timezone('US/Pacific')
        pacific_time_now=datetime.datetime.now(pacific_timezone)
        pacific_time_now_string=pacific_time_now.strftime("%Y:%m:%d-%H:%M:%S")
        
        data = request.form.to_dict(flat=True)
        data['consentDateTime']=pacific_time_now

        uploadFileName = 'extID%s_afsID%s_time%s.json' % (data['generatedConsentId'], data['externalSurveyId'], pacific_time_now_string)
        upload_file(json.dumps(googleCredentials),"consentPending/" + uploadFileName, "application/json",current_app.config['CLOUD_STORAGE_BUCKET_PRIVATE'])

        #meta={"externalSurveyId": data['externalSurveyId'], "generatedConsentId":data['generatedConsentId']}
        #upload_file(json.dumps(meta),"consentMetaData/" + uploadFileName, "application/json", current_app.config['CLOUD_STORAGE_BUCKET_PRIVATE'])
        #del data['generatedConsentId']
        #del data['externalSurveyId']
         
        consent = get_model().create(data)
      
        return redirect(url_for('.view', id=consent['id']))

    return render_template("form.html", action="Add", consent=consent)

@crud.route("/mine")
@oauth2.required
def list_mine():
    token = request.args.get('page_token', None)
    if token:
        token = token.encode('utf-8')

    consents, next_page_token = get_model().list_by_user(
        user_id=session['profile']['id'],
        cursor=token)

    return render_template(
        "list_user_consents.html",
        consents=consents,
        next_page_token=next_page_token)

@crud.route('/<id>/delete')
@oauth2.required
def delete(id):
    get_model().delete(id)
    consent_files = list_blobs("consentPending/"+session['profile']['id'],current_app.config['CLOUD_STORAGE_BUCKET_PRIVATE'])
    for file in consent_files:
        file.delete()
    takeout_files = list_blobs("takeoutArchive/"+session['profile']['id'],current_app.config['CLOUD_STORAGE_BUCKET_PRIVATE'])
    for file in takeout_files:
        file.delete()
    return render_template("removed.html")
