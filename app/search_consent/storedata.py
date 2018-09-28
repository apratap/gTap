from __future__ import absolute_import

import datetime

from flask import current_app
from google.cloud import storage
import six
from werkzeug import secure_filename
from werkzeug.exceptions import BadRequest


def _get_storage_client():
    return storage.Client()
        #project=current_app.config['PROJECT_ID'])





def upload_file(file_stream, filename, content_type, source_bucket):
    """
    Uploads a file to a given Cloud Storage bucket and returns the public url
    to the new object.
    """
    filename = filename

    client = _get_storage_client()
    bucket = client.get_bucket(source_bucket)

    blob = bucket.blob(filename)

    blob.upload_from_string(
        file_stream,
        content_type=content_type)

    url = blob.public_url

    if isinstance(url, six.binary_type):
        url = url.decode('utf-8')

    return url


def list_blobs(prefix, source_bucket):
    client = _get_storage_client()
    bucket = client.get_bucket(source_bucket)
    blobs=bucket.list_blobs(prefix=prefix)
    return blobs
