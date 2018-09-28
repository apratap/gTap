from flask import Flask, current_app
from flask_sqlalchemy import SQLAlchemy
from sqlalchemy import create_engine, Column, Integer, String
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import Session
from sqlalchemy.ext.hybrid import hybrid_property, Comparator
from Crypto.Cipher import AES
import binascii
import uuid

key = uuid.uuid4().bytes
"""The encryption key.   Random for this example."""

def aes_encrypt(data):
    cipher = AES.new(key)
    data = data + (" " * (16 - (len(data) % 16)))
    return binascii.hexlify(cipher.encrypt(data))

def aes_decrypt(data):
    cipher = AES.new(key)
    return cipher.decrypt(binascii.unhexlify(data)).rstrip()





builtin_list = list


db = SQLAlchemy()


def init_app(app):
    # Disable track modifications, as it unnecessarily uses memory.
    app.config.setdefault('SQLALCHEMY_TRACK_MODIFICATIONS', False)
    db.init_app(app)


def from_sql(row):
    """Translates a SQLAlchemy model instance into a dictionary"""
    data = row.__dict__.copy()
    data['id'] = row.id
    data.pop('_sa_instance_state')
    return data


class Consent(db.Model):
    __tablename__ = 'consent'

    id = db.Column(db.Integer, primary_key=True)
    firstName = db.Column(db.String(255))
    lastName = db.Column(db.String(255))
    googleUserId = db.Column(db.String(255))
    consentDateTime = db.Column(db.DateTime)
    generatedConsentId = db.Column(db.String(255))
    externalSurveyId = db.Column(db.String(255))
    

    def __repr__(self):
        return "<Consent(firstName='%s', lastName='%s', googleUserId='%s',consentDateTime='%s')" % (self.firstName, self.lastName, self.googleUserId, self.consentDateTime.strftime('%Y%m%d %H:%M:%S'))


def list(limit=1000, cursor=None):
    cursor = int(cursor) if cursor else 0
    query = (Consent.query
             .order_by(Consent.consentDateTime.desc())
             .limit(limit)
             .offset(cursor))
    consents = builtin_list(map(from_sql, query.all()))
    next_page = cursor + limit if len(consents) == limit else None
    print("the consents: %r " % consents)
    return (consents, next_page)

def list_by_user(user_id, limit=10, cursor=None):
    cursor = int(cursor) if cursor else 0
    query = (Consent.query
             .filter_by(googleUserId=user_id)
             .order_by(Consent.consentDateTime.desc())
             .limit(limit)
             .offset(cursor))
    consents = builtin_list(map(from_sql, query.all()))
    next_page = cursor + limit if len(consents) == limit else None
    return (consents, next_page)


def read(id):
    result = Consent.query.get(id)
    if not result:
        return None
    return from_sql(result)


def create(data):
    #current_app.logger.debug(data)
    consent = Consent(**data)
    db.session.add(consent)
    db.session.commit()
    return from_sql(consent)


def update(data, id):
    consent = Consent.query.get(id)
    for k, v in data.items():
        setattr(consent, k, v)
    db.session.commit()
    return from_sql(consent)


def delete(id):
    Consent.query.filter_by(id=id).delete()
    db.session.commit()


def _create_database():
    """
    If this script is run directly, create all the tables necessary to run the
    application.
    """
    app = Flask(__name__)
    app.config.from_pyfile('../config.py')
    init_app(app)
    with app.app_context():
        db.create_all()
    print("All tables created")


if __name__ == '__main__':
    _create_database()
