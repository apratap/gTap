from base64 import b64encode, b64decode
from contextlib import contextmanager

from sqlalchemy import create_engine, Boolean, Column, Integer, String, DateTime, Index, ForeignKey
from sqlalchemy.engine.url import URL
from sqlalchemy.exc import IntegrityError
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker
from simplecrypt import encrypt, decrypt
import synapseclient
from synapseclient import Schema, Table
from synapseclient import Column as SynColumn

import app.config as secrets

syn = synapseclient.Synapse()
syn.login()

Base = declarative_base()

SYN_SCHEMA = Schema(
    name='Consents',
    columns=[
        SynColumn(name='guid', columnType='STRING', maximumSize=55),
        SynColumn(name='first_name', columnType='STRING', maximumSize=255),
        SynColumn(name='last_name',  columnType='STRING', maximumSize=255),
        SynColumn(name='email',      columnType='STRING', maximumSize=255),
        SynColumn(name='gender',     columnType='STRING', maximumSize=255),
        SynColumn(name='timestamp',  columnType='STRING', maximumSize=63),
        SynColumn(name='has_location_data', columnType='BOOLEAN'),
        SynColumn(name='has_search_data',   columnType='BOOLEAN'),
    ],
    parent=secrets.PROJECT_SYNID
)
BLANK_CONSENT = (
    'blank', 'blank', 'blank', 'blank@blank.com', 'blank', 'blank', False, False
)


class Consent(Base):
    __tablename__ = 'consent'

    guid = Column(String, primary_key=True)
    consent_dt = Column(DateTime)
    data = Column(String)
    error_state = Column(Boolean)

    Index('idx_guid', 'guid')
    Index('idx_dt', 'consent_dt')

    def __init__(self, **kwargs):
        self.guid = kwargs['guid']
        self.consent_dt = kwargs['consent_dt']
        self.data = self.__encrypt(kwargs['credentials'])
        self.error_state = False

    def __repr__(self):
        return "<Consent(guid='%s', consentDateTime='%s')>" % \
               (self.guid, self.consent_dt.strftime(secrets.DTFORMAT))

    def __str__(self):
        return "consent_%s_%s" % (self.guid, str(self.consent_dt.timestamp()))

    @property
    def dict(self):
        return dict(
            guid=self.guid,
            consent_dt=self.consent_dt,
            error_state=self.error_state
        )

    @property
    def credentials(self):
        decoded = b64decode(self.data)
        return decrypt(secrets.CIPHER_KEY, decoded)

    @property
    def tuple(self):
        return (
            self.guid, self.consent_dt.strftime(secrets.DTFORMAT), self.error_state
        )

    @staticmethod
    def __encrypt(data):
        cypher_text = encrypt(secrets.CIPHER_KEY, data)
        encoded = b64encode(cypher_text)
        return encoded


class TakeoutError(Base):
    __tablename__ = 'takeout_errors'

    eid = Column(Integer, primary_key=True)
    guid = Column(ForeignKey('consent.guid'))
    error = Column(String(31))
    message = Column(String)

    def __repr__(self):
        return "<TakeoutError(cid=%s, error=%s, message=%s)>" % (self.guid, self.error, self.message)

    def __str__(self):
        return "%s: %s" % (self.error, self.message)


@contextmanager
def session_scope(conn):
    """Provide a transactional scope around a series of operations."""

    engine_ = create_engine(URL(**conn))
    session = sessionmaker(bind=engine_)()

    try:
        yield session
        session.commit()
    except IntegrityError:
        session.rollback()
    except Exception as e:
        session.rollback()
        raise e
    finally:
        session.close()


def add_consent(data, conn=None):
    consent = Consent(**data)

    with session_scope(connection(conn)) as s:
        s.add(consent)

    return consent


def add_error(data, conn=None):
    with session_scope(connection(conn)) as s:
        consent = s.query(Consent).filter(Consent.guid == data['guid']).first()
        if consent is None:
            raise KeyError
        else:
            s.add(data)
            consent.error_state = True


def get_all_pending(conn=None):
    with session_scope(connection(conn)) as s:
        pending = s.query(Consent)\
                .filter(Consent.error_state == False)\
                .order_by(Consent.consent_dt.desc())\
                .all()

    return pending


def connection(conn):
    if conn is None:
        return secrets.DATABASE
    else:
        return conn


def get_consent(guid, conn=None):
    with session_scope(connection(conn)) as s:
        consent = s.query(Consent).filter(Consent.guid == guid).first()

    return consent


def remove_consent(consent):
    if isinstance(consent, Consent):
        guid = consent.guid
    elif isinstance(consent, int):
        guid = consent
    else:
        raise TypeError('Type %s not supported' % type(consent))

    with session_scope(secrets.DATABASE) as s:
        s.query(Consent).filter_by(Consent.guid == guid).delete()


def put_consent_to_synapse(data):
    data = [[
        data['guid'],
        data['first'],
        data['last'],
        data['email'],
        data['gender'],
        data['consent_dt'],
        False,
        False
    ]]
    syn.store(Table(SYN_SCHEMA, data))


def update_flag(consent, field):
    results = syn.tableQuery(
        "select * from %s where guid = '%s'" %
        (secrets.CONSENTS_SYNID, consent.guid)
    ).asDataFrame()

    results[field] ^= results[field]
    syn.store(Table(SYN_SCHEMA, results, etag=results.etag))


def create_database(conn):
    engine = create_engine(URL(**conn))
    Base.metadata.create_all(engine)


def build_synapse_table():
    table = Table(SYN_SCHEMA, values=[BLANK_CONSENT])
    table = syn.store(table)

    results = syn.tableQuery("select * from %s where guid = '%s'" % (table.tableId, BLANK_CONSENT[0]))
    syn.delete(results)

    syn.setProvenance(
        entity=table.tableId,
        activity=synapseclient.Activity(
            name='Created',
            description='Table generated by gTap.'
        )
    )


if __name__ == '__main__':
    create_database(secrets.DATABASE)
    build_synapse_table()
