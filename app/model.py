from contextlib import contextmanager
import json

from simplecrypt import encrypt, decrypt
from sqlalchemy import create_engine, Boolean, Column, Integer, LargeBinary, String, DateTime, Index, ForeignKey
from sqlalchemy.engine.url import URL
from sqlalchemy.exc import IntegrityError
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker
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
        SynColumn(name='guid',        columnType='STRING', maximumSize=55),
        SynColumn(name='first_name',  columnType='STRING', maximumSize=255),
        SynColumn(name='last_name',   columnType='STRING', maximumSize=255),
        SynColumn(name='email',       columnType='STRING', maximumSize=255),
        SynColumn(name='gender',      columnType='STRING', maximumSize=255),
        SynColumn(name='timestamp',   columnType='STRING', maximumSize=63),
        SynColumn(name='takeout_sid', columnType='STRING')
    ],
    parent=secrets.PROJECT_SYNID
)
BLANK_CONSENT = (
    'blank', 'blank', 'blank', 'blank@blank.com', 'blank', 'blank', 'blank'
)


class Consent(Base):
    __tablename__ = 'consent'

    guid = Column(String, primary_key=True)
    consent_dt = Column(DateTime)
    data = Column(LargeBinary)
    # blob = Column(String)
    # salt = Column(String)
    error_state = Column(Boolean)
    processed = Column(Boolean)

    Index('idx_guid', 'guid')
    Index('idx_dt', 'consent_dt')

    def __init__(self, **kwargs):
        self.guid = kwargs['guid']
        self.consent_dt = kwargs['consent_dt']
        self.data = self.__encrypt(kwargs['credentials'])
        self.error_state = False
        self.processed = False

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
            error_state=self.error_state,
            processed=self.processed
        )

    @property
    def credentials(self):
        s = decrypt(secrets.CIPHER_KEY, self.data)
        s = s.decode('utf-8')

        return json.loads(s)

    @property
    def tuple(self):
        return (
            self.guid, self.consent_dt.strftime(secrets.DTFORMAT), self.error_state, self.processed
        )

    @staticmethod
    def __encrypt(data):
        s = json.dumps(data)
        s = s.encode('utf-8')

        msg = encrypt(secrets.CIPHER_KEY, s)

        return msg


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
    conn = connection(conn)
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

    with session_scope(conn) as s:
        s.add(consent)

    return consent


def add_error(e, consent, conn=None):
    with session_scope(conn) as s:
        error = TakeoutError(
            guid=consent.guid,
            error=str(type(e)),
            message=str(e.args)
        )
        s.add(error)
        consent.error_state = True


def get_all_pending(conn=None, session=None):
    if session is None:
        session = session_scope(conn)
        close = True
    else:
        close = False

    pending = session.query(Consent)\
        .filter(
            Consent.error_state == False,
            Consent.processed == False
        )\
        .order_by(Consent.consent_dt.desc())\
        .all()

    if close:
        session.close()

    return pending


def connection(conn):
    if conn is None:
        return secrets.DATABASE
    else:
        return conn


def get_consent(guid, conn=None):
    with session_scope(conn) as s:
        consent = s.query(Consent).filter(Consent.guid == guid).first()

    return consent


def remove_consent(consent):
    guid = verify_consent(consent)

    with session_scope(secrets.DATABASE) as s:
        s.query(Consent).filter_by(Consent.guid == guid).delete()


def mark_completed(consent):
    guid = verify_consent(consent)

    with session_scope(secrets.DATABASE) as s:
        c = s.query(Consent).filter_by(Consent.guid == guid).first()

        if c is not None:
            c.data = None
            c.processed = True
        else:
            raise KeyError


def verify_consent(consent):
    if isinstance(consent, Consent):
        guid = consent.guid
    elif isinstance(consent, int):
        guid = consent
    else:
        raise TypeError('Type %s not supported' % type(consent))

    return guid


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


def update_synapse_flag(consent, flag):
    results = syn.tableQuery(
        "select * from %s where guid = '%s'" %
        (secrets.CONSENTS_SYNID, consent.guid)
    ).asDataFrame()

    results[flag] ^= results[flag]
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
    #build_synapse_table()
