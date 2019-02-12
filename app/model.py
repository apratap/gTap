import argparse
from contextlib import contextmanager
import datetime as dt
import json
import re
from pytz import timezone as tz
from ssl import SSLError
import sys
import time

from flask_simple_crypt import SimpleCrypt
from sqlalchemy import and_, or_, create_engine, Column, Integer, LargeBinary, String, DateTime, Index, ForeignKey
from sqlalchemy.engine.url import URL
from sqlalchemy.exc import IntegrityError
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker, relationship
import synapseclient
from synapseclient import Schema, Table
from synapseclient import Column as SynColumn
from synapseclient.exceptions import SynapseHTTPError

import app.config as secrets

syn = secrets.syn

Base = declarative_base()

SYN_SCHEMA = Schema(
    name='Consents',
    columns=[
        SynColumn(name='participant_id', columnType='STRING', maximumSize=31),
        SynColumn(name='eid',          columnType='INTEGER'),
        SynColumn(name='consent_dt',   columnType='STRING', maximumSize=63),
        SynColumn(name='location_sid', columnType='STRING', maximumSize=31),
        SynColumn(name='search_sid',   columnType='STRING', maximumSize=31),
        SynColumn(name='notes',        columnType='STRING', maximumSize=1000),
    ],
    parent=secrets.PROJECT_SYNID
)
BLANK_CONSENT = (
    'blank', 0, 'blank', 'blank', 'blank', 'blank'
)


class AppWrap(object):
    def __init__(self, config):
        self.config = dict(
            SECRET_KEY=config.SECRET_KEY,
            FSC_EXPANSION_COUNT=config.FSC_EXPANSION_COUNT
        )


cypher = SimpleCrypt()
cypher.init_app(AppWrap(secrets))


class Consent(Base):
    __tablename__ = 'consent'

    eid = Column(Integer, primary_key=True)
    pid = Column(String)
    email = Column(String)
    first_name = Column(String)
    last_name = Column(String)
    gender = Column(String)
    consent_dt = Column(DateTime)
    data = Column(LargeBinary)

    location_sid = Column(String)
    location_eid = Column(ForeignKey('takeout_errors.errid'))
    location_err = relationship('TakeoutError', primaryjoin="Consent.location_eid == foreign(TakeoutError.errid)")

    search_sid = Column(String)
    search_eid = Column(ForeignKey('takeout_errors.errid'))
    search_err = relationship('TakeoutError', primaryjoin="Consent.search_eid == foreign(TakeoutError.errid)")

    Index('idx_eid', 'eid')
    Index('idx_dt', 'consent_dt')

    def __init__(self, **kwargs):
        self.pid = kwargs['participant_id']
        self.eid = kwargs.get('eid')
        self.consent_dt = kwargs['consent_dt']
        self.data = self.__encrypt(kwargs['credentials'])
        self.location_sid = kwargs.get('location_sid')
        self.search_sid = kwargs.get('search_sid')
        self.email = kwargs.get('email')
        self.first_name = kwargs.get('first_name')
        self.last_name = kwargs.get('last_name')
        self.gender = kwargs.get('gender')

    def __repr__(self):
        return "<Consent(eid='%s', consentDateTime='%s', err='%s')>" % (
            f'{self.eid}',
            self.consent_dt.strftime(secrets.DTFORMAT),
            (self.search_err or self.location_err)
        )

    def __str__(self):
        return f'participant: {self.pid}, eid: {self.eid}'

    @property
    def credentials(self):
        s = cypher.decrypt(self.data)
        s = s.decode('utf-8')

        return json.loads(s)

    @staticmethod
    def __encrypt(data):
        s = json.dumps(data)
        s = s.encode('utf-8')

        msg = cypher.encrypt(s)

        return msg


class TakeoutError(Base):
    __tablename__ = 'takeout_errors'

    errid = Column(Integer, primary_key=True)
    error = Column(String(31))
    message = Column(String)

    def __init__(self, err):
        self.error = re.sub(r'(<|>|class|\')+', '', str(type(err)))
        self.message = re.sub(r'(\(|\)|\')+', '', str(err.args))

    def __repr__(self):
        return "<TakeoutError(error=%s, message=%s)>" % (self.error, self.message)

    def __str__(self):
        return "%s: %s" % (self.error, self.message)


def get_engine(conn):
    driver = conn['drivername']

    if driver == 'sqlite':
        args = f'sqlite+pysqlite:///{conn["path"]}'
    elif driver == 'postgres':
        args = URL(**conn)
    else:
        raise Exception('driver undefined')

    return create_engine(args)


@contextmanager
def session_scope(conn):
    conn = connection(conn)
    engine = get_engine(conn)
    session = sessionmaker(bind=engine)()

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


def add_task(data, conn=None):
    consent = Consent(**data)

    with session_scope(conn) as s:
        s.add(consent)

    return consent


def add_entity(session, entity):
    try:
        session.add(entity)
        session.commit()
    except:
        session.rollback()

    return entity


def add_search_error(session, consent, err):
    err = TakeoutError(err)

    add_entity(session, err)

    consent.search_eid = err.errid
    consent.search_err = [err]

    return consent


def add_location_error(session, consent, err):
    err = TakeoutError(err)

    add_entity(session, err)

    consent.location_eid = err.errid
    consent.location_err = [err]

    return consent


def get_all_pending(conn=None, session=None):
    if session is None:
        session = session_scope(conn)
        close = True
    else:
        close = False

    pending = session.query(Consent)\
        .filter(or_(
            and_(Consent.location_sid == None, Consent.location_eid == None),
            and_(Consent.search_sid == None, Consent.search_eid == None)
        ))\
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


def get_consent(pid, eid, session):
    c = session.query(Consent).filter(
        and_(Consent.pid == pid, Consent.eid == eid)
    ).first()

    return c


def update_consent(consent):
    with session_scope(secrets.DATABASE) as s:
        c = get_consent(consent.pid, consent.eid, s)

        if c is not None:
            if consent.location_err is None:
                c.location_sid = consent.location_sid
                c.location_eid = None

            if consent.search_err is None:
                c.search_sid = consent.search_sid
                c.search_eid = None

            update_synapse_sids(consent)
        else:
            raise KeyError


def get_next_eid():
    results = syn.tableQuery(
        f"select max(eid) as cnt from {secrets.CONSENTS_SYNID}"
    ).asDataFrame()
    results = results.cnt.tolist()

    idx = results[0] + 1 if len(results) > 0 else 1
    return idx


def put_consent_to_synapse(data):
    rmeow = dt.datetime.now(tz("US/Pacific")).strftime(secrets.DTFORMAT).upper()
    data = [[
        data['participant_id'],
        data['eid'],
        data['consent_dt'].strftime(secrets.DTFORMAT).upper(),
        data.get('location_sid'),
        data.get('search_sid'),
        data.get('notes')
    ]]

    def put():
        syn.store(Table(SYN_SCHEMA, data))

    def write_to_stdout(e):
        sys.stdout.write(f'{rmeow}: failed to push consent <{data}> to Synapse {e}\n')

    retries, completed = 10, False
    while not completed and retries > 0:
        try:
            put()
            completed = True
        except SSLError:
            write_to_stdout('SSLError')
        except SynapseHTTPError:
            write_to_stdout('SynapseHTTPError')
        except Exception as e:
            write_to_stdout(str(e))

        retries -= 1
        time.sleep(3)


def update_synapse_sids(consent):
    results = syn.tableQuery(
        f"select * from {secrets.CONSENTS_SYNID} "
        f"where participant_id='{consent.pid}'"
        f"  and eid='{consent.eid}'"
    ).asDataFrame()

    assert len(results) == 1

    notes = ""

    def stringy(err):
        return ', '.join([str(e) for e in err])

    if len(consent.search_err) > 0:
        notes += f'search: {stringy(consent.search_err)}'

    if len(consent.location_err) > 0:
        notes += f'location: {stringy(consent.location_err)}'

    results['location_sid'] = consent.location_sid
    results['search_sid'] = consent.search_sid
    results['notes'] = notes

    syn.store(Table(SYN_SCHEMA, results))


def create_database(conn):
    engine = get_engine(conn)
    Base.metadata.create_all(engine)


def build_synapse_table():
    table = Table(SYN_SCHEMA, values=[BLANK_CONSENT])
    table = syn.store(table)

    results = syn.tableQuery("select * from %s where participant_id = '%s'" % (table.tableId, BLANK_CONSENT[0]))
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
    # build_synapse_table()
