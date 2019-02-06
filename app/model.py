from contextlib import contextmanager
import json
import re

from simplecrypt import encrypt, decrypt
from sqlalchemy import and_, or_, create_engine, Boolean, Column, Integer, LargeBinary, String, DateTime, Index, ForeignKey
from sqlalchemy.engine.url import URL
from sqlalchemy.exc import IntegrityError
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker, relationship
import synapseclient
from synapseclient import Schema, Table
from synapseclient import Column as SynColumn

import app.config as secrets

syn = secrets.syn

Base = declarative_base()

SYN_SCHEMA = Schema(
    name='Consents',
    columns=[
        SynColumn(name='participant_id', columnType='STRING', maximumSize=31),
        SynColumn(name='guid',          columnType='STRING', maximumSize=31),
        SynColumn(name='guid_counter', columnType='INTEGER'),
        SynColumn(name='ext_id',       columnType='STRING', maximumSize=31),
        SynColumn(name='consent_dt',   columnType='STRING', maximumSize=63),
        SynColumn(name='location_sid', columnType='STRING', maximumSize=31),
        SynColumn(name='search_sid',   columnType='STRING', maximumSize=31),
        SynColumn(name='notes',        columnType='STRING', maximumSize=1000),
    ],
    parent=secrets.PROJECT_SYNID
)
BLANK_CONSENT = (
    'blank', 'blank', 0, 'blank', 'blank', 'blank', 'blank', 'blank', 'blank'
)


class Consent(Base):
    __tablename__ = 'consent'

    pid = Column(String)
    guid = Column(String, primary_key=True)
    guid_counter = Column(Integer, primary_key=True)
    email = Column(String)
    consent_dt = Column(DateTime)
    data = Column(LargeBinary)

    location_sid = Column(String)
    location_eid = Column(ForeignKey('takeout_errors.eid'))
    location_err = relationship('TakeoutError', primaryjoin="Consent.location_eid == foreign(TakeoutError.eid)")

    search_sid = Column(String)
    search_eid = Column(ForeignKey('takeout_errors.eid'))
    search_err = relationship('TakeoutError', primaryjoin="Consent.search_eid == foreign(TakeoutError.eid)")

    Index('idx_ext', 'ext_id')
    Index('idx_dt', 'consent_dt')

    def __init__(self, **kwargs):
        self.pid = kwargs['participant_id']
        self.guid = kwargs['guid']
        self.guid_counter = kwargs['guid_counter']
        self.consent_dt = kwargs['consent_dt']
        self.data = self.__encrypt(kwargs['credentials'])
        self.location_sid = kwargs.get('location_sid')
        self.search_sid = kwargs.get('search_sid')
        self.email = kwargs.get('email')

    def __repr__(self):
        return "<Consent(ext_id='%s', consentDateTime='%s', err='%s')>" % (
            f'{str(self.guid_counter).zfill(secrets.EXT_ZERO_FILL)}{self.guid}',
            self.consent_dt.strftime(secrets.DTFORMAT),
            (self.search_err or self.location_err)
        )

    def __str__(self):
        return f'participant: {self.pid}, ext_id: {self.ext_id}'

    @property
    def credentials(self):
        s = decrypt(secrets.CIPHER_KEY, self.data)
        s = s.decode('utf-8')

        return json.loads(s)

    @property
    def ext_id(self):
        return f'{str(self.guid_counter).zfill(secrets.EXT_ZERO_FILL)}{self.guid}'

    @staticmethod
    def __encrypt(data):
        s = json.dumps(data)
        s = s.encode('utf-8')

        msg = encrypt(secrets.CIPHER_KEY, s)

        return msg


class TakeoutError(Base):
    __tablename__ = 'takeout_errors'

    eid = Column(Integer, primary_key=True)
    error = Column(String(31))
    message = Column(String)

    def __init__(self, err):
        self.error = re.sub(r'(<|>|class|\')+', '', str(type(err)))
        self.message = re.sub(r'(\(|\)|\')+', '', str(err.args))

    def __repr__(self):
        return "<TakeoutError(error=%s, message=%s)>" % (self.error, self.message)

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

    consent.search_eid = err.eid
    consent.search_err = [err]

    return consent


def add_location_error(session, consent, err):
    err = TakeoutError(err)

    add_entity(session, err)

    consent.location_eid = err.eid
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


def get_consent(guid, guid_counter, session):
    c = session.query(Consent).filter(
        and_(Consent.guid == guid, Consent.guid_counter == guid_counter)
    ).first()

    return c


def update_consent(consent):
    with session_scope(secrets.DATABASE) as s:
        c = get_consent(consent.guid, consent.guid_counter, s)

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


def get_next_guid_counter(guid):
    results = syn.tableQuery(
        f"select max(guid_counter) as cnt from {secrets.CONSENTS_SYNID} where guid='{guid}'"
    ).asDataFrame()
    results = results.cnt.tolist()

    return results[0] + 1 if len(results) > 0 else 1


def put_consent_to_synapse(data):
    data = [[
        data['participant_id'],
        data['guid'],
        data['guid_counter'],
        f'{str(data["guid_counter"]).zfill(secrets.EXT_ZERO_FILL)}{data["guid"]}',
        data['consent_dt'],
        data.get('location_sid'),
        data.get('search_sid'),
        data.get('notes')
    ]]
    syn.store(Table(SYN_SCHEMA, data))


def update_synapse_sids(consent):
    results = syn.tableQuery(
        f"select * from {secrets.CONSENTS_SYNID} "
        f"where participant_id='{consent.pid}'"
        f"  and guid='{consent.guid}'"
        f"  and guid_counter='{consent.guid_counter}'"
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
    engine = create_engine(URL(**conn))
    Base.metadata.create_all(engine)


def build_synapse_table():
    table = Table(SYN_SCHEMA, values=[BLANK_CONSENT])
    table = syn.store(table)

    results = syn.tableQuery("select * from %s where ext_id = '%s'" % (table.tableId, BLANK_CONSENT[0]))
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
