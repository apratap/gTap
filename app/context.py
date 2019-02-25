from contextlib import contextmanager
import datetime as dt
import json
from pytz import timezone as tz
from ssl import SSLError
import time

from flask_simple_crypt import SimpleCrypt
import numpy as np
from sendgrid import Email, SendGridAPIClient
from sendgrid.helpers.mail import Content, Mail
from sqlalchemy import and_, or_, cast, \
    create_engine, inspect, Column, Integer, LargeBinary, \
    String, Date, DateTime, Index, ForeignKey
from sqlalchemy.engine.url import URL
from sqlalchemy.exc import IntegrityError, OperationalError
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.ext.hybrid import hybrid_property
from sqlalchemy.orm import sessionmaker, relationship
import synapseclient
from synapseclient import Schema, Table
from synapseclient import Column as SynColumn
from synapseclient.exceptions import SynapseHTTPError

import app.config as secrets

syn = secrets.syn

# ------------------------------------------------------
# Model
# ------------------------------------------------------
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


class StringArray(String):
    def __init__(self, s='', delimiter=', '):
        self.__delimiter = delimiter

        if s is not None and len(s) > 0:
            self.__contents = sorted(s.split(delimiter))
        else:
            self.__contents = []

        super().__init__(str(self))

    def __str__(self):
        return self.__delimiter.join(self.__contents)

    def __repr__(self):
        return f'<StringArray({self.__delimiter.join(self.__contents)})>'

    def merge(self, s):
        if isinstance(s, str):
            x = [si.strip() for si in s.lower().split(self.__delimiter)]

        elif isinstance(s, list):
            def isstr(s_):
                try:
                    str(s_)
                    return True
                except Exception:
                    return False

            assert all([isstr(si) for si in s])
            x = [si.lower().strip() for si in s]

        else:
            raise TypeError

        self.__contents = sorted(list(set(self.__contents + x)))
        return self

    def remove(self, s):
        if not isinstance(s, str):
            raise TypeError

        try:
            s = s.strip().lower()
            self.__contents.remove(s)
        except ValueError:
            pass


class Consent(Base):
    __tablename__ = 'consent'

    eid = Column(Integer, primary_key=True, autoincrement=True)
    pid = Column(String)
    email = Column(String)
    first_name = Column(String)
    last_name = Column(String)
    consent_dt = Column(DateTime)
    data = Column(LargeBinary)
    location_sid = Column(String)
    search_sid = Column(String)

    logs = relationship("LogEntry", lazy='joined')

    Index('idx_eid', 'eid')
    Index('idx_dt', 'consent_dt')

    def __init__(self, **kwargs):
        self.pid = kwargs['participant_id']
        self.consent_dt = kwargs['consent_dt']
        self.data = self.__encrypt(kwargs['credentials'])
        self.location_sid = kwargs.get('location_sid')
        self.search_sid = kwargs.get('search_sid')
        self.email = kwargs.get('email')
        self.first_name = kwargs.get('first_name')
        self.last_name = kwargs.get('last_name')

    def __repr__(self):
        return "<Consent(eid='%s', consentDateTime='%s')>" % (
            f'{self.eid}',
            self.consent_dt.strftime(secrets.DTFORMAT)
        )

    def __str__(self):
        return f'participant: {self.pid}, eid: {self.eid}'

    @property
    def credentials(self):
        s = cypher.decrypt(self.data)
        s = s.decode('utf-8')

        return json.loads(s)

    @property
    def dict(self):
        return {
            'eid': self.eid,
            'pid': self.pid,
            'email': self.email,
            'first_name': self.first_name,
            'last_name': self.last_name,
            'consent_dt': self.consent_dt,
            'location_sid': self.location_sid,
            'search_sid': self.search_sid,
            'notes': [l.dict for l in self.logs]
        }

    @property
    def notes(self):
        logs = sorted(self.logs, key=lambda x: x.ts)

        return '  ' + str(logs[0]) + '\n  ' + '\n  '.join([
            str(entry) for entry in logs[1:]
        ])

    @hybrid_property
    def date(self):
        return self.consent_dt.date()

    @hybrid_property
    def hours_since_consent(self):
        return np.ceil((dt.datetime.now()-self.consent_dt).seconds/3600)

    @staticmethod
    def __encrypt(data):
        s = json.dumps(data)
        s = s.encode('utf-8')

        msg = cypher.encrypt(s)

        return msg

    def clear_credentials(self):
        self.data = None

        session = inspect(self).session
        commit(session)

        add_log_entry(f'credentials for eid={self.eid} have been cleared', self.eid)

    def add_search_error(self, msg=None, session=None):
        if self.search_sid is not None and len(self.search_sid) > 0:
            self.search_sid += ', err'
        else:
            self.search_sid = 'err'

        if msg is not None:
            add_log_entry(msg, session)

        return self

    def add_location_error(self, msg=None, session=None):
        if self.location_sid is not None and len(self.location_sid) > 0:
            self.location_sid += ', err'
        else:
            self.location_sid = 'err'

        if msg is not None:
            add_log_entry(msg, session)

        return self

    def put_to_synapse(self):
        rmeow = dt.datetime.now(tz("US/Pacific")).strftime(secrets.DTFORMAT).upper()

        data = [[
            self.pid,
            self.eid,
            rmeow,
            self.location_sid,
            self.search_sid,
            self.notes
        ]]

        def put():
            syn.store(Table(SYN_SCHEMA, data))

        retries = 10
        while retries > 0:
            try:
                put()
                retries = 0
            except SSLError:
                pass
            except SynapseHTTPError:
                pass
            except Exception as e:
                add_log_entry(f'consent for eid={self.eid} failed to push to Synapse with error={str(e)}')
                retries = 0

            retries -= 1
            time.sleep(3)

    def update_synapse(self):
        results = syn.tableQuery(
            f"select * from {secrets.CONSENTS_SYNID} "
            f"where participant_id='{self.pid}'"
            f"  and eid='{self.eid}'"
        ).asDataFrame()

        if len(results) == 0:
            self.put_to_synapse()
        else:
            results['location_sid'] = self.location_sid
            results['search_sid'] = self.search_sid
            results['notes'] = self.notes

            syn.store(Table(SYN_SCHEMA, results))

    def set_search_sid(self, sid):
        if self.search_sid is None:
            self.search_sid = str(StringArray(sid))
        else:
            self.search_sid = str(StringArray(self.search_sid).merge(sid))

        session = inspect(self).session
        commit(session)

        self.update_synapse()

    def set_location_sid(self, sid):
        if self.location_sid is None:
            self.location_sid = str(StringArray(sid))
        else:
            self.location_sid = str(StringArray(self.search_sid).merge(sid))

        session = inspect(self).session
        commit(session)

        self.update_synapse()

    def notify_participant(self):
        from_ = Email(secrets.FROM_STUDY_EMAIL)
        to_ = Email(self.email)

        content = Content(
            "text/plain",
            secrets.PARTICIPANT_EMAIL_BODY.format(
                eid=self.eid,
                search_status='OK' if self.search_sid != 'err' and self.search_sid is not None else 'FAILED',
                location_status='OK' if self.location_sid != 'err' and self.location_sid is not None else 'FAILED',
                notes=self.notes
            )
        )

        subject_ = secrets.PARTICIPANT_EMAIL_SUBJECT.format(eid=self.eid)
        mail = Mail(from_, subject_, to_, content)

        sg = SendGridAPIClient(apikey=secrets.SENDGRID_API_KEY)
        response = sg.client.mail.send.post(request_body=mail.get())

        return response.status_code


class LogEntry(Base):
    __tablename__ = 'log'

    id = Column(Integer, autoincrement=True, primary_key=True)
    cid = Column(ForeignKey('consent.eid'), nullable=True)
    ts = Column(DateTime)
    msg = Column(String)

    def __init__(self, msg, cid=None):
        self.ts = dt.datetime.now(tz("US/Pacific"))
        self.cid = cid
        self.msg = msg

    @property
    def dict(self):
        return {
            'ts': self.ts,
            'cid': self.cid,
            'msg': self.msg
        }

    @property
    def ts_formatted(self):
        return self.ts.strftime(secrets.DTFORMAT).upper()

    def __repr__(self):
        return f'<LogEntry(ts={self.ts_formatted}, msg={self.msg})>'

    def __str__(self):
        return f'{self.ts_formatted}: {self.msg}'


# ------------------------------------------------------
# Database Context
# ------------------------------------------------------
def get_engine(conn):
    driver = conn['drivername']

    if driver == 'sqlite':
        args = f'sqlite+pysqlite:///{conn["path"]}'
    elif driver == 'postgres':
        args = URL(**conn)
    else:
        raise Exception('driver undefined')

    return create_engine(args)


def connection(conn):
    if conn is None:
        return secrets.DATABASE
    else:
        return conn


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


# ------------------------------------------------------
# Interactions
# ------------------------------------------------------
def add_task(data, conn=None, session=None):
    consent = Consent(**data)

    if session is not None:
        consent = add_entity(session, consent)
        return consent

    else:
        with session_scope(conn) as s:
            add_entity(s, consent)

        # don't return the consent from here, will cause
        # an error because the consent will detach from the
        # session when it closes
        return None


def add_entity(session, entity):
    if session is None:
        with session_scope(None) as s:
            s.add(entity)
            commit(s)
    else:
        session.add(entity)
        commit(session)

    return entity


def commit(s):
    try:
        s.commit()
    except IntegrityError:
        s.rollback()
    except OperationalError as e:
        s.rollback()
        add_log_entry(f'SQL operational error occurred: <{str(e)}>')
    except Exception as e:
        s.rollback()
        add_log_entry(f'Something bad happened <{str(e)}>')
        raise e


def add_log_entry(entry, cid=None, session=None):
    if isinstance(entry, str):
        entry = LogEntry(entry, cid)

    elif isinstance(entry, LogEntry):
        pass
    else:
        try:
            entry = LogEntry(str(entry), cid)
        except TypeError:
            raise Exception('can only log an object with a __str__ method')

    return add_entity(session, entry)


def get_all_pending(conn=None, session=None):
    if session is None:
        session = session_scope(conn)
        close = True
    else:
        close = False

    pending = session.query(Consent).filter(or_(
        Consent.location_sid == None,
        Consent.search_sid == None)
    ).order_by(Consent.consent_dt.desc()).all()

    ready = []
    for c in pending:
        if c.hours_since_consent > 24:
            c.clear_credentials()
            c.add_location_error('credentials expired', session=session)
            c.add_search_error('credentials expired', session=session)
        else:
            ready.append(c)

    if close:
        commit(session)
        session.close()

    return ready


def get_consent(pid, eid, session):
    c = session.query(Consent).filter(
        and_(Consent.pid == pid, Consent.eid == eid)
    ).first()

    return c


def daily_digest(conn=None):
    today = dt.date.today()

    with session_scope(conn) as s:
        consents = s.query(Consent).filter(
            cast(Consent.consent_dt, Date) == today
        ).all()

        n = len(consents)
        n_searches = sum([
            1 for c in consents
            if c.search_sid is not None and c.search_sid != 'err'
        ])
        n_locations = sum([
            1 for c in consents
            if c.location_sid is not None and c.location_sid != 'err'
        ])

        digest = {
            'today': today.strftime('%B %d, %Y'),
            'consents_added': n,
            'searches': n_searches,
            'locations': n_locations,
            'consents': [c.dict for c in consents]
        }

        for c in digest['consents']:
            c['consent_dt'] = c['consent_dt'].strftime(secrets.DTFORMAT).upper()

            for note in c['notes']:
                del note['cid']
                note['ts'] = note['ts'].strftime(secrets.DTFORMAT).upper()

    return digest


if __name__ == '__main__':
    pass
    # create_database(secrets.DATABASE)
    # build_synapse_table()
