from contextlib import contextmanager
import datetime as dt
from enum import Enum
import json
from pytz import timezone as tz
from ssl import SSLError
import sys
import time

from botocore.exceptions import ClientError
import boto3
from flask_simple_crypt import SimpleCrypt
from jinja2 import Template
import numpy as np
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

# ----------------------------------------------------------------------------------------------------------------------
# Model
# ----------------------------------------------------------------------------------------------------------------------
Base = declarative_base()

SYN_SCHEMA = Schema(
    name=secrets.CONSENTS_TABLE_NAME,
    columns=[
        SynColumn(name='study_id',     columnType='STRING', maximumSize=31),
        SynColumn(name='internal_id',  columnType='STRING'),
        SynColumn(name='consent_dt',   columnType='STRING', maximumSize=63),
        SynColumn(name='location_sid', columnType='STRING', maximumSize=127),
        SynColumn(name='search_sid',   columnType='STRING', maximumSize=127),
        SynColumn(name='notes',        columnType='STRING', maximumSize=1000),
    ],
    parent=secrets.PROJECT_SYNID
)
BLANK_CONSENT = ('blank', 0, 'blank', 'blank', 'blank', 'blank')


class AppWrap(object):
    """a class used to wrap the application configuration options required to initialize the encryption cypher"""
    def __init__(self, config):
        self.config = dict(
            SECRET_KEY=config.SECRET_KEY,
            FSC_EXPANSION_COUNT=config.FSC_EXPANSION_COUNT
        )


cypher = SimpleCrypt()
cypher.init_app(AppWrap(secrets))


class StringArray(String):
    """a class used to manage an array of strings, representing them as a single"""
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


class ConsentStatus(Enum):
    READY = 'ready'
    PROCESSING = 'processing'
    COMPLETE = 'complete'
    FAILED = 'failed'
    DRIVE_NOT_READY = 'drive_not_ready'


class Consent(Base):
    """datatype used to represent a consent and is used for both the DB backend and Synapse consents table"""
    __tablename__ = 'consent'

    internal_id = Column(Integer, primary_key=True, autoincrement=True)
    study_id = Column(String)
    email = Column(String)
    first_name = Column(String)
    last_name = Column(String)
    consent_dt = Column(DateTime)
    data = Column(LargeBinary)
    location_sid = Column(String)
    search_sid = Column(String)
    status = Column(String)

    logs = relationship("LogEntry")

    Index('idx_iid', 'iid')
    Index('idx_dt', 'consent_dt')

    def __init__(self, **kwargs):
        self.study_id = kwargs['study_id']
        self.consent_dt = kwargs['consent_dt']

        if kwargs.get('credentials') is not None:
            self.data = self.__encrypt(kwargs['credentials'])
        else:
            self.data = None

        self.location_sid = kwargs.get('location_sid')
        self.search_sid = kwargs.get('search_sid')
        self.email = kwargs.get('email')
        self.first_name = kwargs.get('first_name')
        self.last_name = kwargs.get('last_name')

    def __repr__(self):
        return "<Consent(internal_id='%s', consentDateTime='%s')>" % (
            f'{self.internal_id}',
            self.consent_dt.strftime(secrets.DTFORMAT)
        )

    def __str__(self):
        return f'{self.internal_id}, {self.consent_dt.strftime(secrets.DTFORMAT)}'

    def __eq__(self, other):
        return self.internal_id == other.internal_id and self.study_id == other.study_id

    def __ne__(self, other):
        return not self.__eq__(other)

    def __gt__(self, other):
        return self.last_modified >= other.last_modified

    @property
    def credentials(self):
        """decrypt oauth credentials"""
        if self.data is None:
            raise ValueError('credentials do not exist for participant')

        s = cypher.decrypt(self.data)
        s = s.decode('utf-8')

        return json.loads(s)

    @property
    def dict(self):
        """a dict representation of the consent. credentials are not included"""
        return {
            'internal_id': self.internal_id,
            'study_id': self.study_id,
            'email': self.email,
            'first_name': self.first_name,
            'last_name': self.last_name,
            'consent_dt': self.consent_dt,
            'location_sid': self.location_sid,
            'search_sid': self.search_sid,
            'notes': [l.dict for l in sorted(self.logs)],
            'status': self.status
        }

    @hybrid_property
    def date(self):
        return self.consent_dt.date()

    @hybrid_property
    def hours_since_consent(self):
        return np.ceil((dt.datetime.now()-self.consent_dt).seconds/3600)

    @property
    def last_modified(self):
        """timestamp from latest log entry or consent submission if none exist"""
        latest = self.latest_archive_transactions(1)
        return latest[0].ts if latest is not None else self.consent_dt

    @staticmethod
    def __encrypt(data):
        """private method used to encrypt oauth contents"""
        s = json.dumps(data)
        s = s.encode('utf-8')

        msg = cypher.encrypt(s)

        return msg

    def add_search_error(self, msg=None, session=None):
        """add search data related error

        Notes:
            If the message contains 'not found' it refers to a Google drive failure to locate takeout data. In this case
            do not log as an error. Credentials will be cleared from the db and Synapse is updated

        Args:
            msg: (str) optional message to log
            session: (sqlalchemy.sessionmaker()) optional managed session with db. will be generated if not provided

        Returns:
            self
        """
        if 'not found' in msg:
            state = 'not found'
            failed = False
        else:
            state = 'err'
            failed = True

        if self.search_sid is not None and len(self.search_sid) > 0:
            self.search_sid += f', {state}'
        else:
            self.search_sid = state

        if msg is not None:
            add_log_entry(msg, cid=self.internal_id, session=session)

        if failed:
            self.set_status(ConsentStatus.FAILED)

        self.clear_credentials()
        self.update_synapse()
        return self

    def add_location_error(self, msg=None, session=None):
        """add location data related error

        Notes:
            If the message contains 'not found' it refers to a Google drive failure to locate takeout data. In this case
            do not log as an error. Credentials will be cleared from the db and Synapse is updated

        Args:
            msg: (str) optional message to log
            session: (sqlalchemy.sessionmaker()) optional managed session with db. will be generated if not provided

        Returns:
            self
        """
        if 'not found' in msg:
            state = 'not found'
            failed = False
        else:
            state = 'err'
            failed = True

        if self.location_sid is not None and len(self.location_sid) > 0:
            self.location_sid += f', {state}'
        else:
            self.location_sid = state

        if msg is not None:
            add_log_entry(msg, cid=self.internal_id, session=session)

        if failed:
            self.set_status(ConsentStatus.FAILED)

        self.clear_credentials()
        self.update_synapse()
        return self

    def clear_credentials(self):
        """delete credentials from the db and update synapse"""
        if self.data is None:
            return

        self.data = None

        session = inspect(self).session
        commit(session)

        add_log_entry(f'credentials cleared', self.internal_id)
        self.update_synapse()

    def latest_archive_transactions(self, n=-1):
        """get the latest n log messages for this consent, sorted by decreasing timestamp

        Args:
            n: number of logs to return

        Returns:
            [context.LogEntry,]
        """
        logs = sorted(self.logs, key=lambda x: x.ts, reverse=True)

        if logs is not None and len(logs) > 0:
            n = n if -1 < n <= len(logs) else len(logs)
            return logs[-n:]

        return None

    def mark_as_failure(self, s=None):
        """mark this consent as failed

        Notes: log message is submitted, status is set to failed, credentials are cleared, and Synapse is updated

        Args:
            s: (str) optional message for log entry

        Returns:
            None
        """
        self.set_status(ConsentStatus.FAILED)
        self.clear_credentials()

        if s is None:
            add_log_entry(
                f'Marked as failed. Google Drive not ready after {int(secrets.MAX_TIME_FOR_DRIVE_WAIT / 3600)} hours.',
                self.internal_id
            )
        else:
            add_log_entry(s, self.internal_id)

        self.update_synapse()

    def notes(self, n=-1):
        """build combined message of log entries

        Notes:
            Dates are only printed once per day (i.e. 18MAR2018 transaction1; transaction2, 19MARCH2018 transaction3, ..
            Return value is limited to 1000 characters in order to meet Synapse max table length. The most recent logs
            are kept truncating the oldest to ...

        Args:
            n: (int) optional. how many of the most recent logs to include in the message. defaults to all

        Returns:
            str
        """
        logs = self.latest_archive_transactions(n)

        if logs is not None and len(logs) > 0:
            msg, start = '', None

            for i, l in enumerate(logs[::-1]):
                if 'not ready' in l or len(msg) >= 996:
                    continue

                if i != 0:
                    msg += ';'

                date = l.ts.strftime('%d%b%Y').upper()
                if start != date:
                    start = date
                    msg += date

                msg += f' {l.ts.strftime("%H:%M:%S")} {str(l.msg)}'

            msg = msg if len(msg) <= 996 else msg[-996:] + '...'

            return msg
        else:
            return 'none'

    def notify_admins(self):
        """send email to admins with log messages

        Notes: sends to email addresses ADMIN_EMAILS defined in application config
        Returns:
            dict - AWS Simple Email Service response
        """
        try:
            x = dict(
                study_id=self.study_id,
                logs=[str(log) for log in sorted(self.logs, reverse=True)]
            )
            template = Template(secrets.PARTICIPANT_EMAIL_BODY)

            client = boto3.client('ses', 
                    aws_access_key_id=secrets.AWS_ACCESS_KEY_ID,
                    aws_secret_access_key=secrets.AWS_SECRET_ACCESS_KEY,
                    region_name=secrets.REGION_NAME)
            
            response = client.send_email(
                Source=secrets.FROM_STUDY_EMAIL,
                Destination={
                    'ToAddresses': secrets.ADMIN_EMAILS
                },
                Message={
                    'Subject': {
                        'Data': secrets.PARTICIPANT_EMAIL_SUBJECT,
                        'Charset': secrets.CHARSET
                    },
                    'Body': {
                        'Html': {
                            'Data': template.render(x=x),
                            'Charset': secrets.CHARSET
                        }
                    }
                },
                ReplyToAddresses=[secrets.FROM_STUDY_EMAIL]
            )
        except ClientError as e:
            raise Exception(f'email failed with <{str(e.response["Error"]["Message"])}>')
        else:
            return response

    def put_to_synapse(self):
        """generate a new row in Synapse table for this consent"""
        if self.internal_id is None:
            return

        data = [[
            self.study_id,
            self.internal_id,
            self.consent_dt.strftime(secrets.DTFORMAT).upper(),
            self.location_sid,
            self.search_sid,
            self.notes()
        ]]
        self.__syn_store(data)

    def seconds_since_last_drive_attempt(self):
        """seconds since the latest attempt to query Google Drive"
        
        Returns:
            int - seconds since latest attempt or maxint if no attempt has been made
        """""
        logs = sorted(self.logs, key=lambda x: x.ts)
        logs = [l for l in logs if 'Google Drive' in l.msg and 'not ready' in l.msg]

        if len(logs) > 0:
            return (dt.datetime.now(tz('utc')) - tz('utc').localize(logs[-1].ts)).total_seconds()
        else:
            return sys.maxsize

    def seconds_since_consent(self):
        return (dt.datetime.now(tz('utc')) - tz('utc').localize(self.consent_dt)).total_seconds()

    def set_location_sid(self, sid):
        """an object oriented way to set the location Synapse id

        Notes: use this rather setting the attribute directly in order to maintain lexicographic ordering if multiple
        sids have been uploaded to represent location data. The Synapse consents table is updated.

        Args:
            sid: (str) to merge with existing sids

        Returns:
            None
        """
        if self.location_sid is None:
            self.location_sid = str(StringArray(sid))
        else:
            self.location_sid = str(StringArray(self.search_sid).merge(sid))

        session = inspect(self).session
        commit(session)

        self.update_synapse()

    def set_search_sid(self, sid):
        """an object oriented way to set the search Synapse id

        Notes: use this rather setting the attribute directly in order to maintain lexicographic ordering if multiple
        sids have been uploaded to represent location data. The Synapse consents table is updated.

        Args:
            sid: (str) to merge with existing sids

        Returns:
            None
        """

        if self.search_sid is None:
            self.search_sid = str(StringArray(sid))
        else:
            self.search_sid = str(StringArray(self.search_sid).merge(sid))

        session = inspect(self).session
        commit(session)

        self.update_synapse()

    def set_status(self, status):
        """set the consent status and update the Synapse consents table"""
        self.status = status.value
        self.update_synapse()

    def __syn_store(self, data):
        """store data to Synapse

        Notes: Synapse frequently encounters SSL and other connection errors. This method will retry the push however
        many times are defined in the application config setting SYNAPSE_RETRIES. Sleeps three seconds between attempts

        Args:
            data: (dict) should match SYN_SCHEMA defined above

        Returns:
            None
        """
        retries = secrets.SYNAPSE_RETRIES

        while retries > 0:
            try:
                syn.store(Table(SYN_SCHEMA, data))
                retries = 0
            except SSLError:
                pass
            except SynapseHTTPError:
                pass
            except Exception as e:
                add_log_entry(f'consent failed to push to Synapse with <{str(e)}>', self.internal_id)
                retries = 0

            retries -= 1
            time.sleep(3)

    def update_synapse(self):
        """update an existing Synapse row for this consent

        Notes: will create a new row if a matching is not found

        Returns:
            None
        """
        results = syn.tableQuery(
            f"select * from {secrets.CONSENTS_SYNID} "
            f"where study_id='{self.study_id}'"
            f"  and internal_id='{self.internal_id}'"
        ).asDataFrame()

        if len(results) == 0:
            self.put_to_synapse()
        else:
            results['location_sid'] = self.location_sid
            results['search_sid'] = self.search_sid
            results['notes'] = self.notes()

            self.__syn_store(results)


class LogEntry(Base):
    """datatype used to represent a log entry"""
    __tablename__ = 'log'

    id = Column(Integer, autoincrement=True, primary_key=True)
    cid = Column(ForeignKey('consent.internal_id'), nullable=True)
    ts = Column(DateTime)
    msg = Column(String)

    def __init__(self, msg, cid=None):
        self.ts = dt.datetime.now(tz(secrets.TIMEZONE))
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
        """a pretty printed string representation of the timestamp"""
        return self.ts.strftime(secrets.DTFORMAT).upper()

    def __repr__(self):
        return f'<LogEntry(ts={self.ts_formatted}, msg={self.msg})>'

    def __str__(self):
        return f'{self.ts_formatted}: {self.msg}'

    def __eq__(self, other):
        return self.id == other.id or self.cid == other.cid and self.ts == other.ts and self.msg == other.msg

    def __ne__(self, other):
        return not self.__eq__(other)

    def __gt__(self, other):
        return self.ts > other.ts

    def __contains__(self, item):
        return item in self.msg

    
# ----------------------------------------------------------------------------------------------------------------------
# Database Context
# ----------------------------------------------------------------------------------------------------------------------
def get_engine(conn):
    """generate a DB engine as defined in the application config"""
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
    """ALWAYS us this to generate a managed session with the database

    Notes: DB may be left corrupted (SQLITE) or locked (POSTGRES) if a session is generated without this wrapper

    Yields:
        sqlalchemy.sessionmaker()

    Returns:
        None
    """
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
    """create the database defined by objects inheriting Base in this file"""
    engine = get_engine(conn)
    Base.metadata.create_all(engine)


def build_synapse_table():
    """build the table in Synapse to match the schema defined above"""
    table = Table(SYN_SCHEMA, values=[BLANK_CONSENT])
    table = syn.store(table)

    results = syn.tableQuery("select * from %s where study_id = '%s'" % (table.tableId, BLANK_CONSENT[0]))
    syn.delete(results)

    syn.setProvenance(
        entity=table.tableId,
        activity=synapseclient.Activity(
            name='Created',
            description='Table generated by gTap.'
        )
    )


# ----------------------------------------------------------------------------------------------------------------------
# Interactions
# ----------------------------------------------------------------------------------------------------------------------
def add_task(data, conn=None, session=None):
    """add a task for the archive manager to process

    Args:
        data: (dict) containing attributes required to initialize a new Consent object
        conn: (dict) optional DB connection paramaters
        session: (sqlalchemy.session_maker()) optional managed session with DB. Will be generated if not provided

    Returns:
        None
    """
    consent = Consent(**data)
    consent.set_status(ConsentStatus.READY)

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
    """add an object to the database"""
    if session is None:
        with session_scope(None) as s:
            s.add(entity)
            commit(s)
    else:
        session.add(entity)
        commit(session)

    return entity


def commit(s):
    """immediately commit pending transactions to the database"""
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
    """add a log entry to the database

    Args:
        entry: (LogEntry)
        cid: (int) - optional foreign key to Consent.internal_id
        session: (sqlalchemy.session_maker()) optional managed session with db. will be generated if not provided

    Returns:
        the updated entity
    """
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


def mark_as_permanently_failed(internal_id, session=None):
    """mark a consent as permanently failed

    Args:
        internal_id: (int) the consent internal_id to mark
        session: (sqlalchemy.session_maker()) optional managed session with db. will be generated if not provided

    Returns:
        None
    """
    if internal_id == np.nan:
        return

    def get_n_commit(session_):
        consent = session_.query(Consent).filter(
            Consent.internal_id == internal_id
        ).with_for_update().first()

        if consent is not None:
            consent.clear_credentials()
            consent.set_status(ConsentStatus.FAILED)
            commit(s)

    if session is None:
        with session_scope(None) as s:
            get_n_commit(s)
    else:
        get_n_commit(session)


def get_pending(conn=None, session=None):
    """get all consents waiting to be processed

    Args:
        conn: (dict) optional DB connection. will use application config if not provided
        session: (sqlalchemy.session_maker()) optional managed session with db. will be generated if not provided

    Returns:
        [Consent,]
    """
    if session is None:
        session = session_scope(conn)
        close = True
    else:
        close = False

    pending = sorted(session.query(Consent).filter(or_(
        Consent.status == ConsentStatus.READY.value,
        Consent.status == ConsentStatus.DRIVE_NOT_READY.value
    )).with_for_update().all(), reverse=True)

    ready = []

    def add_to_ready(p):
        p.set_status(ConsentStatus.PROCESSING)
        ready.append(p)

    while len(pending) > 0:
        p = pending.pop()

        if p.status == ConsentStatus.DRIVE_NOT_READY.value:
            if p.seconds_since_consent() > secrets.MAX_TIME_FOR_DRIVE_WAIT:
                p.mark_as_failure()

            elif p.seconds_since_last_drive_attempt() < secrets.WAIT_TIME_BETWEEN_DRIVE_NOT_READY:
                pass

            else:
                add_to_ready(p)
        else:
            add_to_ready(p)

    commit(session)
    if close:
        session.close()

    return ready


def get_consent(study_id, internal_id, session):
    """get the associated consent from the database"""
    c = session.query(Consent).filter(
        and_(Consent.study_id == study_id, Consent.internal_id == internal_id)
    ).first()

    return c


def daily_digest(conn=None):
    """generate the daily digest of consents processed

    Args:
        conn: (dict) optional DB connection. will use application config if not provided

    Returns:
        dict
    """
    today = dt.date.today()

    with session_scope(conn) as s:
        consents = s.query(Consent).filter(
            cast(Consent.consent_dt, Date) == today
        ).all()

        n = len(consents)
        n_searches = sum([
            1 for c in consents
            if c.search_sid is not None and
               'err' not in c.search_sid and
               'not found' not in c.search_sid
        ])
        n_locations = sum([
            1 for c in consents
            if c.location_sid is not None and
               'err' not in c.location_sid and
               'not found' not in c.location_sid
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
    # create_database(secrets.DATABASE)
    # build_synapse_table()
    pass
