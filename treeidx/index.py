import datetime
import sqlite3


_migrations = []

def _migration(f):
    _migrations.append(f)

@_migration
def _create_initial_tables(con):
    con.execute('''CREATE TABLE scans (
        id INTEGER PRIMARY KEY,
        started_at TIMESTAMP NOT NULL,
        hostname TEXT NOT NULL,
        root TEXT NOT NULL
    )''')
    con.execute('''CREATE TABLE directories (
        id INTEGER PRIMARY KEY,
        path TEXT UNIQUE NOT NULL
    )''')
    con.execute('''CREATE TABLE files (
        scan_id INTEGER REFERENCES scans(id) NOT NULL,
        dir_id INTEGER REFERENCES directories(id) NOT NULL,
        name TEXT NOT NULL,
        size INT NOT NULL,
        mtime INT NOT NULL,
        ctime INT NOT NULL,
        checksum BLOB NOT NULL,
        CONSTRAINT dir_and_name PRIMARY KEY (dir_id, name)
    )''')

def _migrate(con):
    with con:
        con.execute('''CREATE TABLE IF NOT EXISTS migrations (
            name TEXT NOT NULL,
            applied_at TIMESTAMP NOT NULL
        )''')
        cur = con.execute('SELECT name FROM migrations')
        existing = set(row[0] for row in cur)
    for f in _migrations:
        name = f.__name__.strip('_')
        if name not in existing:
            with con.begin():
                f(con)
                con.execute('INSERT INTO migrations VALUES (?, ?)', (name, datetime.datetime.utcnow()))



class _Connection(sqlite3.Connection):
    
    def __init__(self, *args, **kwargs):
        super(_Connection, self).__init__(*args, **kwargs)
        self.row_factory = sqlite3.Row

    def cursor(self):
        return super(_Connection, self).cursor(_Cursor)

    def begin(self):
        self.execute("BEGIN")
        return self


class _Cursor(sqlite3.Cursor):
    pass


class Index(object):

    def __init__(self, path):
        self.path = path
        _migrate(self.connect())

    def connect(self):
        return sqlite3.connect(self.path, factory=_Connection, isolation_level=None)

    def cursor(self):
        yield self.connect().cursor()


