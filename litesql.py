from collections.abc import MutableMapping
import json
from typing import Callable
import sqlite3


class SQLCounter:
    def __init__(
        self, dbname, check_same_thread=False, fast=True, **kwargs,
    ):
        self.dbname = dbname
        self.conn = sqlite3.connect(
            self.dbname, check_same_thread=check_same_thread, **kwargs
        )

        with self.conn as c:

            c.execute(
                "CREATE TABLE IF NOT EXISTS Counter (key text NOT NULL PRIMARY KEY, value integer)"
            )

            if fast:
                c.execute("PRAGMA journal_mode = 'WAL';")
                c.execute("PRAGMA temp_store = 2;")
                c.execute("PRAGMA synchronous = 1;")
                c.execute(f"PRAGMA cache_size = {-1 * 64_000};")

    def incr(self, key):
        with self.conn as c:
            c.execute(
                "INSERT INTO Counter VALUES (?, 1) ON CONFLICT(key) DO UPDATE SET value = value + 1",
                (key,),
            )
        return

    def decr(self, key):
        with self.conn as c:
            c.execute(
                "INSERT INTO Counter VALUES (?, -1) ON CONFLICT(key) DO UPDATE SET value = value - 1",
                (key,),
            )
        return

    def count(self, key):
        c = self.conn.execute("SELECT value FROM Counter WHERE Key=?", (key,))
        row = c.fetchone()
        if row is None:
            return 0
        return row[0]

    def zero(self, key):
        with self.conn as c:
            c.execute(
                "INSERT INTO Counter VALUES (?, 0) ON CONFLICT(key) DO UPDATE SET value = 0",
                (key,),
            )
        return

    def delete(self, key):
        # TODO
        raise NotImplementedError
        # with self.conn as c:
        #     c.execute(
        #         "INSERT INTO Counter VALUES (?, 1) ON CONFLICT(key) DO UPDATE SET value = value + 1",
        #         (key,),
        #     )
        # return

    def vacuum(self):
        with self.conn as c:
            c.execute("VACUUM;")

    def close(self):
        self.conn.close()


class SQLDict(MutableMapping):
    def __init__(
        self,
        dbname,
        check_same_thread=False,
        fast=True,
        encoder: Callable = lambda x: json.dumps(x),
        decoder: Callable = lambda x: json.loads(x),
        **kwargs,
    ):
        self.dbname = dbname

        self.conn = sqlite3.connect(
            self.dbname, check_same_thread=check_same_thread, **kwargs
        )
        self.encoder = encoder
        self.decoder = decoder
        # c = self.conn.cursor()

        # with suppress(sqlite3.OperationalError):

        with self.conn as c:
            # WITHOUT ROWID?
            c.execute(
                "CREATE TABLE IF NOT EXISTS Dict (key text NOT NULL PRIMARY KEY, value text)"
            )

            c.execute(
                "CREATE TABLE IF NOT EXISTS Counter (key text NOT NULL PRIMARY KEY, value integer)"
            )

            if fast:
                c.execute("PRAGMA journal_mode = 'WAL';")
                c.execute("PRAGMA temp_store = 2;")
                c.execute("PRAGMA synchronous = 1;")
                c.execute(f"PRAGMA cache_size = {-1 * 64_000};")

    def __setitem__(self, key, value):
        with self.conn as c:
            c.execute(
                "INSERT OR REPLACE INTO  Dict VALUES (?, ?)", (key, self.encoder(value))
            )

    def __getitem__(self, key):
        c = self.conn.execute("SELECT value FROM Dict WHERE Key=?", (key,))
        row = c.fetchone()
        if row is None:
            raise KeyError(key)
        return self.decoder(row[0])

    def __delitem__(self, key):
        if key not in self:
            raise KeyError(key)
        with self.conn as c:
            c.execute("DELETE FROM Dict WHERE key=?", (key,))

    def __len__(self):
        return next(self.conn.execute("SELECT COUNT(*) FROM Dict"))[0]

    def __iter__(self):
        c = self.conn.execute("SELECT key FROM Dict")
        return map(itemgetter(0), c.fetchall())

    def __repr__(self):
        return (
            f"{type(self).__name__}(dbname={self.dbname!r}, items={list(self.items())})"
        )

    def glob(self, pat: str):
        c = self.conn.execute("SELECT value FROM Dict WHERE Key GOLB ?", (pat,))
        row = c.fetchone()
        if row is None:
            raise KeyError(pat)
        return self.decoder(row[0])

    def incr(self, key):
        with self.conn as c:
            c.execute(
                "INSERT INTO Counter VALUES (?, 1) ON CONFLICT(key) DO UPDATE SET value = value + 1",
                (key,),
            )
        return

    def decr(self, key):
        with self.conn as c:
            c.execute(
                "INSERT INTO Counter VALUES (?, -1) ON CONFLICT(key) DO UPDATE SET value = value - 1",
                (key,),
            )
        return

    def count(self, key):
        c = self.conn.execute("SELECT value FROM Counter WHERE Key=?", (key,))
        row = c.fetchone()
        if row is None:
            return 0
            # raise KeyError(key)
        return row[0]

    def resetcount(self, key):
        with self.conn as c:
            c.execute(
                "INSERT INTO Counter VALUES (?, 0) ON CONFLICT(key) DO UPDATE SET value = 0",
                (key,),
            )
        return

    def vacuum(self):
        with self.conn as c:
            c.execute("VACUUM;")

    def close(self):
        self.conn.close()


class SQLQueue:
    def __init__(
        self, dbname, maxsize: int = None, check_same_thread=False, fast=True, **kwargs,
    ):
        self.dbname = dbname
        self.conn = sqlite3.connect(
            self.dbname, check_same_thread=check_same_thread, **kwargs
        )

        with self.conn as c:
            # int == bool in SQLite
            # will have rowid as primary key by default
            c.execute(
                "CREATE TABLE IF NOT EXISTS Queue (message TEXT NOT NULL, locked TEXT, done INTEGER, in_time INTEGER NOT NULL, lock_time INTEGER, out_time INTEGER)"
            )

            if fast:
                c.execute("PRAGMA journal_mode = 'WAL';")
                c.execute("PRAGMA temp_store = 2;")
                c.execute("PRAGMA synchronous = 1;")
                c.execute(f"PRAGMA cache_size = {-1 * 64_000};")

            if maxsize is not None:
                raise NotImplementedError
                # c.execute("CREATE TRIGGER ... ON INSERT ... DELETE")

    def put(self, message: str, timeout: int = None):
        with self.conn as c:
            rid = c.execute(
                r"INSERT INTO Queue VALUES (:message, NULL, 0, strftime('%s','now'), NULL, NULL)",
                {"message": message},
            ).lastrowid
        return rid

    def pop(self):
        with self.conn as c:
            message = c.executescript(
                """
with randhash as (select lower(hex(randomblob(16))) as hash)

-- LOCK
UPDATE Queue
SET locked = (SELECT hash FROM randhash)
WHERE rowid = (SELECT rowid FROM Queue
               WHERE locked IS NULL
               ORDER BY rowid LIMIT 1);

SELECT message FROM Queue WHERE locked = (SELECT hash FROM randhash);
""".strip()
            )

        return message

    def peek(self):
        value = self.conn.execute(
            "SELECT * FROM Queue WHERE done = 0 LIMIT 1"
        ).fetchone()
        return value

    def qsize(self):
        return next(self.conn.execute("SELECT COUNT(*) FROM Queue"))[0]

    def empty(self) -> bool:
        value = self.conn.execute(
            """
SELECT
CASE WHEN
    SELECT COUNT(*) FROM Queue WHERE done = 0
-- empty
        WHEN 0 THEN 1
-- not empy
        ELSE 0
    END
        """
        ).fetchone()

        return bool(value)

    def full(self) -> bool:
        return False

    def close(self):
        self.conn.close()
