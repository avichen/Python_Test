'''
Created on 2012-1-19

@author: Eric
'''
import mysql.connector
import itertools
import time
import logging

class Connection(object):
    '''
    classdocs
    '''
    def __init__(self, host, database, user=None, password=None, max_idle_time=7*3600):
        '''
        Constructor
        '''
        self.host = host
        self.database = database
        self.max_idle_time = max_idle_time
        
        args = dict(use_unicode="Ture", charset="utf8", db=database, 
        )

        if user is not None:
            args["user"] = user
        if password is not None:
            args["passwd"] = password

        pair = host.split(":")
        if len(pair) == 2:
            args["host"] = pair[0]
            args["port"] = int(pair[1])
        else:
            args["host"] = host
            args["port"] = 3306

        self._db = None
        self._db_args = args
        self._last_use_time = time.time()
        
        try:
            self.reconnection()
        except Exception:
            logging.error("Cannot connection to the MySQL Server on %s", host, exc_info=True)

    def close(self):
        """Close the DB connection"""
        if getattr(self, "_db", None) is not None:
            self._db.close()
            self._db = None

    def reconnection(self):
        """Close the connection and reconnection the DB"""
#       self.close()
        self._db = None
        self._db = mysql.connector.connect(**self._db_args)
#        print "========"+self._db.get_charset()
#        self._db.set_autocommit("ON")

    def _ensure_connection(self):
        """ensure the connection """
        if (self._db is None or
            (time.time() - self._last_use_time > self.max_idle_time)):
            self._last_use_time = time.time()

    def _cursor(self):
        self._ensure_connection()
        return self._db.cursor()

    def _execute(self, cursor, strsql):
        try:
            return cursor.execute(strsql)
        except Exception:
            logging.error("Error connecting to MySQL on %s", self.host)
            self.close()
            raise
    
    def _executemany(self, cursor, strsql):
        try:
            return cursor.executemany(strsql)
        except Exception:
            logging.error("Error connecting to MySQL on %s", self.host)
            self.close()
            raise

    def query(self, query):
        """query the result by query sql"""
        cursor = self._cursor()
        try:
            self._execute(cursor, query)
            column_names = [d[0] for d in cursor.description]
#            for row in cursor:
#                print cursor[row]
            return [Row(itertools.izip(column_names, row)) for row in cursor]
        finally:
            cursor.close()
            
    def querycount(self,query):
        """get the count of the query"""
        cursor = self._cursor()
        try:
            self._execut(cursor, query)
            return cursor.rowcount
        finally:
            cursor.close()
            
    def get(self,get):
        """get the first row from the query result"""
        rows = self.query(get)
        if not rows:
            return None
        elif len(rows) > 1:
            raise Exception("Multiple rows returned for Database.get() query")
        else:
            return rows[0]
            
    def insert(self, insert ,autocommit=True):
        """insert the data by insert sql"""
        cursor = self._cursor()
        try:
            self._execute(cursor, insert)
            if autocommit:
                self._db.commit()
            return cursor.lastrowid
        except Exception:
            logging.error("insert error")
            self.close()
            raise
        finally:
            cursor.close()
            
    def update(self, update, autocommit=True):
        """update the data by update sql"""
        cursor = self._cursor()
        
            
    def delete(self, delete, autocommit=True):
        """delete the date by delete sql"""
        cursor = self._cursor()
        try:
            self._execute(cursor, delete)
            if autocommit:
                self._db.commit()
        except Exception:
#            self._db.rollback()
            logging.error("delete error")
        finally:
            cursor.close()
        


class Row(dict):
    """A dict that allows for object-like property access syntax."""
    def __getattr__(self, name):
        try:
            return self[name]
        except KeyError:
            raise AttributeError(name)
