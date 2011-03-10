
try:
    from pysqlite2 import dbapi2 as sqlite3
except:
    import sqlite3
    
if sqlite3.version_info[0]<2 and sqlite3.version_info[1]<6:
    import sys
    print "pysqlite/sqlite3 version 2.6+ is required for fsdfs"
    sys.exit(0)

from filedb.sql import sqlFileDb

import os,sys

from threading import Lock

def dict_factory(cursor, row):
    d = {}
    for idx, col in enumerate(cursor.description):
        d[col[0]] = row[idx]
    return d

class sqliteFileDb(sqlFileDb):
    """
    FileDb class for sqlite
    
    """
    
    unixtimefunction = ""
    
    
    def _getFileId(self,filename):
        result = self.execute("""SELECT id FROM """+self.t_files+""" WHERE filename=%s LIMIT 1""", (filename,))
        
        if result:
            return int(result[0]['id'])
        else:
            self.execute("""INSERT OR IGNORE INTO """+self.t_files+"""(filename) VALUES (%s)""", (filename,))
            return self._getFileId(filename)
        
    def _getNodeId(self,node):
        result = self.execute("""SELECT id FROM """+self.t_nodes+""" WHERE address=%s LIMIT 1""", (node,))
        if result:
            return result[0]['id']
        else:
            self.execute("""INSERT OR IGNORE INTO """+self.t_nodes+"""(address) VALUES (%s)""", (node,))
            return self._getNodeId(node)
    
    
    def execute(self,sql,args=tuple()):
        #print "%s - %s %s" % (self.fs.host,sql,args)
        
        sql = sql.replace("""%s""","?")
        
        self.sqlLock.acquire()
        
        ret = None
        try:
            self.cur.execute(sql,args)
            ret = self.cur.fetchall()
        finally:
            self.sqlLock.release()
        
        return ret
        
    def connect(self):
        
        self.dbdir = os.path.join(self.fs.config["datadir"],".fsdfs")
        if not os.path.isdir(self.dbdir):
            os.makedirs(self.dbdir)

        self.con = sqlite3.connect(os.path.join(self.dbdir,"filedb.sqlite"),check_same_thread=False,isolation_level=None,timeout=5) #,
        self.con.row_factory = dict_factory
        self.cur = self.con.cursor()
    
    def __init__(self, fs, options={}):
        sqlFileDb.__init__(self, fs, options)
        
        self.options = options
        
        self.sqlLock = Lock()
        
        self.connect()
        
        self.t_files = "files"
        self.t_nodes = "nodes"
        self.t_files_nodes = "files_nodes"
        
        self.execute("""CREATE TABLE IF NOT EXISTS """+self.t_files+""" (
          id INTEGER PRIMARY KEY,
          filename TEXT,
          size INTEGER,
          t INTEGER,
          n INTEGER,
          nuked INTEGER
        );""")
        
        self.execute("""CREATE UNIQUE INDEX IF NOT EXISTS i_filename ON """+self.t_files+"""(filename)""")
        self.execute("""CREATE INDEX IF NOT EXISTS i_nuked ON """+self.t_files+"""(nuked)""")
        
        self.execute("""CREATE TABLE IF NOT EXISTS """+self.t_files_nodes+""" (
          file_id INTEGER,
          node_id INTEGER,
          PRIMARY KEY (file_id,node_id)
        );""")
        
        self.execute("""CREATE INDEX IF NOT EXISTS i_node ON """+self.t_files_nodes+"""(node_id)""")
        
        
        self.execute("""CREATE TABLE IF NOT EXISTS """+self.t_nodes+""" (
          id INTEGER PRIMARY KEY,
          address TEXT
        );""")
      
        self.execute("""CREATE UNIQUE INDEX IF NOT EXISTS i_address ON """+self.t_nodes+"""(address)""")
        
    
    def reset(self):
        self.execute("""DELETE FROM """+self.t_files_nodes)
        self.execute("""DELETE FROM """+self.t_files)
        self.execute("""DELETE FROM """+self.t_nodes)
        