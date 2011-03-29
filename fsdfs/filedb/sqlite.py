
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
    
    # save some writes! Or too much locking happening
    select_before_update = True
    
    fileidcache = {}
    nodeidcache = {}
    
    def _getFileId(self,filename, insert=True):
        
        if filename in self.fileidcache:
            return self.fileidcache[filename]
        
        result = self.execute("""SELECT id FROM """+self.t_files+""" WHERE filename=%s LIMIT 1""", (filename,))
        
        if result:
            self.fileidcache[filename] = int(result[0]['id'])
            return int(result[0]['id'])
        elif not insert:
            return None
        else:
            self.execute("""INSERT IGNORE INTO """+self.t_files+"""(filename) VALUES (%s)""", (filename,))
            return self._getFileId(filename)
        
    def _getNodeId(self,node, insert=True):
        #if node in self.nodeidcache:
        #    print self.nodeidcache,node,self.nodeidcache[node]
        #    return self.nodeidcache[node]
            
        
        result = self.execute("""SELECT id FROM """+self.t_nodes+""" WHERE address=%s LIMIT 1""", (node,))


        if result:
            self.nodeidcache[node] = int(result[0]['id'])
            return result[0]['id']
        elif not insert:
            return None
        else:
            self.execute("""INSERT IGNORE INTO """+self.t_nodes+"""(address) VALUES (%s)""", (node,))
            return self._getNodeId(node)
    
    
    def execute(self,sql,args=tuple()):
        #print "%s - %s %s" % (self.fs.host,sql,args)
        
        sql = sql.replace("""%s""","?")
        if sql[0:14]=="INSERT IGNORE ":
            sql = "INSERT OR IGNORE "+sql[14:]
        
        self.sqlLock.acquire()
        
        ret = None
        try:
            ret = self._execute(sql,args)
        finally:
            self.sqlLock.release()
        
        return ret
        
    def _execute(self,sql,args):
        self.cur.execute(sql,args)
        return self.cur.fetchall()
        
    def connect(self):
        
        self.dbdir = os.path.join(self.fs.config["datadir"],".fsdfs")
        if not os.path.isdir(self.dbdir):
            os.makedirs(self.dbdir)

        self.con = sqlite3.connect(os.path.join(self.dbdir,"filedb_v2.sqlite"),check_same_thread=False,timeout=5,isolation_level=None) # 
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
          n REAL,
          kn REAL,
          nuked INTEGER
        );""")
        
        self.execute("""CREATE UNIQUE INDEX IF NOT EXISTS i_filename ON """+self.t_files+"""(filename)""")
        self.execute("""CREATE INDEX IF NOT EXISTS i_nuked ON """+self.t_files+"""(nuked)""")
        self.execute("""CREATE INDEX IF NOT EXISTS i_kn ON """+self.t_files+"""(kn)""")
        
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
        