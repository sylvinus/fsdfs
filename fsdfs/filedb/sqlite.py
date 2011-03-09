
try:
    from pysqlite2 import dbapi2 as sqlite3
except:
    import sqlite3
    
if sqlite3.version_info[0]<2 and sqlite3.version_info[1]<6:
    import sys
    print "pysqlite/sqlite3 version 2.6+ is required for fsdfs"
    sys.exit(0)

from filedb.sql import sqlFileDb


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
    
    def execute(self,sql,args=tuple()):
        #print "%s - %s %s" % (self.fs.host,sql,args)
        
        sql = sql.replace("""%s""","?")
        
        cur = self.con.cursor()
    
        cur.execute(sql,args)
        
        return cur.fetchall()
        
    def connect(self):
        
        self.dbdir = os.path.join(self.fs.config["datadir"],".fsdfs")
        if not os.path.isdir(self.dbdir):
            os.makedirs(self.dbdir)

        self.con = sqlite3.connect(os.path.join(self.dbdir,"filedb.sqlite"),isolation_level=None,check_same_thread=False)
        self.con.row_factory = dict_factory
    
    def __init__(self, fs, options={}):
        sqlFileDb.__init__(self, fs, options)
        
        self.options = options
        
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
        