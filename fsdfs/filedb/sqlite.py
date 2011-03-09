import sqlite3

from filedb.sql import sqlFileDb


# http://code.activestate.com/recipes/496799-run-sqlite-connections-in-isolated-threads/
import Queue, time, thread, os
from threading import Thread

_threadex = thread.allocate_lock()
qthreads = 0
sqlqueue = Queue.Queue()

ConnectCmd = "connect"
SqlCmd = "SQL"
StopCmd = "stop"

class DbCmd:
    def __init__(self, cmd, params=[]):
        self.cmd = cmd
        self.params = params

class DbWrapper(Thread):
    def __init__(self, path, nr):
        Thread.__init__(self)
        self.path = path
        self.nr = nr
    def run(self):
        global qthreads
        con = sqlite3.connect(self.path)
        con.row_factory = dict_factory
        cur = con.cursor()
        while True:
            s = sqlqueue.get()
            print "Conn %d -> %s -> %s" % (self.nr, s.cmd, s.params)
            if s.cmd == SqlCmd:
                commitneeded = False
                res = []
#               s.params is a list to bundle statements into a "transaction"
                for sql in s.params:
                    cur.execute(sql[0],sql[1])
                    if not sql[0].upper().startswith("SELECT"):
                        commitneeded = True
                    for row in cur.fetchall(): res.append(row)
                if commitneeded: 
                    # print "commit needed for "+sql[0]
                    con.commit()
                s.resultqueue.put(res)
            else:
                _threadex.acquire()
                qthreads -= 1
                _threadex.release()
#               allow other threads to stop
                sqlqueue.put(s)
                s.resultqueue.put(None)
                break

def execSQL(s):
    if s.cmd == ConnectCmd:
        global qthreads
        _threadex.acquire()
        qthreads += 1
        _threadex.release()
        wrap = DbWrapper(s.params, qthreads)
        wrap.start()
    elif s.cmd == StopCmd:
        s.resultqueue = Queue.Queue()
        sqlqueue.put(s)
#       sleep until all threads are stopped
        while qthreads > 0: time.sleep(0.1)
    else:
        s.resultqueue = Queue.Queue()
        sqlqueue.put(s)
        return s.resultqueue.get()





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
        sql = sql.replace("""%s""","?")
        
        execSQL(DbCmd(SqlCmd,[(sql,args)]))
    
    def connect(self):
        
        self.dbdir = os.path.join(self.fs.config["datadir"],".fsdfs")
        if not os.path.isdir(self.dbdir):
            os.makedirs(self.dbdir)
        execSQL(DbCmd(ConnectCmd, os.path.join(self.dbdir,"filedb.sqlite")))
    
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
          nuked INTEGER,
          UNIQUE (filename)
        );""")
        
        self.execute("""CREATE INDEX IF NOT EXISTS i_nuked ON """+self.t_files+"""(nuked)""")
        
        self.execute("""CREATE TABLE IF NOT EXISTS """+self.t_files_nodes+""" (
          file_id INTEGER,
          node_id INTEGER,
          PRIMARY KEY (file_id,node_id)
        );""")
        
        self.execute("""CREATE INDEX IF NOT EXISTS i_node ON """+self.t_files_nodes+"""(node_id)""")
        
        
        self.execute("""CREATE TABLE IF NOT EXISTS """+self.t_nodes+""" (
          id INTEGER PRIMARY KEY,
          address TEXT,
          UNIQUE (address)
        );""")
      
    
    def reset(self):
        self.execute("""DELETE FROM """+self.t_files_nodes)
        self.execute("""DELETE FROM """+self.t_files)
        self.execute("""DELETE FROM """+self.t_nodes)
        