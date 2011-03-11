import MySQLdb,os

from filedb.sql import sqlFileDb

from threading import Lock

class mysqlFileDb(sqlFileDb):
    """
    FileDb class for MySQL
    
    """
    
    unixtimefunction = "FROM_UNIXTIME"
    
    def connect(self):
        self.conn = MySQLdb.connect(self.options["host"],self.options["user"],self.options["passwd"],self.options["db"], use_unicode=1) #, init_command="set names utf8")

        # change the row output by dictionnary
        # result is now like:
        # [{'row1': value1, 'row2': value2},
        #  {'row1': value1-2, 'row2': value2-2}]
        self.cursor = self.conn.cursor(MySQLdb.cursors.DictCursor)
        
    def execute(self,sql,*args):
        self.sqlLock.acquire()
        
        ret = None
        try:
            self.cursor.execute(sql,*args)        
            ret = self.cursor.fetchall()
        finally:
            self.sqlLock.release()
            
        return ret

        
    def __init__(self, fs, options={}):
        sqlFileDb.__init__(self, fs, options)
        
        self.options = options
        
        self.connect()
        
        self.sqlLock = Lock()
        
        prefix = options.get("prefix",fs.config["host"].replace(":","_").replace(".","_"))
        
        self.t_files = prefix+"_files"
        self.t_nodes = prefix+"_nodes"
        self.t_files_nodes = prefix+"_files_nodes"
        

        
        from warnings import filterwarnings,resetwarnings
        filterwarnings( 'ignore', category = MySQLdb.Warning )

        self.execute("""CREATE TABLE IF NOT EXISTS `"""+self.t_files+"""` (
          `id` int(11) NOT NULL AUTO_INCREMENT,
          `filename` varchar(255) CHARACTER SET utf8 DEFAULT NULL,
          `size` int(11) DEFAULT NULL,
          `t` timestamp NULL DEFAULT NULL,
          `n` float DEFAULT NULL,
          `kn` float DEFAULT NULL,
          `nuked` tinyint(1) NOT NULL DEFAULT '0',
          PRIMARY KEY (`id`),
          UNIQUE KEY `filename` (`filename`),
          KEY `nuked` (`nuked`)
        );""")
        
        self.execute("""CREATE TABLE IF NOT EXISTS `"""+self.t_files_nodes+"""` (
          `file_id` int(11) NOT NULL DEFAULT '0',
          `node_id` int(11) NOT NULL,
          PRIMARY KEY (`file_id`,`node_id`),
          KEY `node` (`node_id`)
        );""")
        
        self.execute("""CREATE TABLE IF NOT EXISTS `"""+self.t_nodes+"""` (
          `id` int(11) NOT NULL AUTO_INCREMENT,
          `address` varchar(255) NOT NULL,
          PRIMARY KEY (`id`),
          UNIQUE KEY `address` (`address`)
        );""")
        
        resetwarnings()


    def reset(self):
        self.execute("""TRUNCATE """+self.t_files_nodes)
        self.execute("""TRUNCATE """+self.t_files)
        self.execute("""TRUNCATE """+self.t_nodes)
        