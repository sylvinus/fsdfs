import MySQLdb,os

from filedb import FileDbBase

class mysqlFileDb(FileDbBase):
	"""
	FileDb class for MySQL
	
	"""
	
	def __init__(self, fs, options={}):
		FileDbBase.__init__(self, fs, options)
		
		self.conn = MySQLdb.connect(options["host"],options["user"],options["passwd"],options["db"], use_unicode=1) #, init_command="set names utf8")
		
		prefix = options.get("prefix",fs.config["host"].replace(":","_").replace(".","_"))
		
		self.t_files = prefix+"_files"
		self.t_nodes = prefix+"_nodes"
		self.t_files_nodes = prefix+"_files_nodes"
		

		# change the row output by dictionnary
		# result is now like:
		# [{'row1': value1, 'row2': value2},
		#  {'row1': value1-2, 'row2': value2-2}]
		self.cursor = self.conn.cursor(MySQLdb.cursors.DictCursor)
		
		
		from warnings import filterwarnings,resetwarnings
		filterwarnings( 'ignore', category = MySQLdb.Warning )

		self.cursor.execute("""CREATE TABLE IF NOT EXISTS `"""+self.t_files+"""` (
          `id` int(11) NOT NULL AUTO_INCREMENT,
          `filename` varchar(255) CHARACTER SET utf8 DEFAULT NULL,
          `size` int(11) DEFAULT NULL,
          `t` timestamp NULL DEFAULT NULL,
          `n` int(11) DEFAULT NULL,
          `nuked` tinyint(1) NOT NULL DEFAULT '0',
          PRIMARY KEY (`id`),
          UNIQUE KEY `filename` (`filename`),
          KEY `nuked` (`nuked`)
        );""")
		
		self.cursor.execute("""CREATE TABLE IF NOT EXISTS `"""+self.t_files_nodes+"""` (
		  `file_id` int(11) NOT NULL DEFAULT '0',
		  `node_id` int(11) NOT NULL,
		  PRIMARY KEY (`file_id`,`node_id`),
		  KEY `node` (`node_id`)
		);""")
		
		self.cursor.execute("""CREATE TABLE IF NOT EXISTS `"""+self.t_nodes+"""` (
		  `id` int(11) NOT NULL AUTO_INCREMENT,
		  `address` varchar(255) NOT NULL,
		  PRIMARY KEY (`id`),
		  KEY `address` (`address`)
		);""")
		
		resetwarnings()


	def reset(self):
		self.cursor.execute("""TRUNCATE """+self.t_files_nodes)
		self.cursor.execute("""TRUNCATE """+self.t_files)
		self.cursor.execute("""TRUNCATE """+self.t_nodes)
		
		
	def _getFileId(self,filename):
		self.cursor.execute("""SELECT id FROM """+self.t_files+""" WHERE filename=%s LIMIT 1""", (filename,))
		result = self.cursor.fetchone()
		
		if result:
			return int(result['id'])
		else:
			self.cursor.execute("""INSERT INTO """+self.t_files+"""(filename) VALUES (%s)""", (filename,))
			return self._getFileId(filename)
		
	def _getNodeId(self,node):
		self.cursor.execute("""SELECT id FROM """+self.t_nodes+""" WHERE address=%s LIMIT 1""", (node,))
		result = self.cursor.fetchone()
		
		if result:
			return result['id']
		else:
			self.cursor.execute("""INSERT INTO """+self.t_nodes+"""(address) VALUES (%s)""", (node,))
			return self._getNodeId(node)

	

	def update(self, file, data):
	    
	
		file_id = self._getFileId(file)
	
		if "nuked" in data:
			if data["nuked"]:
				data["nuked"]=1
			else:
				data["nuked"]=0
	
		
		
		arg_list = []
		req_str=[]

		for key, value in data.iteritems():
			
			if key!="nodes":
				if key=="t":
					req_str.append(key+"""=FROM_UNIXTIME(%s) """)
				else:
					req_str.append(key+"""=%s """)
				arg_list.append(value)
		arg_list.append(file_id)
		
		if len(req_str):
			self.cursor.execute("""UPDATE """+self.t_files+""" SET """+(','.join(req_str))+""" WHERE id=%s""",tuple(arg_list))
		
	
		if "nodes" in data:
			self.cursor.execute("""DELETE FROM """+self.t_files_nodes+""" WHERE file_id=%s""", (file_id,))
			for node in data["nodes"]:
				self.addFileToNode(file,node)

			
	def getKn(self, file):
		self.cursor.execute("""SELECT id,n FROM """+self.t_files+""" WHERE filename=%s LIMIT 1""", (file,))
		result = self.cursor.fetchone()
		
		self.cursor.execute("""SELECT count(*) as c FROM """+self.t_files_nodes+""" WHERE file_id=%s LIMIT 1""", (result['id']))
		nodes = self.cursor.fetchone()
        
		
		if len(result):
			return int(nodes['c']) - result['n']
		else:
			return None
	
	def addFileToNode(self, file, node):

		#todo unique key
		self.removeFileFromNode(file,node)
		
		file_id = self._getFileId(file)
		node_id = self._getNodeId(node)
		
		self.cursor.execute("""INSERT INTO """+self.t_files_nodes+"""(file_id,node_id) VALUES (%s,%s)""", (file_id,node_id))
		
		
	def removeFileFromNode(self, file, node):
		file_id = self._getFileId(file)
		node_id = self._getNodeId(node)
		self.cursor.execute("""DELETE FROM """+self.t_files_nodes+""" WHERE file_id=%s and node_id=%s""", (file_id,node_id))
		
	def getNodes(self, file):
		file_id = self._getFileId(file)
		self.cursor.execute("""SELECT """+self.t_nodes+""".address FROM """+self.t_files_nodes+""","""+self.t_nodes+""" WHERE """+self.t_nodes+""".id="""+self.t_files_nodes+""".node_id AND """+self.t_files_nodes+""".file_id=%s""", (file_id,))
		result = self.cursor.fetchall()

		return set([ i['address'] for i in result ])
	
	def getSize(self, file):
		self.cursor.execute("""SELECT size FROM """+self.t_files+""" WHERE filename=%s LIMIT 1""", (file,))
		result = self.cursor.fetchone()

		if result:
			return result['size']
		else:
			return None
			
	def listAll(self):
		self.cursor.execute("""SELECT filename FROM """+self.t_files+""" WHERE nuked=0 """, ())
		result = self.cursor.fetchall()

		return [ i['filename'] for i in result ]
	
	def listNukes(self):
		self.cursor.execute("""SELECT filename FROM """+self.t_files+""" WHERE nuked=1 """, ())
		result = self.cursor.fetchall()

		return [ i['filename'] for i in result ]
		
	def listInNode(self, node):
		
		node_id = self._getNodeId(node)
		self.cursor.execute("""SELECT """+self.t_files+""".filename FROM """+self.t_files_nodes+""","""+self.t_files+""" WHERE """+self.t_files_nodes+""".file_id="""+self.t_files+""".id AND """+self.t_files_nodes+""".node_id=%s""", (node_id,))
		result = self.cursor.fetchall()
		
		return [ i['filename'] for i in result ]
		