import sqlite3,os

from filedb import FileDbBase

def dict_factory(cursor, row):
	d = {}
	for idx, col in enumerate(cursor.description):
		d[col[0]] = row[idx]
	return d

class sqliteFileDb(FileDbBase):
	'''
	FileDb class for SQLite

	Table files has rows:
	* filename (text),
	* size (integer),
	* date (text),
	* n (integer)
	
	Table files_nodes has rows:
	* filename (text),
	* node (text),
    
	
	'''
	
	def __init__(self, fs, options):
		FileDbBase.__init__(self, fs, options)
		
		self.dbdir = os.path.join(self.fs.config["datadir"],".fsdfs")
		if not os.path.isdir(self.dbdir):
			os.makedirs(self.dbdir)
		self.conn = sqlite3.connect(os.path.join(self.dbdir,"filedb.sqlite"))
		
		# change the row output by dictionnary
		# result is now like:
		# [{'row1': value1, 'row2': value2},
		#  {'row1': value1-2, 'row2': value2-2}]
		self.conn.row_factory = dict_factory

		self.cursor = self.conn.cursor()
		
		self.cursor.execute('''CREATE TABLE IF NOT EXISTS files
					(filename text,
					size integer,
					date text,
					n integer)''')
					
		self.cursor.execute('''CREATE TABLE IF NOT EXISTS files_nodes
					(filename text,
					node text)''')


		self.conn.commit()
	
	def update(self, file, data):
	    
		if "nodes" in data:
			self.cursor.execute('''DELETE FROM files_nodes WHERE filename=?''', (file,))
			for node in data["nodes"]:
				self.addFileToNode(file,node)
			del data["nodes"]

		if len(data.keys())==0:
			return

		self.cursor.execute('''SELECT COUNT(*) FROM files WHERE filename=?''', (file,))
		nb_files = self.cursor.fetchall()
		if len(nb_files):
			# create a generic sql request depending of the content of data
			# dirty..
			req_str = []
			arg_list = []
			for key, value in data.iteritems():
				req_str.append("%s=? " % (key))
				arg_list.append(value)
			arg_list.append(file)
			self.cursor.execute('''UPDATE %s FROM files WHERE filename=?''' % (' '.join(req_str)), tuple(arg_list))
			self.conn.commit()
		else:
			# same thing here
			# create a generic sql request depending of the content of data
			# very very dirty...
			req_str = []
			arg_list = []
			for key, value in data.iteritems():
				req_str.append(key)
				arg_list.append(value)
			arg_list.append(file)			
			self.cursor.execute('''INSERT INTO files(%s) VALUES (%s)''' % (','.join(req_str), ','.join(['?' for i in xrange(len(req_str))])), tuple(arg_list))		
			self.conn.commit()
			
	def getKn(self, file):
		self.cursor.execute('''SELECT n FROM files WHERE filename=? LIMIT 1''', (file,))
		result = self.cursor.fetchall()
		
		self.cursor.execute('''SELECT count(*) as c FROM files_nodes WHERE filename=? LIMIT 1''', (file,))
		nodes = self.cursor.fetchrow()
        
		
		if len(result):
			return int(nodes['c']) - result[0]['n']
		else:
			return None
	
	def addFileToNode(self, file, node):

		#todo unique key
		self.removeFileFromNode(file,node)
        
		self.cursor.execute('''INSERT INTO files_nodes(filename,node) VALUES (?,?)''', (file,node))
		
		
	def removeFileFromNode(self, file, node):
		self.cursor.execute('''DELETE FROM files_nodes WHERE filename=? and node=?''', (file,node))
		
							
	def getNodes(self, file):
		self.cursor.execute('''SELECT node FROM files_nodes WHERE filename=?''', (file,))
		result = self.cursor.fetchall()

		return set([ i['node'] for i in result ])
	
	def getSize(self, file):
		self.cursor.execute('''SELECT size FROM files WHERE filename=? LIMIT 1''', (file,))
		result = self.cursor.fetchrow()
		
		if len(result):
			return result['size']
		else:
			return None
			
	def listAll(self):
		self.cursor.execute('''SELECT filename FROM files''', (file,))
		result = self.cursor.fetchall()

		return [ i['filename'] for i in result ]
	
	
	def listInNode(self, node):
		self.cursor.execute('''SELECT filename FROM files_nodes WHERE node=?''', (node,))
		result = self.cursor.fetchall()

		return [ i['filename'] for i in result ]
        
