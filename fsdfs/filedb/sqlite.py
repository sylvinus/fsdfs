import sqlite3
from filedb import FileDbBase

def dict_factory(cursor, row):
	d = {}
	for idx, col in enumerate(cursor.description):
		d[col[0]] = row[idx]
	return d

class SqlLiteDb(FileDbBase):
	'''
	FileDb class for *sqlite*

	Table files has row:
	filename (text),
	nodes (text),
	size (integer),
	date (text),
	n (integer)
	'''
	
	def __init__(self, fs):
		FileDbBase.__init__(self, fs)
		self.files = {}
		
		self.conn = sqlite3.connect('./db.sqlite')

		# change the row output by dictionnary
		# result is now like:
		# [{'row1': value1, 'row2': value2},
		#  {'row1': value1-2, 'row2': value2-2}]
		self.conn.row_factory = dict_factory

		self.cursor = conn.cursor()
		
		self.cursor.execute('''CREATE TABLE files
					(filename text,
					nodes text,
					size integer,
					date text,
					n integer)''')
		self.conn.commit()
	
	def update(self, file, data):

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
			arg_list.append(filename)
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
			arg_list.append(filename)			
			self.cursor.execute('''INSERT INTO files(%s) VALUES (%s)''' % (','.join(req_str), ','.join(['?' for i in xrange(len(req_str))])), tuple(arg_list))		
			self.conn.commit()
			
	def getKn(self, file):
		self.cursor.execute('''SELECT nodes, n FROM files WHERE filename=? LIMIT 1''', (file,))
		result = self.cursor.fetchall()
		
		if len(result):
			return len(result[0]['nodes'].split()) - result[0]['n']
		else:
			return -1
	
	def addFileToNode(self, file, node):
		self.cursor.execute('''SELECT nodes FROM files WHERE filename=? LIMIT 1''', (file,))
		result = self.cursor.fetchall()
		
		list_node = result[0]['nodes'].split()
		list_node = set(list_node)
		list_node.add(node)

		self.cursor.execute('''UPDATE nodes=:nodes FROM files WHERE filename=:filename''',
							{'filename': file},
							{'nodes': ' '.join(list_node)})
		
		
	def removeFileFromNode(self, file, node):
		self.cursor.execute('''SELECT nodes FROM files WHERE filename=? LIMIT 1''', (file,))
		result = self.cursor.fetchall()
		
		list_node = result[0]['nodes'].split()
		list_node = set(list_node)
		list_node.discard(node)
		
		self.cursor.execute('''UPDATE nodes=:nodes FROM files WHERE filename=:filename''',
							{'filename': file},
							{'nodes': ' '.join(list_node)})
							
	def getNodes(self, file):
		self.cursor.execute('''SELECT nodes FROM files WHERE filename=? LIMIT 1''', (file,))
		result = self.cursor.fetchall()

		if len(result):
			return set(result[0]['nodes'].split())
		else:
			return set()
	
	def getSize(self, file):
		self.cursor.execute('''SELECT size FROM files WHERE filename=? LIMIT 1''', (file,))
		result = self.cursor.fetchall()
		
		if len(result):
			return result[0]['size']
		else:
			return 0 # what do we do when the request fails ?
			
	def listAll(self):
		self.cursor.execute('''SELECT * FROM files''', (file,))
		result = self.cursor.fetchall()

		return [ i['filename'] for i in result ]
	
	def listInNode(self, node):
		# todo
		pass
