import MySQLdb,os

from filedb import FileDbBase

class sqlFileDb(FileDbBase):
    """
    Abstract class for SQL storages
    
    """
    
    def connect(self):
        pass
        
    def __init__(self, fs, options={}):
        FileDbBase.__init__(self, fs, options)
        
        self.hasChanged=True

    def execute(self,sql,*args):
        self.cursor.execute(sql,*args)        
        return self.cursor.fetchall()
        
    def reset(self):
        pass
        
    def _getFileId(self,filename):
        result = self.execute("""SELECT id FROM """+self.t_files+""" WHERE filename=%s LIMIT 1""", (filename,))
        
        if result:
            return int(result[0]['id'])
        else:
            self.execute("""INSERT IGNORE INTO """+self.t_files+"""(filename) VALUES (%s)""", (filename,))
            return self._getFileId(filename)
        
    def _getNodeId(self,node):
        result = self.execute("""SELECT id FROM """+self.t_nodes+""" WHERE address=%s LIMIT 1""", (node,))
        if result:
            return result[0]['id']
        else:
            self.execute("""INSERT IGNORE INTO """+self.t_nodes+"""(address) VALUES (%s)""", (node,))
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
                    req_str.append(key+"""="""+self.unixtimefunction+"""(%s) """)
                else:
                    req_str.append(key+"""=%s """)
                arg_list.append(value)
        arg_list.append(file_id)
        
        if len(req_str):
            self.execute("""UPDATE """+self.t_files+""" SET """+(','.join(req_str))+""" WHERE id=%s""",tuple(arg_list))
        
    
        if "nodes" in data:
            self.execute("""DELETE FROM """+self.t_files_nodes+""" WHERE file_id=%s""", (file_id,))
            for node in data["nodes"]:
                self.addFileToNode(file,node)

        self.hasChanged=True
            
    def getKn(self, file):
        result = self.execute("""SELECT id,n FROM """+self.t_files+""" WHERE filename=%s LIMIT 1""", (file,))
        
        nodes = self.execute("""SELECT count(*) as c FROM """+self.t_files_nodes+""" WHERE file_id=%s LIMIT 1""", (result[0]['id'],))
        
        
        if len(result):
            return int(nodes[0]['c']) - result[0]['n']
        else:
            return None
    
    def addFileToNode(self, file, node):

        #todo unique key
        self.removeFileFromNode(file,node)
        
        file_id = self._getFileId(file)
        node_id = self._getNodeId(node)
        
        self.execute("""INSERT INTO """+self.t_files_nodes+"""(file_id,node_id) VALUES (%s,%s)""", (file_id,node_id))
        
        self.update(file,{'kn':self.getKn(file)})
        
        self.hasChanged=True
        
    def removeFileFromNode(self, file, node):
        file_id = self._getFileId(file)
        node_id = self._getNodeId(node)
        self.execute("""DELETE FROM """+self.t_files_nodes+""" WHERE file_id=%s and node_id=%s""", (file_id,node_id))
        
        self.update(file,{'kn':self.getKn(file)})
        
        self.hasChanged=True
        
    def getNodes(self, file):
        file_id = self._getFileId(file)
        result = self.execute("""SELECT """+self.t_nodes+""".address FROM """+self.t_files_nodes+""","""+self.t_nodes+""" WHERE """+self.t_nodes+""".id="""+self.t_files_nodes+""".node_id AND """+self.t_files_nodes+""".file_id=%s""", (file_id,))

        return set([ i['address'] for i in result ])
    
    def getSize(self, file):
        result = self.execute("""SELECT size FROM """+self.t_files+""" WHERE filename=%s LIMIT 1""", (file,))

        if result:
            return result[0]['size']
        else:
            return None
            
    def listAll(self):
        result = self.execute("""SELECT filename FROM """+self.t_files+""" WHERE nuked=0 """, ())

        return [ i['filename'] for i in result ]
    
    def listNukes(self):
        result = self.execute("""SELECT filename FROM """+self.t_files+""" WHERE nuked=1 """, ())

        return [ i['filename'] for i in result ]
        
    def listInNode(self, node):
        
        node_id = self._getNodeId(node)
        result = self.execute("""SELECT """+self.t_files+""".filename FROM """+self.t_files_nodes+""","""+self.t_files+""" WHERE """+self.t_files_nodes+""".file_id="""+self.t_files+""".id AND """+self.t_files_nodes+""".node_id=%s""", (node_id,))
        
        return [ i['filename'] for i in result ]


    def getMaxKnInNode(self, node, num=1):
        '''
        to write
        '''
        node_id = self._getNodeId(node)
        
        result = self.execute("""SELECT F.filename FROM """+self.t_files+""" F,"""+self.t_files_nodes+""" FN WHERE F.id=FN.file_id AND FN.node_id=%s AND F.nuked=0 ORDER BY F.kn DESC LIMIT 0,%s""", (node_id,num))
        
        return [ i['filename'] for i in result ]
        
        
    def getMinKnAll(self, num=1):
        '''
        to write
        '''
        
        result = self.execute("""SELECT F.filename FROM """+self.t_files+""" F WHERE F.nuked=0 ORDER BY F.kn ASC LIMIT 0,%s""", (num,))
        
        return [ i['filename'] for i in result ]

    def getSizeAll(self):
        
        result = self.execute("""SELECT SUM(F.size) as s FROM """+self.t_files+""" F WHERE F.nuked=0""")
        
        if result and result[0]['s'] is not None:
            return long(result[0]['s'])
        else:
            return 0
    
    def getCountAll(self):
    
        result = self.execute("""SELECT COUNT(*) as s FROM """+self.t_files+""" F WHERE F.nuked=0""")
    
        if result:
            return long(result[0]['s'])
        else:
            return 0
                

    def getSizeInNode(self, node):
        '''
        to write
        '''
        node_id = self._getNodeId(node)
    
        result = self.execute("""SELECT SUM(F.size) as s FROM """+self.t_files+""" F,"""+self.t_files_nodes+""" FN WHERE F.id=FN.file_id AND FN.node_id=%s AND F.nuked=0""", (node_id,))
        
        if result and result[0]['s'] is not None:
            return long(result[0]['s'])
        else:
            return 0
            
    def getCountInNode(self, node):
        '''
        to write
        '''
        node_id = self._getNodeId(node)
    
        result = self.execute("""SELECT COUNT(*) as s FROM """+self.t_files+""" F,"""+self.t_files_nodes+""" FN WHERE F.id=FN.file_id AND FN.node_id=%s AND F.nuked=0""", (node_id,))

        if result:
            return long(result[0]['s'])
        else:
            return 0