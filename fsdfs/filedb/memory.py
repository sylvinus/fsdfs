from filedb import FileDbBase

class FileDb(FileDbBase):
    '''
    FileDb class for *memory*
    Everything is stored in a big dictionnary with filename as key:
    {filename:
		{'nodes': set([192.168.2.1, 192.168.2.2])} # set of node
		{'size': 424242} # integer size
		{'n': 4} # replication number
		....
    }
    '''
    
    def __init__(self,fs):
        FileDbBase.__init__(self, fs)
        
        self.files = {}
    
    def update(self, file, data):

        if file in self.files:
            self.files[file].update(data)
        else:
            self.files[file] = data
        
    
    def getKn(self,file):
        return len(self.files[file]["nodes"]) - self.files[file]["n"]    
    
    
    def addFileToNode(self, file, node):
        self.files[file]["nodes"].add(node)
   
    def removeFileFromNode(self, file, node):
        self.files[file]["nodes"].discard(node)
          
    def getNodes(self, file):
        if not file in self.files:
            return set()
        return self.files[file]["nodes"]
    
    def getSize(self, file):
        return self.files[file]["size"]
    
    def listAll(self):
        return self.files.keys()
    
    def listInNode(self,node):
        
        innode = []

        for f in self.files:
            if node in self.getNodes(f):
                innode.append(f)

        return innode
    
