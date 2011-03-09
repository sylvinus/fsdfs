from filedb import FileDbBase

class mongodbFileDb(FileDbBase):
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
    
    def connect(self):
        
        self.connection = pymongo.Connection(self.options["host"],self.options["port"])
        self.db = self.connection[self.options["db"]]
    
    def __init__(self,fs, options):
        FileDbBase.__init__(self, fs, options)
        
        self.options = options
        
        self.reset()

        self.t_files = prefix+"_files"
        self.t_nodes = prefix+"_nodes"
        self.t_files_nodes = prefix+"_files_nodes"

        
        
        
    def reset(self):
        self.files = {}
    
    def update(self, file, data):

        data["_id"] = file
        self.db[self.t_files].upsert(data)
        
    def getKn(self,file):
        
        f = self.db[self.t_files].find_one({"_id":file})
        return len(f["nodes"]) - f["n"]    
    
    def addFileToNode(self, file, node):
        f = self.db[self.t_files].find_one({"_id":file})
        nodes = set(f["nodes"])
        nodes.add(node)
        f["nodes"] = list(nodes)
        self.db[self.t_files].upsert(f)
        
    def removeFileFromNode(self, file, node):
        f = self.db[self.t_files].find_one({"_id":file})
        nodes = set(f["nodes"])
        nodes.discard(node)
        f["nodes"] = list(nodes)
        self.db[self.t_files].upsert(f)
        
        #todo continue this
          
    def listNukes(self):
        n = set()
        for f in self.files:
            if self.files[f].get("nuked", False) and len(self.files[f]["nodes"])>0:
                n.add(f)
        return n
          
    def getNodes(self, file):
        if not file in self.files:
            return set()
        return self.files[file]["nodes"]
    
    def getSize(self, file):
        return self.files[file]["size"]
    
    def listAll(self):
        return [f for f in self.files if not self.files[f].get("nuked",False)]
    
    def listInNode(self,node):
        
        innode = []

        for f in self.files:
            if node in self.getNodes(f):
                innode.append(f)

        return innode
    
