from filedb import FileDbBase
import time

class memoryFileDb(FileDbBase):
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
    
    def __init__(self,fs, options):
        FileDbBase.__init__(self, fs, options)
        
        self.reset()
        
    def reset(self):
        self.files = {}
        self.nodes = {}
        
        self.hasChanged=True
    
    def update(self, file, data):


        if file in self.files:
            self.files[file].update(data)
        else:
            self.files[file] = data

        if data.get("nuked",False) is None:
            del self.files[file]["nuked"]
            
        self.hasChanged=True
        
    def getKn(self,file):
        return len(self.files[file]["nodes"]) - self.files[file]["n"]    
    
    def addFileToNode(self, file, node):
        self.files[file]["nodes"].add(node)
        self.hasChanged=True
   
    def removeFileFromNode(self, file, node):
        self.files[file]["nodes"].discard(node)
        self.hasChanged=True
          
    def isNuked(self,file):
        if file not in self.files:
            return False
        return self.files[file].get("nuked", False)
          
    def getNodes(self, file):
        if not file in self.files:
            return set()
        return self.files[file]["nodes"]
    
    def getSize(self, file):
        return self.files[file]["size"]
    
    def listAll(self):
        return set([f for f in self.files if not self.files[f].get("nuked",False)])
    
    def listInNode(self,node):
        
        innode = set()

        for f in self.files:
            if node in self.getNodes(f):
                innode.add(f)

        return innode
        
    def addNode(self,node,data):
        data["lastUpdate"] = time.time()
        
        if "files" in data:
            self.processFilesData(node,data["files"])
            del data["files"]
        
        self.nodes[node] = data
        self.hasChanged=True
        
    def listNodes(self):
        return set(self.nodes.keys())
        
    def getNode(self,node):
        if not node in self.nodes:
            return None
        else:
            return self.nodes[node]
            
    def removeNode(self,node):
        if node in self.nodes:
            del self.nodes[node]
        
        #also delete in all files
        for f in self.files:
            if node in self.files[f]["nodes"]:
                self.files[f]["nodes"].discard(node)
    
        self.hasChanged=True
