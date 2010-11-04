from filedb import FileDbBase

class FileDb(FileDbBase):
    
    def __init__(self,fs):
        FileDbBase.__init__(self,fs)
        
        self.files = {}
    
    def update(self,file,data):


        if file in self.files:
            self.files[file].update(data)
        else:
            self.files[file]=data

        if data.get("nuked",False) is None:
            del self.files[file]["nuked"]
        
    
    def getKn(self,file):
        return len(self.files[file]["nodes"])-self.files[file]["n"]
    
    def addFileToNode(self,file,node):
        self.files[file]["nodes"].add(node)
   
    def removeFileFromNode(self,file,node):
        self.files[file]["nodes"].discard(node)
          
    def listNukes(self):
        n = set()
        for f in self.files:
            if self.files[f].get("nuked",False) and len(self.files[f]["nodes"])>0:
                n.add(f)
        return n
          
    def getNodes(self,file):
        if not file in self.files:
            return set()
        return self.files[file]["nodes"]
    
    def getSize(self,file):
        return self.files[file]["size"]
    
    def listAll(self):
        return [f for f in self.files if not self.files[f].get("nuked",False)]
    
    def listInNode(self,node):
        
        innode=[]

        for f in self.files:
            if node in self.getNodes(f):
                innode.append(f)
    
        return innode
    