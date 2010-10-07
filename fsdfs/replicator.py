import threading,time
    
    
    
class Replicator(threading.Thread):
    
    
    def __init__(self,fs):
        threading.Thread.__init__(self)
        self.fs = fs
        self.stopnow=False
        
    def shutdown(self):
        self.stopnow=True
        
    def run(self):
        
        while not self.stopnow:
            
            self.updateAllFileDb()
            
            self.performReplication(10)
            time.sleep(1)
        
    def performReplication(self,maxOperations=10):
        
        
        knownNeeds = self.fs.filedb.getMinKnAll(num=maxOperations)
        
        for file in knownNeeds:
            
            self.replicateFile(file)

    def updateAllFileDb(self):
        for f in self.fs.filedb.listAll():
            self.updateFileDb(f)
            
    def updateFileDb(self,file):
        
        #no P2P search yet. master always knows where files are
        #nodes = set(self.fs.searchFile(file,p2p=True))
        
        rules = self.fs.getReplicationRules(file)
        
        self.fs.filedb.update(file,{"n":rules["n"]}) #"nodes":nodes,

    #
    # suggestList elements are (filepath,known need,known copies)
    #
    def replicateFile(self,file):
        
        knownNodes = self.fs.getKnownNodes()
        
        # 1. filter nodes already having the file
        alreadyNodes = self.fs.filedb.getNodes(file)
        
        #all the nodes already have the file!
        if len(knownNodes)<=len(alreadyNodes):
            return False
        
        newnodes = []
        for node in knownNodes:
            if node not in alreadyNodes:
                newnodes.append(node)
        
        
        # 2. pick a node depending on load, available size and max(kn)
        
        #currently just the first one...
        node = newnodes[0]
        
        
        # 3. make the node replicate it
        downloaded = ("ok"==self.fs.nodeRPC(node,"SUGGEST",{"filepath":file}).read())
        
        #add it directly to filedb
        if downloaded:
            self.fs.filedb.addFileToNode(file,node)
        
        