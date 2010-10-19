import threading,time,random
    
    
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
        
        size = self.fs.filedb.getSize(file)
        
        #don't risk deleting everything on a node just to make space
        if size>10*1024*1024*1024:
            return False
        
        #all the nodes already have the file!
        if len(knownNodes)<=len(alreadyNodes):
            return False
        
        newnodes = []
        for node in knownNodes:
            if node not in alreadyNodes:
                newnodes.append(node)
        
        
        # 2. pick a node depending on load, available size and max(kn)
        
        foundHost=False
        
        while not foundHost:
                
            #order by free disk desc
            newnodes.sort(cmp=lambda x,y:cmp(self.fs.nodedb[x]["df"],self.fs.nodedb[y]["df"]),reverse=True)
            
            #if the node with the most free disk has a place for the file, store it there
            if self.fs.nodedb[newnodes[0]]["df"]>=size:
                node=newnodes[0]
                foundHost=True
                
            #no more space anywhere. not a problem, pick the node with max(k-n) and delete that copy
            else:
                newnodes.sort(cmp=lambda x,y:cmp(self.fs.filedb.getKn(self.fs.filedb.getMaxKnInNode(x)[0]),self.fs.filedb.getKn(self.fs.filedb.getMaxKnInNode(y)[0])),reverse=True)
                
                node = newnodes[0]
                
                file_to_remove = self.fs.filedb.getMaxKnInNode(node)[0]
                
                
                self.fs.nodedb[x]["df"]-=self.fs.filedb.getSize(file_to_remove)
                
                deleted = ("ok"==self.fs.nodeRPC(node,"DELETE",{"filepath":file}).read())
                
                if deleted:
                    self.fs.filedb.removeFileFromNode(file_to_remove,node)
            
        
        # 3. make the node replicate it
        downloaded = ("ok"==self.fs.nodeRPC(node,"SUGGEST",{"filepath":file}).read())
        
        #add it directly to filedb
        if downloaded:
            self.fs.filedb.addFileToNode(file,node)
        
        