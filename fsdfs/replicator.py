import threading,time,random
        
import logging as logger

    
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
        
        #print "\n".join(["%s has %s" % (n,self.fs.filedb.listInNode(n)) for n in knownNodes])
            
        
        random.shuffle(knownNodes)
        
        # 1. filter nodes already having the file
        alreadyNodes = self.fs.filedb.getNodes(file)
        
        size = self.fs.filedb.getSize(file)
        
        kn = self.fs.filedb.getKn(file)
        
        logger.debug("Replicating file %s (size=%s, kn=%s)" % (file,size,kn))
        
        #don't risk deleting everything on a node just to make space
        if size>10*1024*1024*1024:
            return False
        
        #all the nodes already have the file!
        if len(knownNodes)<=len(alreadyNodes):
            logger.debug("All known nodes (%s) already have the file %s" % (len(knownNodes),file))
            return False
        
        
        
        newnodes = []
        for node in knownNodes:
            
            #only target nodes which don't have the file
            if node not in alreadyNodes:
                
                #only target nodes with enough free disk or with some files to delete to make space
                if self.fs.nodedb[node]["df"]+self.fs.nodedb[node]["size"]>=size:
                    newnodes.append(node)
        
        #print (knownNodes,alreadyNodes,newnodes,self.fs.nodedb)
    
        if len(newnodes)==0:
            logger.debug("No more known nodes (%s) are big enough for the file %s" % (len(knownNodes)-len(alreadyNodes),file))
            return False
        
        # 2. pick a node depending on load, available size and max(kn)
        
        foundHost=False
        
        while not foundHost:
            
                
            #order by free disk desc
            newnodes.sort(cmp=lambda x,y:cmp(self.fs.nodedb[x]["df"],self.fs.nodedb[y]["df"]),reverse=True)

            #if the node with the most free disk has a place for the file, store it there
            if self.fs.nodedb[newnodes[0]]["df"]>=size:
                node=newnodes[0]
                foundHost=True
                
                logger.debug("Node %s still has enough space (%s) for %s" % (node,self.fs.nodedb[newnodes[0]]["df"],file))
                
            #no more space anywhere. not a problem, pick the node with max(k-n) and delete that copy
            else:
                
                newnodes.sort(cmp=lambda x,y:cmp(self.fs.filedb.getKn(self.fs.filedb.getMaxKnInNode(x)[0]),self.fs.filedb.getKn(self.fs.filedb.getMaxKnInNode(y)[0])),reverse=True)
                
                node = newnodes[0]
                
                file_to_remove = self.fs.filedb.getMaxKnInNode(node)[0]
                
                #print (file_to_remove,self.fs.filedb.getKn(file_to_remove),kn,newnodes)
                #print [self.fs.filedb.getMaxKnInNode(x) for x in newnodes]
                #print self.fs.filedb.files
                
                # file with highest k-N has an inferior or equal k-N to the file we're replicating (+1 because if we replicate it will increase)
                # it doesn't need replication.
                if self.fs.filedb.getKn(file_to_remove)<=(kn+1):
                    logger.debug("No file to remove with higher kn!")
                    
                    break
                

                logger.debug("Deleting file %s on %s because it has kn=%s" % (file_to_remove,node,self.fs.filedb.getKn(file_to_remove)))
                
                self.fs.nodedb[node]["df"]+=self.fs.filedb.getSize(file_to_remove)
                self.fs.nodedb[node]["size"]-=self.fs.filedb.getSize(file_to_remove)
                
                
                deleted = ("ok"==self.fs.nodeRPC(node,"DELETE",{"filepath":file_to_remove}).read())
                
                if deleted:
                    self.fs.filedb.removeFileFromNode(file_to_remove,node)
                    
        
        if foundHost:
            
            # 3. make the node replicate it
            downloaded = ("ok"==self.fs.nodeRPC(node,"SUGGEST",{"filepath":file}).read())
            
            #add it directly to filedb
            if downloaded:
                self.fs.filedb.addFileToNode(file,node)
            
