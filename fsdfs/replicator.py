import threading
import time
import random

from filedb import loadFileDb
    

class Replicator(threading.Thread):
    '''
	This is the Replicator class.
	It manages files on every node.
	
	It takes a Filesystem instance in argument which is saved in self.fs.
	
	It's because a Replicator is also a Filesystem :-)
    '''
    
    def __init__(self, fs):
        threading.Thread.__init__(self)
        self.fs = fs
        self.stopnow = False

    
    def shutdown(self):
		'''
		Shutdown the server by setting the variable self.stopnow to True.
		
		It breaks the while which runs the Replicator.
		'''
		
		self.stopnow = True
    
    def run(self):
        '''
		to write
        '''
        
        self.filedb = self.fs.filedb

        while not self.stopnow:
            
            self.updateAllFileDb()
            
            self.performNukes()
            
            self.performReplication(10)
            time.sleep(1)
    
    
    def performNukes(self):
	'''
	to write.
	'''
        
        nukes = self.filedb.listNukes()
        
        for file in nukes:
            #do a set() because list may change while looping
            nodes = set(self.filedb.getNodes(file))
            for node in nodes:
                deleted = ("ok"==self.fs.nodeRPC(node,"DELETE",{"filepath":file}).read())
                
                if deleted:
                    self.filedb.removeFileFromNode(file,node)
    
    
    def performReplication(self, maxOperations=10):
 	'''
	to write.
	'''
        
        knownNeeds = self.filedb.getMinKnAll(num=maxOperations)
        for file in knownNeeds:
            self.replicateFile(file)
    
    def updateAllFileDb(self):
	'''
	to write.
	'''
        
        for f in self.filedb.listAll():
            self.updateFileDb(f)
    
    def updateFileDb(self, file):
 	'''
	to write.
	'''
        
        #no P2P search yet. master always knows where files are
        #nodes = set(self.fs.searchFile(file,p2p=True))
        
        rules = self.fs.getReplicationRules(file)
        
        self.filedb.update(file, {"n": rules["n"]}) #"nodes":nodes,
    
    #
    # suggestList elements are (filepath,known need,known copies)
    #
    def replicateFile(self, file):
	'''
	Describe the algorithm...
	'''
        
        knownNodes = self.fs.getKnownNodes()
        
        #print "\n".join(["%s has %s" % (n,self.filedb.listInNode(n)) for n in knownNodes])
        
        
        random.shuffle(knownNodes)
        
        # 1. filter nodes already having the file
        alreadyNodes = self.filedb.getNodes(file)
        
        size = self.filedb.getSize(file)
        
        kn = self.filedb.getKn(file)
        
        self.fs.debug("Replicating file %s (size=%s, kn=%s)" % (file, size, kn),"repl")
        
        #don't risk deleting everything on a node just to make space
        if size > 10*1024*1024*1024:
            return False
        
        #all the nodes already have the file!
        if len(knownNodes) <= len(alreadyNodes):
            self.fs.debug("All known nodes (%s) already have the file %s" % (len(knownNodes), file),"repl")
            return False
        
        
        
        newnodes = []
        for node in knownNodes:
            #only target nodes which don't have the file
            if node not in alreadyNodes:
                #only target nodes with enough free disk or with some files to delete to make space
                if self.fs.nodedb[node]["df"] + self.fs.nodedb[node]["size"] >= size:
                    newnodes.append(node)
        
        #print (knownNodes,alreadyNodes,newnodes,self.fs.nodedb)
        if len(newnodes) == 0:
            self.fs.debug("No more known nodes (%s) are big enough for the file %s" % (
							len(knownNodes) - len(alreadyNodes),
							file
						),"repl")
            return False
        
        # 2. pick a node depending on load, available size and max(kn)
        
        foundHost = False
        
        while not foundHost:
            #order by free disk desc
            newnodes.sort(cmp=lambda x,y:cmp(self.fs.nodedb[x]["df"],
							self.fs.nodedb[y]["df"]),
							reverse=True)
            
            #if the node with the most free disk has a place for the file, store it there
            if self.fs.nodedb[newnodes[0]]["df"] >= size:
                node = newnodes[0]
                foundHost = True
                
                self.fs.debug("Node %s still has enough space (%s) for %s" % (
								node,
								self.fs.nodedb[newnodes[0]]["df"],
								file
							),"repl")
            
            #no more space anywhere. not a problem, pick the node with max(k-n) and delete that copy
            else:
                newnodes.sort(cmp=lambda x,y:cmp(self.filedb.getKn(self.filedb.getMaxKnInNode(x)[0]),
								self.filedb.getKn(self.filedb.getMaxKnInNode(y)[0])),
								reverse=True)
                
                node = newnodes[0]
                
                file_to_remove = self.filedb.getMaxKnInNode(node)[0]
                
                #print (file_to_remove,self.filedb.getKn(file_to_remove),kn,newnodes)
                #print [self.filedb.getMaxKnInNode(x) for x in newnodes]
                #print self.filedb.files
                
                # file with highest k-N has an inferior or equal k-N to the file we're replicating (+1 because if we replicate it will increase)
                # it doesn't need replication.
                if self.filedb.getKn(file_to_remove) <= (kn + 1):
                    self.fs.debug("No file to remove with higher kn!","repl")
                    break

                
                self.fs.debug("Deleting file %s on %s because it has kn=%s" % (
								file_to_remove,
								node,
								self.filedb.getKn(file_to_remove)
							),"repl")
                
                self.fs.nodedb[node]["df"] += self.filedb.getSize(file_to_remove)
                self.fs.nodedb[node]["size"] -= self.filedb.getSize(file_to_remove)
                
                
                deleted = ("ok" == self.fs.nodeRPC(node, "DELETE", {"filepath": file_to_remove}).read())
                
                if deleted:
                    self.filedb.removeFileFromNode(file_to_remove,node)
        
        
        if foundHost:
            # 3. make the node replicate it
            downloaded = ("ok" == self.fs.nodeRPC(node, "SUGGEST", {"filepath": file}).read())
            
            #add it directly to filedb
            if downloaded:
                self.filedb.addFileToNode(file,node)
            
