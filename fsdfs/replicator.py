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
        
        self.lastIterationDidSomething = True

    
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
        
        self.nodewatcher = NodeWatcher(self.fs)
        self.nodewatcher.start()

        while not self.stopnow:
            
            try:
                
                self.updateAllFileDb()
            
                self.filedb.hasChanged=False
            
                self.performNukes()
            
                self.lastIterationDidSomething=False
            
                self.performReplication(self.fs.config["replicatorDepth"])
            
            except Exception,e:
                
                self.fs.error("When replicating : %s" % (e))
            
            #Sleep for one minute unless something happens..
            if not self.lastIterationDidSomething and self.fs.config["replicatorIdleTime"]>0:
                self.fs.debug("Idling for %s seconds max..." % self.fs.config["replicatorIdleTime"],"repl")
                [time.sleep(1) for i in range(self.fs.config["replicatorIdleTime"]) if not self.lastIterationDidSomething and not self.stopnow and not self.filedb.hasChanged]
            else:
                time.sleep(self.fs.config["replicatorInterval"])
    
    
    def performNukes(self):
	'''
	to write.
	'''
        
        nukes = self.filedb.listNukes()
        
        for file in nukes:
            #do a set() because list may change while looping
            nodes = set(self.filedb.getNodes(file))
            for node in nodes:
                deleted = ("ok"==self.fs.nodeRPC(node,"DELETE",{"filepath":file}))
                
                if deleted:
                    self.filedb.removeFileFromNode(file,node)
    
    
    def performReplication(self, maxOperations=10):
 	'''
	to write.
	'''
        
        ops = 0
        self.downloadThreads = []
        self.previsionalSizeAdded = {}

        for file in self.filedb.iterMinKnAll():
            
            if self.replicateFile(file):
                ops+=1
                if ops==maxOperations:
                    break
            
        for t in self.downloadThreads:
            t.join()
    
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
       
        #print "try rep %s" % file
        
        def df(node):
            return self.filedb.getNode(node).get("df",0) - self.previsionalSizeAdded.get(node,0)
        
        knownNodes = self.filedb.listNodes()
        
        #print "\n".join(["%s has %s" % (n,self.filedb.listInNode(n)) for n in knownNodes])
        
        
        random.shuffle(knownNodes)
        
        # 1. filter nodes already having the file
        alreadyNodes = self.filedb.getNodes(file)
        
        size = self.filedb.getSize(file)
        
        kn = self.filedb.getKn(file)
        
        self.fs.debug("File %s (size=%s, kn=%s)" % (file, size, kn),"repl")
        
        #don't risk deleting everything on a node just to make space
        if size > 10*1024*1024*1024:
            self.fs.debug("File too big, don't risk messing with the nodes","repl")
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
                if df(node) + self.filedb.getNode(node)["size"] >= size:
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
            newnodes.sort(cmp=lambda x,y:cmp(df(x),df(y)),
							reverse=True)
            
            #if the node with the most free disk has a place for the file, store it there
            if df(newnodes[0]) >= size:
                node = newnodes[0]
                foundHost = True
                
                self.fs.debug("Node %s still has enough space (%s) for %s" % (
								node,
								df(newnodes[0]),
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
                
                self.filedb.getNode(node)["df"] += self.filedb.getSize(file_to_remove)
                self.filedb.getNode(node)["size"] -= self.filedb.getSize(file_to_remove)
                
                
                deleted = ("ok" == self.fs.nodeRPC(node, "DELETE", {"filepath": file_to_remove}))
                
                if deleted:
                    self.filedb.removeFileFromNode(file_to_remove,node)
        
        
        if foundHost:
            
            # avoid sending everything in the current replication batch to the same server
            # by already taking into account how much less space we'll use
            self.previsionalSizeAdded.setdefault(node,0)
            self.previsionalSizeAdded[node]+=size
            
            self.lastIterationDidSomething=True
            
            t = DownloadThread(self.fs,node,file,size)
            t.start()
            
            self.downloadThreads.append(t)
            
            # try to join the first thread we find that's finished downloading
            while len(self.downloadThreads)>=self.fs.config["replicatorConcurrency"]:
                
                for i in range(len(self.downloadThreads)):
                    if not self.downloadThreads[i].isAlive():
                        th = self.downloadThreads.pop(i)
                        th.join()
                        self.previsionalSizeAdded[th.node]-=th.size
                        break
                        
                time.sleep(0.1)
                
            return True
                
        return False


class NodeWatcher(threading.Thread):
    def __init__(self,fs):
        threading.Thread.__init__(self)
        self.daemon = True
        self.fs = fs
        
    def run(self):
        
        while True:
            time.sleep(self.fs.config["reportInterval"]/2)
            for node in self.fs.filedb.listNodes():
                lastUpdate = self.fs.filedb.getNode(node)["lastUpdate"]
                if lastUpdate<(time.time()-self.fs.config["reportInterval"]*self.fs.config["maxMissedReports"]):
                    self.fs.debug("Node %s missed %s reports, removing it from the swarm" % (node,self.fs.config["maxMissedReports"]))
                    self.fs.filedb.removeNode(node)
                

class DownloadThread(threading.Thread):

    def __init__(self,fs,node,file,size):
        threading.Thread.__init__(self)
        self.fs = fs
        self.node = node
        self.file = file
        self.size = size

        self.daemon = True
        
    def run(self):

        # 3. make the node replicate it
        downloaded = ("ok" == self.fs.nodeRPC(self.node, "SUGGEST", {"filepath": self.file},timeout=self.fs.config["downloadTimeout"]+10))
        
        #add it directly to filedb
        if downloaded:
            self.fs.filedb.addFileToNode(self.file,self.node)