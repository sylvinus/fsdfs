import imp,traceback

def loadFileDb(id,*args,**kwargs):
    
    try:
        fp, pathname, description = imp.find_module(id,__path__)
        assert fp
        return getattr(imp.load_module(id.replace(".",""),fp, pathname, description),"%sFileDb" % id)(*args,**kwargs)
    except Exception, err:
        print "error while loading filedb : %s" % err
        traceback.print_exc()
        return False



class FileDbBase:
    '''
    Parent class for files database.
    '''
    
    def __init__(self, fs, options):
        self.fs = fs
        self.options = options
    
    
    def reset(self):
        '''
        Empty the database
        '''
        pass
    
    def update(self, file, data):
        '''
        Update file's data.
        
        Data is a environment in a dictionnary which may contains:
        * nodes
        * size 
        * date 
        * replication level (n)
        * time
        '''
        
        pass
    
    def listAll(self):
        '''
        List all files in the database.
        '''
        
        pass
    
    def listInNode(self, node):
        '''
        to write
        '''
        
        pass
    
    def getKn(self, file):
        '''
        to write
        '''
        
        pass
    
    def getNodes(self, file):
        '''
        Return file's node list.
        '''
        
        pass

    def listNodes(self):
        '''
        Return all known nodes
        '''
        pass
        
    def getNode(self,node):
        '''
        Get data about a node
        '''
        
    def addNode(self,node,data):
        '''
        Add a known nodes
        '''
        pass
    
    def removeNode(self,node):
        '''
        Remove a known nodes
        '''
        pass
    
    
    def getSize(self, file):
        '''
        Return the size of a file.
        '''
        
        pass
    
    def addFileToNode(self, file, node):
        '''
        Add a file to a node (in argument).
        '''
        
        pass
    
    def removeFileFromNode(self, file, node):
        '''
        Remove a file from a node.
        '''
        
        pass
    
    def listNukes(self):
        '''
        to write
        '''
        
        return []
    
    def getMaxKnInNode(self, node, num=1):
        '''
        to write
        '''
        
        files = self.listInNode(node)
        
        if len(files) == 0:
            return []
        
        files.sort(lambda x,y:cmp(self.getKn(x),self.getKn(y)),reverse=True)
        
        return files[0:num]
        
    def getMinKnAll(self, num=1):
        '''
        to write
        '''
        
        files = self.listAll()
        
        if len(files) == 0:
            return []
        
        files.sort(lambda x, y:cmp(self.getKn(x), self.getKn(y)))
        
        return files[0:num]
        
    def iterMinKnAll(self):
        for f in self.getMinKnAll(num=self.getCountAll()):
            yield f
    
    def getSizeAll(self):
        return sum([self.getSize(f) for f in self.listAll()])
    
    def getCountAll(self):
        return len(self.listAll())       
        
    def getSizeInNode(self, node):
        '''
        Return the sum of each files managed by a node.
        '''

        return sum([self.getSize(f) for f in self.listInNode(node)])
        
    def getCountInNode(self, node):
        '''
        to write
        '''
        
        return len(self.listInNode(node))
