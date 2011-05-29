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
        
        
    def processFilesData(self,node,data):
        
        data.setdefault("imported",[])
        data["imported"] = set(data["imported"])
        
        data.setdefault("deleted",[])
        data["deleted"] = set(data["deleted"])
        
        if "all" in data:
            data["all"] = set(data["all"])
            supposed = self.listInNode(node)
            
            #add files we didn't know about
            data["imported"].update(data["all"].difference(supposed))
            
            #delete missing files
            data["deleted"].update(supposed.difference(data["all"]))
            
        for f in data["imported"]:
            if self.isNuked(f):
                self.fs.performNuke(f,[node])
            else:
                self.addFileToNode(f,node)
            
        for f in data["deleted"]:
            self.removeFileFromNode(f,node)
        
    def addNode(self,node,data):
        '''
        Add a known node
        '''
    
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
    
    def isNuked(self,file):
        '''
        to write
        '''
        
        return False
    
    def getMaxKnInNode(self, node, num=1):
        '''
        to write
        '''
        
        files = list(self.listInNode(node))
        
        if len(files) == 0:
            return []
        
        files.sort(lambda x,y:cmp(self.getKn(x),self.getKn(y)),reverse=True)
        
        return files[0:num]
        
    def getMinKnAll(self, num=1):
        '''
        to write
        '''
        
        files = list(self.listAll())
        
        if len(files) == 0:
            return []
        
        files.sort(lambda x, y:cmp(self.getKn(x), self.getKn(y)))
        
        return files[0:num]
        
    def iterMinKnAll(self):
        for f in self.getMinKnAll(num=self.getCountAll()):
            yield f
            
    def getMinKnNotInNode(self,node):
        for f in self.iterMinKnAll():
            if not node in self.getNodes(f):
                if len(self.getNodes(f))>0:
                    return f
        return None
    
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
