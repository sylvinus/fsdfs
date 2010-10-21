import imp

def loadFileDb(id,*args,**kwargs):
    
    try:
        fp, pathname, description = imp.find_module(id,__path__)
        assert fp
        return imp.load_module(id.replace(".",""),fp, pathname, description).FileDb(*args,**kwargs)
    except Exception, e:
        print "error while loading filedb : %s" % e
        return False



class FileDbBase:
    
    
    def __init__(self,fs):
        self.fs = fs
    
    def update(self,file):
        pass
    
    def listAll(self):
        pass
    
    def listInNode(self,node):
        pass
    
    def getKn(self,file):
        pass
    
    def getNodes(self,file):
        pass
    
    def getSize(self,file):
        pass
    
    def addFileToNode(self,file,node):
        pass
    
    def removeFileFromNode(self,file,node):
        pass
    
    def getMaxKnInNode(self,node,num=1):
        
        files = self.listInNode(node)
        
        if len(files)==0: return []
        
        files.sort(lambda x,y:cmp(self.getKn(x),self.getKn(y)),reverse=True)
        
        return files[0:num]
        
    def getMinKnAll(self,num=1):
        
        files = self.listAll()
        
        if len(files)==0: return []
        
        files.sort(lambda x,y:cmp(self.getKn(x),self.getKn(y)))
        
        return files[0:num]
        
        
    def getSizeInNode(self,node):
        
        return sum([self.getSize(f) for f in self.listInNode(node)])
        
    def getCountInNode(self,node):
        return len(self.listInNode(node))
        