from filedb import FileDbBase
import pymongo
import time
from bson.code import Code

"""
SIZE_MAP = Code("function () {emit('size',this.size);}")
SIZE_REDUCE = Code("function (key,values) {"
            "var total = 0;"
            "  for (var i = 0; i < values.length; i++) {"
            "    total += values[i];"
            "  }"
            "  return total;"
            "}")
"""

GROUP_SIZE_REDUCE=Code("function(obj,prev) { prev.sumsize += obj.size; }")

class mongodbFileDb(FileDbBase):

    
    def connect(self):
        
        self.connection = pymongo.Connection(self.options["host"],self.options["port"])
        self.db = self.connection[self.options["db"]]
        
        try:
            self.db.create_collection(self.t_files)
        except:
            pass
            
        try:
            self.db.create_collection(self.t_nukes)
        except:
            pass
            
        self.files = self.db[self.t_files]
        self.nukes = self.db[self.t_nukes]
        
        
        self.cacheSizeInNode = {}
        
        #nodes only in memory for now
        self.nodes = {} #self.db[self.t_nodes]
    
    def __init__(self,fs, options):
        FileDbBase.__init__(self, fs, options)
        
        self.options = options
        
        self.prefix = self.options.get("prefix","fsdfs_"+fs.config["host"].replace(":","_").replace(".","_"))
        self.t_files = self.prefix+"_files"
        self.t_nukes = self.prefix+"_nukes"
        
        #self.t_nodes = prefix+"_nodes"
        #self.t_files_nodes = prefix+"_files_nodes"

        self.connect()
        
        
        
        
    def reset(self):
        self.files.remove({},safe=True) 
        self.nukes.remove({},safe=True) 
        self.nodes = {self.fs.host:self.fs.getStatus()}
        self.cacheSizeInNode = {}
        
        self.hasChanged=True
    
    def update(self, file, data):
        
        #print "updating %s %s" % (file,data)
        
        toupdate = {}
        
        updatekn = ("nodes" in data) or ("n" in data)
        
        if "nodes" in data:
            data["nodes"] = list(data["nodes"])
            
            if "n" in data:
                updatekn=False
                data["kn"]=len(data["nodes"])-data["n"]
                
            for node in data["nodes"]:
                if node in self.cacheSizeInNode:
                    if "size" in data:
                        self.cacheSizeInNode[node]+=data["size"]
                    else:
                        del self.cacheSizeInNode[node]
        
        if "nuked" in data:
            if data["nuked"]:
                self.files.remove({"_id":file})
                self.nukes.update({"_id":file},{"$set":{"t":time.time()}},upsert=True,safe=True,multi=False)
                return
            del data["nuked"]
            
        toupdate["$set"]=data
        
        #print toupdate
        self.files.update({"_id":file},toupdate,upsert=True,safe=True,multi=False)
        
        self.files.ensure_index([("kn",pymongo.ASCENDING)])
        
        self.files.ensure_index([("nodes",pymongo.ASCENDING)])
        
        
        if updatekn:
            self.files.update({"_id":file},{"$set":{"kn":self.getKn(file)}},safe=True,multi=False)
            
        #print self.files.find_one({"_id":file})
        
        self.hasChanged=True
        
    
    #todo
    def temporaryIncrementKn(self,file):
        self.files.update({"_id":file},{"$set":{"kn":self.getKn(file)+1}},safe=True,multi=False)
        
    def getKn(self,file):
        f = self.files.find_one({"_id":file},fields=["n","nodes"])
        #print f
        if not f:
            return None
        else:
            return len(f["nodes"])-f["n"]
    
    def addFileToNode(self, file, node):
        
        self.files.update({"_id":file},{"$addToSet":{"nodes":node}},safe=True)
        self.hasChanged=True
    
        if node in self.cacheSizeInNode:
            del self.cacheSizeInNode[node]
   
    def removeFileFromNode(self, file, node):
        self.files.update({"_id":file},{"$pull":{"nodes":node}},safe=True)
        self.hasChanged=True
        
        if node in self.cacheSizeInNode:
            del self.cacheSizeInNode[node]
          
    def isNuked(self,file):
        return 1==self.nukes.find({"_id":file},fields=["_id"]).count()
        
        """
        n = set()
        for f in files:
            if len(f["nodes"])>0 and f["nuked"]:
                n.add(f["_id"])
        return n
        """
          
    def getNodes(self, file):
        f = self.files.find_one({"_id":file},fields=["nodes"])
        
        if not f:
            return set()
        else:
            return set(f["nodes"]) #[n for n in f["nodes"] if n in self.nodes])
    
    def getSize(self, file):
        f = self.files.find_one({"_id":file},fields=["size"])
        if not f:
            return 0
        else:
            return f["size"]
    
    def listAll(self):
        return set([f["_id"] for f in self.files.find({},fields=["_id"])])
    
    def listInNode(self,node):
        
        return set([f["_id"] for f in self.files.find({"nodes":node},fields=["_id"])])
        
    def addNode(self,node,data):
        data["lastUpdate"] = time.time()
        
        #print "node added %s : %s" % (node,data)
        
        if "files" in data:
            self.processFilesData(node,data["files"])
            del data["files"]
            
            if node in self.cacheSizeInNode:
                del self.cacheSizeInNode[node]
        
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
        for f in self.listInNode(node):
            self.removeFileFromNode(f,node)
    
        self.hasChanged=True
        if node in self.cacheSizeInNode:
            del self.cacheSizeInNode[node]


    def getMaxKnInNode(self, node, num=1):
        '''
        to write
        '''
        return [f["_id"] for f in self.files.find({"nodes":node },sort=[("kn",-1)],limit=num,fields=["_id"])]
        
    def getMinKnAll(self, num=1):
        '''
        to write
        '''

        return [f["_id"] for f in self.files.find({"nodes":{"$not":{"$size":0}}},sort=[("kn",1)],limit=num,fields=["_id"])]
        
    def iterMinKnAll(self):
        for f in self.getMinKnAll(num=self.getCountAll()):
            yield f

    
    def getMinKnNotInNode(self,node):
        file = list(self.files.find({"$nor":[{"nodes":{"$size":0}},{"nodes":node}]},limit=1,fields=["_id"]))
        
        if len(file):
            return file[0]["_id"]
        else:
            return None
    

    def getSizeAll(self):
        
        size = self.files.group({},{},{"sumsize":0},GROUP_SIZE_REDUCE)
                
        #size = self.files.map_reduce(SIZE_MAP,SIZE_REDUCE,out=self.prefix+"_mapreduce_getsizeall",query={"nuked":{ "$exists" : False}}).find_one({"_id":"size"})
        
        if size:
            return size[0]["sumsize"]
        else:
            return 0
    
    def getSizeInNode(self, node):
        
        #Cache this because it's an expensive operation that slowed down bulk imports a lot
        if node in self.cacheSizeInNode:
            return self.cacheSizeInNode[node]
            
        size = self.files.group({},{"nodes":node},{"sumsize":0},GROUP_SIZE_REDUCE)
              
        #size = self.files.map_reduce(SIZE_MAP,SIZE_REDUCE,out=self.prefix+"_mapreduce_getsizeinnode",query={"nuked":{ "$exists" : False},"nodes":node}).find_one({"_id":"size"})
        
        if size:
            self.cacheSizeInNode[node]=size[0]["sumsize"]
            return size[0]["sumsize"]
        else:
            return 0
        

    def getCountAll(self):
        return self.files.find({}).count()

    def getCountInNode(self, node):
        '''
        to write
        '''
        return self.files.find({"nodes":node}).count()
