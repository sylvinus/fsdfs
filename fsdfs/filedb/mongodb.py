from filedb import FileDbBase
import pymongo
import time
from bson.code import Code

SIZE_MAP = Code("function () {emit('size',this.size);}")
SIZE_REDUCE = Code("function (key,values)"
            "var total = 0;"
            "  for (var i = 0; i < values.length; i++) {"
            "    total += values[i];"
            "  }"
            "  return total;"
            "}")


class mongodbFileDb(FileDbBase):

    
    def connect(self):
        
        self.connection = pymongo.Connection(self.options["host"],self.options["port"])
        self.db = self.connection[self.options["db"]]
    
        self.files = self.db[self.t_files]
        
        #nodes only in memory for now
        self.nodes = {} #self.db[self.t_nodes]
    
    def __init__(self,fs, options):
        FileDbBase.__init__(self, fs, options)
        
        self.options = options
        
        self.t_files = self.options.get("prefix","fsdfs_"+fs.config["host"].replace(":","_").replace(".","_"))+"_files"
        #self.t_nodes = prefix+"_nodes"
        #self.t_files_nodes = prefix+"_files_nodes"

        self.connect()
        
        
        
    def reset(self):
        self.files.remove({},safe=True) 
        self.nodes = {self.fs.host:self.fs.getStatus()}
        
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
        
        if data.get("nuked",False) is None:
            toupdate["$unset"] = {"nuked":True}
            del data["nuked"]
            
        toupdate["$set"]=data
        
        #print toupdate
        self.files.update({"_id":file},toupdate,upsert=True,safe=True,multi=False)
        
        if updatekn:
            self.files.update({"_id":file},{"$set":{"kn":self.getKn(file)}},safe=True,multi=False)
            
        #print self.files.find_one({"_id":file})
        
        self.hasChanged=True
        
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
   
    def removeFileFromNode(self, file, node):
        self.files.update({"_id":file},{"$pull":{"nodes":node}},safe=True)
        self.hasChanged=True
          
    def listNukes(self):
        files = self.files.find({"nuked":{"$exists":True}},fields=["nuked","nodes","_id"])
        n = set()
        for f in files:
            if len(f["nodes"])>0 and f["nuked"]:
                n.add(f["_id"])
        return n
          
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
        return [f["_id"] for f in self.files.find({"nuked":{ "$exists" : False }},fields=["_id"])]
    
    def listInNode(self,node):
        
        return [f["_id"] for f in self.files.find({"nodes":node},fields=["_id"])]
        
    def addNode(self,node,data):
        data["lastUpdate"] = time.time()
        
        #print "node added %s : %s" % (node,data)
        
        if "files" in data:
            
            for f in data["files"]:
                self.addFileToNode(f,node)
                
            del data["files"]
        
        self.nodes[node] = data
        self.hasChanged=True
        
    def listNodes(self):
        return self.nodes.keys()
        
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


    def getMaxKnInNode(self, node, num=1):
        '''
        to write
        '''
        return [f["_id"] for f in self.files.find({"nuked":{ "$exists" : False}, "nodes":node },sort=[("kn",-1)],limit=num,fields=["_id"])]
        
    def getMinKnAll(self, num=1):
        '''
        to write
        '''

        return [f["_id"] for f in self.files.find({"nuked":{ "$exists" : False}},sort=[("kn",1)],limit=num,fields=["_id"])]
        
    def iterMinKnAll(self):
        for f in self.getMinKnAll(num=self.getCountAll()):
            yield f


    def getSizeAll(self):
        
        
                    
        size = self.files.map_reduce(SIZE_MAP,SIZE_REDUCE,query={"nuked":{ "$exists" : False}}).find_one({"_id":"size"})
        
        if size:
            return size["value"]
        else:
            return 0
    
    def getSizeInNode(self, node):
                    
        size = self.files.map_reduce(SIZE_MAP,SIZE_REDUCE,query={"nuked":{ "$exists" : False},"nodes":node}).find_one({"_id":"size"})
        
        if size:
            return size["value"]
        else:
            return 0

    def getCountAll(self):
        return self.files.find({"nuked":{ "$exists" : False}}).count()

    def getCountInNode(self, node):
        '''
        to write
        '''

        return self.files.find({"nuked":{ "$exists" : False},"nodes":node}).count()
