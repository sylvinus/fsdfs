import os,sys,shutil,re,time

import threading, shutil
import BaseHTTPServer, urllib2, urllib, urlparse
from hashlib import sha1

from SocketServer import ThreadingMixIn


class Filesystem:
    
    def __init__(self,config):
    
        self.config = config
        
        self.host = self.config["host"]
        
        self.filedb = {}
        
        self.nodedb = {}
    
    def getReplicationRules(self,filepath):
        return {
            "n":3
        }
        
    def getLocalFilePath(self,filepath):
        return os.path.join(self.config["datadir"],filepath)
    

    def sortFilesByReplicationNeeds(self,fileList):
        
        knownNeeds = []
        
        for f in fileList:
            copies = self.searchFile(f)
            rules = self.getReplicationRules(f)
            
            knownNeeds.append((f,rules["n"] - len(copies),copies))
            
        knownNeeds.sort(cmp=lambda x,y:cmp(x[1],y[1]),reverse=True)
    
        return knownNeeds

    def watchReplication(self,maxOperations=100):
        
        knownNeeds = self.sortFilesByReplicationNeeds(self.getKnownFiles())
        
        self.suggestFiles(knownNeeds[:maxOperations])


    def getLocalFiles(self):
        return self.filedb.keys()
    
    #
    # Get the highest replication factor of any local file
    #
    def getMaxLocalReplicationFactor(self):
        
        knownNeeds = self.sortFilesByReplicationNeeds(self.getLocalFiles())
        
        if len(knownNeeds)==0:
            return None
        else:
            return knownNeeds[0][1]
            
    
    
    def getLocalFreeDisk(self):
        
        #test
        r = subprocess.Popen("/bin/df",["-kP","./"]).read()
        return int(re.split("\s+",o.split("\n")[1])[3])
        
        
    
    #
    # suggestList elements are (filepath,known need,known copies)
    #
    def suggestFiles(self,suggestList):
        
        knownNodes = self.getKnownNodes()
        
        if self.host in knownNodes:
            knownNodes.remove(self.host)
        
        for (f,n,copies) in suggestList:
            
            newnodes = []
            for node in knownNodes:
                if node not in copies:
                    newnodes.append(node)
                    
            #todo ceil?
            for y in range(min(len(newnodes),int(n+0.99999))):
                self.nodeRPC(newnodes[y],"SUGGEST",{"filepath":f}).read()
            
                
    
    
    def addFileToNode(self,src,filepath,mode="copy"):
        
        destpath = self.getLocalFilePath(filepath)
        
        if not os.path.isdir(os.path.dirname(destpath)):
            os.makedirs(os.path.dirname(destpath))
        
        if mode=="copy":
            shutil.copy(src,destpath)
        elif mode=="move":
            shutil.move(src,destpath)
        elif mode=="copyobj":
            f = open(destpath,"wb")
            shutil.copyfileobj(src,f)
            f.close()
        
        self.filedb[filepath]=True
        
        self.report()
    
    
    def report(self):
        
        self.reportFiles(self.getLocalFiles())
    
    def start(self):
        
        
        self.httpinterface = HTTPInterface(self)
        self.httpinterface.start()
        
        self.report()
        
        
    def stop(self):
        #print "stopping %s" % self.host
        self.httpinterface.server.shutdown()
        
        #self.httpinterface.join()
    
    
    def nodeRPC(self,host,method,params):
        
        params["_time"]=int(time.time())
        
        query = urllib.urlencode(params)
        
        #print "http://%s/%s" % (host,method)
        return urllib2.urlopen("http://%s/%s" % (host,method),"_h="+self.hashQuery(query)+"&"+query)
    
    def hashQuery(self,query):
        return sha1(sha1(query).hexdigest()+self.config["secret"]).hexdigest()
    
    def importFile(self,localpath,filepath):
        
        #1. import file in the current node
        
        self.addFileToNode(localpath,filepath,mode="copy")
        
        #2. start its replication
        
        rules = self.getReplicationRules(filepath)
        
        self.suggestFiles([(filepath,rules["n"]-1,[])])
        
        return True
    
       
    def fetchFile(self,filepath):
        
        hosts = self.searchFile(filepath)
        
        for host in hosts:
            
            try:
                remote = self.nodeRPC(host,"DOWNLOAD",{"filepath":filepath})
            except Exception,e:
                print e
                continue
            
            #We don't need checksumming here... we're using TCP
            self.addFileToNode(remote,filepath,mode="copyobj")
            
            remote.close()
            
            return True
            
        
        return False
        
  
class HTTPInterface(threading.Thread):
    
    def __init__(self,fs):
        threading.Thread.__init__(self)
        self.server = ThreadingBaseHTTPServer((re.split(":",fs.host)[0],int(re.split(":",fs.host)[1])),myHandler)
        self.server.setFS(fs)
        
    def run(self):
        
        self.server.serve_forever(poll_interval=0.1)
            

class ThreadingBaseHTTPServer(ThreadingMixIn,BaseHTTPServer.HTTPServer):
    daemon_threads=False
    allow_reuse_address=True # ?
    
    def setFS(self,fs):
        self.fs = fs


class myHandler(BaseHTTPServer.BaseHTTPRequestHandler):
    
    
        
    def do_POST(self):
        
        #print "got %s at %s" % (self.path,self.server.fs.host)
        
        params = self._getPostParams()
        
        if params==False:
            return
        
        p = self.path.split("/")
        
        if p[1]=="DOWNLOAD":
                
            local = self.server.fs.getLocalFilePath(params["filepath"][0])
            
            if not os.path.isfile(local):
                self.send_response(404)
            else:
                self.send_response(200)
                self.end_headers()
                
                f = open(local,"rb")
                shutil.copyfileobj(f,self.wfile)
                f.close()
            
            self.connection.shutdown(1)
        
        elif p[1]=="SUGGEST":
            
            self.send_response(200,"ok")
            self.connection.shutdown(1)
        
            
            
            local = self.server.fs.getLocalFilePath(params["filepath"][0])
            
            #print "local is %s" % local
            
            if not os.path.isfile(local):
                
                #print "fetch!!"
                
                #fetch it
                self.server.fs.fetchFile(params["filepath"][0])
        
        
        elif p[2]=="STATUS":
            
            self.send_response(200,"ok")
            self.connection.shutdown(1)
        
            
            
            local = self.server.fs.getLocalFilePath(params["filepath"][0])
            
            #print "local is %s" % local
            
            if not os.path.isfile(local):
                
                #print "fetch!!"
                
                #fetch it
                self.server.fs.fetchFile(params["filepath"][0])
                
    
    
    def _getPostParams(self):
        
        #why the f*** isn't this in the python stdlib?
        max_chunk_size = 1024
        size_remaining = int(self.headers["content-length"])
        L = []
        while size_remaining:
            chunk_size = min(size_remaining, max_chunk_size)
            L.append(self.rfile.read(chunk_size))
            size_remaining -= len(L[-1])
        data = ''.join(L)
        

        #make sure the hash matches
        calcHash = self.server.fs.hashQuery(data[44:])
        
        if calcHash!=data[3:43]:
            self.send_response(401)
            self.connection.shutdown(1)
            return False
        
        params = urlparse.parse_qs(data[44:])
        
        #more than 1 day time diff, request is considered expired...
        if abs(int(params.get("_time",[0])[0])-time.time())>3600*24:
            self.send_response(401)
            self.connection.shutdown(1)
            return False
        
        return params
        