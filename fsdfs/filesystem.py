import os,sys,shutil,re,time

try:
    import simplejson as json
except:
    import json

import threading, shutil
import BaseHTTPServer, urllib2, urllib, urlparse
from hashlib import sha1

from SocketServer import ThreadingMixIn

from replicator import Replicator

sys.path.insert(0, os.path.dirname(__file__))
from filedb import loadFileDb

class Filesystem:
    
    def __init__(self,config):
    
        self.config = config
        
        self.host = self.config["host"]
        
        self.filedb = loadFileDb("memory",self)
        
        self.nodedb = {}
        
        self.ismaster = (self.config["master"]==self.config["host"])
    
    def getReplicationRules(self,filepath):
        return {
            "n":3
        }
        
    def getLocalFilePath(self,filepath):
        return os.path.join(self.config["datadir"],filepath)
    
    
    def getLocalFreeDisk(self):
        
        #test
        r = subprocess.Popen("/bin/df",["-kP",self.config["datadir"]]).read()
        return int(re.split("\s+",o.split("\n")[1])[3])
        
        
    
    def importFile(self,src,filepath,mode="copy"):
        
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
        
        self.filedb.update(filepath,{"nodes":set([self.host]),"t":int(time.time())})
        
        self.report()
    
    def start(self):
        
        
        self.httpinterface = HTTPInterface(self)
        self.httpinterface.start()
        
        self.report()
        
        if self.ismaster:
            self.replicator = Replicator(self)
            self.replicator.start()
        
        
    def stop(self):
        #print "stopping %s" % self.host
        self.httpinterface.server.shutdown()
        
        #self.httpinterface.join()
    
        if self.ismaster:
            self.replicator.shutdown()
    
    
    def searchFile(self,file):
        
        nodes = self.nodeRPC(self.config["master"],"SEARCH",{"filepath":file},parse=True)
        
        return nodes
    
    def nodeRPC(self,host,method,params,parse=False):
        
        params["_time"]=int(time.time())
        
        query = json.dumps(params)

        #print "http://%s/%s" % (host,method)
        ret = urllib2.urlopen("http://%s/%s" % (host,method),"h="+self.hashQuery(query)+"&p="+urllib.quote(query))
    
        if parse:
            return json.loads(ret.read())
        else:
            return ret
    
    def hashQuery(self,query):
        return sha1(sha1(query).hexdigest()+self.config["secret"]).hexdigest()
    
    def downloadFile(self,filepath):
        
        hosts = self.searchFile(filepath)
        
        for host in hosts:
            
            try:
                remote = self.nodeRPC(host,"DOWNLOAD",{"filepath":filepath})
            except Exception,e:
                print e
                continue
            
            #We don't need checksumming here... we're using TCP
            self.importFile(remote,filepath,mode="copyobj")
            
            remote.close()
            
            return True
            
        
        return False
    
    def report(self):
        self.nodeRPC(self.config["master"],"REPORT",self.getStatus())
        
    def getStatus(self):
        return {
            "node":self.host,
            #"df":self.getLocalFreeDisk(),
            "load":0
        }
        #"files":self.filedb.listAll()
        
    def getKnownNodes(self):
        return self.nodedb.keys()
        
  
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
                
            local = self.server.fs.getLocalFilePath(params["filepath"])
            
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
            
            
            #local = self.server.fs.getLocalFilePath(params["filepath"])
            
            downloaded = self.server.fs.downloadFile(params["filepath"])
        
            self.simpleResponse(200,"ok" if downloaded else "nok")
        
        
        elif p[1]=="STATUS":
            
            self.simpleResponse(200,json.dumps(self.server.fs.getStatus()))
        
        elif p[1]=="SEARCH":
            
            nodes = self.server.fs.filedb.getNodes(params["filepath"])
            
            self.simpleResponse(200,json.dumps(list(nodes)))
        
        
        elif p[1]=="REPORT":
            
            params["lastReport"] = time.time()
            self.server.fs.nodedb[params["node"]]=params
            
            self.simpleResponse(200,"ok")
    
    
    def simpleResponse(self,code,content):
        self.send_response(code)
        self.end_headers()
        self.wfile.write(content)
        self.connection.shutdown(1)
    
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
        
        qs = urlparse.parse_qs(data)
        
        #make sure the hash matches
        query = qs["p"][0]
        
        calcHash = self.server.fs.hashQuery(query)

        if calcHash!=qs["h"][0]:
            self.send_response(401)
            self.connection.shutdown(1)
            return False
        
        params = json.loads(query)
        
        #more than 1 day time diff, request is considered expired...
        if abs(int(params.get("_time",0))-time.time())>3600*24:
            self.send_response(401)
            self.connection.shutdown(1)
            return False
        
        return params
        