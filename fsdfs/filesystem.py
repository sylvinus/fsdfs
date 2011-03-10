import os
import sys
import re
import time
import copy
import threading
import shutil
import BaseHTTPServer
import urllib2
import urllib
import urlparse
import logging

try:
    import simplejson as json
except:
    import json

from hashlib import sha1
from SocketServer import ThreadingMixIn
from replicator import Replicator
sys.path.insert(0, os.path.dirname(__file__))
from filedb import loadFileDb

class Filesystem:
    '''
    This is the Filesystem class.
    
    It takes a dictionnary in parameter which contains the configuration of the pseudo-filesystem such as:
    
    * host -- IP:Port of the new node (ie: localhost:4242)
    * datadir -- path of the folder where the files are to be stored
    * secret -- password for joining the distributed filesystem
    * master -- the IP:Port of the master Filesystem server. If it's the same as host, then this server is the master
    * maxstorage -- A number of bytes (or a string like "10G") that are available for fsdfs
    '''
    
    def __init__(self, config):
        self.config = copy.deepcopy(config)
        
        
        self.startTime = time.time()
        
        # We didn't get any hostname but we got a port. Ask the master what is our IP then.
        if "host" not in self.config and "port" in self.config:
            self.debug("fsdfs only has port %s, autodetecting IP with master at %s ..." % (self.config["port"],self.config["master"]))
            
            for i in range(30):
                try:
                    ip = self.nodeRPC(self.config["master"],"GETIP",parse=True)
                    break
                except:
                    time.sleep(2)
                    pass
                    
            self.config["host"] = "%s:%s" % (ip,self.config["port"])
        
        
        self.nodedb = {}
        
        self.ismaster = (self.config["master"] == self.config["host"]) or (self.config["master"] is True)
        
        
        self.host = self.config["host"]

        if self.config["master"] is True:
            self.config["master"]=self.host

        
        if not self.config.get("maxstorage", False):
            self.maxstorage = 10 * 1024 * 1024 * 1024 #default 10G
        elif type(self.config["maxstorage"]) == int:
            self.maxstorage = self.config["maxstorage"]
        elif re.match("[0-9]+G", self.config["maxstorage"]):
             self.maxstorage=int(self.config["maxstorage"][0:-1]) * 1024 * 1024 * 1024
        else:
            raise Exception, "Unknown maxstorage format"
            
        self.debug("fsdfs node starting on %s ; master is %s" % (self.host,self.config["master"]))
    
    def getReplicationRules(self, filepath):
        '''
        Return the replication level.
        '''
        
        return {
            "n": 3
        }
    
    def getLocalFilePath(self, filepath):
        '''
        Return the absolute local path of a file.
        '''
        
        return os.path.join(self.config["datadir"], filepath)
        
    def debug(self,msg,type="debug"):
        logging.debug("%s - %s" % (type,msg))
        
    def error(self,msg):
        logging.error(msg)
    
    
    def deleteFile(self, filepath):
        '''
        Delete a file from the database.
        '''
        
        destpath = self.getLocalFilePath(filepath)
        
        if not os.path.isfile(destpath):
            return True
        
        try:
            os.unlink(destpath)
            self.filedb.removeFileFromNode(filepath, self.host)
            self.report()
            return True
        except:
            return False
    
    def nukeFile(self, filepath):
        '''
        Deletes a file on all the nodes
        '''
        
        if not self.ismaster:
            return False
        else:
            self.filedb.update(filepath,{"nuked": time.time()})
            
            return True
    
    def importFile(self, src, filepath, mode="copy"):
        '''
        Adds a file to the global filesystem
        '''
        
        destpath = self.getLocalFilePath(filepath)
        
        if not os.path.isdir(os.path.dirname(destpath)):
            os.makedirs(os.path.dirname(destpath))
        
        if mode == "download" or ((type(src)==str or type(src)==unicode) and src.startswith("http://")):
            
            #todo do we need further checks of complete transfer with this?
            src = urllib2.urlopen(src,None,timeout=60)
            mode = "copyobj"
            
        if mode == "copy":
            shutil.copy(src, destpath)
        elif mode == "move":
            shutil.move(src, destpath)
        elif mode == "copyobj":
            f = open(destpath, "wb")
            shutil.copyfileobj(src, f)
            try:
                f.close()
                src.close()
            except:
                pass
        
        size = os.stat(destpath).st_size
        
        self.filedb.update(filepath, {
            "nodes": set([self.host]).union(self.filedb.getNodes(filepath)),
            "t": int(time.time()),
            "size": size,
            "nuked":None,
            "n":self.getReplicationRules(filepath)["n"]
            })
        
        self.report()

    
    def start(self):
        '''
        Start the node
        If the server is master, also start the replicator.
        '''
        #print "starting %s" % self.host
        
        filedb_options = {}
        filedb_backend = self.config.get("filedb","memory")
        if type(filedb_backend)==dict:
            filedb_options=filedb_backend
            filedb_backend=filedb_options["backend"]
        
        self.filedb = loadFileDb(filedb_backend, self, filedb_options)
        
        
        
        self.httpinterface = HTTPInterface(self)
        self.httpinterface.start()
        
        #self.report()
        
        self.reporter = Reporter(self)
        self.reporter.start()
        
        if self.ismaster:
            self.replicator = Replicator(self)
            self.replicator.start()
        
    
    def stop(self):
        '''
        Stops the filesystem
        '''
        
        #print "stopping %s" % self.host
        self.httpinterface.server.shutdown()
        
        self.httpinterface.server.server_close()
        
        self.reporter.shutdown()
        self.reporter.join()
        
        #self.httpinterface.join()
        
        if self.ismaster:
            self.replicator.shutdown()
            
            self.replicator.join()
    
    
    def searchFile(self, file):
        '''
        Returns the nodes where a file is stored
        '''
        
        nodes = self.nodeRPC(self.config["master"], "SEARCH", {"filepath": file}, parse=True)
        
        return nodes
    
    def nodeRPC(self,host,method,params={},parse=False):
        '''
        Inter-node communication method
        '''
        
        params["_time"] = int(time.time())
        
        query = json.dumps(params)
        
        #print "http://%s/%s" % (host,method)
        ret = urllib2.urlopen("http://%s/%s" % (host, method),"h=" + self.hashQuery(query) + "&p=" + urllib.quote(query),timeout=60)
        
        if parse:
            return json.loads(ret.read())
        else:
            return ret
    
    def hashQuery(self, query):
        '''
        to write.
        '''
        
        return sha1(sha1(query).hexdigest() + self.config["secret"]).hexdigest()
    
    def downloadFile(self, filepath):
        '''
        Downloads a file from the global filesystem to the local server
        '''
        
        hosts = self.searchFile(filepath)
        
        for host in hosts:
            try:
                remote = self.nodeRPC(host, "DOWNLOAD", {"filepath": filepath})
            except Exception, err:
                #print err
                continue
            
            #We don't need checksumming here... we're using TCP
            self.importFile(remote, filepath, mode="copyobj")
            
            remote.close()
            
            return True
        
        
        return False
    
    def report(self):
        '''
        Sends the local status to the master server
        '''
        if self.ismaster:
            self.addNode(self.host,self.getStatus())
        else:
            self.nodeRPC(self.config["master"], "REPORT", self.getStatus())
    
    def addNode(self,node,status):
        status["lastReport"] = time.time()
        
        if not node in self.nodedb:
            self.filedb.hasChanged=True
            
        self.nodedb[node] = status

    
    def getGlobalStatus(self):
        
        '''
        Returns the global status of the distributed filesystem
        '''
        
        if not self.ismaster:
            return self.nodeRPC(self.config["master"], "GLOBALSTATUS", parse=True)
        else:
            
            status = self.getStatus()
            
            minKns = [(self.filedb.getKn(f),f) for f in self.filedb.getMinKnAll(num=1)]
            
            status["nodes"] = self.nodedb
            status["sizeGlobal"] = self.filedb.getSizeAll()
            status["countGlobal"] = self.filedb.getCountAll()
            status["minKnGlobal"] = minKns
            
            #pass thru JSON to have the same exact returns as if in remote fetch
            return json.loads(json.dumps(status))

    
    def getStatus(self):
        '''
        Return the status of a Filesystem server.
        
        It's a dictionnary which these informations:
        * node -- the host
        * df -- the free space left
        * load -- system load
        * uptime -- program's uptime in seconds
        * size -- the size of the node (sum of the files)
        '''
        
        return {
            "node": self.host,
            "df": self.maxstorage - self.filedb.getSizeInNode(self.host),
            "uptime":int(time.time()-self.startTime),
            "load": 0,
            "size": self.filedb.getSizeInNode(self.host)
        }
        #"files":self.filedb.listAll()
    
    def getKnownNodes(self):
        '''
        to write.
        '''
        
        return self.nodedb.keys()
  

class Reporter(threading.Thread):

    def __init__(self, fs):
        threading.Thread.__init__(self)
        self.fs = fs
        self.stopnow = False
        
    def run(self):

        while not self.stopnow:
            try:
                self.fs.report()
            except Exception,e:
                self.fs.error("While reporting : %s" % e)
                
            [time.sleep(1) for i in range(60) if not self.stopnow]
        
    def shutdown(self):

        self.stopnow = True

class HTTPInterface(threading.Thread):
    
    def __init__(self, fs):
        threading.Thread.__init__(self)
        self.server = ThreadingBaseHTTPServer((re.split(":", fs.host)[0], int(re.split(":", fs.host)[1])), myHandler)
        self.server.setFS(fs)
    
    def run(self):
        
        self.server.serve_forever(poll_interval=0.1)
        
            


class ThreadingBaseHTTPServer(ThreadingMixIn,BaseHTTPServer.HTTPServer):
    daemon_threads=False
    allow_reuse_address=1 # ?
    
    def setFS(self,fs):
        self.fs = fs
        


class myHandler(BaseHTTPServer.BaseHTTPRequestHandler):
    
    def log_message(self,format,*args):
        self.server.fs.debug("%s - - [%s] %s" % (self.address_string(), self.log_date_time_string(), format%args),"request")
    
    #def log_request(self, code='-', size='-'):
    #    print "haaa"
    
    def do_POST(self):
        
        try:
            
            #print "got %s at %s" % (self.path,self.server.fs.host)
        
            params = self._getPostParams()
        
            if params == False:
                return
        
            p = self.path.split("/")
        
            if p[1] == "DOWNLOAD":
            
                local = self.server.fs.getLocalFilePath(params["filepath"])
            
                if not os.path.isfile(local):
                    self.send_response(404)
                else:
                    self.send_response(200)
                    self.end_headers()
                
                    f = open(local, "rb")
                    shutil.copyfileobj(f, self.wfile)
                    f.close()
            
                self.connection.shutdown(1)
        
        
            elif p[1] == "DELETE":
            
                deleted = self.server.fs.deleteFile(params["filepath"])
            
                self.simpleResponse(200, "ok" if deleted else "nok")
        
        
            elif p[1] == "IMPORT":

                self.server.fs.importFile(params["url"],params["filepath"])

                self.simpleResponse(200, "ok")

        
            elif p[1]=="NUKE":
            
                nuked = self.server.fs.nukeFile(params["filepath"])
            
                if not nuked:
                    self.simpleResponse(403,"can only nuke files on master")
                else:
                    self.simpleResponse(200,"ok")
        
        
            elif p[1] == "SUGGEST":
            
                #local = self.server.fs.getLocalFilePath(params["filepath"])
            
                downloaded = self.server.fs.downloadFile(params["filepath"])
            
                self.simpleResponse(200,"ok" if downloaded else "nok")
        
        
            elif p[1] == "STATUS":
            
                self.simpleResponse(200,json.dumps(self.server.fs.getStatus()))
        
            elif p[1] == "GLOBALSTATUS":

            
                if not self.server.fs.ismaster:
                    self.simpleResponse(403,"not on master")
                else:
                    status = self.server.fs.getGlobalStatus()
                
                    self.simpleResponse(200,json.dumps(status))

        
        
            elif p[1] == "SEARCH":
            
                nodes = self.server.fs.filedb.getNodes(params["filepath"])
            
                self.simpleResponse(200,json.dumps(list(nodes)))
        
            elif p[1] == "GETIP":
            
                self.simpleResponse(200,json.dumps(self.client_address[0]))
        
        
            elif p[1] == "REPORT":
            
                self.server.fs.addNode(params["node"],params)
            
                self.simpleResponse(200,"ok")
                
            elif p[1] == "RAISE":
                
                #error test
                raise Exception, "test error"
                
        except Exception,e:
            
            self.simpleResponse(500,str(e))
            
            self.server.fs.error("When serving %s : %s" % (p,e))
            
            
            
    
    def simpleResponse(self, code, content):
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
        
        if calcHash != qs["h"][0]:
            self.send_response(401)
            self.connection.shutdown(1)
            return False
        
        params = json.loads(query)
        
        #more than 1 day time diff, request is considered expired...
        if abs(int(params.get("_time",0)) - time.time()) > 3600 * 24:
            self.send_response(401)
            self.connection.shutdown(1)
            return False
        
        return params
        
