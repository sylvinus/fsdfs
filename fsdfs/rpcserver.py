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
import random
import urlparse
import logging
import socket

try:
    import simplejson as json
except:
    import json

from hashlib import sha1
from SocketServer import ThreadingMixIn
from replicator import Replicator
sys.path.insert(0, os.path.dirname(__file__))
from filedb import loadFileDb
from reporter import Reporter

    
class RPCServer(threading.Thread):
    
    def __init__(self, fs):
        threading.Thread.__init__(self)
        self.server = ThreadingBaseHTTPServer((re.split(":", fs.host)[0], int(re.split(":", fs.host)[1])), myHandler)
        self.server.setFS(fs)
    
    def run(self):
        
        self.server.serve_forever(poll_interval=0.1)
        
            


class ThreadingBaseHTTPServer(ThreadingMixIn,BaseHTTPServer.HTTPServer):
    daemon_threads=False
    allow_reuse_address=1
    
    def setFS(self,fs):
        self.fs = fs
        


class myHandler(BaseHTTPServer.BaseHTTPRequestHandler):
    
    def log_message(self,format,*args):
        self.server.fs.debug("%s - - [%s] %s" % (self.address_string(), self.log_date_time_string(), format%args),"request")
    
    #def log_request(self, code='-', size='-'):
    #    print "haaa"
    
    def do_POST(self):
        
        
        #force no keepalive
        self.close_connection = 1
        
        
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

                if self.server.fs.importFile(params["url"],params["filepath"]):
                    self.simpleResponse(200, "ok")
                else:
                    self.simpleResponse(503, "couldn't make space for this file")

        
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
            
                self.simpleResponse(200,self.server.fs.getStatus())
        
            elif p[1] == "GLOBALSTATUS":

            
                if not self.server.fs.ismaster:
                    self.simpleResponse(403,"not on master")
                else:
                    status = self.server.fs.getGlobalStatus()
                
                    self.simpleResponse(200,status)

        
        
            elif p[1] == "SEARCH":
            
                nodes = self.server.fs.filedb.getNodes(params["filepath"])
                
                #randomize nodes and always put the master at the end to avoid overloading it
                
                nodes = list(nodes)
                random.shuffle(nodes)
                
                master = self.server.fs.config["master"]
                if master in nodes:
                    nodes.remove(master)
                    nodes.append(master)
                
                self.simpleResponse(200,nodes)
        
            elif p[1] == "GETIP":
            
                self.simpleResponse(200,self.client_address[0])
        
        
            elif p[1] == "REPORT":
            
                if not self.server.fs.ismaster:
                    self.simpleResponse(403,"not on master")
                else:
                    
                    #answer before because addNode can take lots of time if getting first packet with_files
                    self.simpleResponse(200,"ok")
                    
                    self.server.fs.filedb.addNode(params["node"],params)
            
                    
                
                
                
            elif p[1] == "RAISE":
                
                #error test
                raise Exception, "test error"
                
        except Exception,e:
            
            self.simpleResponse(500,str(e))
            
            self.server.fs.error("When serving %s %s : %s" % (self.path,params,e))
            
            
    def simpleResponse(self, code, content):
        
        self.send_response(code)
        self.send_header("Access-Control-Allow-Origin","*")
        self.end_headers()
        self.wfile.write(json.dumps(content))
        self.connection.shutdown(socket.SHUT_RDWR)
        
        try:
            self.finish()
        except:
            pass
    
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
        
        
        if not "p" in qs:
            self.send_response(403)
            self.send_header("Access-Control-Allow-Origin","*")
            self.connection.shutdown(1)
            return False
        
        #make sure the hash matches
        query = qs["p"][0]
        
        calcHash = self.server.fs.hashQuery(query)
        
        
        
        if calcHash != qs["h"][0]:
            print "*"*200
            print calcHash,qs["h"][0],query
            self.send_response(401)
            self.send_header("Access-Control-Allow-Origin","*")
            self.connection.shutdown(1)
            return False
        
        params = json.loads(query)
        
        #more than 1 day time diff, request is considered expired...
        if abs(int(params.get("_time",0)) - time.time()) > 3600 * 24:
            self.send_response(401)
            self.send_header("Access-Control-Allow-Origin","*")
            self.connection.shutdown(1)
            return False
        
        return params
