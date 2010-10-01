#!/usr/bin/env python
# encoding: utf-8

import os, sys, re
from time import sleep
import unittest
import threading
import shutil


import logging
logging.basicConfig(level=logging.DEBUG)

sys.path.insert(0, os.path.join('.'))


from fsdfs.filesystem import Filesystem


masterlock=threading.RLock()
FILEAVAILABILITY={}
KNOWNNODES = set()

class TestFS(Filesystem):
    
    _rules = {"n":2}
    
    def getReplicationRules(self,file):
        return self._rules
    
    def getKnownNodes(self):
        
        #TODO shuffle
        return list(KNOWNNODES)

    def getKnownFiles(self):
        
        return FILEAVAILABILITY.keys()
        
        
    def searchFile(self,file):
        
        return FILEAVAILABILITY.get(file,[])

    
    def reportFiles(self,files):
        
        masterlock.acquire()
        try:
            KNOWNNODES.add(self.host)
            
            for f in files:
                FILEAVAILABILITY.setdefault(f,[])
                FILEAVAILABILITY[f] = list(set(FILEAVAILABILITY[f]+[self.host]))
                
        finally:
            masterlock.release()
              
    
   
class basicTests(unittest.TestCase):
    def setUp(self):
        
        global FILEAVAILABILITY,KNOWNNODES
        
        shutil.rmtree("./tests/datadirs")
        os.makedirs("./tests/datadirs")
        
        FILEAVAILABILITY={}
        KNOWNNODES = set()  
        
    def testTwoNodes(self):
        
        secret = "azpdoazrRR"
        
        nodeA = TestFS({
            "host":"localhost:42342",
            "datadir":"./tests/datadirs/A",
            "secret":secret
        })
        
        nodeB = TestFS({
            "host":"localhost:42352",
            "datadir":"./tests/datadirs/B",
            "secret":secret
        })
        
        nodeA.start()
        nodeB.start()
        
        
        
        nodeA.importFile("./tests/fixtures/test.txt","dir1/dir2/filename.ext")
        nodeB.importFile("./tests/fixtures/test2.txt","dir3/dir4/filename2.ext")
        
        
        sleep(2)
        
        self.assertEquals(open(nodeB.getLocalFilePath("dir1/dir2/filename.ext")).read(),open("./tests/fixtures/test.txt").read())
        self.assertEquals(open(nodeA.getLocalFilePath("dir3/dir4/filename2.ext")).read(),open("./tests/fixtures/test2.txt").read())
        
        nodeA.stop()
        nodeB.stop()
        
    
    def testManyNodes(self):
        
        secret = "azpdoazrRR"
        
        numNodes = 100
        
        nodes = []
        for i in range(numNodes):
            nodes.append(TestFS({
                "host":"localhost:%s"%(42362+2*i),
                "datadir":"./tests/datadirs/node%s" % i,
                "secret":secret
            }))
            nodes[i]._rules["n"]=42
            nodes[i].start()
            
        
        nodes[42].importFile("./tests/fixtures/test.txt","dir1/dir2/filename.ext")
        nodes[47].importFile("./tests/fixtures/test2.txt","dir3/dir4/filename2.ext")
        
        
        sleep(1)
        
        
        for node in nodes:
            node.fetchFile("dir1/dir2/filename.ext")
            node.fetchFile("dir3/dir4/filename2.ext")
        
        sleep(1)
        
        for node in nodes:
            self.assertEquals(open(node.getLocalFilePath("dir1/dir2/filename.ext")).read(),open("./tests/fixtures/test.txt").read())
            self.assertEquals(open(node.getLocalFilePath("dir3/dir4/filename2.ext")).read(),open("./tests/fixtures/test2.txt").read())
        
        print "Stopping %s nodes... takes a few seconds" % numNodes
        for node in nodes:
            node.stop()
            
        
        
        
if __name__ == '__main__':
    unittest.main()