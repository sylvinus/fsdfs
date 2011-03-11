#!/usr/bin/env python
# encoding: utf-8

import os, sys, re
from time import sleep
import unittest
import threading
import shutil,time


import logging
logging.basicConfig(level=logging.DEBUG)

sys.path.insert(0, os.path.join('.'))
from fsdfs.filesystem import Filesystem


class TestFS(Filesystem):
    pass
    
   
class basicTests(unittest.TestCase):
    filedb = "memory"
    
    def setUp(self):
        if os.path.exists("./tests/datadirs"):
			shutil.rmtree("./tests/datadirs")
        os.makedirs("./tests/datadirs")
        
       
    def testFileDownloadImport(self):
    
        secret = "azpdoazrRR"
    
        nodeA = TestFS({
            "host":"localhost:42342",
            "datadir":"./tests/datadirs/A",
            "secret":secret,
            "master":"localhost:42342",
            "filedb":self.filedb
        })
    
        nodeB = TestFS({
            "host":"localhost:42352",
            "datadir":"./tests/datadirs/B",
            "secret":secret,
            "master":"localhost:42342",
            "filedb":self.filedb
        })
    
        nodeA.start()
        nodeB.start()
    
        nodeA.filedb.reset()
        nodeB.filedb.reset()
    
        nodeA.importFile("http://api.jamendo.com/get2/stream/track/redirect/?id=241","dir1/dir2/filename.ext")
    
        sleep(5)
    
        #file should be on B
        statusB = nodeB.getStatus()

        self.assertTrue(statusB["size"]>3*1024*1024)
        
        #try to import it from B !
        
        nodeB.nodeRPC(nodeB.config["master"],"IMPORT",{"url":"http://api.jamendo.com/get2/stream/track/redirect/?id=242","filepath":"dir1/dir2/filename2.ext"})
        
        sleep(5)
        
        #file should be on B
        statusB = nodeB.getStatus()

        self.assertTrue(statusB["size"]>5*1024*1024)


        
        nodeA.stop()
        nodeB.stop()

        
        
    def testTwoNodes(self):
        
        secret = "azpdoazrRR"
        
        nodeA = TestFS({
            "host":"localhost:42342",
            "datadir":"./tests/datadirs/A",
            "secret":secret,
            "master":"localhost:42342",
            "filedb":self.filedb
        })
        
        nodeB = TestFS({
            "host":"localhost:42352",
            "datadir":"./tests/datadirs/B",
            "secret":secret,
            "master":"localhost:42342",
            "filedb":self.filedb
        })
        
        nodeA.start()
        nodeB.start()
        
        nodeA.filedb.reset()
        nodeB.filedb.reset()
        

        
        
        nodeA.importFile("./tests/fixtures/test.txt","dir1/dir2/filename.ext")
        nodeA.importFile("./tests/fixtures/test2.txt","dir3/dir4/filename2.ext")
        
        self.assertEquals(-2,nodeA.filedb.getKn("dir3/dir4/filename2.ext"))
        
        sleep(5)
        
        self.assertEquals(2,nodeA.filedb.getCountInNode("localhost:42352"))
        self.assertEquals(26+13,nodeA.filedb.getSizeInNode("localhost:42352"))
        self.assertEquals(2,nodeA.filedb.getCountAll())
        self.assertEquals(26+13,nodeA.filedb.getSizeAll())
        
        
        self.assertEquals(open(nodeB.getLocalFilePath("dir1/dir2/filename.ext")).read(),open("./tests/fixtures/test.txt").read())
        self.assertEquals(open(nodeA.getLocalFilePath("dir3/dir4/filename2.ext")).read(),open("./tests/fixtures/test2.txt").read())
        
        globalStatusA=nodeA.getGlobalStatus()
        globalStatusB=nodeB.getGlobalStatus()

        self.assertTrue(4<=globalStatusA["uptime"])
        self.assertTrue(6>=globalStatusA["uptime"])
        
        
        del globalStatusA["uptime"]
        del globalStatusB["uptime"]
        
        self.assertEquals(globalStatusA,globalStatusB)
        
        self.assertEquals(2,globalStatusA["countGlobal"])
        self.assertEquals(-1,globalStatusA["minKnGlobal"][0][0])
        self.assertEquals(2,len(globalStatusA["nodes"].keys()))
        self.assertEquals("localhost:42342",globalStatusA["node"])
        
        
        nodeA.nukeFile("dir1/dir2/filename.ext")
        
        sleep(2)
        
        self.assertFalse(os.path.isfile(nodeA.getLocalFilePath("dir1/dir2/filename.ext")))
        self.assertFalse(os.path.isfile(nodeB.getLocalFilePath("dir1/dir2/filename.ext")))


        
        nodeA.stop()
        nodeB.stop()
        
    
    def testManyNodes(self):
        
        secret = "azpdoazrRR"
        
        numNodes = 20
        
        nodes = []
        for i in range(numNodes):
            
            opts = {
                "port":(42362+2*i),
                "datadir":"./tests/datadirs/node%s" % i,
                "secret":secret,
                "master":"localhost:%s"%(42362+2*0),
                "filedb":self.filedb
            }
            
            if i==0:
                opts["master"]=True
                opts["host"]="localhost:%s" % opts["port"]

            nodes.append(TestFS(opts))
            nodes[i].start()
            nodes[i].filedb.reset()
            
        
        nodes[0].importFile("./tests/fixtures/test.txt","dir1/dir2/filename.ext")
        nodes[0].importFile("./tests/fixtures/test2.txt","dir3/dir4/filename2.ext")
        
        #max repl/sec = 1 new node per file
        sleep(numNodes*1.1*0.5)
        
        
        for node in nodes:
            self.assertEquals(open(node.getLocalFilePath("dir1/dir2/filename.ext")).read(),open("./tests/fixtures/test.txt").read())
            self.assertEquals(open(node.getLocalFilePath("dir3/dir4/filename2.ext")).read(),open("./tests/fixtures/test2.txt").read())
        
        
        #todo stop one node and make it come back after nuke
        
        nodes[0].nukeFile("dir1/dir2/filename.ext")
        
        
        
        sleep(3)
        
        for node in nodes:
            self.assertFalse(os.path.isfile(node.getLocalFilePath("dir1/dir2/filename.ext")))
            self.assertEquals(open(node.getLocalFilePath("dir3/dir4/filename2.ext")).read(),open("./tests/fixtures/test2.txt").read())
        
        
        
        nodes[0].importFile("./tests/fixtures/test2.txt","dir1/dir2/filename.ext")
        
        #max repl/sec = 1 new node per file
        sleep(numNodes*1.1*0.5)
        
        for node in nodes:
            self.assertEquals(open(node.getLocalFilePath("dir1/dir2/filename.ext")).read(),open("./tests/fixtures/test2.txt").read())
            self.assertEquals(open(node.getLocalFilePath("dir3/dir4/filename2.ext")).read(),open("./tests/fixtures/test2.txt").read())
        
        
        print "Stopping %s nodes... takes a few seconds" % numNodes
        for node in nodes:
            node.stop()
            
        
        
        
if __name__ == '__main__':
    unittest.main()
