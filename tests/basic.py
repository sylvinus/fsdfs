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
    
    """
    filedb = "memory"
    """
    filedb = {
        "backend":"mongodb",
        "host":"localhost",
        "db":"fsdfs_test",
        "port":27017
    }
    
    
    def setUp(self):
        if os.path.exists("./tests/datadirs"):
			shutil.rmtree("./tests/datadirs")
        os.makedirs("./tests/datadirs")
        
       
    def _testFileDownloadImport(self):
    
        secret = "azpdoazrRR"
    
        nodeA = TestFS({
            "host":"localhost:42342",
            "datadir":"./tests/datadirs/A",
            "secret":secret,
            "resetFileDbOnStart":True,
            "master":"localhost:42342",
            "filedb":self.filedb
        })
    
        nodeB = TestFS({
            "host":"localhost:42352",
            "datadir":"./tests/datadirs/B",
            "secret":secret,
            "resetFileDbOnStart":True,
            "master":"localhost:42342",
            "filedb":self.filedb
        })
    
        nodeA.start()
        nodeB.start()
    
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
            "resetFileDbOnStart":True,
            "replicatorIdleTime":2,
            "replicatorSkipMaster":False,
            "master":"localhost:42342",
            "filedb":self.filedb
        })
        
        nodeB = TestFS({
            "host":"localhost:42352",
            "datadir":"./tests/datadirs/B",
            "secret":secret,
            "resetFileDbOnStart":False,
            "replicatorIdleTime":2,
            "master":"localhost:42342",
            "filedb":self.filedb
        })
        
        nodeA.start()
        nodeB.start()

        sleep(1)
        
        self.assertEquals(0,nodeA.filedb.getCountInNode("localhost:42352"))
        self.assertEquals(0,nodeA.filedb.getSizeInNode("localhost:42352"))
        self.assertEquals(0,nodeA.filedb.getCountAll())
        self.assertEquals(0,nodeA.filedb.getSizeAll())
        
        nodeA.importFile("./tests/fixtures/test.txt","dir1/dir2/filename.ext")
        nodeA.importFile("./tests/fixtures/test2.txt","dir3/dir4/filename2.ext")
        
        self.assertEquals(-2,nodeA.filedb.getKn("dir3/dir4/filename2.ext"))
        
        sleep(6)
        
        print "files : %s" % {
            "A":nodeA.filedb.listInNode(nodeA.host),
            "B":nodeA.filedb.listInNode(nodeB.host)
        }
        
        
        #self.assertEquals(2,nodeB.filedb.getCountInNode(nodeB.host))
        self.assertEquals(2,nodeA.filedb.getCountInNode(nodeB.host))
        self.assertEquals(26+13,nodeA.filedb.getSizeInNode("localhost:42352"))
        self.assertEquals(2,nodeA.filedb.getCountAll())
        self.assertEquals(26+13,nodeA.filedb.getSizeAll())
        
        
        self.assertEquals(open(nodeB.getLocalFilePath("dir1/dir2/filename.ext")).read(),open("./tests/fixtures/test.txt").read())
        self.assertEquals(open(nodeA.getLocalFilePath("dir3/dir4/filename2.ext")).read(),open("./tests/fixtures/test2.txt").read())
        
        globalStatusA=nodeA.getGlobalStatus()
        globalStatusB=nodeB.getGlobalStatus()

        self.assertTrue(4<=globalStatusA["uptime"])
        self.assertTrue(9>=globalStatusA["uptime"])
        
        
        del globalStatusA["uptime"]
        del globalStatusB["uptime"]
        
        self.assertEquals(globalStatusA,globalStatusB)
        
        self.assertEquals(2,globalStatusA["countGlobal"])
        self.assertEquals(-1,globalStatusA["minKnGlobal"][0][0])
        self.assertEquals(2,len(globalStatusA["nodes"].keys()))
        self.assertEquals("localhost:42342",globalStatusA["node"])
        
        
        nodeA.nukeFile("dir1/dir2/filename.ext")
        
        self.assertTrue(nodeA.filedb.isNuked("dir1/dir2/filename.ext"))
        
        sleep(2)
        
        self.assertFalse(os.path.isfile(nodeA.getLocalFilePath("dir1/dir2/filename.ext")))
        self.assertFalse(os.path.isfile(nodeB.getLocalFilePath("dir1/dir2/filename.ext")))


        
        nodeA.stop()
        nodeB.stop()
        
    
    def _testManyNodes(self):
        
        secret = "azpdoazrRR"
        
        numNodes = 100
        
        nodes = []
        for i in range(numNodes):
            
            opts = {
                "port":(42362+2*i),
                "datadir":"./tests/datadirs/node%s" % i,
                "secret":secret,
                "resetFileDbOnStart":True,
                "master":"localhost:%s"%(42362+2*0),
                "replicatorIdleTime":2,
                "filedb":self.filedb
            }
            
            if i==0:
                opts["master"]=True
                opts["host"]="localhost:%s" % opts["port"]

            nodes.append(TestFS(opts))
            nodes[i].start()
            
        
        nodes[0].importFile("./tests/fixtures/test.txt","dir1/dir2/filename.ext")
        nodes[0].importFile("./tests/fixtures/test2.txt","dir3/dir4/filename2.ext")

        
        sleep(4+numNodes*0.1)
        
        
        for node in nodes:
            self.assertEquals(open(node.getLocalFilePath("dir1/dir2/filename.ext")).read(),open("./tests/fixtures/test.txt").read())
            self.assertEquals(open(node.getLocalFilePath("dir3/dir4/filename2.ext")).read(),open("./tests/fixtures/test2.txt").read())
        
        
        #master should be always returned last when searching for a file
        nodeswithfile = nodes[4].searchFile("dir1/dir2/filename.ext")
        self.assertEquals(numNodes,len(nodeswithfile))
        #self.assertEquals("localhost:%s"%(42362+2*0),nodeswithfile[-1])
        
        #todo stop one node and make it come back after nuke
        
        nodes[0].nukeFile("dir1/dir2/filename.ext")
        
        
        
        sleep(3)
        
        for node in nodes:
            self.assertFalse(os.path.isfile(node.getLocalFilePath("dir1/dir2/filename.ext")))
            self.assertEquals(open(node.getLocalFilePath("dir3/dir4/filename2.ext")).read(),open("./tests/fixtures/test2.txt").read())
        
        

        print "-"*80
        print "adding dir1/dir2/filename.ext again"

        
        nodes[0].importFile("./tests/fixtures/test2.txt","dir1/dir2/filename.ext")
        
        sleep(4+numNodes*0.05)
        
        for node in nodes:
            print node
            self.assertEquals(open(node.getLocalFilePath("dir1/dir2/filename.ext")).read(),open("./tests/fixtures/test2.txt").read())
            self.assertEquals(open(node.getLocalFilePath("dir3/dir4/filename2.ext")).read(),open("./tests/fixtures/test2.txt").read())
        
        
        print "Stopping %s nodes... takes a few seconds" % numNodes
        nodes.reverse()
        for node in nodes:
            node.stop(False)
            
        time.sleep(3)
        
        
        
if __name__ == '__main__':
    unittest.main()
