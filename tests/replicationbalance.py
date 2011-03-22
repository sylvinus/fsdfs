#!/usr/bin/env python
# encoding: utf-8

import os, sys, re
import unittest
import threading
import shutil
import logging
logging.basicConfig(level=logging.DEBUG)
from time import sleep

sys.path.insert(0, os.path.join('.'))
from fsdfs.filesystem import Filesystem


class TestFS(Filesystem):
    
    _rules = {"n":3}
    
    def getReplicationRules(self,file):
        return self._rules
    
    
   
class replicatiobalanceTests(unittest.TestCase):
    filedb = "sqlite"
    
    def setUp(self):
		
		if os.path.exists("./tests/datadirs"):
			shutil.rmtree("./tests/datadirs")
		os.makedirs("./tests/datadirs")

    def testRearrange(self):
        
        
        
        secret = "azpdoazrRR"
        
        masteropts = {
            "host":"localhost:52342",
            "datadir":"./tests/datadirs/A",
            "secret":secret,
            "master":"localhost:52342",
            "maxstorage":10,
            "filedb":self.filedb,
            "garbageMinKn":-1,
            "replicatorConcurrency":4,
            "replicatorDepth":4,
            "replicatorInterval":0
        }
        
        nodeA = TestFS(masteropts)
        
        nodeB = TestFS({
            "host":"localhost:52352",
            "datadir":"./tests/datadirs/B",
            "secret":secret,
            "master":"localhost:52342",
            "maxstorage":10,
            "filedb":self.filedb,
            "reportInterval":4
        })
        
        nodeC = TestFS({
            "host":"localhost:52362",
            "datadir":"./tests/datadirs/C",
            "secret":secret,
            "master":"localhost:52342",
            "maxstorage":10,
            "filedb":self.filedb,
            "reportInterval":4
        })
        
        
        
        nodeA.start()
        nodeB.start()
        nodeC.start()
        
        for i in range(10):
            self.assertTrue(nodeA.importFile("tests/fixtures/1b.txt","a%s" % i))
        
        sleep(5)
        
        for i in range(10):
            self.assertEquals(0,nodeA.filedb.getKn("a%s" % i))
            self.assertHasFile(nodeB, "tests/fixtures/1b.txt","a%s" % i)
            self.assertHasFile(nodeC, "tests/fixtures/1b.txt","a%s" % i)
            
        
        self.assertEquals(0,nodeA.getStatus()["df"])
        self.assertEquals(0,nodeB.getStatus()["df"])
        self.assertEquals(0,nodeC.getStatus()["df"])
        
        for i in range(10):
            self.assertTrue(nodeA.importFile("tests/fixtures/1b.txt","b%s" % i))
        
        sleep(5)
        
        

        for i in range(10):
            self.assertHasFile(nodeB, "tests/fixtures/1b.txt","a%s" % i)
            self.assertHasFile(nodeC, "tests/fixtures/1b.txt","a%s" % i)

        
        self.assertEquals(0,nodeA.getStatus()["df"])
        self.assertEquals(0,nodeB.getStatus()["df"])
        self.assertEquals(0,nodeC.getStatus()["df"])
        
        for i in range(10):
            #all on nodes
            self.assertEquals(-1,nodeA.filedb.getKn("a%s" % i))
            
            #all on master
            self.assertEquals(-2,nodeA.filedb.getKn("b%s" % i))
            self.assertHasFile(nodeA, "tests/fixtures/1b.txt","b%s" % i)
            
        self.assertFalse(nodeA.importFile("tests/fixtures/1b.txt","c0"))
        
        
        # so far so good. now stop master
        nodeA.stop()
        
        print "-"*80
        print "restarting master with more free space"
        print "-"*80
        
        
        # give it more space
        masteropts["maxstorage"] = 30
        
        #restart 
        
        nodeA = TestFS(masteropts)
        nodeA.start()
        
        #by the way also test the "reportInterval":4 option
        
        sleep(7)
        
        self.assertEquals(3,len(nodeA.getKnownNodes()))
        
        #nodeA should be maxed out with "a" files even if they are not the most critical
        self.assertEquals(10,nodeA.getStatus()["df"])
        
        sleep(3)
        
        # then after a while "b" files should replace "a" files on nodes B and C
        for i in range(10):
            #all on nodes
            self.assertEquals(-1,nodeA.filedb.getKn("a%s" % i))
            
            #all on master
            self.assertEquals(-1,nodeA.filedb.getKn("b%s" % i))
            self.assertHasFile(nodeA, "tests/fixtures/1b.txt","b%s" % i)
            self.assertHasFile(nodeA, "tests/fixtures/1b.txt","a%s" % i)
            
        
        nodeA.stop()
        nodeB.stop()
        nodeC.stop()
        
       
        
    def assertHasFile(self,node,localpath,destpath):
        self.assertTrue(os.path.isfile(node.getLocalFilePath(destpath)))
        if os.path.isfile(node.getLocalFilePath(destpath)):
            self.assertEquals(open(node.getLocalFilePath(destpath)).read(),open(localpath).read())
        
        
if __name__ == '__main__':
    unittest.main()
