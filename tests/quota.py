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


class TestFS(Filesystem):
    
    _rules = {"n":2}
    
    def getReplicationRules(self,file):
        return self._rules
    
    
   
class quotaTests(unittest.TestCase):
    def setUp(self):
        
        shutil.rmtree("./tests/datadirs")
        os.makedirs("./tests/datadirs")

    
    def testConflict(self):
        
        
        
        secret = "azpdoazrRR"
        
        nodeA = TestFS({
            "host":"localhost:52342",
            "datadir":"./tests/datadirs/A",
            "secret":secret,
            "master":"localhost:52342",
            "maxstorage":10
        })
        
        nodeB = TestFS({
            "host":"localhost:52352",
            "datadir":"./tests/datadirs/B",
            "secret":secret,
            "master":"localhost:52342",
            "maxstorage":10
        })
        
        nodeC = TestFS({
            "host":"localhost:52362",
            "datadir":"./tests/datadirs/C",
            "secret":secret,
            "master":"localhost:52342",
            "maxstorage":10
        })
        
        
        nodeA.start()
        nodeB.start()
        nodeC.start()
        
        
        
        nodeA.importFile("tests/fixtures/10b.txt","tests/fixtures/10b.txt")
        
        sleep(4)
        
        self.assertHasFile(nodeA, "tests/fixtures/10b.txt")
        self.assertHasFile(nodeB, "tests/fixtures/10b.txt")
        self.assertHasFile(nodeC, "tests/fixtures/10b.txt")
        self.assertEquals(0,nodeA.getStatus()["df"])
        self.assertEquals(0,nodeB.getStatus()["df"])
        self.assertEquals(0,nodeC.getStatus()["df"])
        
        
        self.assertTrue(nodeA.deleteFile("tests/fixtures/10b.txt"))
        
        nodeA.importFile("tests/fixtures/10b.2.txt","tests/fixtures/10b.2.txt")
        
        sleep(3)
        
        for i in range(0,3):
                
            self.assertHasFile(nodeA, "tests/fixtures/10b.2.txt")
            self.assertHasFile(nodeB, "tests/fixtures/10b.txt")
            self.assertHasFile(nodeC, "tests/fixtures/10b.txt")
            
            self.assertEquals(0,nodeA.getStatus()["df"])
            self.assertEquals(0,nodeB.getStatus()["df"])
            self.assertEquals(0,nodeC.getStatus()["df"])
                
            sleep(1)
        
        
        
        nodeA.stop()
        nodeB.stop()
        nodeC.stop()
        
        
    def testSimple(self):
        
        secret = "azpdoazrRR"
        
        nodeA = TestFS({
            "host":"localhost:42342",
            "datadir":"./tests/datadirs/A",
            "secret":secret,
            "master":"localhost:42342",
            "maxstorage":13
        })
        
        nodeB = TestFS({
            "host":"localhost:42352",
            "datadir":"./tests/datadirs/B",
            "secret":secret,
            "master":"localhost:42342",
            "maxstorage":100000
        })
        
        nodeC = TestFS({
            "host":"localhost:42362",
            "datadir":"./tests/datadirs/C",
            "secret":secret,
            "master":"localhost:42342",
            "maxstorage":12
        })
        
        nodeD = TestFS({
            "host":"localhost:42372",
            "datadir":"./tests/datadirs/D",
            "secret":secret,
            "master":"localhost:42342",
            "maxstorage":1
        })
        
        nodeA.start()
        nodeB.start()
        nodeC.start()
        nodeD.start()
        
        
        
        nodeA.importFile("tests/fixtures/10b.txt","tests/fixtures/10b.txt")
        nodeA.importFile("tests/fixtures/2b.txt","tests/fixtures/2b.txt")
        nodeA.importFile("tests/fixtures/1b.txt","tests/fixtures/1b.txt")
        
        sleep(6)
        
        
        for i in range(0,3):
                
            self.assertHasFile(nodeA, "tests/fixtures/2b.txt")
            self.assertHasFile(nodeA, "tests/fixtures/10b.txt")
            self.assertHasFile(nodeA, "tests/fixtures/1b.txt")
            
            self.assertHasFile(nodeB, "tests/fixtures/1b.txt")
            self.assertHasFile(nodeB, "tests/fixtures/1b.txt")
            self.assertHasFile(nodeB, "tests/fixtures/1b.txt")
            
            self.assertHasFile(nodeC, "tests/fixtures/10b.txt")
            self.assertHasFile(nodeC, "tests/fixtures/2b.txt")
            
            self.assertHasFile(nodeD, "tests/fixtures/1b.txt")
            
            self.assertEquals(0,nodeA.getStatus()["df"])
            self.assertEquals(0,nodeC.getStatus()["df"])
            self.assertEquals(0,nodeD.getStatus()["df"])
            
            sleep(1)
        
        self.assertTrue(nodeA.deleteFile("tests/fixtures/10b.txt"))
        
        nodeA.importFile("tests/fixtures/10b.2.txt","tests/fixtures/10b.2.txt")
        
        sleep(2)
        
        for i in range(0,3):
                
            self.assertHasFile(nodeA, "tests/fixtures/10b.2.txt")
            self.assertHasFile(nodeA, "tests/fixtures/1b.txt")
            self.assertHasFile(nodeA, "tests/fixtures/2b.txt")
            
            self.assertHasFile(nodeB, "tests/fixtures/1b.txt")
            self.assertHasFile(nodeB, "tests/fixtures/2b.txt")
            self.assertHasFile(nodeB, "tests/fixtures/10b.txt")
            self.assertHasFile(nodeB, "tests/fixtures/10b.2.txt")
            
            self.assertHasFile(nodeC, "tests/fixtures/2b.txt")
            self.assertHasFile(nodeC, "tests/fixtures/10b.txt")
            
            self.assertHasFile(nodeD, "tests/fixtures/1b.txt")
            
            self.assertEquals(0,nodeA.getStatus()["df"])
            self.assertEquals(0,nodeC.getStatus()["df"])
            self.assertEquals(0,nodeD.getStatus()["df"])
            
            sleep(1)
            
        
        nodeA.stop()
        nodeB.stop()
        nodeC.stop()
        nodeD.stop()
        
        
    def assertHasFile(self,node,destpath):
        self.assertTrue(os.path.isfile(node.getLocalFilePath(destpath)))
        if os.path.isfile(node.getLocalFilePath(destpath)):
            self.assertEquals(open(node.getLocalFilePath(destpath)).read(),open(destpath).read())
        
        
if __name__ == '__main__':
    unittest.main()