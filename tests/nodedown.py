#!/usr/bin/env python
# encoding: utf-8

import os, sys, re
from time import sleep
import unittest
import threading
import shutil,time
import urllib2


import logging
logging.basicConfig(level=logging.DEBUG)

sys.path.insert(0, os.path.join('.'))
from fsdfs.filesystem import Filesystem


class TestFS(Filesystem):
    blockDownloads = False
    
    def downloadFile(self, filepath):
        if self.blockDownloads:
            return None
        else:
            return Filesystem.downloadFile(self,filepath)

class nodedownTests(unittest.TestCase):
    filedb = "memory"
    
    def setUp(self):
        if os.path.exists("./tests/datadirs"):
			shutil.rmtree("./tests/datadirs")
        os.makedirs("./tests/datadirs")
        
        
    def testNodeDown(self):
    
        secret = "azpdoazrRR"
    
        nodeA = TestFS({
            "host":"localhost:42342",
            "datadir":"./tests/datadirs/A",
            "secret":secret,
            "master":"localhost:42342",
            "filedb":self.filedb,
            
            "resetFileDbOnStart":True,
            "reportInterval":2,
            "maxMissedReports":3
        })
    
        nodeB = TestFS({
            "host":"localhost:42352",
            "datadir":"./tests/datadirs/B",
            "secret":secret,
            
            "resetFileDbOnStart":True,
            "master":"localhost:42342",
            "filedb":self.filedb,
            "reportInterval":2
        })
    
        nodeA.start()
        nodeB.start()
    
        nodeA.filedb.reset()
        nodeB.filedb.reset()
        
        
        
        nodeA.importFile("./tests/fixtures/test.txt","dir1/dir2/filename.ext")
        nodeA.importFile("./tests/fixtures/test2.txt","dir3/dir4/filename2.ext")
        
        self.assertEquals(-2,nodeA.filedb.getKn("dir3/dir4/filename2.ext"))
        
        sleep(3)
        
        self.assertEquals(-1,nodeA.filedb.getKn("dir3/dir4/filename2.ext"))
        
        
        self.assertEquals(open(nodeB.getLocalFilePath("dir1/dir2/filename.ext")).read(),open("./tests/fixtures/test.txt").read())
        self.assertEquals(open(nodeA.getLocalFilePath("dir3/dir4/filename2.ext")).read(),open("./tests/fixtures/test2.txt").read())
        
        globalStatusA=nodeA.getGlobalStatus()
        
        self.assertTrue(2<=globalStatusA["uptime"])
        self.assertTrue(4>=globalStatusA["uptime"])
        
        nodeB.stop()
        
        #miss 2 reportings
        sleep(2*2)
        
        globalStatusA=nodeA.getGlobalStatus()
        
        self.assertEquals(2,len(globalStatusA["nodes"]))
        
        #miss 1 more reporting : should be downed
        sleep(3)
        
        globalStatusA=nodeA.getGlobalStatus()
        self.assertEquals(1,len(globalStatusA["nodes"]))
        
        #kn should have been updated too.
        self.assertEquals(-2,nodeA.filedb.getKn("dir3/dir4/filename2.ext"))
        
        self.assertEquals([nodeA.host],nodeA.searchFile("dir3/dir4/filename2.ext"))
        
        #block all downloads so that the file doesn't get replicated again
        nodeA.blockDownloads = True
        nodeB.blockDownloads = True
        
        
        #it's back !
        nodeB.config["resetFileDbOnStart"] = False
        nodeA.config["resetFileDbOnStart"] = False
        
        nodeB.start()
        
        sleep(2)
        
        globalStatusA=nodeA.getGlobalStatus()
        self.assertEquals(2,len(globalStatusA["nodes"]))
        self.assertEquals(-1,nodeA.filedb.getKn("dir3/dir4/filename2.ext"))
        self.assertEquals(open(nodeB.getLocalFilePath("dir1/dir2/filename.ext")).read(),open("./tests/fixtures/test.txt").read())
        
        
        nodeA.stop()
        
        shutil.move("./tests/datadirs/A","./tests/datadirs/A2")
        os.makedirs("./tests/datadirs/A")
        
        nodeA.start()
        
        nodeA.reimportDirectory("./tests/datadirs/A2/")

        globalStatusA=nodeA.getGlobalStatus()
        self.assertEquals(2,len(globalStatusA["nodes"]))
        self.assertEquals(-1,nodeA.filedb.getKn("dir3/dir4/filename2.ext"))
        self.assertEquals(open(nodeA.getLocalFilePath("dir1/dir2/filename.ext")).read(),open("./tests/fixtures/test.txt").read())
        self.assertEquals(open(nodeA.getLocalFilePath("dir3/dir4/filename2.ext")).read(),open("./tests/fixtures/test2.txt").read())
        
        
        
if __name__ == '__main__':
    unittest.main()
