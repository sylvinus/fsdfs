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
    
    def downloadFile(self, filepath, nodes):
        if self.blockDownloads:
            return None
        else:
            return Filesystem.downloadFile(self,filepath,nodes)

class nodedownTests(unittest.TestCase):
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
        
        
    def testNodeDown(self):
    
        secret = "azpdoazrRR"
    
        nodeA = TestFS({
            "host":"localhost:42342",
            "datadir":"./tests/datadirs/A",
            "secret":secret,
            "master":"localhost:42342",
            "filedb":self.filedb,
            
            "replicatorIdleTime":1,
            "resetFileDbOnStart":True,
            "reportInterval":2,
            "maxMissedReports":3
        })
    
        nodeB = TestFS({
            "host":"localhost:42352",
            "datadir":"./tests/datadirs/B",
            "secret":secret,
            
            "resetFileDbOnStart":True,
            "replicatorIdleTime":1,
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
        # ??
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
        
        
        #try reimporting a whole directory
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
        
        
        
        #
        # Test nuke while node down
        #
        
        
        nodeB.stop()
        
        #miss 3+ reportings : should be downed
        sleep(4*2)
        
        globalStatusA=nodeA.getGlobalStatus()
        self.assertEquals(1,len(globalStatusA["nodes"]))
        self.assertEquals([nodeA.host],nodeA.searchFile("dir3/dir4/filename2.ext"))
        
        
        #nuke file in the meantime
        nodeA.nukeFile("dir3/dir4/filename2.ext")
        
        self.assertEquals([],nodeA.searchFile("dir3/dir4/filename2.ext"))
        self.assertEquals([nodeA.host],nodeA.searchFile("dir1/dir2/filename.ext"))
        
        self.assertTrue(nodeA.filedb.isNuked("dir3/dir4/filename2.ext"))
        
        #make the node come back with the file
        
        self.assertTrue(os.path.isfile(nodeB.getLocalFilePath("dir3/dir4/filename2.ext")))
        
        nodeB.start()
        
        sleep(4)
        
        self.assertEquals([],nodeA.searchFile("dir3/dir4/filename2.ext"))
        self.assertEquals(set([nodeA.host,nodeB.host]),set(nodeA.searchFile("dir1/dir2/filename.ext")))
        self.assertFalse(os.path.isfile(nodeB.getLocalFilePath("dir3/dir4/filename2.ext")))
        
        
        nodeA.stop()
        nodeB.stop()
        
        
if __name__ == '__main__':
    unittest.main()
