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
    
   
class scalingfilesTests(unittest.TestCase):
    filedb = "sqlite"
    
    def setUp(self):
        if os.path.exists("./tests/datadirs"):
			shutil.rmtree("./tests/datadirs")
        os.makedirs("./tests/datadirs")
        
        
    def testTwoNodes(self):
        
        numFiles = 400
        secret = "azpdoazrRR"
        
        nodeA = TestFS({
            "host":"localhost:42342",
            "datadir":"./tests/datadirs/A",
            "secret":secret,
            "master":"localhost:42342",
            "replicationInterval":0,
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
        

        for i in range(numFiles):
            nodeA.importFile("./tests/fixtures/test.txt","file%s" % i)
        
        for x in range(numFiles):
            statusB = nodeB.getStatus()
            if numFiles==statusB["count"]:
                print "Got all files replicated after %s seconds" % (x*0.1)
                break
            time.sleep(0.1)
        
        
        self.assertEquals(numFiles,statusB["count"])
        
        nodeA.stop()
        nodeB.stop()

    
      
if __name__ == '__main__':
  unittest.main()
