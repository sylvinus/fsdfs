#!/usr/bin/env python
# encoding: utf-8

import os, sys, re
from time import sleep
import unittest
import threading
import shutil,time
import random


import logging
logging.basicConfig(level=logging.DEBUG)

sys.path.insert(0, os.path.join('.'))
from fsdfs.filesystem import Filesystem


class TestFS(Filesystem):
    def downloadFile(self, filepath,nodes):
        
        #sleep for 0 to 1.5 seconds
        #sleep(random.random()*1.5)
        
        return Filesystem.downloadFile(self,filepath,nodes)
    
   
class scalingfilesTests(unittest.TestCase):
    """
    filedb="memory"
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
        
        
    def testTwoNodes(self):
        
        numFiles = 400
        secret = "azpdoazrRR"
        
        nodeA = TestFS({
            "host":"localhost:42342",
            "datadir":"./tests/datadirs/A",
            "secret":secret,
            "resetFileDbOnStart":True,
            "master":"localhost:42342",
            "replicatorInterval":0,
            "replicatorIdleTime":10,
            "filedb":self.filedb,
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
            "replicatorInterval":0,
            "replicatorIdleTime":1,
            "reportInterval":2,
            "maxMissedReports":3
        })
        
        nodeA.start()
        nodeB.start()
        

        for i in range(numFiles):
            nodeA.importFile("./tests/fixtures/test.txt","file%s" % i)
        
        for x in range(numFiles):
            statusB = nodeB.getStatus()
            if numFiles==statusB["count"]:
                print "Got all files replicated after %s seconds" % (x*0.1)
                break
            time.sleep(0.16)
            
        time.sleep(1)
        statusB = nodeB.getStatus()
        
        print statusB
        self.assertEquals(numFiles,statusB["count"])
        self.assertEquals(numFiles*26,statusB["size"])
        
        
        nodeB.stop()
        
        #
        #
        #nodeA.stop()
        #return
        #
        # test doesn't work because replicator puts too much stress on the addFilesToNode loop.
        
        sleep(8)
        
        g = nodeA.getGlobalStatus()
        self.assertEquals(1,len(g["nodes"]))
        
        print "restarting node..."
        
        nodeB.config["resetFileDbOnStart"] = False
        
        nodeB.start()
        
        sleep(0.05*numFiles)
        
        for i in range(numFiles):
            print "testing %s" % i
            self.assertEquals([nodeB.host,nodeA.host],nodeA.searchFile("file%s" % i))
        
        
        nodeA.stop()
        nodeB.stop()
        

    
      
if __name__ == '__main__':
  unittest.main()
