#!/usr/bin/env python
# encoding: utf-8

import os, sys, re
from time import sleep
import unittest
import threading
import shutil

try:
	import simplejson as json
except:
	import json


import logging
logging.basicConfig(level=logging.DEBUG)

sys.path.insert(0, os.path.join('.'))
from fsdfs.filesystem import Filesystem


class TestFS(Filesystem):
    pass
    
   
class adminTests(unittest.TestCase):
    filedb = "memory"
    
    def setUp(self):
        if os.path.exists("./tests/datadirs"):
			shutil.rmtree("./tests/datadirs")
        os.makedirs("./tests/datadirs")
        
    def testTwoNodes(self):
        
        secret = "azpdoazrRR"
        master = "localhost:42342"
        
        nodeA = TestFS({
            "host":master,
            "datadir":"./tests/datadirs/A",
            "secret":secret,
            "master":master,
            "filedb":self.filedb
        })
        
        nodeB = TestFS({
            "host":"localhost:42352",
            "datadir":"./tests/datadirs/B",
            "secret":secret,
            "master":master,
            "filedb":self.filedb
        })
        
        nodeA.start()
        nodeB.start()
        
        
        
        nodeA.importFile("./tests/fixtures/test.txt","dir1/dir2/filename.ext")
        nodeA.importFile("./tests/fixtures/test2.txt","dir3/dir4/filename2.ext")
        
        
        sleep(5)
        
        self.assertEquals(open(nodeB.getLocalFilePath("dir1/dir2/filename.ext")).read(),open("./tests/fixtures/test.txt").read())
        self.assertEquals(open(nodeA.getLocalFilePath("dir3/dir4/filename2.ext")).read(),open("./tests/fixtures/test2.txt").read())
        
        globalStatusA = json.loads(os.popen("python bin/admin.py --json --secret=%s --master=%s globalstatus" % (secret,master)).read())

        print globalStatusA

        self.assertEquals(5,globalStatusA["uptime"])
        
        del globalStatusA["uptime"]
        
        self.assertEquals(2,globalStatusA["countGlobal"])
        self.assertEquals(-1,globalStatusA["minKnGlobal"][0][0])
        self.assertEquals(2,len(globalStatusA["nodes"].keys()))
        self.assertEquals("localhost:42342",globalStatusA["node"])
        
        nodes = json.loads(os.popen("python bin/admin.py --json --secret=%s --master=%s search %s" % (secret,master,"dir3/dir4/filename2.ext")).read())
        
        print nodes
        
        self.assertEquals(2,len(nodes))
        
        nuke = json.loads(os.popen("python bin/admin.py --json --secret=%s --master=%s nuke %s" % (secret,master,"dir3/dir4/filename2.ext")).read())
        
        self.assertEquals("ok",nuke)
        
        globalStatusA = json.loads(os.popen("python bin/admin.py --json --secret=%s --master=%s globalstatus" % (secret,master)).read())
        self.assertEquals(1,globalStatusA["countGlobal"])
        
        
        nodeA.stop()
        nodeB.stop()
        
    
        
        
if __name__ == '__main__':
    unittest.main()
