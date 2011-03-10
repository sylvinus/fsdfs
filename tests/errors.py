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
    pass

class errorsTests(unittest.TestCase):
    filedb = "memory"
    
    def setUp(self):
        if os.path.exists("./tests/datadirs"):
			shutil.rmtree("./tests/datadirs")
        os.makedirs("./tests/datadirs")
        
        
    def testErrors(self):
    
        print "errors"
        
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
        
        
        self.assertRaises(urllib2.HTTPError,nodeB.nodeRPC,nodeB.config["master"],"RAISE",{"p1":"A"})

        
        
if __name__ == '__main__':
    unittest.main()
