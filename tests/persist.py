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
    pass
    
    



chosenFileDb = {
    "backend":"mysql",
    "user":"root",
    "passwd":"",
    "host":"localhost",
    "db":"fsdfs_test"
}


chosenFileDb = {
    "backend":"mongodb",
    "host":"localhost",
    "db":"fsdfs_test",
    "port":27017
}

chosenFileDb =     {
        "backend":"sqlite",
    }
    


   
from basic import basicTests
from quota import quotaTests
   
class persistBasicTests(basicTests):
    
    filedb = chosenFileDb

class persistQuotaTests(quotaTests):

    filedb = chosenFileDb
        
#Don't run basictests again
del basicTests
del quotaTests
        
if __name__ == '__main__':
    unittest.main()
