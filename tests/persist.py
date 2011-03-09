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
    
    
chosenFileDb =     {
        "backend":"sqlite",
    }
    
    

"""
filedb = {
    "backend":"mysql",
    "user":"root",
    "passwd":"",
    "host":"localhost",
    "db":"fsdfs_test"
}


filedb = {
    "backend":"mongodb",
    "host":"localhost",
    "db":"fsdfs_test",
    "port":27017
}
"""

   
from basic import basicTests
from quota import quotaTests
   
class persistBasicTests(basicTests):
    
    filedb = {
        "backend":"sqlite",
    }
    
#Don't run basictests again
del basicTests
del quotaTests
        
if __name__ == '__main__':
    unittest.main()
