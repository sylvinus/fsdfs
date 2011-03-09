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
    
   
from basic import basicTests
   
class persistBasicTests(basicTests):
    filedb = {
		"backend":"sqlite",
		"user":"root",
		"passwd":"",
		"host":"localhost",
		"db":"fsdfs_test"
	}
    
#Don't run basictests again
del basicTests
        
if __name__ == '__main__':
    unittest.main()
