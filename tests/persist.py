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
   
class persistTests(basicTests):
    filedb = "sqlite"
    
        
if __name__ == '__main__':
    unittest.main()
