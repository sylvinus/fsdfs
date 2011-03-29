#!/usr/bin/env python
# encoding: utf-8

import sys,os

from optparse import OptionParser

try:
    import simplejson as json
except:
    import json

def getOptionsParser():
        
    usage = "usage: ./admin.py [options] [globalstatus|restart|...]"
    parser = OptionParser(usage=usage)
    
    parser.add_option("-s", "--secret", action="store", dest="secret", default="", type="str", help="swarm secret")    
    parser.add_option("-m", "--master", action="store", dest="master", default="", type="str", help="master address")    
    parser.add_option("-j", "--json", action="store_true", dest="json", default=False, help="output as JSON")    


    return parser



def main():
    
    (optionsattr,args) = getOptionsParser().parse_args()
    
    
    from fsdfs.filesystem import Filesystem
    
    node = Filesystem({
        "host":"no-host",
        "secret":optionsattr.secret,
        "master":optionsattr.master
    })
    
    if args[0]=="globalstatus":
        
        ret = node.getGlobalStatus()

    elif args[0]=="search":
        
        ret = node.searchFile(args[1])
        
    elif args[0]=="nuke":
        
        ret = node.nodeRPC(node.config["master"],"NUKE",{"filepath":args[1]})

    if optionsattr.json:
        print json.dumps(ret, indent=4)
    else:
        print ret

if __name__ == '__main__':
    main()

