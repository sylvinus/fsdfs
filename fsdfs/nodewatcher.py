import threading
import time

class NodeWatcher(threading.Thread):
    def __init__(self,fs):
        threading.Thread.__init__(self)
        self.daemon = True
        self.fs = fs
        
    def run(self):
        
        while True:
            time.sleep(self.fs.config["reportInterval"]/2)
            for node in self.fs.filedb.listNodes():
                lastUpdate = self.fs.filedb.getNode(node)["lastUpdate"]
                if lastUpdate<(time.time()-self.fs.config["reportInterval"]*self.fs.config["maxMissedReports"]):
                    self.fs.debug("Node %s missed %s reports, removing it from the swarm" % (node,self.fs.config["maxMissedReports"]))
                    self.fs.filedb.removeNode(node)