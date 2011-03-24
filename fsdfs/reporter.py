import threading,time

class Reporter(threading.Thread):

    def __init__(self, fs):
        threading.Thread.__init__(self)
        self.fs = fs
        self.stopnow = False
        
    def run(self):

        first = True

        while not self.stopnow:
            try:
                self.fs.report(with_files=(first and not self.fs.ismaster))
                first=False
            except Exception,e:
                self.fs.error("While reporting : %s" % e)
                
            
            [time.sleep(1) for i in range(self.fs.config["reportInterval"]) if not self.stopnow]
        
    def shutdown(self):

        self.stopnow = True
