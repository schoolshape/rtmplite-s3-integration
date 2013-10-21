from ConfigParser import ConfigParser
import  multitask, time, os, logging, sys
from s3_help import Storage
from rtmp import FlashServer
from msgHandler import rtmpLogHandler

log = logging.getLogger('__main__')

class App():
    """
    main app for schoolshape run rtmplite
    """
    
    dirname = os.path.dirname(os.path.abspath(sys.argv[0])) + "/"
    __config__ = dirname + 'config.ini'
    
    def __init__(self):
        config = ConfigParser()
        config.read(App.__config__)
            
        self.host = config.get('Host','host')
        self.port = config.getint('Host','port')     
        self.logger = config.get('Log','logger')
        self.log_debug = config.getboolean('Log','debug')
        self.log_screen = config.getboolean('Log','screen')
                
        Storage.root = self.root = config.get('Path','root')
        Storage.loadConfig(App.dirname + config.get('Path', 's3_ini'))
        
    def run(self):
        self.initLogging()
        self.uploadRemain()
        self.startRtmp()

    #TODO
    def uploadRemain(self):
        for r,d,f in os.walk(self.root):
            for files in f:
                if files.endswith("flv"):
                    try:
                        fullname = os.path.join(r, files)
                        filename = fullname[len(self.root):]
                        Storage(filename).upload()
                    except (Exception) , e:
                        log.warn(("Unable to upload ", filename))
                    
    def startRtmp(self):
        try:
            agent = FlashServer()
            agent.root = self.root
            agent.start(self.host, self.port)
            log.info(( time.asctime() , 'Flash Server Starts - %s:%d' % (self.host, self.port)))
            multitask.run()
        except KeyboardInterrupt:
            pass
        log.info((time.asctime() ,'Flash Server Stops'))

    def initLogging(self):
        #create a logger
        logger = logging.getLogger('__main__')

        if os.path.isfile('RabbitMQAuth.py'):
            logger.addHandler(rtmpLogHandler())
        else:
            infoHandler = logging.FileHandler(self.logger)
            infoHandler.setLevel(logging.INFO)
            infoHandler.setFormatter(formatter)
            logger.addHandler(infoHandler)
        #init the formatter
        formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
        if self.log_debug:
            logger.setLevel(logging.DEBUG)
        if self.log_screen:    
            debugHandler = logging.StreamHandler()
            debugHandler.setLevel(logging.DEBUG)
            debugHandler.setFormatter(formatter) 
            logger.addHandler(debugHandler)

if __name__=="__main__":
    App().run()
