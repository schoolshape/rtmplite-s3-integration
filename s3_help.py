from ConfigParser import ConfigParser
from boto.s3.connection import S3Connection
import logging, os, time, sys, shutil, subprocess, re, shutil

log = logging.getLogger('__main__')

class Storage():
    """
    operate s3 upload and download
    """
    root = "";
    bucket_public = ""
    bucket_private = ""
    secretKey = None
    hasKey = False
    
    def __init__(self, filename):
        self.key = None
        self.filename = filename
        self.localFileFullname = Storage.root + self.filename
        
        self.bucketName = (Storage.bucket_private if self.filename.startswith('private') else Storage.bucket_public)
        self.s3FileFullname = self.bucketName + r'/' + self.filename
        
        self.tidyFileWithFfmpeg()
    
    @staticmethod 
    def loadConfig(configPath):
        if configPath != '' :
            config = ConfigParser()
            config.read(configPath)
            Storage.bucket_public = config.get('Bucket', 'public')
            Storage.bucket_private = config.get('Bucket', 'private')
            Storage.accessKey = config.get('Credentials', 'aws_access_key_id')
            Storage.secretKey = config.get('Credentials', 'aws_secret_access_key')
            Storage.hasKey = Storage.accessKey != '' and Storage.secretKey != ''
            log.info('loading accessKey & secretKey from ' + configPath)
        
    def startUpload(self):
        if Storage.hasKey :
            self.getKey().set_contents_from_filename(self.localFileFullname)
            self.getKey().set_acl('public-read')
            self.closeKey()
        else :
            ''''Mock upload file to s3'''
            time.sleep(3)
            shutil.copyfile(self.localFileFullname, self.s3FileFullname)
            

    # def uploadByFile(self, fp, cb=None):
    #     self.getKey().set_contents_from_file(fp, rewind=True)
    #     if cb is not None and callable(cb): cb()
    #     self.closeKey()
    #     if not fp.closed: fp.close()

    def closeKey(self):
        if self.key is not None and not self.key.closed : self.key.close()
        self.key = None

    def getKey(self):
        if self.key is None:
            conn = S3Connection(Storage.accessKey, Storage.secretKey)
            bucket = conn.get_bucket(self.bucketName)
            key = bucket.get_key(self.filename)
            if key is None: key = bucket.new_key(self.filename)
            self.key = key
        return self.key
    
    def tidyFileWithFfmpeg(self):
        '''audio only flv playback bug  -  Adobe Flash Player 11.2  -  Bug 3156305'''
        isAudioFlv = r'/audio/' in self.filename and self.filename.endswith(".flv")
        if isAudioFlv:
            oldDuration = self.getFileDuration()
            tidyFile = self.localFileFullname.replace('.flv', '.tidy.flv')
            command = 'ffmpeg -i %s -metadata videocodecid="" -vn -acodec copy -y %s' % (self.localFileFullname, tidyFile)
            os.system(command)
            shutil.move(tidyFile, self.localFileFullname)
            log.debug(('tidyFileWithFfmpeg', self.localFileFullname, 'duration', oldDuration, '->', self.getFileDuration()))
        
    def searchForDuration (self, ffmpeg_output):
        pattern = re.compile(r'Duration: ([\w.-]+):([\w.-]+):([\w.-]+),')   
        match = pattern.search(ffmpeg_output)   
    
        if match:
            hours = match.group(1)
            minutes = match.group(2)
            seconds = match.group(3)
        else:
            hours = minutes = seconds = 0
    
        # return a dictionary containing our duration values
        return (float(hours) * 60  + float(minutes)) * 60 + float(seconds)

    # -----------------------------------------------------------
    # Get the dimensions from the specified file using ffmpeg
    # -----------------------------------------------------------
    def getFFMPEGInfo (self):
        p = subprocess.Popen(['ffmpeg', '-i', self.localFileFullname], stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        stdout, stderr = p.communicate()
        return stderr
        
    # -----------------------------------------------------------
    # Get the duration by pulling out the FFMPEG info and then
    # searching for the line that contains our duration values
    # -----------------------------------------------------------
    def getFileDuration (self):
        ffmpeg_output = self.getFFMPEGInfo ()
        return self.searchForDuration (ffmpeg_output)    

    def hasUpload(self):
        return os.path.isfile(self.localFileFullname + '.uploaded') and not os.path.isfile(self.localFileFullname)

    def upload(self, failTimes=3):
        """
        upload fp to S3 by filename and try 'failTimes' to upload if failed.
        
        Arguments:
        - `failTimes`: defaul try 3 times if upload failed
        """
        log.info("start upload " + self.localFileFullname + " to " + self.s3FileFullname)
        for i in range(failTimes):
            try:
                start = time.time()
                log.info(("upload start: ", self.filename , ', duration: ', self.getFileDuration()))
                self.startUpload()
                uploadedFullname = self.localFileFullname + '.uploaded'
                if os.path.isfile(uploadedFullname): os.remove(uploadedFullname)
                os.rename(self.localFileFullname, uploadedFullname)
                log.info(('upload succeeded: ', self.filename, ', failed: ', i, ', spent: ', time.time() - start))
            except (Exception) , e:
                log.info(("upload failed: ", self.filename , ', failed: ', i, 'exception: ', e))
            else :
                break
        else:
            log.error(("upload failed: ", self.filename))
            
# # just for test
if __name__ == "__main__" :
    Storage.loadConfig('/etc/.rtmplite-s3-integration')
    print "accessKey:", Storage.accessKey, ",secretKey:", Storage.secretKey
    s3 = Storage('vo/1.flv', test=True)
    s3.upload("/videoStreams/1.flv")
