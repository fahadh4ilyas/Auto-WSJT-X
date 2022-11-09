from logging.handlers import RotatingFileHandler

class RollingFileHandler(RotatingFileHandler):

    def __init__(self, filename, mode='a', maxBytes=0, backupCount=0, encoding=None, delay=False):
        self.last_backup_cnt = 0
        super(RollingFileHandler, self).__init__(filename=filename,
                                                 mode=mode,
                                                 maxBytes=maxBytes,
                                                 backupCount=backupCount,
                                                 encoding=encoding,
                                                 delay=delay)

    # override
    def doRollover(self):
        if self.stream:
            self.stream.close()
            self.stream = None
        # my code starts here
        self.last_backup_cnt += 1
        nextName = "%s.%d" % (self.baseFilename, self.last_backup_cnt)
        self.rotate(self.baseFilename, nextName)
        # my code ends here
        if not self.delay:
            self.stream = self._open()