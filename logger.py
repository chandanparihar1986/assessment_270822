import logging
from logging.handlers import TimedRotatingFileHandler

loghandler = TimedRotatingFileHandler("/tmp/assignment_270822.log", when="midnight")
logformat = logging.Formatter("%(asctime)s %(message)s")
loghandler.setFormatter(logformat)
lLogger = logging.getLogger("logger")
lLogger.addHandler(loghandler)
lLogger.setLevel(logging.INFO)


