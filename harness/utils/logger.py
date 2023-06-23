import os
import sys
import logging


def getLogger():
    log = logging.getLogger()
    loglevel = os.environ.get("LOG_LEVEL", "INFO")
    log.setLevel(loglevel)
    handler = logging.StreamHandler(sys.stdout)
    log.addHandler(handler)
    return log
