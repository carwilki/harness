import logging
import sys


def getLogger():
    logging.basicConfig(level=logging.INFO, stream=sys.stdout)
    return logging.getLogger()
