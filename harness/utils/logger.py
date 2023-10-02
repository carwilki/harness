import logging
import sys


def getLogger():
    """
    Returns a logger object with the logging level set to INFO and the output stream set to stdout.

    Returns:
        logging.Logger: A logger object with the logging level set to INFO and the output stream set to stdout.
    """
    logging.basicConfig(level=logging.INFO, stream=sys.stdout)
    return logging.getLogger()
