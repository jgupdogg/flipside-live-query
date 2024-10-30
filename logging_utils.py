import logging

def configure_logging():
    logger = logging.getLogger()
    logger.setLevel(logging.INFO)
    return logger
