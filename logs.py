import logging


def log(log_message, level, log_type):
    """ Logging to a file"""

    # initializing logger file
    logging.basicConfig(filename='transaction_processing.log', filemode='a',
                        format='%(asctime)s - %(levelname)s - %(message)s', level=level)
    log_type(log_message)
