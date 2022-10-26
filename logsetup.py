from distutils.debug import DEBUG
import os
import logging
import timekeeper as tk

envLogLevel = os.getenv('LOG_LEVEL')

match envLogLevel:
    case 'critical':
        logLevel = logging.CRITICAL
    case 'error':
        logLevel = logging.ERROR
    case 'warning':
        logLevel = logging.WARNING
    case 'info':
        logLevel = logging.INFO
    case 'debug':
        logLevel = logging.DEBUG
    case _:
        logLevel = logging.INFO

logging.basicConfig(filename="logs/" + os.getenv('LOG_FILE_PREFIX') + "_" + tk.yearMonthString + ".log", 
                    format='%(asctime)s.%(msecs)03d %(message)s', 
                    datefmt='%Y-%d-%m %H:%M:%S', 
                    level=logLevel)

log = logging.getLogger()