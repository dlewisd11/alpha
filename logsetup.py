from distutils.debug import DEBUG
import os
import logging
import timekeeper as tk

def decodeLogLevel(logLevel):
    match logLevel:
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
    return logLevel

defaultLogLevel = decodeLogLevel(os.getenv('DEFAULT_LOG_LEVEL'))
applicationLogLevel = decodeLogLevel(os.getenv('APPLICATION_LOG_LEVEL'))

logging.basicConfig(filename="logs/" + os.getenv('LOG_FILE_PREFIX') + "_" + tk.yearMonthString + ".log", 
                    format='%(asctime)s.%(msecs)03d %(message)s', 
                    datefmt='%Y-%d-%m %H:%M:%S', 
                    level=defaultLogLevel)

log = logging.getLogger("applicationLogger")
log.setLevel(applicationLogLevel)