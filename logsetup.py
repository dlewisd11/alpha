from distutils.debug import DEBUG
import os
import logging
import timekeeper as tk

logging.basicConfig(filename="logs/" + os.getenv('LOG_FILE_PREFIX') + "_" + tk.yearMonthString + ".log", 
                    format='%(asctime)s.%(msecs)03d %(message)s', 
                    datefmt='%Y-%d-%m %H:%M:%S', 
                    level=logging.DEBUG)

log = logging.getLogger()