import logging
import os
from queue import Queue

from di import get_module_name, create_logger

MODULE_NAME = get_module_name(__file__)
module_logger = create_logger(MODULE_NAME, logging.ERROR)


class FileQueue(Queue):
    def put_files(self, base_dir):
        logger = module_logger.getChild(self.__class__.__name__)
        logger.info(f'filling FileQueue({locals()})')
        for path, dirs, files in os.walk(base_dir):
            if files:
                logger.info(f'adding to queue: path={path}, files={files}')
                self.put((path, files))
