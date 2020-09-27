import logging
import os
from queue import Queue

from di import get_module_name


MODULE_NAME = get_module_name(__file__)


class FileQueue(Queue):
    def __init__(self):
        super(FileQueue, self).__init__()
        self.logger = logging.getLogger(MODULE_NAME).getChild(self.__class__.__name__)
        self.logger.setLevel(logging.INFO)

    def put_files(self, base_dir):
        self.logger.info(f'filling FileQueue({locals()})')
        for path, dirs, files in os.walk(base_dir):
            if files:
                self.logger.info(f'adding to queue: path={path}, files={files}')
                self.put((path, files))
