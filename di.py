import socket

import amqp
import logging
import os

from numba import jit, cuda


def get_module_name(f):
    return str(os.path.basename(f).split(r'.')[0])


def create_logger(modname, ll):
    logformat = '%(asctime)s:%(msecs)f [%(levelname)s] %(message)s at %(name)s.%(funcName)s:%(lineno)s'
    logging.basicConfig(format=logformat)
    logger = logging.getLogger(modname)
    logger.setLevel(ll)
    return logger


def on_start(*args, **kwargs):
    module_logger.info(f'args={args}, kwargs={kwargs}')


def on_error(*args, **kwargs):
    module_logger.info(f'args={args}, kwargs={kwargs}')
    module_logger.error('Exception details:', exc_info=True)


class DI:
    def __init__(self, conn, base_dir, modname):
        logger = module_logger.getChild(self.__class__.__name__)
        logger.info(f'starting {self.__class__.__name__}({locals()})')
        self.base_dir = base_dir
        self.conn = conn
        self.channel = conn.channel()

    def keep_alive(self):
        while True:
            try:
                self.conn.drain_events(3600)
            except socket.timeout as err:
                module_logger.info(f'sending heartbeat to keep alive with err={err}')
                self.conn.heartbeat_tick()


CHUNK_SIZE = 1024
MODULE_NAME = get_module_name(__file__)
module_logger = create_logger(MODULE_NAME, logging.INFO)
