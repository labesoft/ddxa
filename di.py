import socket

import amqp
import logging
import os

from numba import jit, cuda


def get_module_name(f):
    return str(os.path.basename(f).split(r'.')[0])


def create_logger():
    logformat = '%(asctime)s:%(msecs)f [%(levelname)s] %(message)s at %(name)s.%(funcName)s:%(lineno)s'
    logging.basicConfig(format=logformat)
    logger = logging.getLogger(MODULE_NAME)
    logger.setLevel(logging.INFO)
    return logger


def on_start(*args, **kwargs):
    module_logger.info(f'args={args}, kwargs={kwargs}')


def on_error(*args, **kwargs):
    module_logger.info(f'args={args}, kwargs={kwargs}')
    module_logger.error('Exception details:', exc_info=True)


def keep_alive(conn: amqp.Connection):
    while True:
        try:
            conn.drain_events(3600)
        except socket.timeout as err:
            module_logger.info(f'sending heartbeat to keep alive with err={err}')
            conn.heartbeat_tick()


CHUNK_SIZE = 1024
MODULE_NAME = get_module_name(__file__)
module_logger = create_logger()
