import random
import socket
import time
from queue import Queue

import amqp
import logging
import os
import sys

from amqp import Message
from os.path import relpath
from pathlib import Path

from numba import jit, cuda

CHUNK_SIZE = 1024
DEFAULT_SLEEP_TIME = 1.0
MODULE_NAME = str(os.path.basename(__file__).split(r'.')[0])


class GET:
    random.seed()
    r1 = random.randint(0, 100000000)
    r2 = random.randint(0, 100000000)

    def __init__(self, conn: amqp.Connection, queue, base_dir, t, ll):
        self.logger = logging.getLogger(MODULE_NAME).getChild('GET')
        self.logger.setLevel(ll)
        self.logger.info(f'starting GET({locals()})')
        self.qname = queue
        self.base_dir = base_dir
        self.topic = t
        self.channel = conn.channel()
        self.channel.exchange_declare('xpublic', 'topic', auto_delete=False)
        self.channel.queue_declare(queue=self.qname)
        self.channel.queue_bind(queue=self.qname, exchange='xpublic', routing_key=self.topic)
        self.channel.basic_qos(prefetch_size=0, prefetch_count=0, a_global=False)

    def run(self):
        ctag = f'{self.qname}'
        args = (self.qname, ctag)
        kwargs = {'nowait': True, 'callback': self.on_message}
        self.channel.basic_consume(*args, **kwargs)
        keep_alive()

    def on_message(self, msg):
        dir_path = Path(self.base_dir, 'out', msg.headers['rel_path'])
        dir_path.mkdir(parents=True, exist_ok=True)
        file_path = dir_path.joinpath(msg.headers['filename'])
        try:
            if msg.body:
                with file_path.open('ab') as f:
                    f.seek(int(msg.headers['offset'])*CHUNK_SIZE)
                    f.write(msg.body)
            else:
                file_path.touch(exist_ok=True)
            self.logger.info(f"Downloaded: file_path={file_path}")
        except (TypeError, ConnectionResetError, FileNotFoundError) as err:
            self.logger.error(f"err={err}, msg.headers={msg.headers} msg.body={msg.body}")
        self.channel.basic_ack(delivery_tag=msg.delivery_tag)

    @classmethod
    def get_queue(cls):
        return f'q_rabbit_testing_{cls.r1}_{cls.r2}'


class FileQueue(Queue):
    def __init__(self):
        super(FileQueue, self).__init__()
        self.logger = logging.getLogger(MODULE_NAME).getChild(f'get_{self}')
        self.logger.setLevel(logging.INFO)

    def put_files(self, base_dir):
        self.logger.info(f'filling FileQueue({locals()})')
        for path, dirs, files in os.walk(base_dir):
            if files:
                self.logger.info(f'adding to queue: path={path}, files={files}')
                self.put((path, files))


class PUB:
    def __init__(self, conn: amqp.Connection, queue: FileQueue, base_dir, t, ll):
        self.logger = logging.getLogger(MODULE_NAME).getChild(f'pub_{id}')
        self.logger.setLevel(ll)
        self.logger.info(f'starting PUB({locals()})')
        self.queue = queue
        self.base_dir = base_dir
        self.chan = conn.channel()
        self.queue.put_files(self.base_dir)

    def run(self, sleep_count=0):
        while not self.queue.empty():
            sleep_count = 0
            item = self.queue.get()
            self.logger.info(f'qsize={self.queue.qsize()}')
            if item[0].startswith(str(Path.cwd().joinpath('out'))):
                # FIXME temp check to avoid republish of embedded dir
                continue
            self.logger.debug(f'item={item}')
            dir_path, files = item
            rel_path = relpath(dir_path, self.base_dir)
            topic = '.'.join(Path(rel_path).parts)
            self.send_files(files, dir_path, rel_path, topic)
        if sleep_count < 10:
            time.sleep(1)
            self.run(sleep_count + 1)

    def send_files(self, files, dir_path, rel_path, topic):
        for filename in files:
            self.send_file(dir_path, filename, rel_path, topic)

    def send_file(self, dir_path, filename, rel_path, topic):
        file_path = Path(dir_path, filename)
        routing_key = '.'.join([topic, filename]).strip(r'.')
        with file_path.open('rb') as binary_file:
            self.send_filechunks(filename, binary_file, rel_path, routing_key)

    def send_filechunks(self, filename, binary_file, rel_path, routing_key):
        for offset, body in enumerate(iter(lambda: binary_file.read(CHUNK_SIZE), b'')):
            self.publish_msg(body, filename, offset, rel_path, routing_key)

    def publish_msg(self, body, filename, offset_count, rel_path, routing_key):
        header = {
            'basedir': str(self.base_dir), 'rel_path': rel_path, 'filename': filename, 'offset': str(offset_count)
        }
        msg = Message(body, application_headers=header)
        self.chan.basic_publish(msg=msg, exchange='xpublic', routing_key=routing_key)
        self.logger.info(f"Published: msg.headers={msg.headers}, msg.body={msg.body[:100]}, topic={routing_key}")

    @classmethod
    def get_queue(cls):  # FIXME random nb here doesn't make senses
        return FileQueue()


def on_start(*args, **kwargs):
    module_logger.info(f'args={args}, kwargs={kwargs}')


def on_error(*args, **kwargs):
    module_logger.info(f'args={args}, kwargs={kwargs}')
    module_logger.error('Exception details:', exc_info=True)


def keep_alive():
    while True:
        try:
            c.drain_events(3600)
        except socket.timeout as err:
            module_logger.info(f'sending heartbeat to keep alive with err={err}')
            c.heartbeat_tick()


def create_logger(module_name):
    logformat = '%(asctime)s:%(msecs)f [%(levelname)s] %(message)s at %(name)s.%(funcName)s:%(lineno)s'
    logging.basicConfig(format=logformat)
    logger = logging.getLogger(module_name)
    logger.setLevel(logging.INFO)
    return logger


if __name__ == "__main__":
    # init logger
    module_logger = create_logger(MODULE_NAME)

    # Default args
    user = "tfeed"
    pwd = "ZTI0MjFmZGM0YzM3YmQwOWJlNjhlNjMz"
    host = '192.168.1.69:5672'
    module_logger.info(f'default args: user={user}, pwd={pwd}, host={host}')

    # Parse args
    target = locals()[sys.argv[1].upper()]
    nb_thread = int(sys.argv[2])
    topic_routing = sys.argv[3]
    basedir = Path(sys.argv[4])
    log_msg = f'target={sys.argv[1].upper()}, nb_thread={nb_thread}, topic_routing={topic_routing}, basedir={basedir}'
    module_logger.info(log_msg)

    with amqp.Connection(host, user, pwd) as c:
        q = target.get_queue()
        target(c, q, basedir, topic_routing, logging.INFO).run()
