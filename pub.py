import logging
import sys
import time
from os.path import relpath
from pathlib import Path

import amqp
from amqp import Message

from di import CHUNK_SIZE, get_module_name, create_logger
from q import FileQueue


MODULE_NAME = get_module_name(__file__)


class PUB:
    def __init__(self, conn: amqp.Connection, queue: FileQueue, base_dir, t, ll):
        self.logger = logging.getLogger(MODULE_NAME).getChild(self.__class__.__name__)
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

    def send_files(self, filenames, dir_path, rel_path, topic):
        for filename in filenames:
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


if __name__ == "__main__":
    # init logger
    module_logger = create_logger()

    # Default args
    user = "tfeed"
    pwd = "ZTI0MjFmZGM0YzM3YmQwOWJlNjhlNjMz"
    host = '192.168.1.69:5672'
    module_logger.info(f'default args: user={user}, pwd={pwd}, host={host}')

    # Parse args
    nb_thread = int(sys.argv[1])
    topic_routing = sys.argv[2]
    basedir = Path(sys.argv[3])
    log_msg = f'target={sys.argv[1].upper()}, nb_thread={nb_thread}, topic_routing={topic_routing}, basedir={basedir}'
    module_logger.info(log_msg)

    with amqp.Connection(host, user, pwd) as c:
        q = PUB.get_queue()
        PUB(c, q, basedir, topic_routing, logging.INFO).run()
