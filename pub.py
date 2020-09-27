import logging
import sys
import time
from os.path import relpath
from pathlib import Path

import amqp
from amqp import Message

from di import CHUNK_SIZE, get_module_name, create_logger, DI
from q import FileQueue


MODULE_NAME = get_module_name(__file__)
module_logger = create_logger(MODULE_NAME, logging.INFO)


class PUB(DI):
    def __init__(self, conn: amqp.Connection, queue: FileQueue, base_dir):
        super(PUB, self).__init__(conn, base_dir, MODULE_NAME)
        self.queue = queue
        self.queue.put_files(self.base_dir)

    def run(self, sleep_count=0):
        logger = module_logger.getChild(self.__class__.__name__)
        while not self.queue.empty():
            sleep_count = 0
            item = self.queue.get()
            logger.info(f'qsize={self.queue.qsize()}')
            if item[0].startswith(str(Path.cwd().joinpath('out'))):
                # FIXME temp check to avoid republish of embedded dir
                continue
            logger.debug(f'item={item}')
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
        self.channel.basic_publish(msg=msg, exchange='xpublic', routing_key=routing_key)
        logger = module_logger.getChild(self.__class__.__name__)
        logger.info(f"Published: msg.headers={msg.headers}, msg.body={msg.body[:100]}, topic={routing_key}")

    @classmethod
    def get_queue(cls):
        return FileQueue()


if __name__ == "__main__":
    # Default args
    user = "tfeed"
    pwd = "ZTI0MjFmZGM0YzM3YmQwOWJlNjhlNjMz"
    host = '192.168.1.69:5672'
    module_logger.info(f'default args: user={user}, pwd={pwd}, host={host}')

    # Parse args
    nb_thread = int(sys.argv[1])
    basedir = Path(sys.argv[2])
    log_msg = f'PUB: nb_thread={nb_thread}, basedir={basedir}'
    module_logger.info(log_msg)

    # Manage connection
    with amqp.Connection(host, user, pwd) as c:
        q = PUB.get_queue()
        PUB(c, q, basedir).run()
