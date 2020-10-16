import concurrent
import logging
import sys
import time
from concurrent.futures.thread import ThreadPoolExecutor

from os.path import relpath
from pathlib import Path

import amqp
from amqp import Message
from typing.io import BinaryIO

from di import CHUNK_SIZE, get_module_name, create_logger, DI
from q import FileQueue


MODULE_NAME = get_module_name(__file__)
module_logger = create_logger(MODULE_NAME, logging.ERROR)


class PUB(DI):
    def __init__(self, conn: amqp.Connection, queue: FileQueue, base_dir):
        super(PUB, self).__init__(conn, base_dir, MODULE_NAME)
        self.queue = queue

    def run(self):
        logger = module_logger.getChild(self.__class__.__name__)
        sleep_count = 0
        while sleep_count < 10:
            sleep_count = self.send_files_from_q(logger, sleep_count)

    def send_files_from_q(self, logger, sleep_count):
        while not self.queue.empty():
            sleep_count = 0
            dir_path, files = self.queue.get()
            logger.info(f'qsize={self.queue.qsize()}')
            if not dir_path.startswith(str(Path.cwd().joinpath('out'))):
                logger.debug(f'dir_path={dir_path}, files={files}')
                rel_path = relpath(dir_path, self.base_dir)
                topic = '.'.join(Path(rel_path).parts)
                self.send_files(files, dir_path, rel_path, topic)
        time.sleep(1)
        return sleep_count + 1

    def send_files(self, filenames, dir_path, rel_path, topic):
        for filename in filenames:
            try:
                self.send_file(filename, dir_path, rel_path, topic)
            except FileNotFoundError as err:
                logger = module_logger.getChild(self.__class__.__name__)
                logger.error(f'File was removed: err={err}')

    def send_file(self, filename, dir_path, rel_path, topic):
        file_path = Path(dir_path, filename)
        routing_key = '.'.join([topic, filename]).strip(r'.')
        with file_path.open('rb') as binary_file:
            self.send_filechunks(filename, binary_file, rel_path, routing_key)

    def send_filechunks(self, filename: str, binary_file: BinaryIO, rel_path: str, routing_key: str):
        for offset, body in enumerate(iter(lambda: binary_file.read(CHUNK_SIZE), b'')):
            self.publish_msg(body, filename, offset, rel_path, routing_key)

    def publish_msg(self, body, filename, offset_count, rel_path, routing_key):
        header = {
            'basedir': str(self.base_dir),
            'rel_path': rel_path,
            'filename': filename,
            'offset': str(offset_count)
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
        with ThreadPoolExecutor(max_workers=nb_thread) as ilyn_payne:
            futures = []
            q = PUB.get_queue()
            futures += [ilyn_payne.submit(q.put_files, basedir)]
            futures += [ilyn_payne.submit(PUB(c, q, basedir).run) for i in range(nb_thread-1)]
            for f in concurrent.futures.as_completed(futures):
                try:
                    data = f.result()
                except Exception as err:
                    module_logger(f'f={f}, err={err}', exc_info=True)
