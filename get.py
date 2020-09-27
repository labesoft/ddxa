import logging
import random
import sys
from pathlib import Path

import amqp

from di import keep_alive, CHUNK_SIZE, create_logger, get_module_name


MODULE_NAME = get_module_name(__file__)


class GET:
    random.seed()
    r1 = random.randint(0, 100000000)
    r2 = random.randint(0, 100000000)

    def __init__(self, conn: amqp.Connection, queue, base_dir, t, ll):
        self.logger = logging.getLogger(MODULE_NAME).getChild(self.__class__.__name__)
        self.logger.setLevel(ll)
        self.logger.info(f'starting GET({locals()})')
        self.qname = queue
        self.base_dir = base_dir
        self.topic = t
        self.conn = conn
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
        keep_alive(self.conn)

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
        q = GET.get_queue()
        GET(c, q, basedir, topic_routing, logging.INFO).run()
