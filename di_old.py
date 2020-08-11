import glob
import logging
import os
import random
import sys
import threading
import time
from pathlib import Path

from amqp import Connection, Message

DEFAULT_SLEEP_TIME = 1.0

logformat = '%(asctime)s [%(levelname)s] %(message)s at %(name)s.%(funcName)s:%(lineno)s'
logging.basicConfig(format=logformat)
module_name = str(os.path.basename(__file__).split(r'.')[0])
module_logger = logging.getLogger(module_name)
module_logger.setLevel(logging.INFO)

random.seed()
r1 = random.randint(0, 100000000)
r2 = random.randint(0, 100000000)
module_logger.info(f'r1={r1}, r2={r2}')


class Client:
    """ Client is a base class for receiving msg from queues
    """
    def __init__(self, thread_id, channel, qname):
        """ Create a worker that consume from a queue
        This include the initialisation of an amqp connection and a channel
        """
        self.thread_id = thread_id
        self.logger = logging.getLogger(module_name).getChild(f'{self.__class__.__name__}_{thread_id}')
        self.channel = channel
        self.queue_init(self.channel, qname, topic)
        self.ctag = self.channel.basic_consume(queue=qname, callback=self.on_message_get, on_cancel=self.on_cancel)

    def queue_init(self, channel, qname, topic='#'):
        """ Construct a queue or use it on the current channel
        If the queue already exists it does nothing except if opening without the same parameters
        :param qname: the name of the amqp queue
        :param f_callback: function to callback in consume
        :return: None
        """
        channel.queue_declare(queue=qname, passive=False, durable=False, exclusive=False, auto_delete=False,
                              arguments={'expire': 300000, 'message_ttl': 300000})
        channel.queue_bind(queue=qname, exchange='xpublic', routing_key=topic)
        channel.basic_qos(0, 0, False)

    def on_message_sub(self, msg):
        """Callback method that is call by consumer when a msg is received
        :param msg: the msg received
        """
        self.logger.info(f"Received msg: topic={msg.delivery_info['routing_key']}, filename={msg.headers['filename']}, msg.body={msg.body[:100]}")
        self.logger.debug(f"msg.headers={msg.headers}, msg.body={msg.body}")
        self.channel.basic_ack(delivery_tag=msg.delivery_tag)

    def on_message_pub(self, msg, routing_key):
        """Callback method that is call by consumer when a msg is received
        :param msg: the msg received
        """
        self.logger.info(f"Published: topic={routing_key}, filename={msg.headers['filename']}, msg.body={msg.body}")

    def on_message_get(self, msg):
        """Callback method that is call by consumer when a msg is received
        :param msg: the msg received
        """
        if not os.path.exists('out'):
            os.makedirs('out')
        try:
            with open(os.path.join('out', msg.headers['filename']), 'wb') as f:
                f.write(msg.body)
        except TypeError as err:
            self.logger.error(f"err={err}, filename={msg.headers['filename']} msg.body={msg.body}")
        self.logger.info(f"Downloaded {msg.headers['filename']}")
        self.channel.basic_ack(delivery_tag=msg.delivery_tag)

    def get_msg_loop(self, is_alive=True):
        """ Start the worker job if possible
        :param is_alive: condition that enable the run of the worker
        :return: None
        """
        while is_alive:
            try:
                self.channel.basic_get()
            except (ConnectionRefusedError, ConnectionAbortedError, ConnectionResetError) as run_err:
                self.logger.info('Connection lost, aborting worker %d: %s' % (os.getpid(), run_err))
                is_alive = False
                raise
            time.sleep(DEFAULT_SLEEP_TIME)

    def on_cancel(self):
        self.logger.info('cancelled')

    def consume_msg_loop(self, is_alive, callback):
        """ Start the worker job if possible
        :param is_alive: condition that enable the run of the worker
        :return: None
        """
        while is_alive:
            try:
                # self.channel.basic_cancel(self.ctag)
                if not self.ctag:
                    self.ctag = self.channel.basic_consume(queue=self.qname, callback=callback, on_cancel=self.on_cancel)
                self.channel.basic_get()
            except (ConnectionRefusedError, ConnectionAbortedError, ConnectionResetError) as run_err:
                self.logger.info('Connection lost, aborting worker %d: %s' % (os.getpid(), run_err))
                is_alive = False
                raise
            time.sleep(DEFAULT_SLEEP_TIME)

    def pub_msg_loop(self, is_alive=True, files=None):
        """ Start the worker job if possible
        :param is_alive: condition that enable the run of the worker
        :return: None
        """
        count = 0
        for f in files:
            relpath = f.lstrip(str(Path.home()))
            routing_key = relpath.replace('/', '.').strip('\\.')
            with open(f, 'rb') as rf:
                body = rf.read()
            self.logger.debug(f'body={body}')
            header = {'filename': os.path.basename(relpath)}
            msg = Message(body, application_headers=header)
            try:
                self.channel.basic_publish(msg=msg, exchange='xpublic', routing_key=routing_key)
                self.on_message_pub(msg, routing_key)
            except (ConnectionRefusedError, ConnectionAbortedError, ConnectionResetError) as run_err:
                self.logger.info('Connection lost, aborting worker %d: %s' % (os.getpid(), run_err))
                is_alive = False
                raise
            time.sleep(DEFAULT_SLEEP_TIME)
            count += 1


def sub(thread_id, channel, qname, files):
    client = Client(thread_id, channel, qname)
    module_logger.info(' [*] Waiting for messages. To exit press CTRL+C')
    client.consume_msg_loop(True, client.on_message_sub)


def pub(thread_id, channel, qname, files):
    client = Client(thread_id, channel, qname)
    module_logger.info(' [*] Waiting for messages. To exit press CTRL+C')
    module_logger.info(len(files))
    # TODO make msg get optionnal when msg is created here (like sr_watch)
    client.pub_msg_loop(True, files)


def get(thread_id, channel, qname, files):
    client = Client(thread_id, channel, qname)
    module_logger.info(' [*] Waiting for messages. To exit press CTRL+C')
    client.get_msg_loop(True)


if __name__ == "__main__":
    # Parse args
    target = locals()[sys.argv[1]]
    nb_th = int(sys.argv[2])
    topic = sys.argv[3]
    basedir = Path(sys.argv[4])

    # Default args
    #host = 'hpfx.collab.science.gc.ca:5672'
    #host = 'dd.weather.gc.ca:5672'
    #host = 'localhost:5672'
    host = '192.168.1.69:5672'
    #qname = f'q_anonymous_sr_subscribe.dxpoc.{r1}.{r2}'
    # qname = f'q_rabbit_testing.{r1}.{r2}'
    qname = f'q_rabbit_testing'
    with Connection(host=host, userid="tfeed", password="ZTI0MjFmZGM0YzM3YmQwOWJlNjhlNjMz") as connection:
        connection.connect()

        # Scan files (pub only?)
        files = []
        if target == pub:
            files = glob.glob(os.path.join(str(basedir), '**'), recursive=True)
        files = [f for f in files if os.path.isfile(f)]
        module_logger.info(f'file count: {len(files)}, basedir={basedir}')

        # Threading management
        thrds = [threading.Thread(target=target, args=(i, connection.channel(), qname, files[int(i*th_size):int((i+1)*th_size)])) for i in range(nb_th)]
        th_size = int(len(files) / nb_th) + 1
        try:
            [t.start() for t in thrds]
            while True:
                time.sleep(0.1)
        except Exception as err:
            module_logger.error(f'err={err}', exc_info=True)
        except KeyboardInterrupt:
            [t.join() for t in thrds]
        finally:
            module_logger.info('Closing connection')
            connection.close()

