import glob
import json
import logging
import os
import random
import sys
import threading
import time
import urllib.parse
import urllib.request
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
    def __init__(self, thread_id, host='localhost:5672', userid='anonymous', password='anonymous'):
        """ Create a worker that consume from a queue
        This include the initialisation of an amqp connection and a channel
        """
        self.thread_id = thread_id
        self.ctag = f'ctag-{r1}-{r2}-th{self.thread_id}'
        self.logger = logging.getLogger(module_name).getChild(f'{self.__class__.__name__}_{thread_id}')
        self.connection = Connection(host=host, userid=userid, password=password)
        self.connection.connect()
        self.channel = self.connection.channel()

    def queue_init(self, qname, f_callback, topic='#'):
        """ Construct a queue or use it on the current channel
        If the queue already exists it does nothing except if opening without the same parameters
        :param qname: the name of the amqp queue
        :param f_callback: function to callback in consume
        :return: None
        """
        self.qname = qname
        self.channel.queue_declare(queue=qname, passive=False, durable=False, exclusive=False, auto_delete=True,
                                arguments={'expire': 300000, 'message_ttl': 300000})
        self.channel.queue_bind(queue=qname, exchange='xpublic', routing_key=topic)
        self.channel.basic_qos(0, 1, False)

    def on_message_sub(self, msg):
        """Callback method that is call by consumer when a msg is received
        :param msg: the msg received
        """
        self.logger.info(f"Received msg: topic={msg.delivery_info['routing_key']}, filename={msg.headers['filename']}, msg.body={msg.body}")
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
        host, absolute_filepath = tuple(msg.body.split(' ')[1:])
        relative_filepath = absolute_filepath.lstrip('/')
        url = urllib.parse.urljoin(host, relative_filepath)
        if not os.path.exists(os.path.dirname(relative_filepath)):
            os.makedirs(os.path.dirname(relative_filepath))
        try:
            urllib.request.urlretrieve(url, relative_filepath)
        except Exception as err:
            msg_dump = {'properties': msg.properties, 'body': msg.body, 'delivery_info': msg.delivery_info}
            msg_dump = json.dumps(msg_dump, indent=1)
            self.logger.error(f'err={err}, msg={msg}')
            self.logger.debug(f'msg_dump={msg_dump}')
        self.logger.info(f'Downloaded {os.path.join(os.path.dirname(__file__), relative_filepath)}')
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

    def consume_msg_loop(self, is_alive=True):
        """ Start the worker job if possible
        :param is_alive: condition that enable the run of the worker
        :return: None
        """
        first_run = True

        while is_alive:
            try:
                self.channel.basic_cancel(self.ctag)
                self.ctag = self.channel.basic_consume(queue=self.qname, callback=self.on_message_sub, on_cancel=self.on_cancel)
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

    def close(self):
        """ Directly close the connection
        :return: None
        """
        self.logger.info('Closing connection')
        self.connection.close()


def sub(thread_id=1, files=None, topic='#'):
    # host = 'hpfx.collab.science.gc.ca:5672'
    # host = 'dd.weather.gc.ca:5672'
    host = 'localhost:5672'
    # topic = '*.*.20200728.WXO-DD.#'
    qname = f'q_rabbit_testing_{r1}_{r2}'
    # qname = f'q_anonymous_sr_subscribe.dxpoc.{r1}.{r2}'
    client = Client(thread_id, host, userid="tfeed", password="ZTI0MjFmZGM0YzM3YmQwOWJlNjhlNjMz")
    client.queue_init(qname, client.on_message_sub, topic)
    module_logger.info(' [*] Waiting for messages. To exit press CTRL+C')
    try:
        client.consume_msg_loop()
    except Exception as err:
        module_logger.error(f'err={err}', exc_info=True)
    finally:
        client.close()


def pub(thread_id=1, files=None):
    # host = 'hpfx.collab.science.gc.ca:5672'
    # host = 'dd.weather.gc.ca:5672'
    host = 'localhost:5672'
    client = Client(thread_id, host, userid="tfeed", password="ZTI0MjFmZGM0YzM3YmQwOWJlNjhlNjMz")
    # client.queue_init(f'q_anonymous_sr_subscribe.dxpoc.{r1}.{r2}', client.on_message_pub)
    module_logger.info(' [*] Waiting for messages. To exit press CTRL+C')
    module_logger.info(len(files))
    try:
        # TODO make msg get optionnal when msg is created here (like sr_watch)
        client.pub_msg_loop(True, files)
    except Exception as err:
        module_logger.error(f'err={err}', exc_info=True)
    finally:
        client.close()


def get(thread_id=1):
    # host = 'hpfx.collab.science.gc.ca:5672'
    # host = 'dd.weather.gc.ca:5672'
    host = 'localhost:5672'
    # qname = f'q_anonymous_sr_subscribe.dxpoc.{r1}.{r2}'
    qname = f'q_rabbit_testing'
    client = Client(thread_id, host, userid="tfeed", password="ZTI0MjFmZGM0YzM3YmQwOWJlNjhlNjMz")
    client.queue_init(qname, client.on_message_get)
    module_logger.info(' [*] Waiting for messages. To exit press CTRL+C')
    try:
        client.get_msg_loop()
    except Exception as err:
        module_logger.error(f'err={err}', exc_info=True)
    finally:
        client.close()


if __name__ == "__main__":
    target = locals()[sys.argv[1]]
    nb_th = int(sys.argv[2])
    topic = sys.argv[3]
    files = []
    if target == pub:
        files = glob.glob(os.path.join(str(Path.home()), '**'), recursive=True)
    files = [f for f in files if os.path.isfile(f)]
    module_logger.info(f'file number: {len(files)}, dir={os.getcwd()}')
    th_size = int(len(files) / nb_th) + 1
    thrds = [threading.Thread(target=target, args=(i, files[int(i*th_size):int((i+1)*th_size)], topic)) for i in range(nb_th)]

    [t.start() for t in thrds]

    try:
        while True:
            time.sleep(0.1)
    except KeyboardInterrupt:
        [t.join() for t in thrds]
