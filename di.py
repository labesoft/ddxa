import logging
import os
import random
import sys
import threading
import time
from concurrent.futures.thread import ThreadPoolExecutor
from os.path import relpath
from pathlib import Path
from queue import Queue

import amqp
from amqp import Message

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


def sub(queue):
    with amqp.Connection(host=host, userid=user, password=pwd) as c:
        ch = c.channel()
        ch.queue_declare(queue=queue, passive=False, durable=False, exclusive=False, auto_delete=False,
                         arguments={'expire': 300000, 'message_ttl': 300000})
        ch.queue_bind(queue=queue, exchange='xpublic', routing_key='#')
        ch.basic_qos(0, 0, False)

        def on_message(message):
            print('Received message (delivery tag: {}): {}'.format(message.delivery_tag, message.body))
            ch.basic_ack(message.delivery_tag)
        ch.basic_consume(queue=queue, callback=on_message)
        while True:
            c.drain_events()


def get(id, qname, basedir, host, user, pwd, topic):
    logger = logging.getLogger(module_name).getChild(f'get_{id}')
    logger.setLevel(logging.ERROR)
    try:
        with amqp.Connection(host=host, userid=user, password=pwd) as c:

            ch = c.channel()
            ch.queue_declare(queue=qname, passive=False, durable=False, exclusive=False, auto_delete=True,
                             arguments={'expire': 300000, 'message_ttl': 300000})
            ch.queue_bind(queue=qname, exchange='xpublic', routing_key=topic)
            ch.basic_qos(0, 0, False)

            def on_message(msg):
                dir_path = Path(basedir, 'out', msg.headers['rel_path'])
                dir_path.mkdir(parents=True, exist_ok=True)
                file_path = dir_path.joinpath(msg.headers['filename'])
                try:
                    if msg.body:
                        with file_path.open('wb') as f:
                            f.write(msg.body)
                    else:
                        file_path.touch(exist_ok=True)
                    logger.info(f"Downloaded: file_path={file_path}")
                except (TypeError, ConnectionResetError, FileNotFoundError) as err:
                    logger.error(f"err={err}, msg.headers={msg.headers} msg.body={msg.body}")
                ch.basic_ack(delivery_tag=msg.delivery_tag)
            ch.basic_consume(queue=qname, callback=on_message)
            while True:
                c.drain_events()
    except ConnectionResetError as err:
        logger.error(f'err={err}')
        get(id, qname, basedir, host, user, pwd, topic)


def pub(id, queue, base_dir, host, user, pwd, topic):
    logger = logging.getLogger(module_name).getChild(f'pub_{id}')
    logger.setLevel(logging.INFO)
    with amqp.Connection(host=host, userid=user, password=pwd) as c:
        channel = c.channel()

        is_alive = True
        was_empty = False
        while is_alive:
            if not queue.empty():
                was_empty = False
                item = queue.get()
                if item[0].startswith(str(Path.cwd().joinpath('out'))): continue
                logger.debug(f'item={item}')
                dir_path, files = item
                rel_path = relpath(dir_path, base_dir)
                topic = '.'.join(Path(rel_path).parts)
                for f in files:
                    file_path = Path(dir_path, f)
                    if file_path.exists() and relpath(dir_path, Path(Path.cwd(), 'out')) != rel_path and file_path.stat().st_size < 1024:
                        try:
                            with file_path.open('rb') as rf:
                                body = rf.read()
                        except (PermissionError, OSError) as err:
                            logger.error(f'err={err}, file_path={file_path}')
                            continue

                        routing_key = '.'.join([topic, f]).strip(r'.')
                        header = {'basedir': str(base_dir), 'rel_path': rel_path, 'filename': f}
                        msg = Message(body, application_headers=header)
                        try:
                            # Publish
                            channel.basic_publish(msg=msg, exchange='xpublic', routing_key=routing_key)
                            logger.info(f"Published: msg.headers={msg.headers}, msg.body={msg.body[:100]}, "
                                        f"topic={routing_key}")
                        except (ConnectionRefusedError, ConnectionAbortedError, ConnectionResetError) as run_err:
                            logger.error('Connection lost, aborting worker %d: %s' % (os.getpid(), run_err))
            elif not was_empty:
                logger.info('queue is empty')
                time.sleep(10)
                was_empty = True
            else:
                is_alive = False

def fill_files_queue(basedir, files_q):
    for dirpath, dir_node, file_node in os.walk(basedir):
        if file_node:
            files_q.put((dirpath, file_node))


if __name__ == "__main__":
    # Parse args
    target = locals()[sys.argv[1]]
    nb_thread = int(sys.argv[2])
    topic = sys.argv[3]
    basedir = Path(sys.argv[4])
    module_logger.info(f'target={sys.argv[1]}, nb_thread={nb_thread}, topic={topic}, basedir={basedir}')

    # Default args
    user = "tfeed"
    pwd = "ZTI0MjFmZGM0YzM3YmQwOWJlNjhlNjMz"
    host = '192.168.1.69:5672'

    # Threading management
    with ThreadPoolExecutor(max_workers=nb_thread+1) as ilyn_payne:
        if target == pub:
            q = Queue()
            ilyn_payne.submit(fill_files_queue, basedir, q)
        else:
            q = f'q_rabbit_testing_{r1}_{r2}'

        for i in range(nb_thread):
            ilyn_payne.submit(target, i, q, basedir, host, user, pwd, topic)

