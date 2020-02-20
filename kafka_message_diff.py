from jsondiff import diff, delete, update, insert
from psycopg2.errors import UniqueViolation
import psycopg2
from confluent_kafka import Consumer, Producer, KafkaError
import json
import logging as log
import configparser

config = configparser.ConfigParser()
config.read('config.ini')

log.basicConfig(level=config['logging']['level'])

conn = psycopg2.connect(
    host=config['db']['host'],
    database=config['db']['name'],
    user=config['db']['user'],
    password=config['db']['password'],
    port=config['db']['port']
    )

MESSAGE_KEY = config['app']['message_key']


def process_message(msg_text):
    """
    Process the message, insert
    """
    log.info('Received message: {}'.format(msg_text))
    cur = conn.cursor()

    try:
        msg_text_dict = json.loads(msg_text)
        try:
            if len(msg_text_dict[MESSAGE_KEY]) > 0:
                try:
                    cur.execute(
                        "INSERT INTO messages (message_key, message) VALUES (%s,%s)",
                        (msg_text_dict[MESSAGE_KEY], msg_text)
                    )
                    conn.commit()
                    log.info("INSERTED {}".format(msg_text_dict[MESSAGE_KEY]))
                    push_message = build_push_message(msg_text_dict)
                    kafka_publish(push_message, config['kafka-write']['topic'])
                    


                except UniqueViolation:
                    conn.rollback()
                    push_message_diff(msg_text_dict)
                    cur.execute(
                        "UPDATE messages set message = %s where message_key = %s",
                        (msg_text, msg_text_dict[MESSAGE_KEY])
                    )
                    conn.commit()
                    log.info("UPDATED: {}".format(msg_text_dict[MESSAGE_KEY]))

        except KeyError as e:
            log.error("MESSAGE_KEY: {} not found".format(MESSAGE_KEY))
            log.error(e)

    except json.JSONDecodeError as e:
        log.error("Message is not a valid JSON")

    cur.close()


def listen_to_kafka():
    c = Consumer({
        'bootstrap.servers': config['kafka-read']['bootstrap-servers'],
        'group.id': config['kafka-read']['group-id'],
    })

    c.subscribe([config['kafka-read']['topic']])

    while True:
        msg = c.consume()

        if msg is None:
            continue

        for message in msg:
            msg_text = message.value().decode('utf-8')
            process_message(msg_text)

    c.close()


def push_message_diff(new_message):
    """
    Generate JSON diff with existing DB message and push diff to kafka
    """
    cur = conn.cursor()
    cur.execute(
        "SELECT message from messages where message_key = '%s'"
        % new_message[MESSAGE_KEY]
    )

    existing_message = cur.fetchone()[0]
    diff_message = diff(existing_message, new_message,
                        syntax='explicit')

    diff_message_dict = {}

    try:
        diff_message_dict['inserted'] = diff_message[insert]
    except:
        pass
    
    try:
        diff_message_dict['updated'] = diff_message[update]
    except:
        pass
    
    try:
        diff_message_dict['deleted'] = diff_message[delete]
    except:
        pass


    if len(diff_message) > 0 :
        push_message = build_push_message(new_message, diff_message_dict)
        kafka_publish(push_message, config['kafka-write']['topic'])

    log.debug(diff_message)
    cur.close()


def kafka_publish(message, topic):
    log.debug("Sending message" + message)
    p = Producer({'bootstrap.servers': config['kafka-write']['bootstrap-servers']})
    p.poll(0)

    p.produce(topic, message.encode('utf-8'), callback=delivery_report)

    p.flush()


def delivery_report(err, msg):
    """ Called once for each message produced to indicate delivery result.
        Triggered by poll() or flush(). """
    if err is not None:
        print('Message delivery failed: {}'.format(err))
    else:
        print('Message delivered to {} [{}]'.format(
            msg.topic(), msg.partition()))

def build_push_message(message,diff=None):
    push_message = { MESSAGE_KEY : message[MESSAGE_KEY] }
    if diff:
        push_message['status'] = "updated"
        push_message['data'] = diff
    else:
        push_message['status'] = "created"
        push_message['data'] = message
    
    return json.dumps(push_message)



if __name__ == "__main__":
    listen_to_kafka()
