import configparser
from confluent_kafka import Consumer, KafkaError

def error_cb(error):
    print(error)

def main():
    config = configparser.ConfigParser()
    config.read('application.ini')

    c = Consumer({
        'bootstrap.servers': config['DEFAULT']['endpoints'],
        'auto.offset.reset': config['DEFAULT']['auto-offset-reset-policy'],
        'group.id' : config['DEFAULT']['consumer-group'],
        'client.id' : config['DEFAULT']['client-id'],
        'security.protocol': config['DEFAULT']['security-protocol'],
        'sasl.mechanism':   config['DEFAULT']['security-sasl-mechanism'],
        'sasl.username':  config['DEFAULT']['username'],
        'sasl.password':  config['DEFAULT']['password'],
        'error_cb': error_cb
    })

    c.subscribe([config['DEFAULT']['topic']])

    while True:
        msg = c.poll(10.0)

        if msg is None:
            continue
        if msg.error():
            print("Consumer error: {}".format(msg.error()))
            continue

        print('Received message: {}'.format(msg.value().decode('utf-8')))

if __name__ == "__main__":
    main()