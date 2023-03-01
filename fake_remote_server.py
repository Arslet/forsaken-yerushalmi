from confluent_kafka import Producer

from pymongo import MongoClient
import json, time

client = MongoClient()

#System-specific setup

db = client.forsaken
book_schemas = db.rawSchemas
book_links = db.rawLinks

def receipt(err,msg):
    if err is not None:
        print('Error: {}'.format(err))
    else:
        message = 'Produced message on topic {} with value of {}\n'.format(msg.topic(), msg.value().decode('utf-8'))
        print(message)

def send(producer, topic, callback, data):
  json_msg = json.dumps(data)
  producer.produce(topic, json_msg.encode('utf-8'), callback=callback)
  producer.flush()

def collection_len(collection):
    return collection.count_documents(dict())

def documents_generator(collection, num_of_documents):
    offset = 0
    amount_of_documents = collection_len(collection)
    
    while offset < amount_of_documents:
        yield list(collection.find(projection={"_id":0}).skip(offset).limit(num_of_documents))
        
        offset += num_of_documents

def continuously_produce(collection, topic, batch_size, timeout):
    producer = Producer({'bootstrap.servers':'localhost:9092'})
    schema_generator = documents_generator(collection, batch_size)

    from functools import partial

    send_schemas = partial(send, producer, topic, receipt)
    for schemas in schema_generator:
        send_schemas(schemas)
        time.sleep(timeout)

def create_topics(topics):
    from confluent_kafka.admin import AdminClient, NewTopic

    PARTITIONS = 1
    REPLICAS = 1

    admin_client = AdminClient({
        "bootstrap.servers": "localhost:9092"
    })

    existing_topics = admin_client.list_topics().topics.keys()

    for topic in topics:
        if topic not in existing_topics:
            topic_list = [NewTopic(topic, PARTITIONS, REPLICAS)]
            admin_client.create_topics(topic_list)

if __name__ == '__main__':
    import argparse
    argparser = argparse.ArgumentParser(description='Start producing')

    collections = {
        'schemas': {'mongo_collection': book_schemas, 'topic': 'book_schemas'}, 
        'links': {'mongo_collection': book_links, 'topic': 'book_links'}
    }

    create_topics([collection["topic"] for collection in collections.values()])

    argparser.add_argument('-c', '--collection', choices=collections.keys(), help='Collection to produce', required=True, dest="collection")
    argparser.add_argument('-t', '--timeout', help='Timeout between producings', default=1, type=float, dest="timeout")
    argparser.add_argument('-b', '--batch-size', help='How many documents to send each loop', default=2, type=int, dest="batch_size")

    args = argparser.parse_args()
    chosen_collection = collections[args.collection]
    mongo_collection = chosen_collection['mongo_collection']
    topic = chosen_collection['topic']
    batch_size = args.batch_size
    timeout = args.timeout

    continuously_produce(mongo_collection, topic, batch_size, timeout)