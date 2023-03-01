from confluent_kafka import Consumer
from schema_transformation import process_book_schema_dict_list
from link_transformation import process_book_links_dict_list
import pandas as pd
import json, time, random, itertools
from backup_restore import backup_schemas, backup_links, restore_schemas, restore_links

def poll(consumer):
  TIMEOUT = 5
  MAX_MESSAGES_AMOUNT = 1000

  messages = consumer.consume(MAX_MESSAGES_AMOUNT, TIMEOUT)

  if len(messages) == 0:
      return None
      
  messages_data = [
    message.value().decode('utf-8') \
    if not message.error() \
    else print('Error: {}'.format(message.error())) \
    for message in messages
  ]

  json_data_list = [json.loads(data) for data in messages_data]
  dict_list = list(itertools.chain(*json_data_list))

  return dict_list

def concat_dataframes(old, new):
  if new is None:
    return old
  
  if old is None:
    return new

  return pd.concat([old, new])

def drop_duplicates_schemas(schemas_dataframe):
  if schemas_dataframe is None:
    return None
  
  return schemas_dataframe.drop_duplicates(subset=["title"], keep="last")

def drop_duplicates_links(links_dataframe):
  if links_dataframe is None:
    return None
  
  # Order of Text and Cited Text shouldn't be reversed!
  return links_dataframe.drop_duplicates(subset=["Text Title", "Cited Text Title"], keep="last")

def continuously_consume():
  LOOP_TIMEOUT = 1

  schemas_consumer = Consumer({'bootstrap.servers':'localhost:9092','group.id': str(random.randint(1, 10000)), 'auto.offset.reset':'earliest'})
  schemas_consumer.subscribe(['book_schemas'])

  links_consumer = Consumer({'bootstrap.servers':'localhost:9092','group.id': str(random.randint(1, 10000)), 'auto.offset.reset':'earliest'})
  links_consumer.subscribe(['book_links'])

  schemas_dataframe = restore_schemas()
  links_dataframe = restore_links()

  try:
    while True:
      schemas_dict_list = poll(schemas_consumer)
      new_schemas_dataframe = process_book_schema_dict_list(schemas_dict_list)
      schemas_dataframe = concat_dataframes(schemas_dataframe, new_schemas_dataframe)
      schemas_dataframe = drop_duplicates_schemas(schemas_dataframe)
      
      print(schemas_dataframe)

      if schemas_dataframe is not None:
        links_dict_list = poll(links_consumer)
        new_links_dataframe = process_book_links_dict_list(links_dict_list, schemas_dataframe)
        links_dataframe = concat_dataframes(links_dataframe, new_links_dataframe)
        links_dataframe = drop_duplicates_links(links_dataframe)
        
        print(links_dataframe)

      time.sleep(LOOP_TIMEOUT)
  
  except (Exception, KeyboardInterrupt):
    backup_schemas(schemas_dataframe)
    backup_links(links_dataframe)
    
if __name__ == '__main__':
  continuously_consume()
