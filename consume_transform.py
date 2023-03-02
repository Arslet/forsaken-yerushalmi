from confluent_kafka import Consumer
from schema_transformation import process_book_schema_dict_list
from link_transformation import process_book_links_dict_list, processed_leftover_book_links_dataframe
import pandas as pd
import json, time, random, itertools
from backup_restore import backup_schemas, backup_processed_links, backup_leftover_links, restore_schemas, restore_leftover_links, restore_processed_links

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
  
  return schemas_dataframe.drop_duplicates(subset=["title"], keep="first")

def drop_duplicates_processed_links(links_dataframe):
  if links_dataframe is None:
    return None
  
  # Order of Text and Cited Text shouldn't be reversed!
  return links_dataframe.drop_duplicates(subset=["Text Title", "Cited Text Title"], keep="first")

def drop_duplicates_leftover_links(leftover_links_dataframe):
  if leftover_links_dataframe is None:
    return None
  
  return leftover_links_dataframe.drop_duplicates(subset=["Text 1", "Text 2"], keep="first")

def continuously_consume():
  LOOP_TIMEOUT = 1

  schemas_consumer = Consumer({'bootstrap.servers':'localhost:9092','group.id': str(random.randint(1, 10000)), 'auto.offset.reset':'earliest'})
  schemas_consumer.subscribe(['book_schemas'])

  links_consumer = Consumer({'bootstrap.servers':'localhost:9092','group.id': str(random.randint(1, 10000)), 'auto.offset.reset':'earliest'})
  links_consumer.subscribe(['book_links'])

  schemas_dataframe = restore_schemas()
  links_dataframe = restore_processed_links()
  leftover_links_dataframe = restore_leftover_links()

  try:
    while True:
      schemas_dict_list = poll(schemas_consumer)
      new_schemas_dataframe = process_book_schema_dict_list(schemas_dict_list)
      schemas_dataframe = concat_dataframes(schemas_dataframe, new_schemas_dataframe)
      schemas_dataframe = drop_duplicates_schemas(schemas_dataframe)
      
      print(f"{schemas_dataframe=}")

      if schemas_dataframe is not None:
        new_leftover_links_transformation_datas = processed_leftover_book_links_dataframe(leftover_links_dataframe, schemas_dataframe)
        matched_leftover_links = new_leftover_links_transformation_datas.links
        leftover_links_dataframe = new_leftover_links_transformation_datas.leftovers
        
        links_dict_list = poll(links_consumer)
        new_links_transformation_datas = process_book_links_dict_list(links_dict_list, schemas_dataframe)
        new_links_dataframe = new_links_transformation_datas.links
        new_leftover_links_dataframe = new_links_transformation_datas.leftovers

        links_dataframe = concat_dataframes(links_dataframe, new_links_dataframe)
        links_dataframe = concat_dataframes(links_dataframe, matched_leftover_links)
        links_dataframe = drop_duplicates_processed_links(links_dataframe)
        
        leftover_links_dataframe = concat_dataframes(leftover_links_dataframe, new_leftover_links_dataframe)
        leftover_links_dataframe = drop_duplicates_leftover_links(leftover_links_dataframe)


        print(f"{links_dataframe=}")
        print(f"{leftover_links_dataframe=}")

      time.sleep(LOOP_TIMEOUT)
  
  except (Exception, KeyboardInterrupt) as e:
    print(f"Backing up, exception occured: {e}")
    backup_schemas(schemas_dataframe)
    backup_processed_links(links_dataframe)
    backup_leftover_links(leftover_links_dataframe)

    raise
    
if __name__ == '__main__':
  continuously_consume()
