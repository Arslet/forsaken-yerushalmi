from pymongo import MongoClient
import pandas as pd

client = MongoClient()

#System-specific setup

db = client.forsaken
processed_book_schemas = db["processedSchemas"]
processed_book_links = db["processedLinks"]
leftover_book_links = db["leftoverLinks"]

def backup(collection, dataframe):
  if (dataframe is None):
    return None
  
  collection.drop()
  data_as_series_without_nan = dataframe.apply(lambda x : x.dropna().to_dict(),axis=1)
  data_as_dict_list = data_as_series_without_nan.to_dict().values()
  
  return collection.insert_many(data_as_dict_list)

def backup_schemas(schemas_dataframe):
  return backup(collection=processed_book_schemas, dataframe=schemas_dataframe)

def backup_processed_links(links_dataframe):
  return backup(collection=processed_book_links, dataframe=links_dataframe)

def backup_leftover_links(leftover_links_dataframe):
  return backup(collection=leftover_book_links, dataframe=leftover_links_dataframe)

def get_dataframe_from_collection(collection):
  dict_list = list(collection.find(projection={"_id": 0}))
  
  return pd.DataFrame(dict_list)

def restore_schemas():
  restored_schema_dataframe = get_dataframe_from_collection(processed_book_schemas)
  
  if len(restored_schema_dataframe) == 0:
    return None

  restored_schema_dataframe["era"] = restored_schema_dataframe["era"].astype("Int64")
  restored_schema_dataframe["compDate"] = restored_schema_dataframe["compDate"].astype("Int64")

  return restored_schema_dataframe

def restore_links(link_collection):
  restored_links_dataframe = get_dataframe_from_collection(link_collection)

  if len(restored_links_dataframe) == 0:
    return None

  restored_links_dataframe["Link Count"] = restored_links_dataframe["Link Count"].astype("Int64")

  return restored_links_dataframe 

def restore_processed_links():
  return restore_links(processed_book_links)

def restore_leftover_links():
  return restore_links(leftover_book_links)