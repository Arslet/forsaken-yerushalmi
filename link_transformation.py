import pandas as pd
import numpy as np
from pipe import pipe
from warnings import warn
from collections import namedtuple

TransformationDatas = namedtuple('TransformationDatas', ['links', 'schemas', 'leftovers'], defaults=[None,None,None])

def update_transformation_datas(datas_tuple, links=None, schemas=None, leftovers=None):
  get_updated_or_old = lambda updated, old: updated if updated is not None else old
  updated_links = get_updated_or_old(links, datas_tuple.links)
  updated_schemas = get_updated_or_old(schemas, datas_tuple.schemas)
  updated_leftovers = get_updated_or_old(leftovers, datas_tuple.leftovers)

  return TransformationDatas(links=updated_links,schemas=updated_schemas,leftovers=updated_leftovers)

def convert_book_link_dict_list_to_dataframe(transformation_datas):
  links_dataframe = pd.DataFrame.from_records(transformation_datas.links)
  
  return update_transformation_datas(transformation_datas, links=links_dataframe)

def drop_self_links(transformation_datas):
  links_dataframe = transformation_datas.links
  
  self_link_mask = links_dataframe["Text 1"] == links_dataframe["Text 2"]
  self_link_entries = links_dataframe[self_link_mask]
  links_dataframe_without_self_links = links_dataframe.drop(self_link_entries.index)

  return update_transformation_datas(transformation_datas, links=links_dataframe_without_self_links)

def convert_link_count_to_int(transformation_datas):
  links_dataframe = transformation_datas.links
  
  numeric_links = pd.to_numeric(links_dataframe["Link Count"], errors="coerce")
  int_links = numeric_links.astype("Int64")

  link_as_int_dataframe = links_dataframe.copy(deep=False)
  link_as_int_dataframe["Link Count"] = int_links

  return update_transformation_datas(transformation_datas, links=link_as_int_dataframe)

def add_date_index_to_schemas(transformation_datas):
  schemas_dataframe = transformation_datas.schemas
  
  order_of_sort_by_columns = ["compDate", "is_modern", "era", "is_commentary"]
  sorted_schemas_dataframe = schemas_dataframe.sort_values(order_of_sort_by_columns, ascending=True, ignore_index=True)
  sorted_schemas_dataframe["index"] = sorted_schemas_dataframe.index

  return update_transformation_datas(transformation_datas, schemas=sorted_schemas_dataframe)

def add_publication_orders_to_links(transformation_datas):
  schemas_dataframe = transformation_datas.schemas

  index_title_for_text_1 = schemas_dataframe[["index", "title"]]
  index_title_for_text_2 = schemas_dataframe[["index", "title"]]

  index_title_for_text_1 = index_title_for_text_1.rename(columns = {"title": "Text 1", "index": "Publication Order 1"})
  index_title_for_text_2 = index_title_for_text_2.rename(columns = {"title": "Text 2", "index": "Publication Order 2"})

  links_dataframe = transformation_datas.links
  
  link_dataframe_with_publication_orders = links_dataframe.copy(deep=False)
  link_dataframe_with_publication_orders = link_dataframe_with_publication_orders.merge(index_title_for_text_1, how="left", on="Text 1")
  link_dataframe_with_publication_orders = link_dataframe_with_publication_orders.merge(index_title_for_text_2, how="left", on="Text 2")

  return update_transformation_datas(transformation_datas, links=link_dataframe_with_publication_orders)

def drop_links_to_nonexisting_books(transformation_datas):
  links_dataframe = transformation_datas.links
  links_to_nonexisting_books_mask = links_dataframe["Publication Order 1"].isna() | links_dataframe["Publication Order 2"].isna()
  links_to_nonexisting_books_entries = links_dataframe[links_to_nonexisting_books_mask]

  links_dataframe_without_nonexisting_books = links_dataframe.drop(links_to_nonexisting_books_entries.index)
  organized_leftover_links = links_to_nonexisting_books_entries[["Text 1", "Text 2", "Link Count"]]

  return update_transformation_datas(transformation_datas, links=links_dataframe_without_nonexisting_books, leftovers=organized_leftover_links) 

def organize_into_text_and_cited(transformation_datas): 
  links_dataframe = transformation_datas.links

  text_date_diff = links_dataframe["Publication Order 1"] - links_dataframe["Publication Order 2"]
  text_title = np.where(text_date_diff.gt(0), links_dataframe["Text 1"], links_dataframe["Text 2"])
  cited_title = np.where(text_date_diff.le(0), links_dataframe["Text 1"], links_dataframe["Text 2"])

  dataframe_with_text_and_cited = links_dataframe.copy(deep=False)

  dataframe_with_text_and_cited.insert(0, "Text Title", text_title)
  dataframe_with_text_and_cited.insert(1, "Cited Text Title", cited_title)

  organized_links_dataframe = dataframe_with_text_and_cited[["Text Title", "Cited Text Title", "Link Count"]]
  
  return update_transformation_datas(transformation_datas, links=organized_links_dataframe) 

def process_book_links_dict_list(links_dict_list, schemas_dataframe):
  if (links_dict_list is None):
    return TransformationDatas()

  datas = TransformationDatas(links=links_dict_list, schemas=schemas_dataframe)

  return pipe(
    datas,
    convert_book_link_dict_list_to_dataframe,
    drop_self_links,
    convert_link_count_to_int,
    add_date_index_to_schemas,
    add_publication_orders_to_links,
    drop_links_to_nonexisting_books,
    organize_into_text_and_cited
  )

def processed_leftover_book_links_dataframe(leftover_links_dataframe, schemas_dataframe):
  if (leftover_links_dataframe is None):
    return TransformationDatas()

  datas = TransformationDatas(links=leftover_links_dataframe, schemas=schemas_dataframe)

  return pipe(
    datas,
    add_date_index_to_schemas,
    add_publication_orders_to_links,
    drop_links_to_nonexisting_books,
    organize_into_text_and_cited
  )