import pandas as pd
import numpy as np
from pipe import nullable_pipe
from warnings import warn

def convert_book_link_dict_list_to_dataframe(dict_list):
  return pd.DataFrame.from_records(dict_list)

def drop_self_links(links_dataframe):
  self_link_mask = links_dataframe["Text 1"] == links_dataframe["Text 2"]
  self_link_entries = links_dataframe[self_link_mask]

  return links_dataframe.drop(self_link_entries.index)

def convert_link_count_to_int(links_dataframe):
    numeric_links = pd.to_numeric(links_dataframe["Link Count"], errors="coerce")
    int_links = numeric_links.astype("Int64")

    link_as_int_dataframe = links_dataframe.copy(deep=False)
    link_as_int_dataframe["Link Count"] = int_links

    return link_as_int_dataframe

def add_publication_orders_closure(schemas_dataframe):
  order_of_sort_by_columns = ["compDate", "is_modern", "era", "is_commentary"]
  sorted_schemas_dataframe = schemas_dataframe.sort_values(order_of_sort_by_columns, ascending=True, ignore_index=True)
  sorted_schemas_dataframe["index"] = sorted_schemas_dataframe.index

  index_title_for_text_1 = sorted_schemas_dataframe[["index", "title"]]
  index_title_for_text_2 = sorted_schemas_dataframe[["index", "title"]]

  index_title_for_text_1 = index_title_for_text_1.rename(columns = {"title": "Text 1", "index": "Publication Order 1"})
  index_title_for_text_2 = index_title_for_text_2.rename(columns = {"title": "Text 2", "index": "Publication Order 2"})

  def add_publication_orders_to_links(links_dataframe):
    link_dataframe_with_publication_orders = links_dataframe.copy(deep=False)
    link_dataframe_with_publication_orders = link_dataframe_with_publication_orders.merge(index_title_for_text_1, on="Text 1")
    link_dataframe_with_publication_orders = link_dataframe_with_publication_orders.merge(index_title_for_text_2, on="Text 2")

    return link_dataframe_with_publication_orders

  return add_publication_orders_to_links

def drop_links_to_nonexisting_books(links_dataframe):
  #by default not needed because of inner merge

  links_to_nonexisting_books_mask = links_dataframe["Publication Order 1"].isna() | links_dataframe["Publication Order 2"].isna()
  links_to_nonexisting_books_entries = links_dataframe[links_to_nonexisting_books_mask]
  nonexisting_books_titles = links_to_nonexisting_books_entries[["Text 1", "Text 2"]]

  if len(links_to_nonexisting_books_entries) > 0:
    warn(f"The following links refer to nonexisting books and will be dropped for now: {nonexisting_books_titles}")

  return links_dataframe.drop(links_to_nonexisting_books_entries.index)

def organize_into_text_and_cited(links_dataframe):  
  text_date_diff = links_dataframe["Publication Order 1"] - links_dataframe["Publication Order 2"]
  text_title = np.where(text_date_diff.gt(0), links_dataframe["Text 1"], links_dataframe["Text 2"])
  cited_title = np.where(text_date_diff.le(0), links_dataframe["Text 1"], links_dataframe["Text 2"])

  dataframe_with_text_and_cited = links_dataframe.copy(deep=False)

  dataframe_with_text_and_cited.insert(0, "Text Title", text_title)
  dataframe_with_text_and_cited.insert(1, "Cited Text Title", cited_title)

  return dataframe_with_text_and_cited[["Text Title", "Cited Text Title", "Link Count"]]

def process_book_links_dict_list(links_dict_list, schemas_dataframe):
  return nullable_pipe(
    links_dict_list,
    convert_book_link_dict_list_to_dataframe,
    drop_self_links,
    convert_link_count_to_int,
    add_publication_orders_closure(schemas_dataframe),
    drop_links_to_nonexisting_books,
    organize_into_text_and_cited
  )