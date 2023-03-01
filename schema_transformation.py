import pandas as pd
import numpy as np
from warnings import warn
from pipe import nullable_pipe, pipe
from functools import partial

def convert_book_schema_dict_list_to_dataframe(dict_list):
    book_schema_fields = ["title",
    "era",
    "compDate",
    "dependence",
    "categories"]

    book_schemas_dataframe = pd.DataFrame.from_records(dict_list, columns=book_schema_fields)

    return book_schemas_dataframe

def normalize_dates(dataframe):
    def regex_replace_in_field(dataframe, pattern, replacement, field):
        column_after_replace = dataframe[field].replace(pattern, replacement, regex=True)
        dataframe_after_replace = dataframe.copy(deep=False)
        dataframe_after_replace[field] = column_after_replace

        return dataframe_after_replace

    replace_date = partial(regex_replace_in_field, field="compDate")

    positive_capture = r"\1"
    negative_capture = r"-\1"

    earlier_date_in_range_pattern = r"(-?\d+)-\d+"
    extract_earlier_date = partial(replace_date, pattern=earlier_date_in_range_pattern, replacement=positive_capture)
    
    date_from_circa_date_pattern = r"ca?\.\s*(-?\d+)"
    extract_date_from_circa = partial(replace_date, pattern=date_from_circa_date_pattern, replacement=positive_capture)
    
    date_from_bce_date_pattern = r"(\d+) BCE?"
    extract_date_from_bce_date = partial(replace_date, pattern=date_from_bce_date_pattern, replacement=negative_capture)

    transform_dates_pipeline = pipe(
        dataframe,
        extract_earlier_date,
        extract_date_from_circa,
        extract_date_from_bce_date
    )

    return transform_dates_pipeline

def warn_invalid_dates(dataframe):
    invalid_date_pattern = r"\d*[^-\d]+\d*"
    missed_entries = dataframe[dataframe["compDate"].str.match(invalid_date_pattern, na=False)]
    missed_compDates = missed_entries["compDate"]

    if len(missed_compDates) > 0:
        warn(f"These compDate values will be dismissed and converted to NaN: {list(missed_compDates)}")

    return dataframe

def convert_dates_to_int(dataframe):
    numeric_dates = pd.to_numeric(dataframe["compDate"], errors="coerce")
    int_dates = numeric_dates.astype("Int64")

    dataframe_with_dates_as_int = dataframe.copy(deep=False)
    dataframe_with_dates_as_int["compDate"] = int_dates

    return dataframe_with_dates_as_int

def convert_eras_to_int(dataframe):
    era_int = {"T": 1, "A": 2, "RI": 3, "AH": 4, "CO": 5}

    era_as_mixed_int = dataframe["era"].replace(era_int)
    era_as_numeric = pd.to_numeric(era_as_mixed_int, errors="coerce")
    era_as_int = era_as_numeric.astype("Int64")

    dataframe_with_eras_as_int = dataframe.copy(deep=False)
    dataframe_with_eras_as_int["era"] = era_as_int

    return dataframe_with_eras_as_int

def add_is_commentary_column(dataframe):
    is_dependence_commentary = dataframe.dependence == "Commentary"

    #Have to use apply because pandas doesn't have operations that support lists in columns.
    has_category_commentary = dataframe.categories.apply(lambda category_list: "Commentary" in category_list)

    #Have to assign to temporary variable, direct assignment makes the whole column False.
    is_any_commentary = is_dependence_commentary | has_category_commentary

    dataframe_with_is_commentary = dataframe.copy(deep=False)
    dataframe_with_is_commentary["is_commentary"] = is_any_commentary

    return dataframe_with_is_commentary

def drop_dependence_column(dataframe):
    return dataframe.drop("dependence", axis=1)

def add_is_modern_column(dataframe):
    is_modern = dataframe.categories.apply(lambda category_list: "Modern Works" in category_list)
    dataframe_with_is_modern = dataframe.copy(deep=False)
    dataframe_with_is_modern["is_modern"] = is_modern

    return dataframe_with_is_modern

def drop_no_date_info_books(dataframe):
    #Drop all titles without any dating info

    no_date_mask = dataframe.era.isna() & dataframe.compDate.isna() & ~dataframe.is_modern & ~dataframe.is_commentary
    no_date_entries = dataframe[no_date_mask]
    no_date_titles = no_date_entries["title"]

    if len(no_date_entries) > 0:
        warn(f"The following entries have no dating info and will be dropped: {no_date_titles}")
    
    return dataframe.drop(no_date_entries.index)
    
def process_book_schema_dict_list(dict_list):
    return nullable_pipe(
        dict_list,
        convert_book_schema_dict_list_to_dataframe,
        normalize_dates,
        warn_invalid_dates,
        convert_dates_to_int,
        convert_eras_to_int,
        add_is_commentary_column,
        drop_dependence_column,
        add_is_modern_column,
        drop_no_date_info_books,
    )