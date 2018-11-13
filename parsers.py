import os
import pandas as pd
from CONSTANTS import RECORD_HANDLING_MAP


def parse_artist(record_iterable):
    # split author list to rows
    # de-dupe paper list
    # zip up into writes -- 1Article:MAuthors
    print(record_iterable)


def normalize(record_iterable, name):
    for record in record_iterable:
        parse_artist(record)


def iter_parse_csv(name, path):
    """ingest and validate csv files
    :yield:    dict(str, str)"""

    df = pd.read_csv(
        os.path.join(path, f"{name}.csv"),
        index_col=False,
        quotechar='"',
        skipinitialspace=True,
        converters=RECORD_HANDLING_MAP[name]["columns"],
    )
    df_cols = set(df.columns)
    # iterate over columns in df, missing_column will be name of that column if exists
    missing_column = next(
        (col for col in RECORD_HANDLING_MAP[name]["columns"] if col not in df_cols),
        None,
    )

    if missing_column is not None:
        raise ValueError(f"Missing or misnamed column: {missing_column} in {name}.csv.")

    if len(RECORD_HANDLING_MAP[name]["columns"]) != len(df_cols):
        extraneous_columns = [
            col for col in df_cols if col not in RECORD_HANDLING_MAP[name]["columns"]
        ]

        raise ValueError(
            f"{len(extraneous_columns)} invalid columns provided:\n\t{extraneous_columns} in {name}.csv."
        )

    for idx, named_tuple_record in enumerate(df.itertuples(index=False)):
        record = dict(named_tuple_record._asdict())
        if any([len(val) == 0 for val in record.values()]):
            raise ValueError(f"Malformed record at row {idx} of {name}.csv.")

        yield record


def upload_csv():
    """upload normalized duos data to db in a sensible order"""
    pass

