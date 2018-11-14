import os
import pandas as pd
import re


def iter_parse_csv(name, path, column_name_converter_dict):
    """ingest and validate csv files
    :yield:    dict(str, str)"""

    df = pd.read_csv(
        os.path.join(path, f"{name}.csv"),
        index_col=False,
        quotechar='"',
        skipinitialspace=True,
        converters=column_name_converter_dict,
    )
    df_cols = set(df.columns)
    # iterate over columns in df, missing_column will be name of that column if exists
    missing_column = next(
        (col for col in column_name_converter_dict if col not in df_cols), None
    )

    if missing_column is not None:
        raise ValueError(f"Missing or misnamed column: {missing_column} in {name}.csv.")

    if len(column_name_converter_dict) != len(df_cols):
        extraneous_columns = [
            col for col in df_cols if col not in column_name_converter_dict
        ]

        raise ValueError(
            f"{len(extraneous_columns)} invalid columns provided:\n\t{extraneous_columns} in {name}.csv."
        )

    for idx, named_tuple_record in enumerate(df.itertuples(index=False)):
        record = dict(named_tuple_record._asdict())
        if any([len(val) == 0 for val in record.values()]):
            raise ValueError(f"Malformed record at row {idx} of {name}.csv.")

        yield record


def iter_norm_article(article_iterable):
    for article in article_iterable:
        authors = re.split(r"\s?,\s?|\sand\s", article["author_list"])
        yield [
            {"article_title": article["title"], "duos_label": article["duos_label"]},
            [{"author_name": author} for author in authors],
        ]


def insert_article_dependent_tables(insert_rows_iterable, engine, metadata):
    for article_rows, author_rows in iter_norm_article(insert_rows_iterable):
        conn = engine.connect()

        author_ids = conn.execute(
            metadata.tables["author"]
            .insert()
            .values(author_rows[0])
            .returning(metadata.tables["author"].c.author_name)
        ).__dict__

        # print(author_ids)

        article_ids = conn.execute(
            metadata.tables["article"].insert().return_defaults(), article_rows
        )  # returned_defaults
        print(article_ids)


def iter_norm_reference(dataset_iterable):
    pass
