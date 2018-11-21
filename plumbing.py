import os
import pandas as pd
import re
from click import echo
from functools import wraps
from sqlalchemy import select


def echo_errors(fn, *args):
    """decorator to make errors presentable for users"""

    @wraps(fn)
    def wrapper(*args):
        try:
            fn(*args)
        except Exception as e:
            echo(str(e))

    return wrapper


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
        raise ValueError(
            f"column {missing_column} in {name}.csv is missing or misnamed."
        )

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
            raise ValueError(f"malformed record at row {idx} of {name}.csv.")

        yield record


def iter_norm_article(article_iterable):
    """unflatten records in articles.csv for inserting into ods"""

    for article in article_iterable:
        authors = re.split(r"\s?,\s?|\sand\s", article["author_list"])
        yield (
            {
                "article_title": article["title"],
                "duos_article_label": article["duos_article_label"],
            },
            [{"author_name": author} for author in authors],
        )


@echo_errors
def insert_article_dependent_tables(article_inserts_iterable, engine, metadata):
    """inserts into: article, writes, & authors tables in duosdb"""

    conn = engine.connect()
    record_count = 0

    for article_row, author_rows in iter_norm_article(article_inserts_iterable):

        record_count += 1

        # only one article to insert; grab ID
        inserted_article_id, = conn.execute(
            metadata.tables["article"]
            .insert()
            .values(article_row)
            .returning(metadata.tables["article"].c.article_id)
        ).fetchone()

        # potentially many authors; save all their IDs
        inserted_author_ids = conn.execute(
            metadata.tables["author"]
            .insert()
            .values(author_rows)
            .returning(metadata.tables["author"].c.author_id)
        )

        writes_to_insert = [
            {"article_id": inserted_article_id, "author_id": id}
            for id, in inserted_author_ids.fetchall()
        ]

        conn.execute(metadata.tables["writes"].insert().values(writes_to_insert))
        echo("   ..." + "." * record_count)
    echo(f"ℹ️  {record_count} records processed.")
    conn.close()
    return


# @echo_errors
def insert_reference_dependent_tables(reference_inserts_iterable, engine, metadata):

    conn = engine.connect()
    record_count = 0

    for reference in reference_inserts_iterable:
        record_count += 1

        # insert dataset if it's not already there
        # grab ID or None
        inserted_dataset_id, = (
            (None,)
            if conn.execute(
                select([metadata.tables["dataset"].c.duos_dataset_label])
                .select_from(metadata.tables["dataset"])
                .where(
                    metadata.tables["dataset"].c.duos_dataset_label
                    == reference["duos_dataset_label"]
                )
            ).fetchone()
            else conn.execute(
                metadata.tables["dataset"]
                .insert()
                .values(
                    {
                        "dataset_name": reference["name"],
                        "duos_dataset_label": reference["duos_dataset_label"],
                    }
                )
                .returning(
                    metadata.tables["dataset"].c.dataset_id,
                    # metadata.tables["dataset"].c.duos_dataset_label
                )
            ).fetchone()
        )

        if inserted_dataset_id is None:
            continue

        # select the article ID related to inserted dataset
        inserted_dataset_related_article_id, = conn.execute(
            select([metadata.tables["article"].c.article_id])
            .select_from(metadata.tables["article"])
            .where(
                metadata.tables["article"].c.duos_article_label
                == reference["duos_article_label"]
            )
        ).fetchone()

        # finally, insert reference
        conn.execute(
            metadata.tables["reference"]
            .insert()
            .values(
                {
                    "dataset_id": inserted_dataset_id,
                    "article_id": inserted_dataset_related_article_id,
                    "ref_hash": str(inserted_dataset_id ** 2),
                }
            )
        )

    echo(f"ℹ️  {record_count} records processed.")
    conn.close()
