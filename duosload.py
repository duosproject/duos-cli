#!/usr/bin/env python
from sqlalchemy import (
    create_engine,
    Table,
    Column,
    Integer,
    String,
    DateTime,
    MetaData,
    ForeignKey,
)
from sqlalchemy.engine.url import URL
from dotenv import load_dotenv
import os
import click
from click import echo
import pandas as pd

RECORD_HANDLING_MAP = {
    "articles": {
        "columns": {"duos_label": str, "title": str, "author_list": str},
        "processor": lambda x: x,
    },
    "datasets": {"columns": {}, "processor": lambda x: x},
}


@click.group()
@click.pass_context
def duosload(ctx):
    """utilitiy for loading the data for the DUOS research study."""

    load_dotenv()
    connstr = URL(
        **{
            "drivername": "postgres",
            "username": os.getenv("DB_USER"),
            "password": os.getenv("DB_PASSWORD"),
            "host": os.getenv("DB_HOST"),
            "port": os.getenv("DB_PORT"),
            "database": os.getenv("DB_NAME"),
        }
    )
    engine = create_engine(connstr)
    metadata = MetaData(bind=engine)
    metadata.reflect(bind=engine)

    ctx.obj = {"metadata": metadata, "engine": engine}


@duosload.command()
@click.option(
    "--from-scratch", is_flag=True, help="drop and recreate the entire database"
)
@click.pass_context
def create(ctx, from_scratch):
    if from_scratch:
        echo(
            f"⬇  dropping every table... all {len(ctx.obj['metadata'].tables)} of them"
        )
        ctx.obj["metadata"].drop_all(ctx.obj["engine"])
        echo("🙌  done!")
        return

    if len(ctx.obj["metadata"].tables) > 0:
        echo(f"🚫  {len(ctx.obj['metadata'].tables)} already exist.")
        echo("consider destroying first or using '--from-scratch'")
        return

    echo("✨  creating tables...")
    make_all_the_tables(ctx.obj["metadata"], ctx.obj["engine"])
    echo("🙌  done!")
    return


@duosload.command()
@click.pass_context
def destroy(ctx):
    echo(f"⬇  dropping every table... all {len(ctx.obj['metadata'].tables)} of them")
    ctx.obj["metadata"].drop_all(ctx.obj["engine"])
    echo("🙌  destroyed!")


@duosload.command(help="insert local csv into the database")
def upload():
    # extract absolute path of wherever we are execxuting
    path = os.path.dirname(os.path.abspath(__file__))
    csv_names = {fname.split(".")[0] for fname in os.listdir(path) if ".csv" in fname}
    echo(f"CSVs discovered: {csv_names}")
    for name in csv_names:
        echo(name in RECORD_HANDLING_MAP)
        # if name in RECORD_HANDLING_MAP:
        iter_parse_csv(name, path)


@duosload.command(help="list basic info about duos db")
@click.pass_context
def info(ctx):
    echo(id)
    echo(f"💻  There are {len(ctx.obj['metadata'].tables)} tables in the DUOS database.")


# no commands past here
def make_all_the_tables(metadata, engine):
    """wrapper for database definitions. consider it config."""

    Table(
        "article",
        metadata,
        # integer pk interpreted as SERIAL by default
        Column("article_id", Integer, primary_key=True),
        Column("article_title", String, nullable=False),
        Column("duos_label", String, nullable=False),
    )

    Table(
        "author",
        metadata,
        Column("author_id", Integer, primary_key=True),
        Column("author_name", String, nullable=False),
        Column("email_address", String, nullable=False),
    )

    Table(
        "dataset",
        metadata,
        Column("dataset_id", Integer, primary_key=True),
        Column("dataset_name", String, nullable=False),
        Column("abbreviation", String),
    )

    Table(
        "writes",
        metadata,
        Column("writes_id", Integer, primary_key=True),
        Column("article_id", Integer, ForeignKey("article.article_id"), nullable=False),
        Column("author_id", Integer, ForeignKey("author.author_id"), nullable=False),
    )

    Table(
        "reference",
        metadata,
        Column("ref_id", Integer, primary_key=True),
        Column("dataset_id", Integer, ForeignKey("dataset.dataset_id"), nullable=False),
        Column("article_id", Integer, ForeignKey("article.article_id"), nullable=False),
        Column("ref_hash", String, nullable=False),
    )

    Table(
        "validates",
        metadata,
        Column("validation_id", Integer, primary_key=True),
        Column("ref_id", Integer, ForeignKey("reference.ref_id"), nullable=False),
        Column("author_id", Integer, ForeignKey("author.author_id"), nullable=False),
        Column("response", String),
        Column("clarification", String),
        Column("insert_date", DateTime),
        Column("action_date", DateTime),
    )

    metadata.create_all()


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
    print("hello")
    df_cols = set(df.columns)
    # iterate over columns in df, missing_column will be name of that column if exists
    missing_column = next(
        (col for col in RECORD_HANDLING_MAP[name]["columns"] if col not in df_cols),
        None,
    )

    if missing_column is not None:
        raise ValueError(f"Missing or misnamed column: {missing_column} in {name}.csv.")

    if len(RECORD_HANDLING_MAP[name]) != len(df_cols):
        extraneous_columns = [
            col for col in df_cols if col not in RECORD_HANDLING_MAP[name]["columns"]
        ]

        raise ValueError(
            f"{len(extraneous_columns)} invalid columns provided:\n\t{extraneous_columns} in {name}.csv."
        )

    for idx, named_tuple_record in enumerate(df.itertuples(index=False)):
        record = dict(named_tuple_record._asdict())
        echo("hello?")
        if any([len(val) == 0 for val in record.values()]):
            raise ValueError(f"Malformed record at row {idx} of {name}.csv.")

        yield record


def upload_csv():
    """upload normalized duos data to db in a sensible order"""
    pass


if __name__ == "__main__":
    duosload(obj={})  # initialize empty context

