#!/usr/bin/env python
# -*- coding: utf-8 -*-
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

from db_schema import build_schema_from_metadata
from plumbing import iter_parse_csv
from CONSTANTS import INPUT_VARIANT_COLUMN_MAP, INPUT_VARIANT_TRANSACTION_MAP


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


@duosload.command(help="create duos database schema in target db.")
@click.pass_context
def create(ctx):

    if len(ctx.obj["metadata"].tables):
        echo(f"🚫  {len(ctx.obj['metadata'].tables)} tables already exist.")
        echo("ℹ️  to drop and recreate call `destroy` first.")
        return

    echo("✨  creating tables...")
    build_schema_from_metadata(ctx.obj["metadata"], ctx.obj["engine"])
    echo("🙌  created!")
    return


@duosload.command(help="drop every table in duos database.")
@click.pass_context
def destroy(ctx):
    echo(f"⬇  dropping every table... all {len(ctx.obj['metadata'].tables)} of them")
    ctx.obj["metadata"].drop_all(ctx.obj["engine"])
    echo("🙌  destroyed!")


@duosload.command(help="insert local csv into the database.")
@click.pass_context
def upload(ctx):
    # extract absolute path of wherever we are execxuting
    path = os.path.dirname(os.path.abspath(__file__))
    csv_names = {fname.split(".")[0] for fname in os.listdir(path) if ".csv" in fname}

    echo(f"CSVs discovered: {csv_names}")

    for name in csv_names:
        if name in INPUT_VARIANT_COLUMN_MAP:
            INPUT_VARIANT_TRANSACTION_MAP[name](
                iter_parse_csv(name, path, INPUT_VARIANT_COLUMN_MAP[name]),
                ctx.obj["engine"],
                ctx.obj["metadata"],
            )


@duosload.command(help="list basic info about duos db")
@click.pass_context
def info(ctx):
    echo(f"💻  There are {len(ctx.obj['metadata'].tables)} tables in the DUOS database.")


if __name__ == "__main__":
    # pylint: disable=E1123,E1120
    duosload(obj={})  # initialize empty context
