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

from make_all_the_tables import make_all_the_tables
from parsers import normalize, iter_parse_csv, parse_artist, upload_csv
from CONSTANTS import RECORD_HANDLING_MAP


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
        if name in RECORD_HANDLING_MAP:
            parse_artist(normalize(iter_parse_csv(name, path), name))


@duosload.command(help="list basic info about duos db")
@click.pass_context
def info(ctx):
    echo(f"💻  There are {len(ctx.obj['metadata'].tables)} tables in the DUOS database.")


if __name__ == "__main__":
    # pylint: disable=E1123,E1120
    duosload(obj={})  # initialize empty context
