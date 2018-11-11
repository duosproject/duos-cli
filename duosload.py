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

# config-ing
load_dotenv()
db = {
    "drivername": "postgres",
    "username": os.getenv("DB_USER"),
    "password": os.getenv("DB_PASSWORD"),
    "host": os.getenv("DB_HOST"),
    "port": os.getenv("DB_PORT"),
    "database": os.getenv("DB_NAME"),
}
connstr = URL(**db)
engine = create_engine(connstr)
metadata = MetaData(bind=engine)
metadata.reflect(bind=engine)


@click.group()
def loadutil():
    """utilitiy for loading the data for the DUOS research study."""
    pass


@loadutil.command()
@click.option(
    "--from-scratch", is_flag=True, help="drop and recreate the entire database"
)
def create(from_scratch):
    if from_scratch:
        echo(f"⬇  dropping every table... all {len(metadata.tables)} of them")
        metadata.drop_all(engine)
        echo("🙌  done!")

    if len(metadata.tables) > 0:
        echo(f"🚫  {len(metadata.tables)} already exist.")
        echo("consider destroying first or using '--from-scratch'")
        return

    echo("✨  creating tables...")
    make_all_the_tables(metadata, engine)
    echo("🙌  done!")


@loadutil.command()
# TODO: option for listing tables to drop
def destroy():
    echo(f"⬇  dropping every table... all {len(metadata.tables)} of them")
    metadata.drop_all(engine)
    echo("🙌  destroyed!")


@loadutil.command(
    help="""
    insert data into the database using a csv\n
    ℹ️  optional if file is named 'articles.csv' 'references.csv'
    """
)
@click.option("--file", help="the file to upload into the db")
@click.option(
    "--mode",
    help="duos data set type: articles or references ",
    type=click.Choice(["articles", "references"]),
)
def upload(file):
    echo(file)


@loadutil.command(help="list basic info about duos db")
def info():
    echo(f"💻  There are {len(metadata.tables)} tables in the DUOS database.")


# no commands past here
def make_all_the_tables(metadata, engine):
    """wrapper for database definitions. consider it config."""

    article = Table(
        "article",
        metadata,
        # integer pk interpreted as SERIAL by default
        Column("article_id", Integer, primary_key=True),
        Column("article_title", String, nullable=False),
    )

    author = Table(
        "author",
        metadata,
        Column("author_id", Integer, primary_key=True),
        Column("author_name", String, nullable=False),
        Column("email_address", String, nullable=False),
    )

    dataset = Table(
        "dataset",
        metadata,
        Column("dataset_id", Integer, primary_key=True),
        Column("dataset_name", String, nullable=False),
        Column("abbreviation", String),
    )

    writes = Table(
        "writes",
        metadata,
        Column("writes_id", Integer, primary_key=True),
        Column("article_id", Integer, ForeignKey("article.article_id"), nullable=False),
        Column("author_id", Integer, ForeignKey("author.author_id"), nullable=False),
    )

    reference = Table(
        "reference",
        metadata,
        Column("ref_id", Integer, primary_key=True),
        Column("dataset_id", Integer, ForeignKey("dataset.dataset_id"), nullable=False),
        Column("article_id", Integer, ForeignKey("article.article_id"), nullable=False),
    )

    validates = Table(
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


if __name__ == "__main__":
    loadutil()
