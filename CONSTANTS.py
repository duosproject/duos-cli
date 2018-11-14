# from db import insert_article_sql, insert_dataset_sql
from plumbing import insert_article_dependent_tables, iter_norm_reference

INPUT_VARIANT_COLUMN_MAP = {
    "articles": {"duos_label": str, "title": str, "author_list": str},
    "datasets": {},
}


TRANSACTIONS = (insert_article_dependent_tables, iter_norm_reference)
INPUT_VARIANT_TRANSACTION_MAP = {
    input: TRANSACTIONS[i] for i, input in enumerate(INPUT_VARIANT_COLUMN_MAP)
}

