from plumbing import insert_article_dependent_tables, insert_reference_dependent_tables

INPUT_VARIANT_COLUMN_MAP = {
    "articles": {"duos_article_label": str, "title": str, "author_list": str},
    "references": {"duos_article_label": str, "name": str, "duos_dataset_label": str},
}


TRANSACTIONS = (insert_article_dependent_tables, insert_reference_dependent_tables)
INPUT_VARIANT_TRANSACTION_MAP = {
    input: TRANSACTIONS[i] for i, input in enumerate(INPUT_VARIANT_COLUMN_MAP)
}

