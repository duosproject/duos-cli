RECORD_HANDLING_MAP = {
    "articles": {
        "columns": {"duos_label": str, "title": str, "author_list": str},
        "processor": lambda x: x,
    },
    "datasets": {"columns": {}, "processor": lambda x: x},
}
