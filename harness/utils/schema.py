def stripPrefix(val: str) -> str:
    parts = val.partition("_")
    pre: str = parts[0]
    if pre.upper() in ["QA", "DEV"]:
        if parts[1] == '_':
            return parts[2]

    raise ValueError("prefix must folow the format 'qa_schema' or 'dev_schema")
