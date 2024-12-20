import json
from typing import Dict, Generator, Iterable


def write_to_jsonlines(dump_filepath: str, rows: Iterable) -> None:
    """write_to_jsonlines
    JSONify each element of an iterable and write it to a newline of a file.

    'JSON lines' may be preferred over CSV or JSON for a few reasons:
    - CSVs have no typing: csv readers are left to guess what types values should be
      interpreted (often leads to more strings than we intend)
    - Often we're working with JSON already (e.g. from API responses)
    - A regular JSON file does not lend itself well to reading from in a streamed/
      batched fashion: you generally need to load the full JSON into memory which is not
      always possible for large files.

    :param dump_filepath: file path to save to
    :type dump_filepath: str
    :param rows: iterable of items to write to the fie. each item must be json
        serializable
    :type rows: Iterable
    """
    with open(dump_filepath, "w") as f:
        f.writelines(f"{json.dumps(row)}\n" for row in rows)


def jsonlines_reader(file) -> Generator[Dict, None, None]:
    """jsonlines_reader

    :param file: a jsonlines file opened in read mode.
    :yield: a dictionary of a json-parsed row
    :rtype: Generator[Dict, None, None]
    """
    for row in file:
        yield json.loads(row)
