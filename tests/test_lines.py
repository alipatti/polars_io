from pathlib import Path

import polars as pl
import requests

import polars_io

URL = "https://gist.githubusercontent.com/dracos/dd0668f281e685bad51479e5acaadb93/raw/6bfa15d263d6d5b63840a8e5b64e04b382fdb079/valid-wordle-words.txt"


def test_read_lines(data=Path("./data/lines/wordle.txt")):
    if not data.exists():
        print("Getting Wordle words")

        with requests.get(URL) as r:
            data.parent.mkdir(exist_ok=True, parents=True)
            data.write_text(r.text)

    string = polars_io.read_lines(data, col_name="word")
    assert string.columns == ["word"]

    categorical = polars_io.read_lines(data, col_name="word", col_dtype=pl.Categorical())
    assert categorical.schema == pl.Schema({"word": pl.Categorical()})

    assert string.get_column("word").to_list() == data.read_text().splitlines()
