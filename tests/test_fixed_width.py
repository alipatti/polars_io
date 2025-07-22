from pathlib import Path
import gzip

import requests
import pandas as pd
import polars as pl
from polars.testing import assert_frame_equal

import polars_io

URL = "https://seer.cancer.gov/popdata/yr1969_2023.20ages/ma.1969_2023.20ages.txt.gz"

COL_LENGTHS = {
    "Year": (0, 4),
    "State postal abbreviation": (4, 6),
    "State FIPS code": (6, 8),
    "County FIPS code": (8, 11),
    "Race": (13, 14),
    "Origin": (14, 15),
    "Sex": (15, 16),
    "Age": (16, 18),
    "Population": (18, 26),
}


def test_fwf(data=Path("./data/fwf/seer.txt")):
    if not data.exists():
        with requests.get(URL) as r:
            print("Getting SEER data")
            data.parent.mkdir(exist_ok=True, parents=True)
            data.write_bytes(gzip.decompress(r.content))

    ours = polars_io.read_fwf(data, COL_LENGTHS)
    pandas = pl.from_pandas(
        pd.read_fwf(data, colspecs=list(COL_LENGTHS.values()), header=None)
    )

    pandas.columns = list(COL_LENGTHS.keys())

    assert_frame_equal(ours, pandas)
