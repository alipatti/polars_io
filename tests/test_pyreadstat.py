from pathlib import Path

import pandas as pd

import polars_io
from tests import _test_reader


def test_sas():
    _test_reader(
        data=Path("./data/sas"),
        url="https://www.alanelliott.com/sased2/ED2_FILES.html",
        suffix="sas7bdat",
        correct_reader=lambda p: pd.read_sas(p, iterator=False),  # type: ignore
        our_reader=polars_io.read_sas,
    )


def test_stata():
    _test_reader(
        data=Path("./data/stata"),
        url="https://principlesofeconometrics.com/stata.htm",
        suffix="dta",
        correct_reader=lambda p: pd.read_stata(p, iterator=False),  # type: ignore
        our_reader=polars_io.read_stata,
    )
