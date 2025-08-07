from pathlib import Path

import polars as pl
import pyreadstat

import polars_io as pio
from tests import run_eager_test, run_lazy_test


def test_eager_sas7bdat(file: Path):
    run_eager_test(
        file,
        lambda p: pyreadstat.read_sas7bdat(file)[0].pipe(pl.from_pandas),  # type: ignore
        pio.read_sas7bdat,
    )


def test_lazy_sas7bdat(file: Path):
    run_lazy_test(file, pio.scan_sas7bdat)
