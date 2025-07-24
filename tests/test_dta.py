from pathlib import Path

import pandas as pd
import polars as pl
import polars_io as pio

from tests import run_eager_test


def test_eager_dta(file: Path):
    run_eager_test(
        file,
        lambda p: pd.read_stata(p, iterator=False).pipe(pl.from_pandas),  # type: ignore
        pio.read_dta,
    )

