from pathlib import Path
from collections.abc import Callable

import polars as pl
from polars.testing import assert_frame_equal


DATA = Path("./data")


def run_lazy_test(
    file: Path,
    scanning_function: Callable[[Path], pl.LazyFrame],
):
    df = scanning_function(file)

    print(df.collect_schema())

    df.head().collect()


def run_eager_test(
    file: Path,
    correct_reader: Callable[[Path], pl.DataFrame],
    our_reader: Callable[[Path], pl.DataFrame],
):
    pandas = (
        correct_reader(file)
        # make sure that binary/null columns read the same as in pyreadstat
        .with_columns(pl.col(pl.Binary, pl.Null).cast(str).fill_null(""))
    )
    ours = our_reader(file)

    try:
        assert_frame_equal(pandas, ours, check_dtypes=False)  # type: ignore
    except:
        print(pandas)
        print(ours)
        raise
