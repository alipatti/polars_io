from collections.abc import Callable
from pathlib import Path

import polars as pl
import pytest
from polars.testing import assert_frame_equal

DATA = Path("./data")


@pytest.mark.timeout(5)
# @pytest.mark.xfail(reason="known upstream unicode issue", raises=Unicode)
def run_lazy_test(
    file: Path,
    scanning_function: Callable[[Path], pl.LazyFrame],
):
    try:
        df = scanning_function(file)

        print(df.collect_schema())

        print(df.head().collect())

    except UnicodeDecodeError as e:
        pytest.xfail(f"known unicode issue: {e}")


def run_eager_test(
    file: Path,
    correct_reader: Callable[[Path], pl.DataFrame],
    our_reader: Callable[[Path], pl.DataFrame],
):
    try:
        pandas = (
            correct_reader(file)
            # make sure that binary/null columns read the same as in pyreadstat
            .with_columns(pl.col(pl.Binary, pl.Null).cast(str).fill_null(""))
        )
        ours = our_reader(file)

    except UnicodeDecodeError as e:
        pytest.xfail(f"known unicode issue: {e}")

    try:
        assert_frame_equal(pandas, ours, check_dtypes=False)  # type: ignore

    except AssertionError:
        print(pandas)
        print(ours)
        raise
