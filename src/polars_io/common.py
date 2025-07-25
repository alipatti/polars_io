from collections.abc import Callable, Iterator
from itertools import count, repeat
from pathlib import Path
from pprint import pprint
from typing import TYPE_CHECKING, Optional

import polars as pl
import pyarrow as pa
import pyreadstat
from polars.io.plugins import register_io_source

if TYPE_CHECKING:
    import numpy as np
    import pandas as pd

MULTIPROCESSING_CELL_CUTOFF = 10_000_000
DEFAULT_BATCH_SIZE = 50_000


TYPE_MAPPING = {
    "double": pl.Float64,
    "string": pl.String,
    "int8": pl.Int8,
    "int16": pl.Int8,
    "int32": pl.Int32,
    "float": pl.Float32,
}


def _get_schema(metadata) -> dict:
    return {v: TYPE_MAPPING[t] for v, t in metadata.readstat_variable_types.items()}


def _scan_with_pyreadstat(
    file: str | Path,
    reading_function: Callable,  # e.g. pyreadstat.read_dta
    *,
    n_threads: Optional[int] = None,
    verbose: bool = False,
    **kwargs,
) -> pl.LazyFrame:
    file = str(file)

    if verbose:
        print(f"Getting metadata for {file}")

    _, metadata = reading_function(file, row_limit=1)
    schema = _get_schema(metadata)

    if verbose:
        pprint(schema)

    if len(schema) * metadata.number_rows > MULTIPROCESSING_CELL_CUTOFF:
        # TODO: implement multiprocessing
        # https://github.com/Roche/pyreadstat?tab=readme-ov-file#reading-rows-in-chunks
        pass

    def source_generator(
        with_columns: list[str] | None,
        predicate: pl.Expr | None,
        n_rows: int | None,
        batch_size: int | None,
    ) -> Iterator[pl.DataFrame]:
        batch_size = batch_size or DEFAULT_BATCH_SIZE

        if verbose:
            print(f"{with_columns=}, {predicate=}, {n_rows=}, {batch_size=}")

        for row_offset in count(start=0, step=batch_size):
            if n_rows and row_offset > n_rows:
                if verbose:
                    print(f"Gathered sufficient rows. {n_rows=}, {row_offset=}")

                return

            this_batch_size = (
                min(batch_size, n_rows - row_offset) if n_rows else batch_size
            )

            if verbose:
                print(f"{row_offset=}, {this_batch_size=}")

            cols, _ = reading_function(
                file,
                row_offset=row_offset,
                row_limit=this_batch_size,
                usecols=with_columns,  # read only requested columns
                output_format="dict",
                **kwargs,
            )

            # cast numpy arrays to pyarrow (allegedly zero-copy for supported types)
            # https://github.com/apache/arrow/issues/31290
            arrow_table = pa.Table.from_arrays(
                arrays=[pa.array(arr) for arr in cols.values()],
                names=list(cols.keys()),
            )

            # create polars df (zero copy from pyarrow table)
            df: pl.DataFrame = pl.from_arrow(arrow_table)  # type: ignore

            yield df if predicate is None else df.filter(predicate)

            if df.height < batch_size:
                if verbose:
                    print("Reached end of file.")

                return

    return register_io_source(io_source=source_generator, schema=schema).fill_nan(None)


def _make_eager[**P](
    lazy_function: Callable[P, pl.LazyFrame],
) -> Callable[P, pl.DataFrame]:
    def f(*args: P.args, **kwargs: P.kwargs) -> pl.DataFrame:
        return lazy_function(*args, **kwargs).collect()

    f.__doc__ = f"""See `{__package__}.{getattr(lazy_function, "__name__")}`"""

    return f
