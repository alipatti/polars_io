from collections.abc import Iterator
from pathlib import Path

import polars as pl
from polars._typing import PolarsDataType
from polars.io.plugins import register_io_source

from polars_io.common import DEFAULT_BATCH_SIZE, _make_eager

SCAN_LINE_KWARGS = dict(
    has_header=False,
    separator="\n",  # read each row as one field
    quote_char=None,
    comment_prefix=None,
)


def scan_lines(
    file: str | Path,
    col_name: str = "line",
    col_dtype: PolarsDataType = pl.String,
    **kwargs,
) -> pl.LazyFrame:
    """
    Read a newline-delimited text file into a single-column LazyFrame.

    Parameters
    ----------
    file
        The file to read.

    col_name
        The name to assign the column.

    col_dtype
        The data type to assign the single column. String by default.

    kwargs
        Other kwargs to pass to [`pl.read_csv_batched`](https://docs.pola.rs/api/python/stable/reference/api/polars.read_csv_batched.html).
    """
    schema = pl.Schema({col_name: col_dtype})

    def source_generator(
        with_columns: list[str] | None,
        predicate: pl.Expr | None,
        n_rows: int | None,
        batch_size: int | None,
    ) -> Iterator[pl.DataFrame]:
        assert not with_columns or with_columns == [col_name]

        reader = pl.read_csv_batched(
            file,
            new_columns=[col_name],
            schema_overrides=schema,
            batch_size=batch_size or DEFAULT_BATCH_SIZE,
            n_rows=n_rows,
            **SCAN_LINE_KWARGS,
            **kwargs,
        )

        while chunks := reader.next_batches(100):
            yield from (
                c.filter(predicate) if predicate is not None else c for c in chunks
            )

    return register_io_source(io_source=source_generator, schema=schema)


read_lines = _make_eager(scan_lines)
