from typing import Callable, Iterator, Optional
from pathlib import Path

import polars as pl
from polars.io.plugins import register_io_source
import pyreadstat

MULTIPROCESSING_CELL_CUTOFF = 10_000_000
DEFAULT_BATCH_SIZE = 100_000


TYPE_MAPPING = {
    "double": pl.Float64,
    "string": pl.String,
    "int8": pl.Int8,
    "int32": pl.Int32,
    "float": pl.Float32,
}


def _get_schema(metadata) -> dict:
    return {v: TYPE_MAPPING[t] for v, t in metadata.readstat_variable_types.items()}


def scan(
    file: str | Path,
    reading_function: Callable,  # e.g. pyreadstat.read_dta
    *,
    n_threads: Optional[int] = None,
    **kwargs,
) -> pl.LazyFrame:
    file = str(file)

    _, metadata = reading_function(file, metadataonly=True)
    schema = _get_schema(metadata)

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
        """
        Inner function that yields chunks
        """

        reader = pyreadstat.read_file_in_chunks(
            reading_function,
            file,
            chunksize=batch_size or DEFAULT_BATCH_SIZE,
            limit=n_rows,
            usecols=with_columns,
            **kwargs,
        )

        for df, meta in reader:
            df = pl.from_pandas(df)

            yield df if predicate is None else df.filter(predicate)

    return register_io_source(io_source=source_generator, schema=schema)


def make_eager[**P](
    lazy_function: Callable[P, pl.LazyFrame],
) -> Callable[P, pl.DataFrame]:
    def f(*args: P.args, **kwargs: P.kwargs) -> pl.DataFrame:
        return lazy_function(*args, **kwargs).collect()

    f.__doc__ = lazy_function.__doc__

    return f
