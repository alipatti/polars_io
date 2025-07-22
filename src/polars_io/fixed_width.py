from pathlib import Path
from typing import Iterator, Mapping, Optional, Sequence
from itertools import pairwise, accumulate

import polars as pl
from beartype.door import is_bearable
from beartype import beartype

from polars.io.plugins import register_io_source
from polars_io.common import DEFAULT_BATCH_SIZE, make_eager

# ways to specify column locations
NameStartEnd = Mapping[str, tuple[int, int]]
NameLength = Sequence[tuple[str | None, int]]  # none as name => discard

ColLocations = NameStartEnd | NameLength


@beartype
def standardize_col_locaions(locs: ColLocations) -> NameStartEnd:
    if is_bearable(locs, NameStartEnd):
        return locs

    if is_bearable(locs, NameLength):
        names, lengths = zip(*locs)

        locations = [
            (end - length, end) for end, length in zip(accumulate(lengths), lengths)
        ]

        return {name: loc for name, loc in zip(names, locations) if name is not None}


def extract_columns(
    df: pl.DataFrame,
    col_locations: NameStartEnd,
    *,
    schema: Optional[dict] = None,
    col_subset: Optional[list[str]] = None,
    predicate: Optional[pl.Expr] = None,
    col_name="raw",
) -> pl.DataFrame:
    return (
        df.select(
            pl.col(col_name).str.slice(start, end - start).alias(name)
            for name, (start, end) in col_locations.items()
        )
        .cast(schema or {})
        .select(col_subset or pl.all())
        .filter(*[predicate] if predicate is not None else [])
    )


def scan_fwf(
    source: str | Path,
    cols: ColLocations,
    infer_schema_length=100,
    **kwargs,
) -> pl.LazyFrame:
    col_locations = standardize_col_locaions(cols)
    
    # HACK: 
    # write a small number of rows to csv and then reread to infer schema
    # hacky, but works...
    schema = pl.read_csv(
        pl.read_csv(
            source,
            n_rows=infer_schema_length,
            new_columns=["raw"],
            has_header=False,
            separator="\n",  # read each row as one field
        )
        .pipe(extract_columns, col_locations)
        .write_csv()
        .encode()
    ).schema

    def source_generator(
        with_columns: list[str] | None,
        predicate: pl.Expr | None,
        n_rows: int | None,
        batch_size: int | None,
    ) -> Iterator[pl.DataFrame]:
        """
        Inner function that yields chunks
        """

        reader = pl.read_csv_batched(
            source,
            has_header=False,
            new_columns=["raw"],
            separator="\n",  # read each row as one field
            batch_size=batch_size or DEFAULT_BATCH_SIZE,
            n_rows=n_rows,
            **kwargs,
        )

        while chunks := reader.next_batches(100):
            yield from (
                chunk.pipe(
                    extract_columns,
                    col_locations,
                    predicate=predicate,
                    col_subset=with_columns,
                    schema=schema,
                )
                for chunk in chunks
            )

        chunks = ...  # TODO:

    return register_io_source(io_source=source_generator, schema=schema)


read_fwf = make_eager(scan_fwf)
