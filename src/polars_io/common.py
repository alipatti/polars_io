from collections import defaultdict
from collections.abc import Callable, Iterator, Mapping
from itertools import count
from pathlib import Path
from pprint import pprint
from typing import Optional, ParamSpec

import polars as pl
import pyarrow as pa
from polars import selectors as cs
from polars.io.plugins import register_io_source

MULTIPROCESSING_CELL_CUTOFF = 10_000_000
DEFAULT_BATCH_SIZE = 50_000


PYREADSTAT_TYPE_MAPPING = {
    "double": pl.Float64,
    "string": pl.String,
    "int8": pl.Int8,
    "int16": pl.Int8,
    "int32": pl.Int32,
    "float": pl.Float32,
}


# sas and stat measure dates from 1960 for some reason
EPOCH_OFFSET = pl.date(1970, 1, 1) - pl.date(1960, 1, 1)

SPECIAL_TYPE_FIXES: Mapping[type[pl.DataType], Callable[[pl.Expr], pl.Expr]] = {
    # SAS date/times (https://documentation.sas.com/doc/en/lrcon/9.4/p1wj0wt2ebe2a0n1lv4lem9hdc0v.htm
    # seconds since 1960
    pl.Datetime: lambda c: pl.from_epoch(c, time_unit="s") - EPOCH_OFFSET,
    # days since 1960
    pl.Date: lambda c: pl.from_epoch(c, time_unit="d") - EPOCH_OFFSET,
    # seconds since midnight
    pl.Time: lambda c: pl.from_epoch(c, time_unit="s").cast(pl.Time),
}

P = ParamSpec("P")


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
                disable_datetime_conversion=True,  # read dates as floats
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

            # create mapping from dtype to list of cols
            type_to_cols = _invert_mapping(schema)
            yield (
                df.filter(*() if predicate is None else (predicate,))
                # fix special types
                .with_columns(
                    cs.by_name(type_to_cols[date_type], require_all=False)
                    .fill_nan(None)
                    .pipe(f)
                    for date_type, f in SPECIAL_TYPE_FIXES.items()
                )
            )

            if df.height < batch_size:
                if verbose:
                    print("Reached end of file.")

                return

    return register_io_source(io_source=source_generator, schema=schema).fill_nan(None)


def _get_schema(metadata) -> dict:
    pyreadstat_schema = {
        v: PYREADSTAT_TYPE_MAPPING[t]
        for v, t in metadata.readstat_variable_types.items()
    }

    schema_overrides = {
        v: polars_type
        for v, sas_or_stata_type in metadata.original_variable_types.items()
        if sas_or_stata_type and (polars_type := _determine_type(sas_or_stata_type))
    }

    return pyreadstat_schema | schema_overrides


def _make_eager(
    lazy_function: Callable[P, pl.LazyFrame],
) -> Callable[P, pl.DataFrame]:
    def f(*args: P.args, **kwargs: P.kwargs) -> pl.DataFrame:
        return lazy_function(*args, **kwargs).collect()

    f.__doc__ = f"""See `{__package__}.{getattr(lazy_function, "__name__")}`"""

    return f


def _invert_mapping(mapping: Mapping) -> Mapping:
    d = defaultdict(list)

    for k, v in mapping.items():
        d[v].append(k)

    if None in d:
        del d[None]

    return d


def _determine_type(sas_type: str) -> type[pl.DataType] | None:
    if "8601" in sas_type:
        return pl.Date

    for dtype, labels in SAS_DATE_TYPES.items():
        for label in labels:
            if sas_type.upper().startswith(label):
                return dtype


SAS_DATE_TYPES = {
    pl.Datetime: ["DATETIME", "DTWKDATX"],
    pl.Time: ["HHMM", "HOUR", "MMSS", "TIME", "TOD"],
    pl.Date: [
        "YYMM",
        "YYMMP",
        "YYQD",
        "YYQN",
        "YYQRN",
        "YYMON",
        "YYQRD",
        "WEEKDAY",
        "YYQ",
        "YYQP",
        "YYQC",
        "DATE",
        "DDMMYY",
        "WEEKDATE",
        "QTRR",
        "WORDDATE",
        "YYQRS",
        "YYMMS",
        "YYQR",
        "MMYY",
        "WORDDATX",
        "WEEKDATX",
        "MMDDYY",
        "MMYYN",
        "MONYY",
        "MMYYC",
        "YYMMDD",
        "YYQRP",
        "YYQS",
        "JULIAN",
        "MMYYS",
        "MONTH",
        "NENGO",
        "MONNAME",
        "YEAR",
        "QTR",
        "DOWNAME",
        "DAY",
        "YYMMC",
        "YYMMN",
        "YYMMD",
        "JULDAY",
        "MMYYD",
        "WEEKV",
        "MMYYP",
        "YYQRC",
    ],
}
