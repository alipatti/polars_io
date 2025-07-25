"""
.. include:: ../../README.md
   :start-line: 1
"""  # noqa

import importlib.metadata
from collections.abc import Callable
from pathlib import Path

import polars as pl

from polars_io.fixed_width import read_fwf, scan_fwf
from polars_io.sas import read_sas7bdat, read_xpt, scan_sas7bdat, scan_xpt
from polars_io.stata import read_dta, scan_dta

__version__ = importlib.metadata.version("polars_io")


_OUR_SUFFIXES = {
    ".dta": scan_dta,
    ".sas7bdat": scan_sas7bdat,
    ".xpt": scan_xpt,
}

_POLARS_SUFFIXES = {
    ".csv": pl.scan_csv,
    ".parquet": pl.scan_parquet,
    ".jsonl": pl.scan_ndjson,
}

_SUFFIXES = _OUR_SUFFIXES | _POLARS_SUFFIXES


def _get_scanning_function(
    file: str | Path,
) -> Callable[[Path | str], pl.LazyFrame] | None:
    return _SUFFIXES.get(Path(file).suffix.lower().strip())


def scan(file: str | Path, **kwargs) -> pl.LazyFrame:
    """
    Scan any file readable by `polars` or `polars_io`.

    Parameters
    ----------
    file
        The file to read.

    kwargs
        Other kwargs to pass to the delegated scanning function.
    """
    f = _get_scanning_function(file)

    if not f:
        raise NotImplementedError(f"Unimplemented file type: {Path(file).suffix}")

    return f(file, **kwargs)


__docformat__ = "numpy"


__all__ = [
    "scan",
    "scan_dta",
    "scan_sas7bdat",
    "scan_xpt",
    "scan_fwf",
    "read_dta",
    "read_sas7bdat",
    "read_xpt",
    "read_fwf",
]
