from typing import Optional
from pathlib import Path

import polars as pl
import pyreadstat

from polars_io.common import scan, make_eager


def scan_stata(
    file: str | Path,
    *,
    n_threads: Optional[int] = None,
    **kwargs,
) -> pl.LazyFrame:
    """
    file
    The file to read.

    n_threads
    Optionally use multiprocessing to read chunks.
    If not passed, will automatically enable or disable parallelization based on the file size.

    kwargs
    Other kwargs to pass to [`pyreadstat.read_dta`](https://ofajardo.github.io/pyreadstat_documentation/_build/html/index.html#pyreadstat.pyreadstat.read_dta)
    """

    return scan(
        file=file,
        reading_function=pyreadstat.read_dta,
        n_threads=n_threads,
        **kwargs,
    )


read_stata = make_eager(scan_stata)
