from pathlib import Path

import polars as pl
import pyreadstat

from polars_io.common import _scan_with_pyreadstat, _make_eager


def scan_stata(
    file: str | Path,
    *,
    n_threads: int | None = None,
    **kwargs,
) -> pl.LazyFrame:
    """
    Lazily read from a Stata `.dta` file.

    Parameters
    ----------
    file
        The file to read.

    n_threads
        Optionally use multiprocessing to read chunks.
        If not passed, will automatically enable or disable parallelization based on the file size.

    kwargs
        Other kwargs to pass to [`pyreadstat.read_dta`](https://ofajardo.github.io/pyreadstat_documentation/_build/html/index.html#pyreadstat.pyreadstat.read_dta)
    """
    return _scan_with_pyreadstat(
        file=file,
        reading_function=pyreadstat.read_dta,
        n_threads=n_threads,
        **kwargs,
    )


read_stata = _make_eager(scan_stata)
