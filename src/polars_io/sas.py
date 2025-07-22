from pathlib import Path

import polars as pl
import pyreadstat

from polars_io.common import _scan_with_pyreadstat, _make_eager


def scan_sas(
    file: str | Path,
    *,
    n_threads: int | None = None,
    catalog: str | Path | None = None,
    **kwargs,
) -> pl.LazyFrame:
    """
    Lazily read from a SAS `.sas7bdat` file.

    Parameters
    ----------
    file
        The file to read.

    n_threads
        Optionally use multiprocessing to read chunks.
        If not passed, will automatically enable or disable parallelization based on the file size.

    catalog
        A sas7bcat file from which to take categorical labels.

    kwargs
        Other kwargs to pass to [`pyreadstat.read_sas7bdat`](https://ofajardo.github.io/pyreadstat_documentation/_build/html/index.html#pyreadstat.pyreadstat.read_sas7bcat)
    """
    return _scan_with_pyreadstat(
        file=file,
        reading_function=pyreadstat.read_sas7bdat,
        n_threads=n_threads,
        catalog_file=catalog,
        **kwargs,
    )


read_sas = _make_eager(scan_sas)
