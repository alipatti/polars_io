# polars_io

Lazily read Stata (`.dta`), SAS (`.sas7bdat`), and fixed-width (`.txt`, `.dat`, etc.) files in [`polars`](https://pola.rs).

## Installation

```bash
pip install polars_io # or uv add polars_io
```

## Usage

```python
import polars as pl
import polars_io as pio

# lazily load a sas file
lf = pio.scan_sas("huge_SAS_file.sas7bdat")

# get its schema
lf.collect_schema()

# take a look at the first few rows
lf.head().collect()

# projection and predicate pushdown works!
(
    lf
    .filter(pl.col("birth_year").is_between(2000, 2010))
    .select(pl.col("usage").mean())
    .collect()
)

# load fixed-width files
col_locations = { "year" : (10, 14), "population" : (14, 20) }
pio.scan_fwf("populations.txt", col_locations)

# eager versions of all functions are also available
pio.read_dta("mortality_rates.dta")
```

See [the documentation](https://alipatti.com/polars_io) for more info.

## Details

The Stata and SAS implementations make use the [`readstat`](https://github.com/WizardMac/ReadStat) C library via the python bindings provided by [`pyreadstat`](https://github.com/Roche/pyreadstat). This is the same implementation used by the `R` library [`haven`](https://github.com/tidyverse/haven).

## Contributing

PRs adding support for reading other formats are very welcome!

