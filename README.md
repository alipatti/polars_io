# polars_io

Lazily read Stata (`.dta`), SAS (`.sas7bdat`, `.xpt`), fixed-width (`.txt`,
`.dat`, etc.), and newline delimited (`.txt`) files in
[`polars`](https://pola.rs).

## Installation

```bash
pip install polars_io
# Or:
uv add polars_io
```

## Usage

```python
import polars as pl
import polars_io as pio

# Lazily load a sas file.
lf = pio.scan_sas7bdat("huge_SAS_file.sas7bdat")

# Get its schema.
lf.collect_schema()

# Take a look at the first few rows.
lf.head().collect()

# Projection and predicate pushdown work!
(
    lf
    .filter(pl.col("birth_year").is_between(2000, 2010))
    .select(pl.col("usage").mean())
    .collect()
)

# Load fixed-width files.
col_locations = {"year": (10, 14), "population": (14, 20)}
pio.scan_fwf("populations.txt", col_locations)

# Eager versions of all functions are also available.
pio.read_dta("mortality_rates.dta")
```

See [the documentation](https://alipatti.com/polars_io) for more info.

## Details

The Stata and SAS implementations make use of the
[`readstat`](https://github.com/WizardMac/ReadStat) C library via the Python
bindings provided by [`pyreadstat`](https://github.com/Roche/pyreadstat). For
numeric types, reading uses zero-copy conversions from
`numpy -> pyarrow -> polars` and should be faster and have lower memory overhead
than reading the data into `pandas` and then calling `pl.from_pandas`
(benchmarks welcome).

## Contributing

PRs adding support for reading other formats are very welcome! (E.g., `.Rdata`,
Stata `.dct`, SPSS files, etc.)

## Known Issues

This packages fails to some read files with non-utf8 metadata (e.g., column
labels, notes on `.dta` files). This is a known issue with upstream packages
that is being worked on (see Roche/pyreadstat#298 and WizardMac/ReadStat#344).
