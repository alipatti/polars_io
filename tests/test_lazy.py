import polars_io as pio
from tests import DATA
from pprint import pprint


def test_lazy_sas7bdat():
    suffix = "sas7bdat"
    dir = DATA / "lazy" / suffix

    for file in dir.glob("*"):
        f = pio._get_scanning_function(file)

        if not f:
            continue

        print(f"Scanning {file}")
        df = f(file)

        pprint(df.collect_schema())
