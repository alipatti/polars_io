from pathlib import Path
import zipfile
from io import BytesIO

import pytest
import requests
import lxml.html
from tqdm import tqdm

import polars_io as pio

from tests import DATA


EAGER_DATA_URLS = {
    "dta": "https://principlesofeconometrics.com/stata.htm",
    "sas7bdat": "https://www.alanelliott.com/sased2/ED2_FILES.html",
}


LAZY_DATA_URLS = {
    "sas7bdat": [
        "https://gss.norc.org/Documents/sas/GSS_sas.zip",
    ],
    "dta": [
        "https://gss.norc.org/documents/stata/GSS_stata.zip",
    ],
    "xpt": [
        "https://www.cdc.gov/brfss/annual_data/2023/files/LLCP2023XPT.zip",
        "https://wwwn.cdc.gov/Nchs/Data/Nhanes/Public/2021/DataFiles/BMX_L.xpt",
    ],
}


def pytest_generate_tests(metafunc: pytest.Metafunc):
    """Main test-generation function"""
    test_name = metafunc.function.__name__
    suffix = test_name.split("_")[-1]

    if "eager" in test_name and suffix in EAGER_DATA_URLS:
        generate_eager_tests(metafunc)


def decompress_if_needed(url: str, content: bytes) -> tuple[str, bytes]:
    if not url.endswith(".zip"):
        return (url.rsplit("/", 1)[-1], content)

    with zipfile.ZipFile(BytesIO(content)) as zf:
        # get first file that we can read
        name = next(
            f.filename
            for f in zf.filelist
            if pio._get_scanning_function(f.filename) is not None
        )
        return Path(name).parts[-1], zf.read(name)


def download_single_file(*, url: str, save_to: Path):
    print(f"Downloading {url}")

    with requests.get(url) as r:
        name, file = decompress_if_needed(url, r.content)
        print(f"Saving {name} to {save_to}")

    save_to.mkdir(exist_ok=True, parents=True)
    (save_to / name).write_bytes(file)


def download_every_linked_file_with_suffix(*, url: str, save_to: Path, suffix: str):
    with requests.get(url) as r:
        tree = lxml.html.fromstring(r.text, base_url=url)

    tree.make_links_absolute()

    files_to_download = [
        link for link in tree.xpath("//a/@href") if link.endswith(suffix)
    ]

    save_to.mkdir(parents=True, exist_ok=True)

    for f in tqdm(files_to_download, desc="Getting SAS test files"):
        with requests.get(f) as r:
            (save_to / f.rsplit("/", 1)[-1]).write_bytes(r.content)


def generate_eager_tests(metafunc: pytest.Metafunc):
    suffix = metafunc.function.__name__.split("_")[-1]
    path = DATA / "eager" / suffix

    if not path.exists():
        url = EAGER_DATA_URLS[suffix]

        print(f"Getting {suffix} files from {url}")

        download_every_linked_file_with_suffix(
            url=url, save_to=DATA / suffix, suffix=suffix
        )

    metafunc.parametrize(
        "file",
        path.glob(f"*.{suffix}"),
    )


def generate_lazy_tests(metafunc: pytest.Metafunc):
    suffix = metafunc.function.__name__.split("_")[-1]
    path = DATA / "lazy" / suffix

    if not path.exists():
        print(f"Getting large {suffix} files")

        for url in LAZY_DATA_URLS[suffix]:
            download_single_file(url=url, save_to=DATA / "lazy" / suffix)
